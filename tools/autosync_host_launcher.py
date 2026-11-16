#!/usr/bin/env python3
"""Stable bootstrap that launches the verified, current AutoSync host supervisor."""
from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
try:
    import msvcrt
except ImportError:
    msvcrt = None
    import fcntl
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

SUPERVISOR_RESTART_EXIT = 75
VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")
REQUIRED_CONFIG = (
    "auto_update_cli",
    "module_source_root",
    "state_root",
    "app_manifest_path",
    "update_policy_path",
    "python_executable",
    "data_dir",
    "backup_dir",
    "log_file",
    "app_id",
)


class LauncherError(RuntimeError):
    pass


def _contains_secret_key(value) -> bool:
    forbidden = {"token", "secret", "password", "private_key", "privatekey",
                 "api_key", "access_token", "refresh_token", "client_secret"}
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = re.sub(r"[-\s]", "_", str(key)).lower()
            if normalized in forbidden or _contains_secret_key(item):
                return True
    elif isinstance(value, list):
        return any(_contains_secret_key(item) for item in value)
    return False


def read_config(path: Path) -> dict:
    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LauncherError("host configuration could not be read") from exc
    if not isinstance(config, dict) or config.get("schema") != 1 or config.get("contract") != "autosync-host-config-v1":
        raise LauncherError("unsupported AutoSync host configuration")
    missing = [name for name in REQUIRED_CONFIG if not isinstance(config.get(name), str) or not config[name].strip()]
    if missing:
        raise LauncherError("host configuration is missing required fields")
    if _contains_secret_key(config):
        raise LauncherError("host configuration must not contain secret values")
    for name in ("state_root", "data_dir", "backup_dir", "log_file"):
        value = Path(config[name])
        if not value.is_absolute() or str(value).startswith("\\\\"):
            raise LauncherError("host state, database, backup, and log paths must be local absolute paths")
    if config.get("app_id") != "AutoSyncHost":
        raise LauncherError("host configuration app_id must be AutoSyncHost")

    apps = config.get("credential_apps")
    if (not isinstance(apps, list) or not apps
            or any(not isinstance(app, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", app) for app in apps)
            or len(set(apps)) != len(apps)):
        raise LauncherError("host configuration must list unique valid credential app IDs")
    prefix = config.get("credential_target_prefix", "AutoSync/server/")
    if (not isinstance(prefix, str) or not re.fullmatch(r"[A-Za-z0-9._/-]{1,220}", prefix)
            or not prefix.endswith("/") or ".." in prefix.split("/")):
        raise LauncherError("host configuration Credential Manager target prefix is invalid")

    host = config.get("bind_host", "127.0.0.1")
    allow_lan = config.get("allow_lan", False)
    port = config.get("port", 8765)
    if not isinstance(host, str) or type(allow_lan) is not bool or type(port) is not int or not 1 <= port <= 65535:
        raise LauncherError("host configuration bind address, port, or allow_lan value is invalid")
    try:
        address = ipaddress.ip_address(host)
    except ValueError as exc:
        raise LauncherError("host bind address must be an IPv4 literal") from exc
    if address.version != 4:
        raise LauncherError("host bind address must be IPv4")

    cert, key = config.get("tls_cert"), config.get("tls_key")
    if bool(cert) != bool(key):
        raise LauncherError("TLS certificate and key must be configured together")
    if cert:
        for value in (cert, key):
            if not isinstance(value, str) or not value.strip():
                raise LauncherError("TLS certificate and key must be non-empty local paths")
            tls_path = Path(value)
            if not tls_path.is_absolute() or str(tls_path).startswith("\\\\") or not tls_path.is_file():
                raise LauncherError("TLS certificate and key must exist at local absolute paths")
    if not address.is_loopback and (not allow_lan or not address.is_private or not cert):
        raise LauncherError("LAN binding requires explicit allow_lan, a private IPv4 address, and TLS")

    for name in ("startup_timeout_seconds", "drain_timeout_seconds"):
        value = config.get(name, 45)
        if type(value) is not int or not 5 <= value <= 600:
            raise LauncherError("host configuration lifecycle timeouts must be from 5 to 600 seconds")
    cache_root = (Path(config["state_root"]) / "modules").resolve()
    for name in ("data_dir", "backup_dir"):
        try:
            Path(config[name]).resolve().relative_to(cache_root)
        except ValueError:
            continue
        raise LauncherError("database and backup paths must be outside the AutoUpdate module cache")
    return config

def _powershell_executable() -> tuple[str, bool]:
    if os.name == "nt":
        windir = os.environ.get("WINDIR") or os.environ.get("SystemRoot") or r"C:\Windows"
        winps = Path(windir) / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe"
        if not winps.is_file():
            raise LauncherError("Windows PowerShell 5.1 is unavailable for AutoUpdate module resolution")
        return str(winps), True
    pwsh = shutil.which("pwsh")
    if pwsh:
        return pwsh, False
    raise LauncherError("PowerShell is unavailable for AutoUpdate module resolution")


def _child_environment(windows_powershell: bool, extra: dict | None = None) -> dict:
    keys = ("SystemRoot", "SYSTEMROOT", "WINDIR", "PATH", "TEMP", "TMP", "USERPROFILE",
            "LOCALAPPDATA", "APPDATA", "PROGRAMDATA")
    env = {key: value for key, value in os.environ.items() if key in keys}
    if windows_powershell:
        windir = env.get("WINDIR") or env.get("SystemRoot") or r"C:\Windows"
        program_files = os.environ.get("ProgramFiles", r"C:\Program Files")
        env["PSModulePath"] = os.pathsep.join((
            str(Path(windir) / "System32" / "WindowsPowerShell" / "v1.0" / "Modules"),
            str(Path(program_files) / "WindowsPowerShell" / "Modules"),
        ))
    if extra:
        env.update(extra)
    return env


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def resolve_verified_host(config: dict) -> tuple[str, Path]:
    executable, windows_powershell = _powershell_executable()
    command = [executable, "-NoLogo", "-NoProfile", "-NonInteractive"]
    if windows_powershell:
        # Scope the execution-policy bypass to this child process; do not change PC policy.
        command.extend(["-ExecutionPolicy", "Bypass"])
    command.extend([
        "-File", config["auto_update_cli"],
        "-Command", "resolve-modules", "-StateRoot", config["state_root"],
        "-AppManifestPath", config["app_manifest_path"], "-ModuleSourceRoot", config["module_source_root"],
    ])
    try:
        completed = subprocess.run(
            command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding="utf-8", errors="replace", timeout=120,
            env=_child_environment(windows_powershell),
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise LauncherError("AutoUpdate module resolution could not run") from exc
    if completed.returncode:
        raise LauncherError(f"AutoUpdate module resolution failed with exit code {completed.returncode}")
    try:
        result = json.loads(completed.stdout.strip())
    except json.JSONDecodeError as exc:
        raise LauncherError("AutoUpdate returned invalid module-resolution JSON") from exc
    if result.get("status") != "ready" or result.get("app_id") != config["app_id"]:
        raise LauncherError("AutoUpdate did not resolve the AutoSync host manifest")
    rows = [row for row in result.get("modules", []) if isinstance(row, dict) and row.get("module_id") == "AutoSync"]
    if len(rows) != 1:
        raise LauncherError("AutoUpdate did not return exactly one AutoSync module")
    row = rows[0]
    version = row.get("version")
    if row.get("verified") is not True or row.get("api_major") != 1 or row.get("activation_mode") != "restart":
        raise LauncherError("AutoUpdate returned an unverified or incompatible AutoSync module")
    if not isinstance(version, str) or not VERSION_RE.fullmatch(version):
        raise LauncherError("AutoUpdate returned an invalid AutoSync module version")
    state_root = Path(config["state_root"]).resolve()
    expected_root = (state_root / "modules" / "AutoSync" / "versions" / version).resolve()
    try:
        module_root = Path(row.get("module_path", "")).resolve(strict=True)
        if os.path.normcase(str(module_root)) != os.path.normcase(str(expected_root)):
            raise LauncherError("resolved AutoSync path is outside its versioned cache")
        manifest_path = module_root / "module.json"
        manifest_bytes = manifest_path.read_bytes()
        manifest_hash = hashlib.sha256(manifest_bytes).hexdigest()
        if row.get("manifest_sha256") and row["manifest_sha256"].lower() != manifest_hash:
            raise LauncherError("resolved AutoSync manifest hash does not match")
        manifest = json.loads(manifest_bytes.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LauncherError("resolved AutoSync cache is incomplete") from exc
    if (manifest.get("schema") != 1 or manifest.get("contract") != "autoupdate-module-v1"
            or manifest.get("module_id") != "AutoSync" or manifest.get("version") != version
            or manifest.get("api_major") != 1 or manifest.get("activation_mode") != "restart"):
        raise LauncherError("resolved AutoSync manifest identity is invalid")
    seen = set()
    host_script = None
    for entry in manifest.get("files", []):
        if not isinstance(entry, dict):
            raise LauncherError("resolved AutoSync file manifest is invalid")
        relative = entry.get("path")
        if not isinstance(relative, str) or not relative or relative.startswith(("/", "\\")) or ":" in relative:
            raise LauncherError("resolved AutoSync file path is unsafe")
        normalized = relative.replace("\\", "/")
        parts = normalized.split("/")
        if any(part in ("", ".", "..") for part in parts) or normalized in seen:
            raise LauncherError("resolved AutoSync file path is unsafe or duplicated")
        seen.add(normalized)
        candidate = module_root.joinpath(*parts).resolve()
        try:
            candidate.relative_to(module_root)
        except ValueError as exc:
            raise LauncherError("resolved AutoSync file escaped its cache root") from exc
        try:
            if not candidate.is_file() or candidate.stat().st_size != int(entry["size"]):
                raise LauncherError("resolved AutoSync package file is incomplete")
            if _sha256(candidate) != str(entry["sha256"]).lower():
                raise LauncherError("resolved AutoSync package file failed hash verification")
        except (KeyError, TypeError, ValueError, OSError) as exc:
            raise LauncherError("resolved AutoSync package file failed verification") from exc
        if normalized == "src/autosync_host.py":
            host_script = candidate
    if host_script is None:
        raise LauncherError("resolved AutoSync package has no host supervisor")
    return version, host_script


class LauncherLock:
    def __init__(self, path: Path):
        self.path = path
        self.stream = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.stream = self.path.open("a+b")
        self.stream.seek(0, os.SEEK_END)
        if self.stream.tell() == 0:
            self.stream.write(b"0")
            self.stream.flush()
        self.stream.seek(0)
        try:
            if msvcrt is not None:
                msvcrt.locking(self.stream.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                fcntl.flock(self.stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            self.stream.close()
            self.stream = None
            return False

    def release(self) -> None:
        if self.stream is None:
            return
        try:
            self.stream.seek(0)
            if msvcrt is not None:
                msvcrt.locking(self.stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(self.stream.fileno(), fcntl.LOCK_UN)
        finally:
            self.stream.close()
            self.stream = None


def append_log(config: dict, event: str, **fields) -> None:
    log_path = Path(config["log_file"]).with_name("autosync-launcher.jsonl")
    log_path.parent.mkdir(parents=True, exist_ok=True)
    row = {"utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "component": "AutoSyncLauncher",
           "event": event, **fields}
    with log_path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def run_launcher(config_path: Path, resolver=resolve_verified_host, process_factory=subprocess.Popen,
                 sleeper=time.sleep, max_failures: int = 8) -> int:
    config = read_config(config_path)
    lock_path = Path(config["state_root"]) / "autosync-host" / "launcher.lock"
    lock = LauncherLock(lock_path)
    if not lock.acquire():
        append_log(config, "duplicate-launcher-ignored")
        return 0
    failures = 0
    try:
        while failures < max_failures:
            try:
                config = read_config(config_path)
                version, host_script = resolver(config)
                command = [config["python_executable"], "-B", str(host_script), "--config", str(config_path.resolve())]
                env = _child_environment(False, {
                    "PYTHONPATH": str(host_script.parent),
                    "PYTHONDONTWRITEBYTECODE": "1",
                })
                log_path = Path(config["log_file"]).with_name("autosync-launcher.jsonl")
                log_path.parent.mkdir(parents=True, exist_ok=True)
                with log_path.open("ab", buffering=0) as output:
                    process = process_factory(
                        command, stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
                        cwd=str(host_script.parent), env=env,
                        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                    )
                    append_log(config, "host-supervisor-started", version=version, pid=process.pid)
                    code = process.wait()
                append_log(config, "host-supervisor-exited", version=version, exit_code=code)
                if code == 0:
                    return 0
                if code == SUPERVISOR_RESTART_EXIT:
                    failures = 0
                    continue
                failures += 1
            except Exception as exc:
                failures += 1
                append_log(config, "launch-attempt-failed", error_type=type(exc).__name__, consecutive_failures=failures)
            if failures < max_failures:
                sleeper(min(30 * (2 ** min(failures - 1, 5)), 600))
        append_log(config, "manual-attention-required", consecutive_failures=failures)
        return 2
    finally:
        lock.release()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Launch the verified AutoSync host supervisor from AutoUpdate cache.")
    parser.add_argument("--config", required=True, type=Path, help="ACL-protected AutoSync host config")
    parser.add_argument("--validate-config", action="store_true", help="validate config and exit without starting anything")
    args = parser.parse_args(argv)
    try:
        config = read_config(args.config)
        if args.validate_config:
            print(json.dumps({"status": "valid", "app_id": config["app_id"]}))
            return 0
        return run_launcher(args.config)
    except LauncherError as exc:
        print(f"AutoSync host launcher stopped safely: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
