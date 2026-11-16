#!/usr/bin/env python3
"""Prepare a per-user, loopback-only AutoSync host configuration.

The setup never publishes anything, changes the existing shared-host setup, or
starts a listener. App-scoped tokens are generated locally and stored only in
Windows Credential Manager; the host and matching client target receive the
same token for each app ID.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
AUTOUPDATE_ROOT = ROOT.parent / "AutoUpdate"
DEFAULT_ROOT = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local")) / "Siny" / "AutoSync" / "host-local"
APP_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
SERVER_PREFIX = "AutoSync/local-server/"
CLIENT_PREFIX = "AutoSync/local-client/"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "tools") not in sys.path:
    sys.path.insert(0, str(ROOT / "tools"))

from src.autosync_credentials import CredentialError, delete_secret, read_secret, write_secret
from autosync_host_launcher import LauncherError, read_config


class SetupError(RuntimeError):
    pass


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    fd, temporary_name = tempfile.mkstemp(prefix=".autosync-setup-", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _validate_app_ids(app_ids: list[str]) -> list[str]:
    if not app_ids or any(not APP_ID_RE.fullmatch(app_id) for app_id in app_ids):
        raise SetupError("Provide one or more valid app IDs (letters, digits, dot, underscore, hyphen).")
    if len({app_id.casefold() for app_id in app_ids}) != len(app_ids):
        raise SetupError("App IDs must be unique, including case-insensitive duplicates.")
    return app_ids


def _ensure_token_pair(app_id: str) -> str:
    server_target = SERVER_PREFIX + app_id
    client_target = CLIENT_PREFIX + app_id
    existing = {}
    for target in (server_target, client_target):
        try:
            existing[target] = read_secret(target)
        except CredentialError:
            existing[target] = None
    server_secret, client_secret = existing[server_target], existing[client_target]
    if server_secret is not None and client_secret is not None:
        if not secrets.compare_digest(server_secret, client_secret):
            raise SetupError(f"Server/client credentials already differ for {app_id}; preserved without changes.")
        return "preserved"
    if server_secret is not None or client_secret is not None:
        raise SetupError(f"Only one credential target exists for {app_id}; preserved without changes.")

    token = secrets.token_urlsafe(48)
    written: list[str] = []
    try:
        write_secret(server_target, token, username="AutoSync local server")
        written.append(server_target)
        write_secret(client_target, token, username="AutoSync local client")
        written.append(client_target)
    except Exception:
        for target in reversed(written):
            try:
                delete_secret(target)
            except Exception:
                pass
        raise
    return "created"


def build_configuration(app_ids: list[str], root: Path = DEFAULT_ROOT) -> tuple[dict, dict]:
    app_ids = _validate_app_ids(app_ids)
    root = Path(root)
    if not root.is_absolute() or str(root).startswith("\\\\"):
        raise SetupError("The local host root must be an absolute local path, not a network path.")
    if os.name != "nt":
        raise SetupError("The local host setup requires Windows Credential Manager.")
    auto_update_cli = AUTOUPDATE_ROOT / "tools" / "autoupdate.ps1"
    update_policy = AUTOUPDATE_ROOT / "config" / "update-policy.json"
    module_source = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local")) / "Siny" / "AutoUpdate" / "local-module-source"
    state_root = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local")) / "Siny" / "AutoUpdate"
    required_local_paths = (auto_update_cli, update_policy, module_source, state_root, ROOT)
    if any(not item.exists() for item in required_local_paths):
        raise SetupError("Local AutoUpdate CLI, policy, module source, or state is missing.")
    paths = {
        "auto_update_cli": str(auto_update_cli.resolve()),
        "module_source_root": str(module_source.resolve()),
        "state_root": str(state_root.resolve()),
        "app_manifest_path": str((root / "host-app-modules.json").resolve()),
        "update_policy_path": str(update_policy.resolve()),
        "python_executable": str(Path(sys.executable).resolve()),
        "data_dir": str((root / "data").resolve()),
        "backup_dir": str((root / "backups").resolve()),
        "log_file": str((root / "logs" / "host.jsonl").resolve()),
    }
    manifest = {
        "schema": 1,
        "contract": "autoupdate-app-modules-v1",
        "app_id": "AutoSyncHost",
        "modules": [{"module_id": "AutoSync", "api_major": 1}],
    }
    config = {
        "schema": 1,
        "contract": "autosync-host-config-v1",
        **paths,
        "bind_host": "127.0.0.1",
        "port": 8765,
        "allow_lan": False,
        "credential_apps": app_ids,
        "credential_target_prefix": SERVER_PREFIX,
        "app_id": "AutoSyncHost",
        "startup_timeout_seconds": 45,
        "drain_timeout_seconds": 45,
    }
    return manifest, config


def setup_local_host(app_ids: list[str], root: Path = DEFAULT_ROOT) -> dict:
    root = root.resolve()
    config_path = root / "host-config.json"
    manifest_path = root / "host-app-modules.json"
    if config_path.exists() or manifest_path.exists():
        raise SetupError("Local host setup already exists; refusing to overwrite it.")
    manifest, config = build_configuration(app_ids, root)
    created_targets: list[str] = []
    try:
        for app_id in app_ids:
            if _ensure_token_pair(app_id) == "created":
                created_targets.extend((SERVER_PREFIX + app_id, CLIENT_PREFIX + app_id))
        _write_json(manifest_path, manifest)
        _write_json(config_path, config)
        read_config(config_path)
    except (CredentialError, LauncherError, OSError, SetupError, ValueError) as exc:
        for target in reversed(created_targets):
            try:
                delete_secret(target)
            except Exception:
                pass
        for path in (config_path, manifest_path):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        raise SetupError(str(exc)) from exc
    return {
        "status": "ready",
        "config_path": str(config_path),
        "manifest_path": str(manifest_path),
        "bind_host": config["bind_host"],
        "port": config["port"],
        "allow_lan": config["allow_lan"],
        "app_ids": list(app_ids),
        "credential_targets": [SERVER_PREFIX + item for item in app_ids],
    }


def install_user_task(config_path: Path, task_name: str = "Siny-AutoSyncLocalHost") -> str:
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,64}", task_name):
        raise SetupError("Invalid per-user task name.")
    powershell = Path(os.environ.get("WINDIR", r"C:\Windows")) / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe"
    if not powershell.is_file():
        raise SetupError("Windows PowerShell 5.1 is unavailable; no task was installed.")
    installer = ROOT / "tools" / "install_autosync_host_task.ps1"
    command = [
        str(powershell), "-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
        "-File", str(installer), "-Mode", "install", "-TaskName", task_name,
        "-ConfigPath", str(config_path), "-PythonExecutable", sys.executable, "-StartNow",
    ]
    try:
        result = subprocess.run(
            command, stdin=subprocess.DEVNULL, capture_output=True, text=True,
            encoding="utf-8", errors="replace", timeout=45,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SetupError("Could not install the hidden per-user AutoSync task.") from exc
    if result.returncode:
        diagnostic = (result.stderr or result.stdout).strip().replace("\r", " ")[-500:]
        if "0x80070005" in diagnostic.casefold():
            startup_path, created = install_user_startup(config_path)
            try:
                pid = start_local_host_now(config_path)
            except Exception:
                if created:
                    try:
                        startup_path.unlink()
                    except FileNotFoundError:
                        pass
                raise
            return json.dumps({
                "mechanism": "current-user-startup",
                "startup_file": str(startup_path),
                "pid": pid,
            }, ensure_ascii=False)
        raise SetupError("AutoSync task installation failed: " + diagnostic)
    return json.dumps({"mechanism": "scheduled-task", "result": result.stdout.strip()}, ensure_ascii=False)


def _pythonw_executable() -> Path:
    path = Path(sys.executable).with_name("pythonw.exe")
    if not path.is_file():
        raise SetupError("pythonw.exe is unavailable; no hidden local host was started.")
    return path


def install_user_startup(config_path: Path) -> tuple[Path, bool]:
    appdata = os.environ.get("APPDATA")
    if not appdata:
        raise SetupError("The signed-in user's APPDATA path is unavailable.")
    startup_dir = Path(appdata) / "Microsoft" / "Windows" / "Start Menu" / "Programs" / "Startup"
    startup_dir.mkdir(parents=True, exist_ok=True)
    startup_path = startup_dir / "Siny-AutoSyncLocalHost.vbs"
    pythonw = _pythonw_executable()
    launcher = ROOT / "tools" / "autosync_host_launcher.py"
    command = f'"{pythonw}" -B "{launcher}" --config "{config_path.resolve()}"'
    vbs = (
        'Set shell = CreateObject("WScript.Shell")\r\n'
        'command = "' + command.replace('"', '""') + '"\r\n'
        'shell.Run command, 0, False\r\n'
    )
    if startup_path.exists():
        raw = startup_path.read_bytes()
        previous = None
        previous_encoding = None
        for encoding in ("utf-16", "utf-8-sig"):
            try:
                previous = raw.decode(encoding)
                previous_encoding = encoding
                break
            except UnicodeError:
                continue
        if previous != vbs:
            raise SetupError("A different AutoSync startup script already exists; it was preserved.")
        if previous_encoding != "utf-16":
            _write_text(startup_path, vbs, encoding="utf-16")
        return startup_path, False
    _write_text(startup_path, vbs, encoding="utf-16")
    return startup_path, True


def _write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=".autosync-startup-", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding=encoding, newline="") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def start_local_host_now(config_path: Path) -> int:
    pythonw = _pythonw_executable()
    launcher = ROOT / "tools" / "autosync_host_launcher.py"
    log_path = config_path.parent / "logs" / "startup-launcher.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("ab", buffering=0) as output:
        process = subprocess.Popen(
            [str(pythonw), "-B", str(launcher), "--config", str(config_path.resolve())],
            cwd=str(ROOT), stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    time.sleep(0.25)
    if process.poll() is not None:
        raise SetupError(f"The local host launcher exited immediately with code {process.returncode}.")
    return process.pid


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Prepare a loopback-only AutoSync host for local apps.")
    parser.add_argument("--app-id", action="append", help="app ID; repeat for multiple apps")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="new per-user host directory")
    parser.add_argument("--install-task", action="store_true", help="install and start the hidden current-user task")
    parser.add_argument("--install-existing-task", action="store_true", help="install/start a task for the existing host config")
    parser.add_argument("--task-name", default="Siny-AutoSyncLocalHost")
    parser.add_argument("--config-path", type=Path, help="existing host config used with --install-existing-task")
    args = parser.parse_args(argv)
    try:
        if args.install_existing_task:
            config_path = args.config_path or (args.root / "host-config.json")
            read_config(config_path)
            result = {"task": install_user_task(config_path, args.task_name)}
        else:
            if not args.app_id:
                parser.error("--app-id is required unless --install-existing-task is selected")
            result = setup_local_host(args.app_id, args.root)
            if args.install_task:
                result["task"] = install_user_task(Path(result["config_path"]), args.task_name)
    except (SetupError, OSError) as exc:
        print("Local AutoSync host setup failed safely: " + str(exc), file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
