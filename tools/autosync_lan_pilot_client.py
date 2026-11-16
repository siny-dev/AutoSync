"""Temporary two-PC pilot client that imports an AutoUpdate-verified AutoSync module."""
from __future__ import annotations
import argparse
import contextlib
import json
import os
from pathlib import Path
import runpy
import shutil
import subprocess
import sys


def resolve_module(auto_update_root: Path, state_root: Path, app_id: str):
    auto_update_root = auto_update_root.resolve()
    cli = auto_update_root / "tools" / "autoupdate.ps1"
    source = auto_update_root / "outbox" / "modules"
    pointer = json.loads((source / "AutoSync" / "current.json").read_text(encoding="utf-8-sig"))
    manifest_path = state_root.parent / "client-app-modules.json"
    manifest_path.write_text(json.dumps({"schema": 1, "contract": "autoupdate-app-modules-v1",
        "app_id": app_id, "modules": [{"module_id": "AutoSync", "api_major": 1}]}), encoding="utf-8")
    powershell = Path(os.environ.get("WINDIR", r"C:\Windows")) / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe"
    if not powershell.is_file():
        candidate = shutil.which("pwsh.exe") or shutil.which("pwsh")
        if not candidate:
            raise RuntimeError("PowerShell is unavailable")
        powershell = Path(candidate)
    command = [str(powershell), "-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
        "-File", str(cli), "-Command", "resolve-modules", "-StateRoot", str(state_root),
        "-AppManifestPath", str(manifest_path), "-ModuleSourceRoot", str(source)]
    completed = subprocess.run(command, capture_output=True, text=True, timeout=180, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    if completed.returncode:
        raise RuntimeError("AutoUpdate module resolution failed: " + (completed.stderr or completed.stdout)[-1000:])
    result = json.loads(completed.stdout.strip())
    matches = [row for row in result.get("modules", []) if row.get("module_id") == "AutoSync"]
    if (result.get("status") != "ready" or len(matches) != 1 or matches[0].get("version") != pointer.get("version")
            or matches[0].get("verified") is not True or matches[0].get("api_major") != 1):
        raise RuntimeError("AutoUpdate did not return a verified current AutoSync module")
    module_root = Path(matches[0]["module_path"])
    if not module_root.is_dir():
        raise RuntimeError("verified AutoSync cache path is missing")
    return pointer["version"], module_root


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--auto-update-root", type=Path, required=True)
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--source-probe", type=Path, required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--ca-file", type=Path, required=True)
    parser.add_argument("--token-file", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--instance-id", required=True)
    parser.add_argument("--peer-id", required=True)
    parser.add_argument("--seconds", type=int, default=25)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--result-file", type=Path, required=True)
    args = parser.parse_args()
    try:
        version, module_root = resolve_module(args.auto_update_root, args.state_root, "autosync-lan-probe-client")
        token_map = json.loads(args.token_file.read_text(encoding="utf-8-sig"))
        token = token_map.get("autosync-pilot-probe")
        if not isinstance(token, str) or len(token) < 32:
            raise RuntimeError("pilot token file is invalid")
        probe_root = args.data_dir.parent / "verified-probe"
        tools = probe_root / "tools"
        tools.mkdir(parents=True, exist_ok=True)
        original = args.source_probe.read_text(encoding="utf-8-sig")
        old = "MODULE_ROOT = Path(__file__).resolve().parents[1]"
        if original.count(old) != 1:
            raise RuntimeError("pilot source no longer has the expected module-root anchor")
        patched = original.replace(old, f"MODULE_ROOT = Path(r'{module_root}')", 1)
        probe_script = tools / "autosync_pilot.py"
        probe_script.write_text(patched, encoding="utf-8")
        os.environ["AUTOSYNC_PILOT_TOKEN"] = token
        sys.argv = [str(probe_script), "--server", args.server, "--ca-file", str(args.ca_file),
            "--run-id", args.run_id, "--instance-id", args.instance_id, "--peer-id", args.peer_id,
            "--seconds", str(args.seconds), "--interval", str(args.interval), "--data-dir", str(args.data_dir)]
        with args.result_file.open("w", encoding="utf-8") as output:
            with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
                try:
                    runpy.run_path(str(probe_script), run_name="__main__")
                except SystemExit:
                    pass
        summary = json.loads(args.result_file.read_text(encoding="utf-8").strip())
        summary["resolved_module_version"] = version
        summary["module_verified"] = True
        args.result_file.write_text(json.dumps(summary, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        print(json.dumps({"status": summary.get("result"), "module_version": version,
            "module_verified": True, "instance_id": args.instance_id,
            "peer_seen": summary.get("peer_record_seen"), "roundtrip_acks": summary.get("roundtrip_acknowledgements")}, ensure_ascii=False))
        return 0 if summary.get("result") == "PASS" else 2
    except Exception as exc:
        args.result_file.parent.mkdir(parents=True, exist_ok=True)
        args.result_file.write_text(json.dumps({"result": "ERROR", "error_type": type(exc).__name__}), encoding="utf-8")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
