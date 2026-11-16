"""Temporary LAN pilot server that imports only an AutoUpdate-verified AutoSync module."""
from __future__ import annotations
import argparse
import contextlib
import json
import os
from pathlib import Path
import shutil
import ssl
import subprocess
import sys
import threading
import time


def resolve_module(auto_update_root: Path, state_root: Path, app_id: str):
    auto_update_root = auto_update_root.resolve()
    cli = auto_update_root / "tools" / "autoupdate.ps1"
    source = auto_update_root / "outbox" / "modules"
    pointer = json.loads((source / "AutoSync" / "current.json").read_text(encoding="utf-8-sig"))
    manifest_path = state_root.parent / "host-app-modules.json"
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
    return pointer["version"], module_root, manifest_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--auto-update-root", type=Path, required=True)
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--tls-cert", type=Path, required=True)
    parser.add_argument("--tls-key", type=Path, required=True)
    parser.add_argument("--token-file", type=Path, required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--ready-file", type=Path, required=True)
    parser.add_argument("--stop-file", type=Path, required=True)
    parser.add_argument("--app-id", default="autosync-pilot-probe")
    parser.add_argument("--log-file", type=Path, required=True)
    args = parser.parse_args()
    server = None
    try:
        version, module_root, app_manifest = resolve_module(args.auto_update_root, args.state_root, args.app_id)
        token_map = json.loads(args.token_file.read_text(encoding="utf-8-sig"))
        if set(token_map) != {args.app_id} or not isinstance(token_map[args.app_id], str) or len(token_map[args.app_id]) < 32:
            raise RuntimeError("pilot token file is invalid")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(str(args.tls_cert), str(args.tls_key))
        sys.path.insert(0, str(module_root))
        from src.autosync_server import create_server
        server = create_server(args.host, args.port, args.data_dir, token_map, context,
                               allow_lan=True, module_version=version)
        args.ready_file.parent.mkdir(parents=True, exist_ok=True)
        args.ready_file.write_text(json.dumps({"status": "ready", "module_version": version,
            "verified": True, "module_path": str(module_root), "port": server.server_address[1],
            "app_id": args.app_id}), encoding="utf-8")
        args.log_file.parent.mkdir(parents=True, exist_ok=True)
        stop_requested = threading.Event()
        def monitor_stop():
            while not stop_requested.is_set():
                if args.stop_file.exists():
                    server.shutdown()
                    return
                time.sleep(0.2)
        monitor = threading.Thread(target=monitor_stop, name="pilot-stop-monitor", daemon=True)
        monitor.start()
        with args.log_file.open("a", encoding="utf-8") as log, contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
            print(json.dumps({"event": "server-started", "module_version": version, "verified": True,
                "host": args.host, "port": server.server_address[1], "app_id": args.app_id}))
            try:
                server.serve_forever(poll_interval=0.2)
            finally:
                stop_requested.set()
                server.server_close()
                monitor.join(timeout=3)
                print(json.dumps({"event": "server-stopped", "module_version": version}))
        return 0
    except Exception as exc:
        args.ready_file.parent.mkdir(parents=True, exist_ok=True)
        args.ready_file.write_text(json.dumps({"status": "error", "error_type": type(exc).__name__}), encoding="utf-8")
        if server is not None:
            try:
                server.shutdown()
                server.server_close()
            except Exception:
                pass
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
