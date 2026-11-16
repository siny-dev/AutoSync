"""Verify that this Windows host can resolve the currently published AutoSync module."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--auto-update-root", type=Path, default=Path(r"\\H255\AI-Work\제작중인 프로그램\AutoUpdate"))
    parser.add_argument("--powershell", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--app-id", default="autosync-host-resolution-probe")
    args = parser.parse_args()
    update_root = args.auto_update_root.resolve()
    cli = update_root / "tools" / "autoupdate.ps1"
    source_root = update_root / "outbox" / "modules"
    pointer_path = source_root / "AutoSync" / "current.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8-sig"))
    version = pointer["version"]
    ps = args.powershell
    if ps is None:
        candidate = Path(os.environ["WINDIR"]) / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe"
        if candidate.is_file():
            ps = candidate
        else:
            ps = Path("pwsh.exe")
    report = {"status": "FAIL", "computer_name": os.environ.get("COMPUTERNAME"),
              "powershell": str(ps), "app_id": args.app_id, "expected_version": version}
    with tempfile.TemporaryDirectory(prefix="AutoSyncModuleResolve-") as temp_name:
        temp = Path(temp_name)
        app_manifest = temp / "app-modules.json"
        app_manifest.write_text(json.dumps({"schema": 1, "contract": "autoupdate-app-modules-v1",
            "app_id": args.app_id, "modules": [{"module_id": "AutoSync", "api_major": 1}]}), encoding="utf-8")
        command = [str(ps), "-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
            "-File", str(cli), "-Command", "resolve-modules", "-StateRoot", str(temp / "state"),
            "-AppManifestPath", str(app_manifest), "-ModuleSourceRoot", str(source_root)]
        result = subprocess.run(command, capture_output=True, text=True, timeout=180)
        report["resolver_exit_code"] = result.returncode
        if result.returncode:
            report["detail"] = (result.stderr or result.stdout)[-2000:]
        else:
            resolved = json.loads(result.stdout.strip())
            matches = [m for m in resolved.get("modules", []) if m.get("module_id") == "AutoSync"]
            good = (resolved.get("status") == "ready" and resolved.get("app_id") == args.app_id
                and len(matches) == 1 and matches[0].get("version") == version
                and matches[0].get("api_major") == 1 and matches[0].get("verified") is True
                and Path(matches[0]["module_path"]).is_dir())
            report.update({"status": "PASS" if good else "FAIL", "resolved_status": resolved.get("status"),
                "resolved_version": matches[0].get("version") if matches else None,
                "api_major": matches[0].get("api_major") if matches else None,
                "verified": matches[0].get("verified") if matches else None,
                "temporary_state_root_cleaned": True})
            if not good:
                report["detail"] = "resolver did not return the verified current AutoSync module"
    output = json.dumps(report, ensure_ascii=False, sort_keys=True)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(output + "\n", encoding="utf-8")
    print(output)
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
