import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock
import subprocess

from tools import autosync_host_launcher as launcher


def make_config(base):
    return {
        "schema": 1,
        "contract": "autosync-host-config-v1",
        "auto_update_cli": str(base / "autoupdate.ps1"),
        "module_source_root": str(base / "modules"),
        "state_root": str(base / "state"),
        "app_manifest_path": str(base / "manifest.json"),
        "update_policy_path": str(base / "policy.json"),
        "python_executable": "python.exe",
        "data_dir": str(base / "data"),
        "backup_dir": str(base / "backups"),
        "log_file": str(base / "logs" / "host.jsonl"),
        "app_id": "AutoSyncHost",
        "bind_host": "127.0.0.1",
        "port": 8765,
        "allow_lan": False,
        "credential_apps": ["sample-app"],
        "credential_target_prefix": "AutoSync/server/",
    }


def make_package(base, version="0.1.1"):
    module_root = base / "state" / "modules" / "AutoSync" / "versions" / version
    (module_root / "src").mkdir(parents=True)
    host = module_root / "src" / "autosync_host.py"
    host.write_text("# verified host " + version + "\n", encoding="utf-8")
    raw = host.read_bytes()
    manifest = {
        "schema": 1, "contract": "autoupdate-module-v1", "module_id": "AutoSync",
        "version": version, "api_major": 1, "activation_mode": "restart",
        "files": [{"path": "src/autosync_host.py", "size": len(raw),
                   "sha256": hashlib.sha256(raw).hexdigest()}],
    }
    manifest_path = module_root / "module.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    manifest_hash = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    result = {
        "status": "ready", "app_id": "AutoSyncHost",
        "modules": [{
            "module_id": "AutoSync", "version": version, "api_major": 1,
            "activation_mode": "restart", "verified": True,
            "module_path": str(module_root), "manifest_sha256": manifest_hash,
        }],
    }
    return result, host


class FakeProcess:
    next_pid = 100

    def __init__(self, code):
        self.pid = FakeProcess.next_pid
        FakeProcess.next_pid += 1
        self.code = code

    def wait(self):
        return self.code


class HostLauncherTests(unittest.TestCase):
    def test_windows_prefers_windows_powershell_without_probing_pwsh(self):
        with tempfile.TemporaryDirectory() as name:
            windir = Path(name)
            expected = windir / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe"
            expected.parent.mkdir(parents=True)
            expected.touch()
            with mock.patch.object(launcher.os, "name", "nt"), \
                 mock.patch.dict(launcher.os.environ, {"WINDIR": str(windir)}), \
                 mock.patch.object(launcher.shutil, "which", side_effect=AssertionError("pwsh must not be probed")):
                executable, windows_powershell = launcher._powershell_executable()
            self.assertEqual(executable, str(expected))
            self.assertTrue(windows_powershell)

    def test_windows_refuses_pwsh_when_windows_powershell_is_missing(self):
        with tempfile.TemporaryDirectory() as name:
            with mock.patch.object(launcher.os, "name", "nt"), \
                 mock.patch.dict(launcher.os.environ, {"WINDIR": name}), \
                 mock.patch.object(launcher.shutil, "which", return_value="pwsh.exe"):
                with self.assertRaisesRegex(launcher.LauncherError, "Windows PowerShell 5.1 is unavailable"):
                    launcher._powershell_executable()

    def test_read_config_rejects_secret_values_and_unc_state(self):
        with tempfile.TemporaryDirectory() as name:
            base = Path(name)
            config = make_config(base)
            config["token"] = "must not be written"
            path = base / "config.json"
            path.write_text(json.dumps(config), encoding="utf-8")
            with self.assertRaisesRegex(launcher.LauncherError, "must not contain secret"):
                launcher.read_config(path)
            config.pop("token")
            config["state_root"] = r"\\server\share\state"
            path.write_text(json.dumps(config), encoding="utf-8")
            with self.assertRaisesRegex(launcher.LauncherError, "local absolute paths"):
                launcher.read_config(path)

    def test_read_config_rejects_nested_secret_and_invalid_host_settings(self):
        with tempfile.TemporaryDirectory() as name:
            base = Path(name)
            config = make_config(base)
            path = base / "config.json"
            config["options"] = {"private-key": "must not be stored"}
            path.write_text(json.dumps(config), encoding="utf-8")
            with self.assertRaisesRegex(launcher.LauncherError, "must not contain secret"):
                launcher.read_config(path)

            config.pop("options")
            config["credential_apps"] = []
            path.write_text(json.dumps(config), encoding="utf-8")
            with self.assertRaisesRegex(launcher.LauncherError, "credential app IDs"):
                launcher.read_config(path)

            config["credential_apps"] = ["sample-app"]
            config["bind_host"] = "192.168.1.25"
            config["allow_lan"] = True
            path.write_text(json.dumps(config), encoding="utf-8")
            with self.assertRaisesRegex(launcher.LauncherError, "LAN binding requires"):
                launcher.read_config(path)

            config["tls_cert"] = str(base / "cert.pem")
            config["tls_key"] = str(base / "key.pem")
            path.write_text(json.dumps(config), encoding="utf-8")
            with self.assertRaisesRegex(launcher.LauncherError, "must exist"):
                launcher.read_config(path)

            (base / "cert.pem").write_bytes(b"test cert")
            (base / "key.pem").write_bytes(b"test key")
            self.assertEqual(launcher.read_config(path)["bind_host"], "192.168.1.25")

    def test_resolve_verified_host_checks_autoupdate_row_and_every_package_hash(self):
        with tempfile.TemporaryDirectory() as name:
            base = Path(name)
            config = make_config(base)
            result, host = make_package(base)
            completed = subprocess.CompletedProcess([], 0, json.dumps(result), "")
            with mock.patch.object(launcher, "_powershell_executable", return_value=("pwsh.exe", False)), \
                 mock.patch.object(launcher.subprocess, "run", return_value=completed) as run:
                version, resolved_host = launcher.resolve_verified_host(config)
            self.assertEqual(version, "0.1.1")
            self.assertEqual(resolved_host, host.resolve())
            self.assertIn("-Command", run.call_args.args[0])

    def test_resolve_verified_host_rejects_tampered_cached_file(self):
        with tempfile.TemporaryDirectory() as name:
            base = Path(name)
            config = make_config(base)
            result, host = make_package(base)
            raw = host.read_bytes()
            host.write_bytes(bytes([raw[0] ^ 1]) + raw[1:])
            completed = subprocess.CompletedProcess([], 0, json.dumps(result), "")
            with mock.patch.object(launcher, "_powershell_executable", return_value=("pwsh.exe", False)), \
                 mock.patch.object(launcher.subprocess, "run", return_value=completed):
                with self.assertRaisesRegex(launcher.LauncherError, "hash verification"):
                    launcher.resolve_verified_host(config)

    def test_windows_powershell_fallback_uses_process_scoped_execution_policy_bypass(self):
        with tempfile.TemporaryDirectory() as name:
            base = Path(name)
            config = make_config(base)
            result, _host = make_package(base)
            completed = subprocess.CompletedProcess([], 0, json.dumps(result), "")
            with mock.patch.object(launcher, "_powershell_executable", return_value=("powershell.exe", True)), \
                 mock.patch.object(launcher.subprocess, "run", return_value=completed) as run:
                launcher.resolve_verified_host(config)
            command = run.call_args.args[0]
            self.assertIn("-ExecutionPolicy", command)
            self.assertEqual(command[command.index("-ExecutionPolicy") + 1], "Bypass")
            self.assertLess(command.index("-ExecutionPolicy"), command.index("-File"))

    def test_bootstrap_resolves_again_after_supervisor_requests_module_restart(self):
        with tempfile.TemporaryDirectory() as name:
            base = Path(name)
            config = make_config(base)
            config_path = base / "host.json"
            config_path.write_text(json.dumps(config), encoding="utf-8")
            host1 = base / "cache" / "0.1.1" / "autosync_host.py"
            host2 = base / "cache" / "0.1.2" / "autosync_host.py"
            host1.parent.mkdir(parents=True)
            host2.parent.mkdir(parents=True)
            host1.write_text("# v1", encoding="utf-8")
            host2.write_text("# v2", encoding="utf-8")
            versions = iter((("0.1.1", host1), ("0.1.2", host2)))
            process_codes = iter((launcher.SUPERVISOR_RESTART_EXIT, 0))
            commands = []
            def resolver(_config):
                return next(versions)
            def popen(command, **_kwargs):
                commands.append(command)
                return FakeProcess(next(process_codes))
            with mock.patch.object(launcher, "append_log"), \
                 mock.patch.object(launcher, "_child_environment", return_value={}), \
                 mock.patch.object(launcher, "time") as clock:
                code = launcher.run_launcher(config_path, resolver=resolver, process_factory=popen, sleeper=clock.sleep)
            self.assertEqual(code, 0)
            self.assertEqual(len(commands), 2)
            self.assertIn(str(host1), commands[0])
            self.assertIn(str(host2), commands[1])
            clock.sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
