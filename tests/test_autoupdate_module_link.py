import json
import os
import shutil
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path

from src.autosync_server import create_server
from src import __version__ as AUTOSYNC_VERSION


APP_ID = "autosync-memo-demo"
TOKEN = "module-link-test-token-with-more-than-32-bytes"
AUTOUPDATE_ROOT = Path(os.environ.get(
    "AUTOSYNC_AUTOUPDATE_ROOT",
    r"\\H255\AI-Work\제작중인 프로그램\AutoUpdate",
))
SHARED_MODULE_SOURCE = Path(os.environ.get(
    "AUTOSYNC_AUTOUPDATE_MODULE_SOURCE_ROOT",
    r"\\H255\AI-Work\제작중인 프로그램\AutoUpdate\outbox\modules",
))
SHARED_AUTOSYNC_POINTER = SHARED_MODULE_SOURCE / "AutoSync" / "current.json"
POWERSHELL = shutil.which("powershell.exe")

def _decode_process_output(data: bytes) -> str:
    if not data:
        return ""
    encodings = ["utf-8"]
    try:
        import locale
        encodings.append(locale.getpreferredencoding(False))
    except Exception:
        pass
    encodings.extend(["mbcs", "cp949"])
    for encoding in dict.fromkeys(encodings):
        try:
            return data.decode(encoding)
        except (LookupError, UnicodeDecodeError):
            continue
    return data.decode("utf-8", errors="replace")


def _run_powershell(*args, **kwargs) -> subprocess.CompletedProcess[str]:
    kwargs.pop("text", None)
    completed = subprocess.run(*args, text=False, **kwargs)
    return subprocess.CompletedProcess(
        completed.args, completed.returncode,
        _decode_process_output(completed.stdout or b""),
        _decode_process_output(completed.stderr or b""),
    )



@unittest.skipUnless(POWERSHELL and (AUTOUPDATE_ROOT / "tools" / "autoupdate.ps1").is_file(),
                     "AutoUpdate PowerShell module tooling is unavailable")
class AutoUpdateModuleLinkTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="autosync-module-link-")
        self.root = Path(self.temp.name)
        self.server = create_server("127.0.0.1", 0, self.root / "server", {APP_ID: TOKEN})
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"

    def tearDown(self):
        if self.thread.is_alive():
            self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.temp.cleanup()

    def test_memo_app_resolves_verified_shared_module_and_syncs_through_it(self):
        project = Path(__file__).resolve().parents[1]
        publish_script = project / "tools" / "publish-autosync-module.ps1"
        app_launcher = project / "examples" / "run-memo-demo.ps1"
        module_source = self.root / "modules"
        state_root = self.root / "autoupdate-state"
        env = os.environ.copy()
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        env["AUTOSYNC_AUTOUPDATE_ROOT"] = str(AUTOUPDATE_ROOT)

        publish = _run_powershell(
            [POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(publish_script),
             "-ModuleSourceRoot", str(module_source), "-StateRoot", str(state_root)],
            cwd=str(project), env=env, capture_output=True, text=True, timeout=60,
        )
        self.assertEqual(publish.returncode, 0, publish.stderr or publish.stdout)
        publish_result = json.loads(publish.stdout.strip().splitlines()[-1])
        self.assertEqual(publish_result["Status"], "published")
        self.assertEqual(publish_result["ModuleId"], "AutoSync")
        self.assertEqual(publish_result["Version"], AUTOSYNC_VERSION)

        env["AUTOSYNC_DEMO_TOKEN"] = TOKEN
        first_data = self.root / "memo-one"
        first = _run_powershell(
            [POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(app_launcher),
             "-Server", self.base, "-DataDirectory", str(first_data),
             "-StateRoot", str(state_root), "-ModuleSourceRoot", str(module_source),
             "-PythonExecutable", shutil.which("python") or "python", "-DemoArgumentsJson",
             json.dumps(["put", "shared-note", "created through verified AutoSync module"])],
            cwd=str(project), env=env, capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(first.returncode, 0, first.stderr or first.stdout)
        first_result = json.loads(first.stdout.strip().splitlines()[-1])
        self.assertEqual(first_result["sync"]["status"], "synced")

        second_data = self.root / "memo-two"
        second = _run_powershell(
            [POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(app_launcher),
             "-Server", self.base, "-DataDirectory", str(second_data),
             "-StateRoot", str(state_root), "-ModuleSourceRoot", str(module_source),
             "-PythonExecutable", shutil.which("python") or "python", "-DemoArgumentsJson", json.dumps(["list"])],
            cwd=str(project), env=env, capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(second.returncode, 0, second.stderr or second.stdout)
        notes = json.loads(second.stdout.strip())
        self.assertEqual(notes[0]["note_id"], "shared-note")
        self.assertEqual(notes[0]["text"], "created through verified AutoSync module")

        resolved = _run_powershell(
            [POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(AUTOUPDATE_ROOT / "tools" / "autoupdate.ps1"),
             "-Command", "resolve-modules", "-AppManifestPath", str(project / "examples" / "app-modules.json"),
             "-ModuleSourceRoot", str(module_source), "-StateRoot", str(state_root)],
            cwd=str(project), env=env, capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(resolved.returncode, 0, resolved.stderr or resolved.stdout)
        resolution = json.loads(resolved.stdout.strip().splitlines()[-1])
        self.assertTrue(resolution["modules"][0]["verified"])
        self.assertEqual(resolution["modules"][0]["source_status"], "already-cached")
        module_path = Path(resolution["modules"][0]["module_path"])
        for filename in (
            "autosync_server.py",
            "autosync_credentials.py",
            "autosync_maintenance.py",
            "autosync_host.py",
            "autosync_firebase_auth.py",
            "autosync_firebase_rtdb.py",
            "autosync_firestore_partitioned.py",
            "autosync_firestore_store.py",
        ):
            self.assertTrue((module_path / "src" / filename).is_file(), f"server runtime file missing from module: {filename}")
        probe_code = (
            "import sys; from src.autosync_server import Store; "
            "from src.autosync_credentials import CredentialError; "
            "from src.autosync_maintenance import compact_events; "
            "from src.autosync_host import HostSupervisor; "
            "import src.autosync_firebase_auth, src.autosync_firebase_rtdb, "
            "src.autosync_firestore_partitioned, src.autosync_firestore_store; "
            "store=Store(sys.argv[1]); status,result=store.write('module-probe','record-1',{'ok':True},False,0,'module-server-op-00001'); "
            "assert status==200 and result['ok']; store.close()"
        )
        probe_env = env.copy()
        probe_env["PYTHONPATH"] = str(module_path)
        probe = subprocess.run([shutil.which("python") or "python", "-B", "-c", probe_code,
                                str(self.root / "module-server-probe.sqlite3")],
                               cwd=str(self.root), env=probe_env, capture_output=True, text=True, timeout=20)
        self.assertEqual(probe.returncode, 0, probe.stderr or probe.stdout)

    @unittest.skipUnless(SHARED_AUTOSYNC_POINTER.is_file(),
                         "AutoSync has not been published to the shared AutoUpdate module source")
    def test_memo_app_uses_the_published_shared_autosync_module(self):
        project = Path(__file__).resolve().parents[1]
        app_launcher = project / "examples" / "run-memo-demo.ps1"
        state_root = self.root / "shared-autoupdate-state"
        env = os.environ.copy()
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        env["AUTOSYNC_DEMO_TOKEN"] = TOKEN
        python = shutil.which("python") or "python"

        def run_demo(data_directory: Path, arguments: list[str]) -> subprocess.CompletedProcess[str]:
            return _run_powershell(
                [POWERSHELL, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File",
                 str(app_launcher), "-Server", self.base, "-DataDirectory", str(data_directory),
                 "-StateRoot", str(state_root), "-ModuleSourceRoot", str(SHARED_MODULE_SOURCE),
                 "-PythonExecutable", python, "-DemoArgumentsJson", json.dumps(arguments)],
                cwd=str(project), env=env, capture_output=True, text=True, timeout=30,
            )

        first = run_demo(self.root / "shared-module-writer", [
            "put", "published-module-note", "loaded from shared AutoUpdate source",
        ])
        self.assertEqual(first.returncode, 0, first.stderr or first.stdout)
        first_result = json.loads(first.stdout.strip().splitlines()[-1])
        self.assertEqual(first_result["sync"]["status"], "synced")

        second = run_demo(self.root / "shared-module-reader", ["list"])
        self.assertEqual(second.returncode, 0, second.stderr or second.stdout)
        notes = json.loads(second.stdout.strip())
        self.assertEqual(notes[0]["note_id"], "published-module-note")
        self.assertEqual(notes[0]["text"], "loaded from shared AutoUpdate source")

        cached_pointer = state_root / "modules" / "AutoSync" / "current.json"
        self.assertTrue(cached_pointer.is_file())
        published_version = json.loads(SHARED_AUTOSYNC_POINTER.read_text(encoding="utf-8-sig"))["version"]
        self.assertEqual(json.loads(cached_pointer.read_text(encoding="utf-8-sig"))["version"], published_version)


if __name__ == "__main__":
    unittest.main(verbosity=2)
