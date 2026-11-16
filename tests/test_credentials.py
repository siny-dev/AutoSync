import sys
import unittest
from unittest import mock

from src.autosync_credentials import CredentialError, _validate_target, load_server_tokens


class CredentialValidationTests(unittest.TestCase):
    def test_rejects_invalid_credential_targets(self):
        for value in ("", "../secret", "name with spaces", "x" * 257):
            with self.subTest(value=value):
                with self.assertRaises(CredentialError):
                    _validate_target(value)

    def test_loads_one_app_scoped_secret_per_unique_app_id(self):
        with mock.patch("src.autosync_credentials.read_secret", return_value="x" * 48) as read:
            result = load_server_tokens(["autosync-pilot-probe", "workboard"])
        self.assertEqual(result, {
            "autosync-pilot-probe": "x" * 48,
            "workboard": "x" * 48,
        })
        self.assertEqual(read.call_args_list, [
            mock.call("AutoSync/server/autosync-pilot-probe"),
            mock.call("AutoSync/server/workboard"),
        ])

    def test_rejects_duplicate_or_empty_app_list(self):
        for app_ids in ([], ["workboard", "workboard"]):
            with self.subTest(app_ids=app_ids):
                with self.assertRaises(CredentialError):
                    load_server_tokens(app_ids)

    def test_rejects_invalid_app_id_before_reading_secret(self):
        with mock.patch("src.autosync_credentials.read_secret") as read:
            with self.assertRaises(ValueError):
                load_server_tokens(["../workboard"])
            read.assert_not_called()

    @unittest.skipUnless(sys.platform == "win32", "Windows Credential Manager integration")
    def test_windows_credential_manager_round_trip(self):
        import uuid
        from src.autosync_credentials import delete_secret, read_secret, write_secret

        target = "AutoSync/test/" + uuid.uuid4().hex
        secret = "test-secret-" + uuid.uuid4().hex + "-at-least-32-bytes"
        try:
            write_secret(target, secret, username="AutoSync test")
            self.assertEqual(read_secret(target), secret)
        finally:
            delete_secret(target)


    @unittest.skipUnless(sys.platform == "win32", "Windows Credential Manager integration")
    def test_server_auth_uses_credential_manager_token(self):
        import json
        import tempfile
        import threading
        import urllib.request
        import uuid
        from pathlib import Path
        from src.autosync_credentials import delete_secret, write_secret
        from src.autosync_server import create_server

        app_id = "autosync-pilot-probe"
        prefix = "AutoSync/test-server/" + uuid.uuid4().hex + "/"
        target = prefix + app_id
        secret = "server-test-" + uuid.uuid4().hex + "-at-least-32-bytes"
        server = None
        thread = None
        directory = tempfile.TemporaryDirectory()
        try:
            write_secret(target, secret, username="AutoSync test")
            tokens = load_server_tokens([app_id], target_prefix=prefix)
            server = create_server("127.0.0.1", 0, Path(directory.name) / "server", tokens)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            request = urllib.request.Request(
                f"http://127.0.0.1:{server.server_address[1]}/health",
                headers={"Authorization": "Bearer " + secret},
            )
            with urllib.request.urlopen(request, timeout=3) as response:
                self.assertEqual(response.status, 200)
                self.assertTrue(json.loads(response.read())["ok"])
        finally:
            if server is not None:
                if thread is not None and thread.is_alive():
                    server.shutdown()
                server.server_close()
                if thread is not None:
                    thread.join(timeout=2)
            delete_secret(target)
            directory.cleanup()

    @unittest.skipUnless(sys.platform == "win32", "Windows Credential Manager integration")
    def test_server_cli_loads_token_from_credential_manager(self):
        import json
        import os
        import socket
        import subprocess
        import tempfile
        import time
        import urllib.error
        import urllib.request
        import uuid
        from pathlib import Path
        from src.autosync_credentials import delete_secret, write_secret

        app_id = "test-app"
        prefix = "AutoSync/test-cli/" + uuid.uuid4().hex + "/"
        target = prefix + app_id
        secret = "server-cli-test-" + uuid.uuid4().hex + "-at-least-32-bytes"
        project = Path(__file__).resolve().parents[1]
        server_script = project / "src" / "autosync_server.py"
        local_temp = tempfile.TemporaryDirectory()
        process = None
        try:
            write_secret(target, secret, username="AutoSync test")
            with socket.socket() as probe:
                probe.bind(("127.0.0.1", 0))
                port = probe.getsockname()[1]
            env = os.environ.copy()
            env.pop("AUTOSYNC_TOKENS", None)
            env["PYTHONDONTWRITEBYTECODE"] = "1"
            process = subprocess.Popen(
                [
                    sys.executable, "-B", str(server_script),
                    "--host", "127.0.0.1", "--port", str(port),
                    "--data-dir", str(Path(local_temp.name) / "server"),
                    "--credential-apps", app_id,
                    "--credential-target-prefix", prefix,
                ],
                cwd=str(project), env=env,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            url = f"http://127.0.0.1:{port}/health"
            deadline = time.monotonic() + 8
            last_error = None
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    out, err = process.communicate(timeout=2)
                    self.fail(f"server CLI exited {process.returncode}: {err!r} {out!r}")
                request = urllib.request.Request(url, headers={"Authorization": "Bearer " + secret})
                try:
                    with urllib.request.urlopen(request, timeout=0.5) as response:
                        self.assertEqual(response.status, 200)
                        self.assertTrue(json.loads(response.read())["ok"])
                        return
                except (urllib.error.URLError, TimeoutError) as exc:
                    last_error = exc
                    time.sleep(0.05)
            self.fail(f"server CLI did not become ready: {last_error!r}")
        finally:
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)
            if process is not None:
                if process.stdout:
                    process.stdout.close()
                if process.stderr:
                    process.stderr.close()
            delete_secret(target)
            local_temp.cleanup()

if __name__ == "__main__":
    unittest.main()