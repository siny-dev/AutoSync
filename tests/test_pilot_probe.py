import json
import os
import pathlib
import subprocess
import ssl
import sys
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path

from src.autosync_server import create_server


APP_ID = "autosync-pilot-probe"
TOKEN = "two-pc-pilot-test-token-with-more-than-32-bytes"


class PilotProbeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        cert = os.environ.get("AUTOSYNC_TEST_TLS_CERT")
        tls_context = None
        if cert:
            tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            tls_context.minimum_version = ssl.TLSVersion.TLSv1_2
            tls_context.load_cert_chain(cert, os.environ.get("AUTOSYNC_TEST_TLS_KEY") or cert)
        self.server = create_server("127.0.0.1", 0, self.root / "server", {APP_ID: TOKEN}, tls_context=tls_context)
        scheme = "https" if tls_context else "http"
        host = "localhost" if tls_context else "127.0.0.1"
        self.base = f"{scheme}://{host}:{self.server.server_address[1]}"
        self.ca_file = cert if tls_context else None
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        if self.thread.is_alive():
            self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.temp.cleanup()

    def test_two_pilot_processes_observe_peer_updates(self):
        project = pathlib.Path(__file__).resolve().parents[1]
        script = project / "tools" / "autosync_pilot.py"
        run_id = "unittest-" + uuid.uuid4().hex[:12]
        env = os.environ.copy()
        env["PYTHONPATH"] = str(project)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        env["AUTOSYNC_PILOT_TOKEN"] = TOKEN
        credential_target = None
        if sys.platform == "win32":
            from src.autosync_credentials import write_secret
            credential_target = "AutoSync/test-pilot/" + uuid.uuid4().hex
            write_secret(credential_target, TOKEN, username="AutoSync test")

        def start(instance, peer):
            data_dir = self.root / ("client-" + instance)
            command = [
                sys.executable, "-B", str(script),
                "--server", self.base,
                "--run-id", run_id,
                "--instance-id", instance,
                "--peer-id", peer,
                "--seconds", "10",
                "--interval", "0.5",
                "--data-dir", str(data_dir),
            ]
            if self.ca_file:
                command.extend(["--ca-file", self.ca_file])
            process_env = env.copy()
            if credential_target and instance == "PC2":
                process_env.pop("AUTOSYNC_PILOT_TOKEN", None)
                command.extend(["--credential-target", credential_target])
            return subprocess.Popen(
                command, cwd=str(project), env=process_env, text=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )

        pc1 = start("PC1", "PC2")
        pc2 = start("PC2", "PC1")
        try:
            try:
                deadline = time.monotonic() + 45
                out1, err1 = pc1.communicate(timeout=45)
                remaining = max(1, deadline - time.monotonic())
                out2, err2 = pc2.communicate(timeout=remaining)
            except subprocess.TimeoutExpired as exc:
                self.fail(f"pilot process timeout; partial stdout={exc.output!r}; partial stderr={exc.stderr!r}")
            self.assertEqual(pc1.returncode, 0, err1 or out1)
            self.assertEqual(pc2.returncode, 0, err2 or out2)
            result1 = json.loads(out1)
            result2 = json.loads(out2)
            self.assertEqual(result1["result"], "PASS")
            self.assertEqual(result2["result"], "PASS")
            self.assertTrue(result1["peer_record_seen"])
            self.assertTrue(result2["peer_record_seen"])
            self.assertGreater(result1["roundtrip_acknowledgements"], 0)
            self.assertGreater(result2["roundtrip_acknowledgements"], 0)
            self.assertIsInstance(result1["roundtrip_rtt_ms_p95"], (int, float))
            self.assertIsInstance(result2["roundtrip_rtt_ms_p95"], (int, float))
        finally:
            for process in (pc1, pc2):
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=3)
                if process.stdout:
                    process.stdout.close()
                if process.stderr:
                    process.stderr.close()
            if credential_target:
                from src.autosync_credentials import delete_secret
                delete_secret(credential_target)


if __name__ == "__main__":
    unittest.main(verbosity=2)
