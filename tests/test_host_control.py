from pathlib import Path
from tempfile import TemporaryDirectory
import json
import threading
import time
import urllib.error
import urllib.request
import unittest

from src.autosync_server import create_server, start_host_control

APP_TOKEN = "t" * 40
CONTROL_TOKEN = "c" * 40


class HostControlTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.data_dir = Path(self.temp.name) / "data"
        self.server = create_server(
            "127.0.0.1", 0, self.data_dir, {"host-test": APP_TOKEN},
            startup_gated=True, module_version="0.1.3",
        )
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"
        self._wait_for_app_listener()
        self.control, self.control_thread = start_host_control(self.server, "127.0.0.1", 0, CONTROL_TOKEN)
        self.control_base = f"http://127.0.0.1:{self.control.server_address[1]}"

    def _wait_for_app_listener(self):
        request = urllib.request.Request(self.base + "/health")
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            try:
                urllib.request.urlopen(request, timeout=0.2)
            except urllib.error.HTTPError as exc:
                code=exc.code
                exc.close()
                if code == 401:
                    return
            except OSError:
                time.sleep(0.02)
        self.fail("main server did not begin accepting requests")

    def _request(self, base, method, path, token):
        req = urllib.request.Request(
            base + path, method=method,
            headers={"Authorization": "Bearer " + token},
        )
        try:
            with urllib.request.urlopen(req, timeout=2) as response:
                return response.status, json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            status=exc.code
            body=json.loads(exc.read().decode("utf-8"))
            exc.close()
            return status, body

    def tearDown(self):
        if self.server_thread.is_alive():
            self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=3)
        if hasattr(self, "control"):
            self.control.shutdown()
            self.control.server_close()
            self.control_thread.join(timeout=3)
        self.temp.cleanup()

    def test_health_poll_does_not_repeat_full_database_integrity_scan(self):
        sql = []
        self.server.store.db.set_trace_callback(sql.append)
        for _ in range(4):
            status, body = self._request(self.control_base, "GET", "/_host/status", CONTROL_TOKEN)
            self.assertEqual(status, 200)
            self.assertEqual(body["integrity"], "ok")
        self.assertFalse(any("quick_check" in statement.lower() for statement in sql))
    def test_local_control_auth_gates_activation_and_drains_single_writer(self):
        status, body = self._request(self.control_base, "GET", "/_host/status", "x" * 40)
        self.assertEqual(status, 401)
        self.assertFalse(self.server.serving)

        status, body = self._request(self.control_base, "GET", "/_host/status", CONTROL_TOKEN)
        self.assertEqual(status, 200)
        self.assertEqual(body["state"], "warming")
        self.assertFalse(body["ready"])
        self.assertEqual(body["module_version"], "0.1.3")
        self.assertEqual(body["integrity"], "ok")
        self.assertEqual(body["schema_version"], 2)

        status, body = self._request(self.base, "GET", "/health", APP_TOKEN)
        self.assertEqual(status, 503)
        self.assertEqual(body["error"], "service_warming")

        status, body = self._request(self.control_base, "POST", "/_host/activate", CONTROL_TOKEN)
        self.assertEqual(status, 200)
        self.assertEqual(body["state"], "serving")
        self.assertTrue(body["ready"])

        status, body = self._request(self.base, "GET", "/health", APP_TOKEN)
        self.assertEqual(status, 200)
        self.assertEqual(body["module_version"], "0.1.3")

        status, body = self._request(self.control_base, "POST", "/_host/drain", CONTROL_TOKEN)
        self.assertEqual(status, 202)
        self.assertEqual(body["state"], "draining")
        self.server_thread.join(timeout=3)
        self.assertFalse(self.server_thread.is_alive(), "drain did not stop the listener")
        self.assertIsNotNone(self.server.instance_lock.handle)
        self.server.server_close()

        replacement = create_server("127.0.0.1", 0, self.data_dir, {"host-test": APP_TOKEN})
        replacement.server_close()


if __name__ == "__main__":
    unittest.main()