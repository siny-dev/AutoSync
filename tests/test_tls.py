import os
import socket
import ssl
import tempfile
import threading
import time
import unittest
from unittest import mock
from pathlib import Path

from src.autosync_client import AutoSyncClient
from src.autosync_server import create_server


CERT = os.environ.get("AUTOSYNC_TEST_TLS_CERT")
KEY = os.environ.get("AUTOSYNC_TEST_TLS_KEY") or CERT


@unittest.skipUnless(CERT, "test-only TLS certificate not supplied")
class TlsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.token = "tls-test-token-with-more-than-32-bytes-123"
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(CERT, KEY if KEY != CERT else None)
        self.server = create_server("127.0.0.1", 0, Path(self.temp.name) / "server", {"test-app": self.token}, tls_context=context)
        self.port = self.server.server_address[1]
        self.base = f"https://localhost:{self.port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        if self.thread.is_alive():
            self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.temp.cleanup()

    def test_hostname_mismatch_blocks_sync_without_sending_record(self):
        root = Path(self.temp.name)
        wrong_host = "autosync-hostname-mismatch.invalid"
        original_getaddrinfo = socket.getaddrinfo

        def resolve_test_host(host, port, *args, **kwargs):
            if host == wrong_host:
                host = "127.0.0.1"
            return original_getaddrinfo(host, port, *args, **kwargs)

        client = AutoSyncClient(
            f"https://{wrong_host}:{self.port}", self.token, "test-app",
            root / "mismatch-client", ca_file=CERT,
        )
        try:
            client.queue_write("mismatch-record", {"text": "must not be sent"})
            with mock.patch("socket.getaddrinfo", side_effect=resolve_test_host):
                result = client.sync_once()
            self.assertEqual(result.get("status"), "blocked")
            self.assertEqual(result.get("detail"), "tls_verification_or_protocol_error")
            self.assertEqual(self.server.store.state("test-app")["records"], [])
            self.assertEqual(client.outbox.pending_count("test-app"), 1)
        finally:
            client.close()
    def test_https_write_and_wss_hint_then_durable_pull(self):
        root = Path(self.temp.name)
        receiver = AutoSyncClient(self.base, self.token, "test-app", root / "receiver", ca_file=CERT)
        sender = AutoSyncClient(self.base, self.token, "test-app", root / "sender", ca_file=CERT)
        stop = threading.Event()
        watcher = threading.Thread(target=receiver.watch, args=(stop,), daemon=True)
        watcher.start()
        try:
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                with self.server.guard:
                    if self.server.clients.get("test-app"):
                        break
                time.sleep(0.02)
            sender.queue_write("tls-record", {"text": "TLS protected"})
            self.assertEqual(sender.sync_once()["status"], "synced")
            deadline = time.monotonic() + 4
            while time.monotonic() < deadline:
                row = receiver.outbox.cached_record("test-app", "tls-record")
                if row and row["payload"].get("text") == "TLS protected":
                    break
                time.sleep(0.05)
            row = receiver.outbox.cached_record("test-app", "tls-record")
            self.assertEqual(row["payload"]["text"], "TLS protected")
        finally:
            stop.set()
            watcher.join(timeout=3)
            receiver.close()
            sender.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
