from __future__ import annotations

import copy
import contextlib
import importlib
import io
import json
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from src.autosync_firebase_auth import FirebaseAuthenticationError

TOOLS = Path(__file__).resolve().parents[1] / "tools"
if not (TOOLS / "autosync_firebase_two_pc_acceptance.py").is_file():
    TOOLS = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS))
probe = importlib.import_module("autosync_firebase_two_pc_acceptance")


class _SharedFakeFirebase:
    lock = threading.RLock()
    condition = threading.Condition(lock)
    record = None


class _FakeAuth:
    def get_id_token(self):
        return 'local-test-id-token'


class _FakeClient:
    def __init__(self, _url, _token_provider, app_id, _local_root, **_kwargs):
        self.app = app_id
        self.cache = None
        self._stop = threading.Event()
        self._watch_thread = None
        self._ops = {}

    def sync_once(self):
        with _SharedFakeFirebase.lock:
            self.cache = copy.deepcopy(_SharedFakeFirebase.record)
        return {"status": "synced"}

    def start(self):
        def watch():
            while not self._stop.is_set():
                with _SharedFakeFirebase.condition:
                    _SharedFakeFirebase.condition.wait(timeout=0.02)
                    self.cache = copy.deepcopy(_SharedFakeFirebase.record)
        self._watch_thread = threading.Thread(target=watch, daemon=True)
        self._watch_thread.start()
        return self

    def cached_record(self, _record_id):
        return copy.deepcopy(self.cache)

    def queue_write(self, record_id, payload, expected_revision=None, deleted=False):
        with _SharedFakeFirebase.condition:
            current = _SharedFakeFirebase.record
            revision = int(current.get("revision", 0)) if current else 0
            if expected_revision != revision:
                raise RuntimeError("fake_revision_conflict")
            next_record = {
                "record_id": record_id,
                "revision": revision + 1,
                "payload": copy.deepcopy(payload),
                "deleted": deleted,
                "updated_utc": payload["updated_utc"],
            }
            _SharedFakeFirebase.record = copy.deepcopy(next_record)
            self.cache = copy.deepcopy(next_record)
            _SharedFakeFirebase.condition.notify_all()
        operation_id = f"fake-operation-{revision + 1:016d}"
        self._ops[operation_id] = {"state": "synced"}
        return operation_id

    def operation_status(self, operation_id):
        return dict(self._ops[operation_id])

    def close(self):
        self._stop.set()
        with _SharedFakeFirebase.condition:
            _SharedFakeFirebase.condition.notify_all()
        if self._watch_thread:
            self._watch_thread.join(timeout=1)


class PairedPhysicalPcProtocolTests(unittest.TestCase):
    def setUp(self):
        with _SharedFakeFirebase.lock:
            _SharedFakeFirebase.record = None
        self.original_client = probe.FirebaseRtdbClient
        self.original_auth = probe.load_auth_session
        self.original_prompt = probe._prompt_value
        self.original_host_id = probe.local_host_id
        self.host_context = threading.local()
        probe.FirebaseRtdbClient = _FakeClient
        probe.load_auth_session = lambda *_args, **_kwargs: _FakeAuth()
        probe._prompt_value = lambda _label, current="": current
        probe.local_host_id = lambda: getattr(self.host_context, "value", "test-host")

    def tearDown(self):
        probe.FirebaseRtdbClient = self.original_client
        probe.load_auth_session = self.original_auth
        probe._prompt_value = self.original_prompt
        probe.local_host_id = self.original_host_id

    def _run_on_host(self, args, host_id):
        self.host_context.value = host_id
        return probe.run(args)

    def test_bidirectional_message_round_trip_and_remote_tombstone(self):
        base = {
            "run_id": "local-pair-0001",
            "database_url": "https://autosync-6766d-default-rtdb.asia-southeast1.firebasedatabase.app",
            "api_key": "local-test-api-key",
            "email": "local-test@example.invalid",
            "timeout_seconds": 3,
        }
        initiator_args = SimpleNamespace(
            **base, role="initiator", device_label="5600G",
            credential_target="AutoSync/autosync-test-spark/account-1",
        )
        peer_args = SimpleNamespace(
            **base, role="peer", device_label="H255",
            credential_target="AutoSync/autosync-test-spark/account-2",
        )
        with ThreadPoolExecutor(max_workers=2) as pool:
            initiator_future = pool.submit(self._run_on_host, initiator_args, "test-5600")
            peer_future = pool.submit(self._run_on_host, peer_args, "test-h255")
            initiator_result = initiator_future.result(timeout=5)
            peer_result = peer_future.result(timeout=5)
        self.assertEqual(initiator_result["status"], "passed", initiator_result)
        self.assertEqual(peer_result["status"], "passed", peer_result)
        self.assertEqual(initiator_result["peer_device"], "H255")
        self.assertEqual(peer_result["initiator_device"], "5600G")
        self.assertEqual(peer_result["checks"]["remote_tombstone_observed"], "passed")
        self.assertTrue(_SharedFakeFirebase.record["deleted"])

    def test_pair_rejects_two_roles_on_same_host(self):
        base = {
            "run_id": "local-pair-0003",
            "database_url": "https://autosync-6766d-default-rtdb.asia-southeast1.firebasedatabase.app",
            "api_key": "local-test-api-key",
            "email": "local-test@example.invalid",
            "timeout_seconds": 3,
        }
        initiator_args = SimpleNamespace(
            **base, role="initiator", device_label="5600G",
            credential_target="AutoSync/autosync-test-spark/account-1",
        )
        peer_args = SimpleNamespace(
            **base, role="peer", device_label="H255",
            credential_target="AutoSync/autosync-test-spark/account-2",
        )
        with ThreadPoolExecutor(max_workers=2) as pool:
            initiator_future = pool.submit(self._run_on_host, initiator_args, "same-host")
            peer_future = pool.submit(self._run_on_host, peer_args, "same-host")
            initiator_result = initiator_future.result(timeout=5)
            peer_result = peer_future.result(timeout=5)
        self.assertNotEqual(initiator_result["status"], "passed")
        self.assertNotEqual(peer_result["status"], "passed")
        self.assertEqual(peer_result["errors"][0]["code"], "paired_roles_must_run_on_different_hosts")
        self.assertTrue(_SharedFakeFirebase.record["deleted"])

    def test_auth_failure_is_reported_without_traceback_or_rtdb_client(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            report = Path(temp_dir) / "peer-report.json"
            stdout = io.StringIO()
            with mock.patch.object(
                probe, "load_auth_session",
                side_effect=FirebaseAuthenticationError("firebase_auth_invalid_refresh_token"),
            ), mock.patch.object(probe, "FirebaseRtdbClient") as client_factory:
                with contextlib.redirect_stdout(stdout):
                    code = probe.main([
                        "--role", "peer", "--run-id", "local-pair-auth1",
                        "--database-url",
                        "https://autosync-6766d-default-rtdb.asia-southeast1.firebasedatabase.app",
                        "--api-key", "local-test-api-key",
                        "--email", "local-test@example.invalid",
                        "--execute", "--report", str(report),
                    ])
            self.assertEqual(code, 1)
            client_factory.assert_not_called()
            result = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["cleanup"], "not_started")
            self.assertEqual(result["errors"], [{
                "type": "FirebaseAuthenticationError",
                "code": "firebase_auth_invalid_refresh_token",
            }])
            self.assertNotIn("local-test-api-key", report.read_text(encoding="utf-8"))
            self.assertNotIn("local-test@example.invalid", report.read_text(encoding="utf-8"))
            self.assertNotIn("Traceback", stdout.getvalue())
            self.assertEqual(json.loads(stdout.getvalue())["errors"], result["errors"])

    def test_cli_writes_optional_report_after_success(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            report = Path(temp_dir) / "peer-report.json"
            with mock.patch.object(probe, "run", return_value={"status": "passed", "role": "peer"}):
                with contextlib.redirect_stdout(io.StringIO()):
                    code = probe.main([
                        "--role", "peer", "--run-id", "local-pair-0002",
                        "--execute", "--report", str(report),
                    ])
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(report.read_text(encoding="utf-8")), {
                "status": "passed", "role": "peer",
            })


if __name__ == "__main__":
    unittest.main()

