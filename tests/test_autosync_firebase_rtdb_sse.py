from __future__ import annotations

import io
import json
import sys
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACKAGE_ROOT))

from src.autosync_firebase_rtdb import FirebaseRtdbClient, FirebaseRtdbError, _segment


class _RecordingOutbox:
    def __init__(self):
        self.records = []
        self.snapshots = []

    def apply_rtdb_record(self, app_id, record):
        self.records.append((app_id, record))

    def apply_rtdb_snapshot(self, app_id, records):
        self.snapshots.append((app_id, records))


class FirebaseSsePatchTests(unittest.TestCase):
    def setUp(self):
        self.client = object.__new__(FirebaseRtdbClient)
        self.client.app = "autosync-test-spark"
        self.client.outbox = _RecordingOutbox()
        self.client._get_record = Mock()

    def _record(self, record_id="memo-1"):
        return {
            "record_id": record_id,
            "revision": 7,
            "payload": {"text": "complete canonical value", "color": "blue"},
            "deleted": False,
            "updated_utc": "2026-10-30T00:00:00Z",
            "operation_receipts": [],
        }

    def test_patch_at_record_path_fetches_full_record_before_cache_apply(self):
        record = self._record()
        self.client._get_record.return_value = (record, '"etag-7"')
        key = _segment("memo-1")

        handled = self.client._apply_sse_value(
            "patch", f"/{key}", {"payload": {"text": "complete canonical value"}}
        )

        self.assertTrue(handled)
        self.client._get_record.assert_called_once_with("memo-1")
        self.assertEqual(self.client.outbox.records, [(self.client.app, record)])

    def test_nested_patch_fetches_full_record_before_cache_apply(self):
        record = self._record()
        self.client._get_record.return_value = (record, '"etag-7"')
        key = _segment("memo-1")

        handled = self.client._apply_sse_value("patch", f"/{key}/payload", {"text": "changed"})

        self.assertTrue(handled)
        self.client._get_record.assert_called_once_with("memo-1")
        self.assertEqual(self.client.outbox.records, [(self.client.app, record)])

    def test_put_at_record_path_applies_complete_event_without_extra_fetch(self):
        record = self._record()
        key = _segment("memo-1")

        handled = self.client._apply_sse_value("put", f"/{key}", record)

        self.assertTrue(handled)
        self.client._get_record.assert_not_called()
        self.assertEqual(self.client.outbox.records, [(self.client.app, record)])

    def test_root_patch_fetches_complete_record_for_partial_child_update(self):
        record = self._record()
        key = _segment("memo-1")
        self.client._get_record.return_value = (record, '"etag-7"')

        handled = self.client._apply_sse_value("patch", "/", {key: {"payload": {"text": "changed"}}})

        self.assertTrue(handled)
        self.client._get_record.assert_called_once_with("memo-1")
        self.assertEqual(self.client.outbox.records, [(self.client.app, record)])

    def test_sse_parser_preserves_patch_envelope_for_canonical_fetch(self):
        record = self._record()
        key = _segment("memo-1")
        self.client._get_record.return_value = (record, '"etag-7"')
        wire = (
            b"event: patch\n"
            + b"data: " + json.dumps({"path": "/", "data": {key: {"payload": {"text": "partial"}}}}).encode("utf-8")
            + b"\n\n"
        )

        event_name, envelope = self.client._read_sse_event(io.BytesIO(wire), threading.Event())
        handled = self.client._apply_sse_value(event_name, envelope["path"], envelope["data"])

        self.assertTrue(handled)
        self.client._get_record.assert_called_once_with("memo-1")
        self.assertEqual(self.client.outbox.records, [(self.client.app, record)])
    def test_malformed_record_key_is_rejected_before_fetch(self):
        for path in ("/!", "/!/payload"):
            with self.subTest(path=path):
                self.client._get_record.reset_mock()
                with self.assertRaises(FirebaseRtdbError) as raised:
                    self.client._apply_sse_value("patch", path, {"text": "partial"})
                self.assertEqual(str(raised.exception), "invalid_rtdb_record_key")
                self.client._get_record.assert_not_called()


if __name__ == "__main__":
    unittest.main()