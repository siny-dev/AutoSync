import tempfile
import threading
import unittest
from pathlib import Path

from examples.memo_demo import APP_ID, MemoDemo
from src.autosync_server import create_server


TOKEN = "memo-demo-token-with-more-than-32-bytes-123"


class MemoDemoTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.server = create_server("127.0.0.1", 0, self.root / "server", {APP_ID: TOKEN})
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        if self.thread.is_alive():
            self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.temp.cleanup()

    def open_app(self, name):
        return MemoDemo(self.base, TOKEN, self.root / name)

    def test_local_save_journal_replays_after_app_restart(self):
        app = self.open_app("pc1")
        event = app.save_note("memo-1", "saved before queue")
        app.close()  # crash boundary: app DB committed; nothing reached AutoSync outbox

        recovered = self.open_app("pc1")
        try:
            self.assertEqual(recovered.sync_once()["status"], "synced")
            self.assertEqual(recovered.client.operation_status(event)["state"], "synced")
            self.assertEqual(recovered.list_notes()[0]["text"], "saved before queue")
            self.assertEqual(self.server.store.state(APP_ID)["records"][0]["payload"]["text"], "saved before queue")
        finally:
            recovered.close()

    def test_crash_after_outbox_commit_replays_same_id_without_duplicate_revision(self):
        app = self.open_app("pc1")
        try:
            event = app.save_note("memo-1", "enqueue commit then crash")
            with self.assertRaisesRegex(RuntimeError, "simulated app crash"):
                app.replay_journal(after_enqueue=lambda _: (_ for _ in ()).throw(RuntimeError("simulated app crash")))
        finally:
            app.close()

        recovered = self.open_app("pc1")
        try:
            self.assertEqual(recovered.sync_once()["status"], "synced")
            self.assertEqual(recovered.client.operation_status(event)["state"], "synced")
            count = self.server.store.db.execute("SELECT COUNT(*) FROM events WHERE app=? AND id=?", (APP_ID, "memo-1")).fetchone()[0]
            self.assertEqual(count, 1)
        finally:
            recovered.close()

    def test_two_app_instances_receive_remote_updates_and_resolve_conflict(self):
        pc1 = self.open_app("pc1")
        pc2 = self.open_app("pc2")
        try:
            self.assertEqual(pc1.sync_once()["status"], "synced")
            self.assertEqual(pc2.sync_once()["status"], "synced")
            pc1.save_note("memo-1", "first edit")
            self.assertEqual(pc1.sync_once()["status"], "synced")
            self.assertEqual(pc2.sync_once()["status"], "synced")
            self.assertEqual(pc2.list_notes()[0]["text"], "first edit")

            pc2.save_note("memo-1", "offline edit from pc2")
            pc1.save_note("memo-1", "newer edit from pc1")
            self.assertEqual(pc1.sync_once()["status"], "synced")
            self.assertEqual(pc2.sync_once()["status"], "conflict")
            conflict = next(item for item in pc2.client.conflicts() if item["error"] == "revision_conflict")
            pc2.resolve_note(conflict["operation_id"], "user merged both edits")
            self.assertEqual(pc2.sync_once()["status"], "synced")
            self.assertEqual(pc1.sync_once()["status"], "synced")
            self.assertEqual(pc1.list_notes()[0]["text"], "user merged both edits")
            self.assertEqual(self.server.store.state(APP_ID)["records"][0]["payload"]["text"], "user merged both edits")
        finally:
            pc1.close()
            pc2.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
