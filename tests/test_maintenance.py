import sqlite3
import tempfile
import unittest
from pathlib import Path

from src.autosync_maintenance import backup_database, compact_events, restore_database
from src.autosync_server import ServerLock, Store, create_server


class MaintenanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_backup_includes_committed_wal_data_and_passes_integrity_check(self):
        database = self.root / "source" / "autosync.sqlite3"
        database.parent.mkdir()
        store = Store(database)
        try:
            status, _ = store.write("test-app", "record-1", {"text": "in WAL"}, False, 0, "operation-key-000001")
            self.assertEqual(status, 200)
            backup = backup_database(database, self.root / "backups" / "snapshot.sqlite3")
            restored = Store(backup)
            try:
                record = restored.state("test-app")["records"][0]
                self.assertEqual(record["payload"]["text"], "in WAL")
            finally:
                restored.close()
        finally:
            store.close()

    def test_restore_replaces_database_and_keeps_pre_restore_copy(self):
        source = self.root / "source.sqlite3"
        source_store = Store(source)
        source_store.write("test-app", "record-1", {"text": "snapshot"}, False, 0, "operation-key-000002")
        snapshot = backup_database(source, self.root / "snapshot.sqlite3")
        source_store.close()

        data_dir = self.root / "host-data"
        data_dir.mkdir()
        target = data_dir / "autosync.sqlite3"
        old_store = Store(target)
        old_store.write("test-app", "old", {"text": "before restore"}, False, 0, "operation-key-000003")
        old_store.close()

        restore_database(snapshot, target)
        restored = Store(target)
        try:
            rows = restored.state("test-app")["records"]
            self.assertEqual([(row["record_id"], row["payload"]["text"]) for row in rows], [("record-1", "snapshot")])
        finally:
            restored.close()
        self.assertEqual(len(list(data_dir.glob("autosync.pre-restore-*.sqlite3"))), 1)

    def test_compaction_creates_verified_backup_before_pruning(self):
        database = self.root / "compact" / "autosync.sqlite3"
        database.parent.mkdir(parents=True)
        store = Store(database)
        first = store.write("workboard", "one", {"v": 1}, False, 0, "compact-operation-0001")[1]
        store.write("workboard", "one", {"v": 2}, False, first["revision"], "compact-operation-0002")
        store.close()
        backup = self.root / "backups" / "before-compact.sqlite3"
        result = compact_events(database, backup, "workboard", first["revision"])
        self.assertTrue(backup.is_file())
        db = sqlite3.connect(str(backup))
        try:
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM events").fetchone()[0], 2)
        finally:
            db.close()
        compacted = Store(database)
        try:
            self.assertEqual(result["deleted_events"], 1)
            self.assertEqual(compacted.db.execute("SELECT COUNT(*) FROM events").fetchone()[0], 1)
            reset = compacted.state("workboard", since=0)
            self.assertEqual(reset["error"], "snapshot_required")
            self.assertEqual(reset["records"][0]["payload"]["v"], 2)
        finally:
            compacted.close()

    def test_compaction_refuses_to_run_while_writer_is_live(self):
        database = self.root / "live" / "autosync.sqlite3"
        database.parent.mkdir(parents=True)
        store = Store(database)
        store.write("workboard", "one", {"v": 1}, False, 0, "live-operation-00001")
        store.close()
        guard = ServerLock(database.parent / "server.lock")
        guard.acquire()
        backup = self.root / "backups" / "blocked.sqlite3"
        try:
            with self.assertRaisesRegex(RuntimeError, "another AutoSync writer"):
                compact_events(database, backup, "workboard", 1)
            self.assertFalse(backup.exists())
        finally:
            guard.release()

    def test_restore_refuses_while_server_holds_writer_lock(self):
        source = self.root / "source.sqlite3"
        source_store = Store(source)
        snapshot = backup_database(source, self.root / "snapshot.sqlite3")
        source_store.close()
        server = create_server("127.0.0.1", 0, self.root / "live", {"test-app": "a-token-with-more-than-32-bytes-123"})
        try:
            with self.assertRaisesRegex(RuntimeError, "another AutoSync writer"):
                restore_database(snapshot, self.root / "live" / "autosync.sqlite3")
        finally:
            server.server_close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
