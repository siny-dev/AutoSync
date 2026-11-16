"""Small app adapter demonstrating durable local journaling with the AutoSync module."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
import uuid
from pathlib import Path

from src import AutoSyncClient
from src.autosync_protocol import RECORD_RE, canon


APP_ID = "autosync-memo-demo"


class MemoDemo:
    """A local memo app whose own DB journal closes the save-to-sync crash window."""

    def __init__(self, base_url: str, token: str, data_dir: Path, ca_file: Path | None = None):
        self.root = Path(data_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(self.root / "memo.sqlite3"), timeout=5, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS notes(
            note_id TEXT PRIMARY KEY, payload TEXT NOT NULL, deleted INTEGER NOT NULL,
            server_revision INTEGER NOT NULL, updated REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sync_journal(
            event_id TEXT PRIMARY KEY, note_id TEXT NOT NULL, expected_revision INTEGER NOT NULL,
            payload TEXT NOT NULL, deleted INTEGER NOT NULL, state TEXT NOT NULL,
            parent_operation TEXT, created REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS sync_journal_note_state ON sync_journal(note_id,state,created);
        """)
        self.client = AutoSyncClient(base_url, token, APP_ID, self.root / "autosync-client", ca_file=ca_file)

    def close(self):
        self.client.close()
        self.db.close()

    def save_note(self, note_id: str, text: str) -> str:
        if not isinstance(note_id, str) or not RECORD_RE.fullmatch(note_id):
            raise ValueError("note_id must use letters, numbers, dot, underscore, colon, or hyphen")
        if not isinstance(text, str):
            raise ValueError("note text must be a string")
        row = self.db.execute("SELECT server_revision FROM notes WHERE note_id=?", (note_id,)).fetchone()
        expected = int(row["server_revision"]) if row else self.client.record_revision(note_id)
        payload = {"text": text}
        body = canon(payload)
        event_id = str(uuid.uuid4())
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute("""INSERT INTO notes(note_id,payload,deleted,server_revision,updated)
                VALUES(?,?,0,?,?) ON CONFLICT(note_id) DO UPDATE SET payload=excluded.payload,
                deleted=0,updated=excluded.updated""", (note_id, body, expected, time.time()))
            self.db.execute("""INSERT INTO sync_journal(event_id,note_id,expected_revision,payload,deleted,state,parent_operation,created)
                VALUES(?,?,?,?,0,'local',NULL,?)""", (event_id, note_id, expected, body, time.time()))
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise
        return event_id

    def delete_note(self, note_id: str) -> str:
        row = self.db.execute("SELECT payload,server_revision FROM notes WHERE note_id=?", (note_id,)).fetchone()
        if row is None:
            raise KeyError(note_id)
        return self._save_tombstone(note_id, json.loads(row["payload"]), int(row["server_revision"]))

    def _save_tombstone(self, note_id, payload, expected):
        event_id = str(uuid.uuid4())
        body = canon(payload)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute("UPDATE notes SET deleted=1,updated=? WHERE note_id=?", (time.time(), note_id))
            self.db.execute("""INSERT INTO sync_journal(event_id,note_id,expected_revision,payload,deleted,state,parent_operation,created)
                VALUES(?,?,?,?,1,'local',NULL,?)""", (event_id, note_id, expected, body, time.time()))
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise
        return event_id

    def replay_journal(self, after_enqueue=None):
        """Idempotently copy durable app journal rows into the module's durable outbox."""
        rows = self.db.execute("SELECT * FROM sync_journal WHERE state IN ('local','enqueued','resolution_pending') ORDER BY created,event_id").fetchall()
        for row in rows:
            payload = json.loads(row["payload"])
            if row["state"] == "resolution_pending":
                self.client.resolve_conflict(row["parent_operation"], payload, int(row["expected_revision"]),
                                              bool(row["deleted"]), row["event_id"])
                self.db.execute("UPDATE sync_journal SET state='resolved' WHERE event_id=?", (row["parent_operation"],))
            else:
                self.client.queue_write(row["note_id"], payload, int(row["expected_revision"]),
                                        bool(row["deleted"]), row["event_id"])
            if after_enqueue:
                after_enqueue(row["event_id"])
            self.db.execute("UPDATE sync_journal SET state='enqueued' WHERE event_id=? AND state IN ('local','enqueued','resolution_pending')",
                            (row["event_id"],))

    def _update_journal_status(self):
        rows = self.db.execute("SELECT event_id,note_id,state FROM sync_journal WHERE state IN ('enqueued','conflict')").fetchall()
        for row in rows:
            status = self.client.operation_status(row["event_id"])["state"]
            if status in ("synced", "conflict"):
                self.db.execute("UPDATE sync_journal SET state=? WHERE event_id=?", (status, row["event_id"]))
            if status == "synced":
                revision = self.client.record_revision(row["note_id"])
                self.db.execute("UPDATE notes SET server_revision=? WHERE note_id=?", (revision, row["note_id"]))

    def _apply_remote_cache(self):
        for record in self.client.cached_records():
            waiting = self.db.execute("SELECT 1 FROM sync_journal WHERE note_id=? AND state IN ('local','enqueued','conflict','resolution_pending') LIMIT 1",
                                      (record["record_id"],)).fetchone()
            if waiting or self.client.has_pending_record(record["record_id"]):
                continue
            local = self.db.execute("SELECT server_revision FROM notes WHERE note_id=?", (record["record_id"],)).fetchone()
            if local is None or int(record["revision"]) >= int(local["server_revision"]):
                self.db.execute("""INSERT INTO notes(note_id,payload,deleted,server_revision,updated)
                    VALUES(?,?,?,?,?) ON CONFLICT(note_id) DO UPDATE SET payload=excluded.payload,
                    deleted=excluded.deleted,server_revision=excluded.server_revision,updated=excluded.updated""",
                    (record["record_id"], canon(record["payload"]), int(record["deleted"]),
                     int(record["revision"]), time.time()))

    def sync_once(self, after_enqueue=None):
        self.replay_journal(after_enqueue)
        result = self.client.sync_once()
        self._update_journal_status()
        self._apply_remote_cache()
        return result

    def resolve_note(self, conflicted_operation: str, merged_text: str) -> str:
        conflict = next((item for item in self.client.conflicts() if item["operation_id"] == conflicted_operation), None)
        if conflict is None or conflict["error"] != "revision_conflict":
            raise ValueError("primary conflict was not found")
        payload = {"text": merged_text}
        event_id = str(uuid.uuid4())
        body = canon(payload)
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute("""INSERT INTO sync_journal(event_id,note_id,expected_revision,payload,deleted,state,parent_operation,created)
                VALUES(?,?,?,?,0,'resolution_pending',?,?)""",
                (event_id, conflict["record_id"], conflict["current_revision"], body, conflicted_operation, time.time()))
            self.db.execute("UPDATE notes SET payload=?,deleted=0,updated=? WHERE note_id=?",
                            (body, time.time(), conflict["record_id"]))
            self.db.execute("COMMIT")
        except Exception:
            self.db.execute("ROLLBACK")
            raise
        self.replay_journal()
        return event_id

    def list_notes(self, include_deleted=False):
        if include_deleted:
            rows = self.db.execute("SELECT * FROM notes ORDER BY note_id").fetchall()
        else:
            rows = self.db.execute("SELECT * FROM notes WHERE deleted=0 ORDER BY note_id").fetchall()
        return [{"note_id": row["note_id"], **json.loads(row["payload"]), "deleted": bool(row["deleted"]),
                 "server_revision": row["server_revision"]} for row in rows]


def main():
    parser = argparse.ArgumentParser(description="AutoSync sample memo app")
    parser.add_argument("--server", required=True, help="AutoSync HTTPS URL (loopback HTTP is accepted for local demo)")
    parser.add_argument("--data-dir", type=Path, default=Path.home() / "AppData" / "Local" / "Siny" / "AutoSyncMemoDemo")
    parser.add_argument("--ca-file", type=Path)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("sync")
    put = sub.add_parser("put"); put.add_argument("note_id"); put.add_argument("text")
    delete = sub.add_parser("delete"); delete.add_argument("note_id")
    listing = sub.add_parser("list"); listing.add_argument("--include-deleted", action="store_true")
    resolve = sub.add_parser("resolve"); resolve.add_argument("operation_id"); resolve.add_argument("text")
    args = parser.parse_args()
    token = os.environ.get("AUTOSYNC_DEMO_TOKEN")
    if not token:
        import getpass
        token = getpass.getpass("AutoSync app token: ")
    app = MemoDemo(args.server, token, args.data_dir, args.ca_file)
    try:
        if args.command == "put":
            operation = app.save_note(args.note_id, args.text)
            result = app.sync_once()
            print(json.dumps({"operation_id": operation, "sync": result}, ensure_ascii=False))
        elif args.command == "delete":
            operation = app.delete_note(args.note_id)
            result = app.sync_once()
            print(json.dumps({"operation_id": operation, "sync": result}, ensure_ascii=False))
        elif args.command == "resolve":
            operation = app.resolve_note(args.operation_id, args.text)
            print(json.dumps({"operation_id": operation, "sync": app.sync_once()}, ensure_ascii=False))
        elif args.command == "list":
            app.sync_once()
            print(json.dumps(app.list_notes(args.include_deleted), ensure_ascii=False, indent=2))
        else:
            print(json.dumps(app.sync_once(), ensure_ascii=False))
    finally:
        app.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
