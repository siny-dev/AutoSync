"""Verified SQLite backup and restore helpers for the AutoSync host database."""
from __future__ import annotations

import argparse
from contextlib import closing
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

try:
    from .autosync_server import ServerLock, Store
except ImportError:
    from autosync_server import ServerLock, Store


def _integrity_check(path: Path) -> None:
    with closing(sqlite3.connect(str(path), timeout=10)) as db:
        db.execute("PRAGMA query_only=ON")
        result = db.execute("PRAGMA integrity_check").fetchone()
    if not result or result[0] != "ok":
        raise RuntimeError(f"SQLite integrity check failed for {path}")


def _copy_sqlite(source: Path, destination: Path) -> None:
    with closing(sqlite3.connect(str(source.resolve()), timeout=10)) as src, \
         closing(sqlite3.connect(str(destination), timeout=10)) as dst:
        src.execute("PRAGMA query_only=ON")
        src.backup(dst, pages=256, sleep=0.05)
        result = dst.execute("PRAGMA integrity_check").fetchone()
        if not result or result[0] != "ok":
            raise RuntimeError("SQLite backup integrity check failed")
        dst.commit()


def backup_database(database: Path, destination: Path) -> Path:
    """Create a consistent snapshot, including committed WAL data, then atomically publish it."""
    source, target = Path(database).resolve(), Path(destination).resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    if source == target:
        raise ValueError("backup destination must differ from source database")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    try:
        _copy_sqlite(source, temporary)
        _integrity_check(temporary)
        os.replace(temporary, target)
        _integrity_check(target)
        return target
    finally:
        try: temporary.unlink()
        except FileNotFoundError: pass


def restore_database(backup: Path, database: Path) -> Path:
    """Restore a verified snapshot while holding the same exclusive host lock as the server."""
    source, target = Path(backup).resolve(), Path(database).resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    if source == target:
        raise ValueError("restore source must differ from target database")
    _integrity_check(source)
    target.parent.mkdir(parents=True, exist_ok=True)
    guard = ServerLock(target.parent / "server.lock")
    guard.acquire()  # fails if the AutoSync writer is still running
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.restore")
    quarantined: list[tuple[Path, Path]] = []
    try:
        _copy_sqlite(source, temporary)
        _integrity_check(temporary)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        # Keep the pre-restore database as a safety copy before replacing it.
        if target.exists():
            safety = target.with_name(f"{target.stem}.pre-restore-{stamp}-{uuid.uuid4().hex[:8]}{target.suffix}")
            _copy_sqlite(target, safety)
            _integrity_check(safety)
        # A stale WAL belongs to the old database. Preserve it outside SQLite's sidecar names.
        for suffix in ("-wal", "-shm"):
            sidecar = Path(str(target) + suffix)
            if sidecar.exists():
                saved = sidecar.with_name(sidecar.name + f".pre-restore-{stamp}-{uuid.uuid4().hex[:8]}")
                os.replace(sidecar, saved)
                quarantined.append((saved, sidecar))
        os.replace(temporary, target)
        _integrity_check(target)
        return target
    except Exception:
        # If target replacement did not happen, put any quarantined sidecars back.
        for saved, original in reversed(quarantined):
            if saved.exists() and not original.exists():
                os.replace(saved, original)
        raise
    finally:
        try: temporary.unlink()
        except FileNotFoundError: pass
        guard.release()


def compact_events(database: Path, backup_destination: Path, app: str, through_revision: int) -> dict:
    """Verify a backup under the writer lock, then compact one app's old event rows."""
    source, backup = Path(database).resolve(), Path(backup_destination).resolve()
    if source == backup:
        raise ValueError("backup destination must differ from source database")
    guard = ServerLock(source.parent / "server.lock")
    guard.acquire()  # Do not race the live writer or create a stale backup.
    store = None
    try:
        backup_database(source, backup)
        store = Store(source)
        result = store.compact_events(app, through_revision)
        return {"backup": str(backup), **result}
    finally:
        if store is not None:
            store.close()
        guard.release()


def main() -> int:
    parser = argparse.ArgumentParser(description="Verified AutoSync SQLite backup/restore/maintenance")
    sub = parser.add_subparsers(dest="command", required=True)
    backup = sub.add_parser("backup", help="create and verify a consistent snapshot")
    backup.add_argument("--database", type=Path, required=True)
    backup.add_argument("--destination", type=Path, required=True)
    restore = sub.add_parser("restore", help="restore a snapshot; the writer must be stopped")
    restore.add_argument("--backup", type=Path, required=True)
    restore.add_argument("--database", type=Path, required=True)
    compact = sub.add_parser("compact-events", help="verified backup then compact one app's acknowledged event history")
    compact.add_argument("--database", type=Path, required=True)
    compact.add_argument("--backup-destination", type=Path, required=True)
    compact.add_argument("--app", required=True)
    compact.add_argument("--through-revision", type=int, required=True)
    args = parser.parse_args()
    try:
        if args.command == "backup":
            result = backup_database(args.database, args.destination)
        elif args.command == "restore":
            result = restore_database(args.backup, args.database)
        else:
            result = compact_events(args.database, args.backup_destination, args.app, args.through_revision)
        print(f"{args.command} verified: {result}")
        return 0
    except Exception as exc:
        parser.exit(2, f"{args.command} failed: {type(exc).__name__}: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
