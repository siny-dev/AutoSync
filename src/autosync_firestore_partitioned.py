"""Firestore partitioned change-feed backend with per-record revisions.

This backend is wired into the HTTP/WebSocket server and is the Cloud Run
staging default. It has passed local contract and transport tests; live Firestore,
Cloud Run and multi-device staging acceptance remain outstanding.
"""
from __future__ import annotations

import hashlib
import json
import random
import re
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

if __package__:
    from .autosync_protocol import APP_RE, OP_RE, RECORD_RE, canon, check_id, now_utc
else:  # Support direct module execution.
    from autosync_protocol import APP_RE, OP_RE, RECORD_RE, canon, check_id, now_utc

MAX_FIRESTORE_BODY = 400 * 1024
DEFAULT_BUCKET_COUNT = 64
DEFAULT_EVENT_RETENTION_DAYS = 90
DEFAULT_TRANSACTION_ATTEMPTS = 10
DEFAULT_WRITE_RETRY_ATTEMPTS = 3
WRITE_RETRY_INITIAL_SECONDS = 0.05
WRITE_RETRY_MAX_SECONDS = 0.5
WATCHDOG_POLL_SECONDS = 2.0
WATCHDOG_RETRY_INITIAL_SECONDS = 1.0
WATCHDOG_RETRY_MAX_SECONDS = 30.0
DELTA_PAGE_SIZE = 500


def _retryable_transaction_error(exc):
    """Return true only for transient Firestore commit failures.

    The Firestore SDK may exhaust its transaction-level retries and raise a
    ValueError such as ``Failed to commit transaction in 10 attempts.``. A
    bounded outer retry is safe because every write carries a stable operation
    id and the receipt makes an uncertain prior commit idempotent.
    """
    if isinstance(exc, ValueError):
        return bool(re.fullmatch(r"Failed to commit transaction in \d+ attempts\.?", str(exc)))

    retryable_names = {
        "Aborted", "DeadlineExceeded", "InternalServerError",
        "ResourceExhausted", "ServiceUnavailable", "Unknown",
    }
    if type(exc).__name__ in retryable_names:
        return True

    code = getattr(exc, "code", None)
    if callable(code):
        try:
            code = code()
        except Exception:
            code = None
    name = getattr(code, "name", None)
    if not name and code is not None:
        name = str(code).rsplit(".", 1)[-1].upper()
    return name in {"ABORTED", "DEADLINE_EXCEEDED", "INTERNAL", "RESOURCE_EXHAUSTED",
                    "UNAVAILABLE", "UNKNOWN"}


class FirestorePartitionedStore:
    """Prototype storage API; unlike v1, it has no app-wide write revision.

    Each successful operation allocates a sequence in one hash-selected feed
    bucket. Per-record ``revision`` remains the only conflict authority. A
    receipt protects idempotency for the configured retention window even if
    cleanup removes the event payload. After that window, an old replay is
    still checked against the current record revision and returns a conflict
    rather than applying a stale revision. ``expires_at`` is metadata only
    until a Firestore TTL policy or cleanup job is configured.
    """

    def __init__(self, project_id: str | None = None,
                 collection_prefix: str = "autosync_v2", bucket_count: int = DEFAULT_BUCKET_COUNT,
                 client=None, transactional=None, event_retention_days: int = DEFAULT_EVENT_RETENTION_DAYS,
                 transaction_attempts: int = DEFAULT_TRANSACTION_ATTEMPTS,
                 write_retry_attempts: int = DEFAULT_WRITE_RETRY_ATTEMPTS):
        if (not isinstance(collection_prefix, str) or
                not collection_prefix.replace("_", "").isalnum()):
            raise ValueError("Firestore collection prefix must contain only letters, digits, and underscores")
        if not isinstance(bucket_count, int) or isinstance(bucket_count, bool) or not 2 <= bucket_count <= 1024:
            raise ValueError("bucket_count must be an integer from 2 through 1024")
        if not isinstance(event_retention_days, int) or isinstance(event_retention_days, bool) or not 1 <= event_retention_days <= 3650:
            raise ValueError("event_retention_days must be an integer from 1 through 3650")
        if not isinstance(transaction_attempts, int) or isinstance(transaction_attempts, bool) or not 1 <= transaction_attempts <= 100:
            raise ValueError("transaction_attempts must be an integer from 1 through 100")
        if not isinstance(write_retry_attempts, int) or isinstance(write_retry_attempts, bool) or not 1 <= write_retry_attempts <= 10:
            raise ValueError("write_retry_attempts must be an integer from 1 through 10")
        if client is None or transactional is None:
            try:
                from google.cloud import firestore
            except ImportError as exc:
                raise RuntimeError("Cloud Firestore backend requires google-cloud-firestore") from exc
            if client is None:
                client = firestore.Client(project=project_id)
            if transactional is None:
                transactional = firestore.transactional
        self.client = client
        self.transactional = transactional
        self.collection_prefix = collection_prefix
        self.bucket_count = bucket_count
        self.event_retention = timedelta(days=event_retention_days)
        self.transaction_attempts = transaction_attempts
        self.write_retry_attempts = write_retry_attempts
        self._lock_guard = threading.Lock()
        self._bucket_locks: dict[tuple[str, int], threading.Lock] = {}
        self._watch_lock = threading.RLock()
        self._watchers: dict[str, dict] = {}
        self._watchdog_stop = threading.Event()
        self._watchdog_thread = None
        self._watchdog_lock = threading.Lock()

    @property
    def api_version(self):
        return "2.0"

    def _app(self, app):
        check_id(app, APP_RE)
        return self.client.collection(self.collection_prefix).document(app)

    def _record(self, app_ref, rid):
        return app_ref.collection("records").document(rid)

    def _bucket(self, app_ref, bucket):
        return app_ref.collection("feed_buckets").document(f"{bucket:04d}")

    @staticmethod
    def _key(op):
        return hashlib.sha256(op.encode("utf-8")).hexdigest()

    def _receipt(self, app_ref, op):
        return app_ref.collection("receipts").document(self._key(op))

    def _event(self, app_ref, op):
        return app_ref.collection("operations").document(self._key(op))

    def _conflict(self, app_ref, conflict_id):
        return app_ref.collection("conflicts").document(conflict_id)

    def _bucket_for(self, op):
        digest = hashlib.sha256(op.encode("utf-8")).digest()
        return int.from_bytes(digest[:8], "big") % self.bucket_count

    def _lock_for(self, app, bucket):
        with self._lock_guard:
            return self._bucket_locks.setdefault((app, bucket), threading.Lock())

    @staticmethod
    def _event_from_snapshot(snapshot):
        item = snapshot.to_dict()
        return {
            "operation_id_hash": item["operation_id_hash"],
            "record_id": item["record_id"],
            "revision": int(item["revision"]),
            "payload": json.loads(item["payload_json"]),
            "deleted": bool(item["deleted"]),
            "updated_utc": item["updated_utc"],
            "bucket_id": int(item["bucket_id"]),
            "bucket_seq": int(item["bucket_seq"]),
        }

    def write(self, app, rid, payload, deleted, expected, op):
        check_id(app, APP_RE)
        check_id(rid, RECORD_RE)
        check_id(op, OP_RE)
        if (not isinstance(payload, dict) or not isinstance(deleted, bool) or
                not isinstance(expected, int) or isinstance(expected, bool) or expected < 0):
            raise ValueError("invalid write")
        body = canon(payload)
        if len(body.encode("utf-8")) > MAX_FIRESTORE_BODY:
            raise ValueError("payload too large for Firestore document limits")

        digest = hashlib.sha256(canon([app, rid, payload, deleted, expected]).encode("utf-8")).hexdigest()
        app_ref = self._app(app)
        receipt_ref = self._receipt(app_ref, op)
        record_ref = self._record(app_ref, rid)
        bucket_id = self._bucket_for(op)
        bucket_ref = self._bucket(app_ref, bucket_id)
        event_ref = self._event(app_ref, op)
        conflict_id = str(uuid.uuid4())

        def apply(transaction):
            receipt_snapshot = receipt_ref.get(transaction=transaction)
            record_snapshot = record_ref.get(transaction=transaction)
            bucket_snapshot = bucket_ref.get(transaction=transaction)
            if receipt_snapshot.exists:
                old = receipt_snapshot.to_dict()
                if old.get("digest") != digest:
                    return 409, {"error": "idempotency_key_reused"}
                response = json.loads(old["response_json"])
                response["duplicate"] = True
                if int(old["status"]) == 409:
                    conflict_ref = self._conflict(app_ref, response["conflict_id"])
                    conflict_snapshot = conflict_ref.get(transaction=transaction)
                    if not conflict_snapshot.exists:
                        return 409, {"error": "conflict_details_expired", "conflict_id": response["conflict_id"]}
                    response = json.loads(conflict_snapshot.to_dict()["response_json"])
                    response["duplicate"] = True
                return int(old["status"]), response

            current = record_snapshot.to_dict() if record_snapshot.exists else None
            actual = int(current["revision"]) if current else 0
            if actual != expected:
                current_view = ({"revision": actual, "payload": json.loads(current["payload_json"]),
                                 "deleted": bool(current["deleted"]), "updated_utc": current["updated_utc"]}
                                if current else None)
                response = {"error": "revision_conflict", "conflict_id": conflict_id,
                            "app_id": app, "record_id": rid, "expected_revision": expected,
                            "current": current_view, "incoming": {"payload": payload, "deleted": deleted}}
                transaction.set(self._conflict(app_ref, conflict_id), {
                    "response_json": canon(response), "created_utc": now_utc(),
                    "expires_at": datetime.now(timezone.utc) + self.event_retention,
                })
                transaction.set(receipt_ref, {"digest": digest, "status": 409,
                                              "response_json": canon({"conflict_id": conflict_id}),
                                              "created_utc": now_utc(),
                                              "expires_at": datetime.now(timezone.utc) + self.event_retention})
                return 409, response

            sequence = int((bucket_snapshot.to_dict() if bucket_snapshot.exists else {}).get("last_seq", 0)) + 1
            revision = actual + 1
            updated = now_utc()
            response = {"ok": True, "app_id": app, "record_id": rid, "revision": revision,
                        "bucket_id": bucket_id, "bucket_seq": sequence,
                        "deleted": deleted, "updated_utc": updated, "duplicate": False}
            transaction.set(record_ref, {"revision": revision, "payload_json": body,
                                         "deleted": deleted, "updated_utc": updated})
            # Receipt, operation payload, and conflict details share the same
            # retention window. After TTL removes a receipt, record revisions
            # still reject stale offline replays without duplicating a write.
            # TTL is asynchronous and expires_at is metadata until configured.
            transaction.set(receipt_ref, {"digest": digest, "status": 200,
                                          "response_json": canon(response), "created_utc": updated,
                                          "expires_at": datetime.now(timezone.utc) + self.event_retention})
            transaction.set(event_ref, {
                "operation_id_hash": self._key(op), "record_id": rid, "revision": revision,
                "payload_json": body, "deleted": deleted, "updated_utc": updated,
                "bucket_id": bucket_id, "bucket_seq": sequence,
                "expires_at": datetime.now(timezone.utc) + self.event_retention,
            })
            transaction.set(bucket_ref, {"last_seq": sequence, "updated_utc": updated,
                                         "schema_version": 2})
            return 200, response

        # In-process locking reduces needless collisions. The SDK first retries
        # transaction conflicts; this bounded operation-level retry handles a
        # transient/exhausted commit while preserving the same idempotency key.
        with self._lock_for(app, bucket_id):
            for attempt in range(self.write_retry_attempts):
                transaction = self.client.transaction(max_attempts=self.transaction_attempts)
                try:
                    return self.transactional(apply)(transaction)
                except Exception as exc:
                    if attempt + 1 >= self.write_retry_attempts or not _retryable_transaction_error(exc):
                        raise
                    ceiling = min(WRITE_RETRY_MAX_SECONDS,
                                  WRITE_RETRY_INITIAL_SECONDS * (2 ** attempt))
                    time.sleep(random.uniform(WRITE_RETRY_INITIAL_SECONDS, ceiling))

    def _read_heads(self, app_ref):
        heads = []
        for bucket in range(self.bucket_count):
            snapshot = self._bucket(app_ref, bucket).get(timeout=10)
            value = snapshot.to_dict() if snapshot.exists else {}
            heads.append(int(value.get("last_seq", 0)))
        return heads

    def current_cursor(self, app):
        """Return the current vector cursor without reading record contents."""
        check_id(app, APP_RE)
        return {"schema_version": 2, "bucket_count": self.bucket_count,
                "sequences": self._read_heads(self._app(app))}

    def cached_cursor(self, app):
        """Return the listener's last observed heads, falling back to Firestore."""
        check_id(app, APP_RE)
        with self._watch_lock:
            entry = self._watchers.get(app)
            if entry and entry["initialized"]:
                return {"schema_version": 2, "bucket_count": self.bucket_count,
                        "sequences": list(entry["heads"])}
        return self.current_cursor(app)

    def _heads_for_delta(self, app, app_ref):
        """Use the active listener watermark for reconnect pulls when possible.

        Every WebSocket client reconciles after reconnect. Reading all bucket
        heads for each device would multiply Firestore reads even when nothing
        changed. Reuse the already active per-app collection listener watermark
        when initialized. A lagging listener can only return an older cursor,
        never advance a client past an unseen event. The listener will push
        that event and a subsequent durable pull will read it. Without an
        initialized listener, read the heads from Firestore directly.
        """
        with self._watch_lock:
            entry = self._watchers.get(app)
            if entry and entry["initialized"]:
                return list(entry["heads"])
        return self._read_heads(app_ref)

    def health(self):
        return {"backend": "firestore-partitioned", "integrity": "ok",
                "schema_version": 2, "bucket_count": self.bucket_count}

    def _read_records(self, app_ref):
        records = []
        for snapshot in app_ref.collection("records").stream():
            item = snapshot.to_dict()
            records.append({"record_id": snapshot.id, "revision": int(item["revision"]),
                            "payload": json.loads(item["payload_json"]),
                            "deleted": bool(item["deleted"]), "updated_utc": item["updated_utc"]})
        records.sort(key=lambda item: item["record_id"])
        return records

    @staticmethod
    def _cursor_valid(cursor, bucket_count):
        return (isinstance(cursor, dict) and cursor.get("schema_version") == 2 and
                cursor.get("bucket_count") == bucket_count and
                isinstance(cursor.get("sequences"), list) and
                len(cursor["sequences"]) == bucket_count and
                all(isinstance(value, int) and not isinstance(value, bool) and value >= 0
                    for value in cursor["sequences"]))

    def _query_events(self, app_ref, bucket, after, through, limit=None):
        try:
            from google.cloud.firestore_v1.base_query import FieldFilter
            query = app_ref.collection("operations").where(filter=FieldFilter("bucket_id", "==", bucket))
            query = query.where(filter=FieldFilter("bucket_seq", ">", after))
            query = query.where(filter=FieldFilter("bucket_seq", "<=", through))
        except ImportError:
            query = app_ref.collection("operations").where("bucket_id", "==", bucket)
            query = query.where("bucket_seq", ">", after).where("bucket_seq", "<=", through)
        query = query.order_by("bucket_seq")
        if limit is not None:
            query = query.limit(limit)
        return [self._event_from_snapshot(snapshot) for snapshot in query.stream()]

    def _events_between(self, app_ref, start, end, limit=DELTA_PAGE_SIZE):
        events = []
        next_heads = list(end)
        for bucket, (after, through) in enumerate(zip(start, end)):
            if through < after:
                return None, None
            if through == after:
                continue
            found = self._query_events(app_ref, bucket, after, through, limit)
            expected = after + 1
            for event in found:
                event["bucket_count"] = self.bucket_count
                if event["bucket_seq"] != expected:
                    return None, None
                expected += 1
                events.append(event)
            if expected <= through:
                if not found:
                    return None, None
                next_heads[bucket] = found[-1]["bucket_seq"]
        return events, next_heads

    def state(self, app, since=None):
        """Return a vector-cursor delta, or an explicit snapshot/reset response.

        A missing/expired log segment triggers a complete snapshot instead of
        silently advancing a cursor. Snapshot barriers replay writes that race
        the record scan, deduplicated by per-record revision.
        """
        check_id(app, APP_RE)
        app_ref = self._app(app)

        def cursor_obj(values):
            return {"schema_version": 2, "bucket_count": self.bucket_count,
                    "sequences": [int(value) for value in values]}

        if since is not None and not self._cursor_valid(since, self.bucket_count):
            since = None
            reset_required = True
        else:
            reset_required = False

        if since is not None:
            saved = since["sequences"]
            heads = self._heads_for_delta(app, app_ref)
            if any(old > current for old, current in zip(saved, heads)):
                since = None
                reset_required = True
            else:
                events, advanced = self._events_between(app_ref, saved, heads)
                if events is not None:
                    return {"api_version": "2.0", "app_id": app, "cursor": cursor_obj(advanced),
                            "reset_required": False, "events": events}
                since = None
                reset_required = True

        # A bounded barrier loop handles concurrent writes while reading the
        # record collection. Events are replayed over the scan and filtered by
        # per-record revision so no app-wide counter is needed.
        for _attempt in range(3):
            before = self._read_heads(app_ref)
            records = self._read_records(app_ref)
            after = self._read_heads(app_ref)
            events, _advanced = self._events_between(app_ref, before, after)
            if events is None:
                continue
            by_id = {item["record_id"]: item for item in records}
            for event in events:
                old = by_id.get(event["record_id"])
                if old is None or event["revision"] > old["revision"]:
                    by_id[event["record_id"]] = {
                        "record_id": event["record_id"], "revision": event["revision"],
                        "payload": event["payload"], "deleted": event["deleted"],
                        "updated_utc": event["updated_utc"],
                    }
            return {"api_version": "2.0", "app_id": app, "cursor": cursor_obj(after),
                    "reset_required": reset_required, "records": sorted(by_id.values(), key=lambda item: item["record_id"])}
        raise RuntimeError("could not obtain a consistent snapshot because the event log has a gap")

    def _make_snapshot_callback(self, app, app_ref, entry, generation):
        """Build a generation-fenced Firestore callback for one listener."""
        def on_snapshot(documents, _changes, _read_time):
            current = [0] * self.bucket_count
            for document in documents:
                try:
                    bucket = int(document.id)
                    value = document.to_dict() or {}
                    sequence = int(value.get("last_seq", 0))
                except (AttributeError, TypeError, ValueError):
                    continue
                if 0 <= bucket < self.bucket_count and sequence >= 0:
                    current[bucket] = max(current[bucket], sequence)

            with self._watch_lock:
                if (self._watchers.get(app) is not entry or
                        entry["watch_generation"] != generation):
                    return
                if not entry["initialized"]:
                    reattached = entry["ever_initialized"]
                    entry["heads"] = current
                    entry["initialized"] = True
                    entry["ever_initialized"] = True
                    entry["ready_event"].set()
                    callbacks = tuple(entry["callbacks"]) if reattached else ()
                    previous = None
                else:
                    reattached = False
                    previous = list(entry["heads"])
                    entry["heads"] = current
                    callbacks = tuple(entry["callbacks"])

            if previous is None:
                if reattached:
                    for current_callback in callbacks:
                        try:
                            current_callback({"type": "resync_required", "app_id": app,
                                              "reason": "firestore_watch_reattached"})
                        except Exception:
                            pass
                return

            notifications = []
            for bucket, (after, through) in enumerate(zip(previous, current)):
                if through <= after:
                    continue
                try:
                    found = self._query_events(app_ref, bucket, after, through)
                    expected = after + 1
                    for event in found:
                        if event["bucket_seq"] != expected:
                            raise RuntimeError("partitioned event gap")
                        event["bucket_count"] = self.bucket_count
                        expected += 1
                        notifications.append({"type": "changed", "app_id": app,
                                              "record_id": event["record_id"],
                                              "revision": event["revision"],
                                              "bucket_id": bucket, "bucket_seq": event["bucket_seq"],
                                              "event": event})
                    if expected <= through:
                        raise RuntimeError("partitioned event gap")
                except Exception:
                    notifications.append({"type": "resync_required", "app_id": app,
                                          "bucket_id": bucket, "through": through})
            for notification in notifications:
                for current_callback in callbacks:
                    try:
                        current_callback(notification)
                    except Exception:
                        # Listener callbacks are best-effort hints. Durable
                        # state remains authoritative and clients reconnect/pull.
                        pass
        return on_snapshot

    @staticmethod
    def _watcher_is_active(watcher):
        """Return the Firestore SDK watch state when that signal is available."""
        try:
            active = getattr(watcher, "is_active", None)
        except Exception:
            return None
        if callable(active):
            try:
                active = active()
            except Exception:
                return None
        return active if isinstance(active, bool) else None

    def _ensure_watchdog(self):
        with self._watchdog_lock:
            if self._watchdog_thread and self._watchdog_thread.is_alive():
                return
            self._watchdog_stop.clear()
            self._watchdog_thread = threading.Thread(
                target=self._watchdog_loop, name="AutoSync-FirestoreWatchdog", daemon=True)
            self._watchdog_thread.start()

    def _watchdog_loop(self):
        while not self._watchdog_stop.wait(WATCHDOG_POLL_SECONDS):
            now = time.monotonic()
            restart = []
            with self._watch_lock:
                for app, entry in tuple(self._watchers.items()):
                    if (not entry["callbacks"] or entry["recovering"] or
                            now < entry["retry_after"]):
                        continue
                    watcher = entry.get("watcher")
                    active = None if watcher is None else self._watcher_is_active(watcher)
                    if watcher is not None and active is not False:
                        continue
                    entry["recovering"] = True
                    entry["watcher"] = None
                    entry["initialized"] = False
                    entry["ready_event"].clear()
                    entry["watch_generation"] += 1
                    restart.append((app, entry, watcher, entry["watch_generation"]))
            for app, entry, old_watcher, generation in restart:
                self._restart_watch(app, entry, old_watcher, generation)

    def _restart_watch(self, app, entry, old_watcher, generation):
        if old_watcher is not None:
            try:
                old_watcher.unsubscribe()
            except Exception:
                pass
        app_ref = self._app(app)
        callback = self._make_snapshot_callback(app, app_ref, entry, generation)
        try:
            watcher = app_ref.collection("feed_buckets").on_snapshot(callback)
        except Exception:
            with self._watch_lock:
                if (self._watchers.get(app) is entry and
                        entry["watch_generation"] == generation):
                    entry["recovering"] = False
                    entry["retry_attempt"] += 1
                    exponent = min(entry["retry_attempt"] - 1, 16)
                    delay = min(WATCHDOG_RETRY_MAX_SECONDS,
                                WATCHDOG_RETRY_INITIAL_SECONDS * (2 ** exponent))
                    entry["retry_after"] = time.monotonic() + delay
            return

        with self._watch_lock:
            keep = (self._watchers.get(app) is entry and entry["callbacks"] and
                    entry["watch_generation"] == generation)
            if keep:
                entry["watcher"] = watcher
                entry["recovering"] = False
                entry["retry_attempt"] = 0
                entry["retry_after"] = 0.0
        if not keep:
            try:
                watcher.unsubscribe()
            except Exception:
                pass

    def subscribe(self, app, callback):
        """Watch bucket heads and reattach a stopped SDK listener automatically.

        One Firestore listener is used per app/server instance rather than one
        listener per feed bucket. A restarted listener emits a durable-resync
        hint after its initial snapshot so clients recover every change that
        occurred while the push stream was unavailable.
        """
        check_id(app, APP_RE)
        if not callable(callback):
            raise ValueError("callback is required")
        with self._watch_lock:
            existing = self._watchers.get(app)
            if existing:
                existing["callbacks"].add(callback)
                entry = existing
                first = False
            else:
                entry = {"watcher": None, "callbacks": {callback},
                         "heads": [0] * self.bucket_count, "initialized": False,
                         "ever_initialized": False, "ready_event": threading.Event(),
                         "watch_generation": 1, "recovering": True,
                         "retry_attempt": 0, "retry_after": 0.0}
                self._watchers[app] = entry
                first = True

        if not first:
            return self._wait_for_initial_cursor(app, callback, entry)

        self._ensure_watchdog()
        app_ref = self._app(app)
        on_snapshot = self._make_snapshot_callback(app, app_ref, entry, entry["watch_generation"])

        try:
            watcher = app_ref.collection("feed_buckets").on_snapshot(on_snapshot)
        except Exception:
            with self._watch_lock:
                if self._watchers.get(app) is entry:
                    self._watchers.pop(app, None)
            raise
        with self._watch_lock:
            if self._watchers.get(app) is entry and entry["callbacks"]:
                entry["watcher"] = watcher
                entry["recovering"] = False
                keep = True
            else:
                keep = False
        if not keep:
            try:
                watcher.unsubscribe()
            except Exception:
                pass
            return lambda: None
        return self._wait_for_initial_cursor(app, callback, entry)

    def _wait_for_initial_cursor(self, app, callback, entry, timeout=10):
        """Do not advertise WebSocket readiness before the shared cursor is cached."""
        if not entry["ready_event"].wait(timeout):
            self._unsubscribe(app, callback)
            raise TimeoutError("Firestore feed listener did not initialize its cursor in time")
        return lambda: self._unsubscribe(app, callback)

    def _unsubscribe(self, app, callback):
        watcher = None
        with self._watch_lock:
            entry = self._watchers.get(app)
            if entry is None:
                return
            entry["callbacks"].discard(callback)
            if not entry["callbacks"]:
                watcher = entry["watcher"]
                self._watchers.pop(app, None)
        if watcher is not None:
            try:
                watcher.unsubscribe()
            except Exception:
                pass

    def close(self):
        self._watchdog_stop.set()
        with self._watch_lock:
            watchers = [entry["watcher"] for entry in self._watchers.values() if entry.get("watcher")]
            self._watchers.clear()
        for watcher in watchers:
            try:
                watcher.unsubscribe()
            except Exception:
                pass
        thread = self._watchdog_thread
        if thread and thread is not threading.current_thread():
            thread.join(timeout=max(1.0, WATCHDOG_POLL_SECONDS + 0.5))
        close = getattr(self.client, "close", None)
        if callable(close):
            close()
