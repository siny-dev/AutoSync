"""Optional Cloud Firestore persistence adapter for the AutoSync HTTP service.

The desktop clients never connect to Firestore directly.  The Cloud Run service
uses its attached runtime identity (Application Default Credentials), and the
existing AutoSync API token gate still scopes each request to one app namespace.
"""
from __future__ import annotations

import hashlib
import json
import threading
import uuid
from datetime import datetime, timedelta, timezone

if __package__:
    from .autosync_protocol import APP_RE, OP_RE, RECORD_RE, canon, check_id, now_utc
else:  # Support direct `python src/autosync_server.py` execution.
    from autosync_protocol import APP_RE, OP_RE, RECORD_RE, canon, check_id, now_utc


# Conflict records retain both alternatives in one Firestore document. Leave
# ample room for metadata and both JSON versions under Firestore's 1 MiB limit.
MAX_FIRESTORE_BODY = 400 * 1024
DEFAULT_EVENT_RETENTION_DAYS = 90
DEFAULT_OPERATION_RETENTION_DAYS = 90
# Keep one request's transaction retry bounded. The desktop client's durable
# outbox retries transient 503s with jitter, so a hot app cursor does not hold
# an HTTP request open for minutes during a multi-instance burst.
MAX_FIRESTORE_TRANSACTION_ATTEMPTS = 5


class FirestoreStore:
    """Firestore-backed implementation of the server Store contract.

    ``transactional`` is injectable so transaction behavior can be unit-tested
    without credentials or a live Firestore project.  In production it is the
    official google-cloud-firestore retrying transaction decorator.
    """

    def __init__(self, project_id: str | None = None, collection_prefix: str = "autosync_v1",
                 client=None, transactional=None,
                 event_retention_days: int = DEFAULT_EVENT_RETENTION_DAYS,
                 operation_retention_days: int = DEFAULT_OPERATION_RETENTION_DAYS):
        if not isinstance(collection_prefix, str) or not collection_prefix.replace("_", "").isalnum():
            raise ValueError("Firestore collection prefix must contain only letters, digits, and underscores")
        for name, value in (("event_retention_days", event_retention_days),
                            ("operation_retention_days", operation_retention_days)):
            if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= 3650:
                raise ValueError(f"{name} must be an integer between 1 and 3650")
        self.event_retention = timedelta(days=event_retention_days)
        self.operation_retention = timedelta(days=operation_retention_days)
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
        self._watch_lock = threading.RLock()
        self._watchers = {}
        # Writes for one app advance a single durable cursor document. Serialize
        # those transactions within this process so a burst from one Cloud Run
        # instance does not make every request race on the same Firestore doc.
        self._write_lock_guard = threading.Lock()
        self._write_locks = {}
        self._health_lock = threading.Lock()
        self._health_checked_at = 0.0
        self._health_result = None

    def _app(self, app):
        check_id(app, APP_RE)
        return self.client.collection(self.collection_prefix).document(app)

    def _meta(self, app):
        return self._app(app).collection("metadata").document("state")

    def _record(self, app, rid):
        return self._app(app).collection("records").document(rid)

    def _operation(self, app, op):
        key = hashlib.sha256(op.encode("utf-8")).hexdigest()
        return self._app(app).collection("operations").document(key)

    def _write_lock_for(self, app):
        with self._write_lock_guard:
            return self._write_locks.setdefault(app, threading.Lock())

    @staticmethod
    def _after_revision(collection, revision):
        try:
            from google.cloud.firestore_v1.base_query import FieldFilter
        except ImportError:
            # Keeps the store testable without the optional cloud dependency.
            return collection.where("revision", ">", revision)
        return collection.where(filter=FieldFilter("revision", ">", revision))

    def health(self):
        # Shared by HTTP keepalive probes: avoid turning 100 client health
        # checks into 100 Firestore reads every heartbeat interval.
        import time
        with self._health_lock:
            if self._health_result is not None and time.monotonic() - self._health_checked_at < 30:
                return dict(self._health_result)
        snapshot = self.client.collection(self.collection_prefix).document("_schema").get(timeout=5)
        if snapshot.exists:
            version = snapshot.to_dict().get("schema_version")
            if version != 1:
                raise RuntimeError("unsupported Firestore AutoSync schema")
        result = {"integrity": "ok", "schema_version": 1, "backend": "firestore"}
        with self._health_lock:
            self._health_checked_at = time.monotonic()
            self._health_result = result
        return dict(result)

    def current_cursor(self, app):
        """Read only the app metadata document; never materialize a snapshot."""
        check_id(app, APP_RE)
        snapshot = self._meta(app).get(timeout=10)
        if not snapshot.exists:
            return 0
        return int(snapshot.to_dict().get("last_rev", 0))

    def close(self):
        with self._watch_lock:
            watchers = [item["watcher"] for item in self._watchers.values()]
            self._watchers.clear()
        for watcher in watchers:
            try:
                watcher.unsubscribe()
            except Exception:
                pass
        close = getattr(self.client, "close", None)
        if callable(close):
            close()

    def state(self, app, since=None):
        check_id(app, APP_RE)
        if since is not None and (not isinstance(since, int) or isinstance(since, bool) or since < 0):
            raise ValueError("bad cursor")
        app_ref = self._app(app)
        meta_snapshot = self._meta(app).get(timeout=10)
        meta = meta_snapshot.to_dict() if meta_snapshot.exists else {}
        cursor = int(meta.get("last_rev", 0))
        floor = int(meta.get("history_floor", 0))

        def snapshot_response(reset_required=False):
            records = []
            for record_snapshot in app_ref.collection("records").stream():
                item = record_snapshot.to_dict()
                records.append({
                    "record_id": record_snapshot.id,
                    "revision": int(item["revision"]),
                    "payload": json.loads(item["payload_json"]),
                    "deleted": bool(item["deleted"]),
                    "updated_utc": item["updated_utc"],
                })
            records.sort(key=lambda item: item["record_id"])
            result = {"api_version": "1.0", "app_id": app, "cursor": cursor, "records": records}
            if reset_required:
                result.update({"history_floor": floor, "reset_required": True,
                               "error": "snapshot_required"})
            return result

        if since is None:
            return snapshot_response()
        if since < floor or since > cursor:
            return snapshot_response(reset_required=True)

        query = self._after_revision(app_ref.collection("events"), since).order_by("revision")
        events = []
        expected_revision = since + 1
        for event_snapshot in query.stream():
            item = event_snapshot.to_dict()
            revision = int(item["revision"])
            # The metadata cursor is read before the event query. A concurrent
            # write may appear in that query after this response's cursor, so
            # do not leak that future event into the older delta response.
            if revision > cursor:
                continue
            if revision != expected_revision:
                # Event history can be compacted or partially unavailable. A
                # full current snapshot safely restores the client without
                # requiring old event rows to remain forever.
                return snapshot_response(reset_required=True)
            events.append({
                "revision": revision,
                "record_id": item["record_id"],
                "payload": json.loads(item["payload_json"]),
                "deleted": bool(item["deleted"]),
                "updated_utc": item["updated_utc"],
            })
            expected_revision += 1
        if expected_revision <= cursor:
            return snapshot_response(reset_required=True)
        events.sort(key=lambda item: item["revision"])
        return {"api_version": "1.0", "app_id": app, "cursor": cursor,
                "reset_required": False, "events": events}

    def write(self, app, rid, payload, deleted, expected, op):
        check_id(app, APP_RE)
        check_id(rid, RECORD_RE)
        check_id(op, OP_RE)
        if (not isinstance(payload, dict) or not isinstance(deleted, bool)
                or not isinstance(expected, int) or isinstance(expected, bool) or expected < 0):
            raise ValueError("invalid write")
        body = canon(payload)
        if len(body.encode("utf-8")) > MAX_FIRESTORE_BODY:
            raise ValueError("payload too large for Firestore document limits")
        digest = hashlib.sha256(canon([app, rid, payload, deleted, expected]).encode("utf-8")).hexdigest()
        app_ref = self._app(app)
        meta_ref = self._meta(app)
        record_ref = self._record(app, rid)
        op_ref = self._operation(app, op)
        conflict_id = str(uuid.uuid4())

        def apply(transaction):
            # Firestore requires all reads before writes. This callback may be
            # retried by the SDK, so all state returned to callers is local to
            # this invocation and no external side effect occurs here.
            op_snapshot = op_ref.get(transaction=transaction)
            meta_snapshot = meta_ref.get(transaction=transaction)
            record_snapshot = record_ref.get(transaction=transaction)
            if op_snapshot.exists:
                old = op_snapshot.to_dict()
                if old.get("digest") != digest:
                    return 409, {"error": "idempotency_key_reused"}
                response = json.loads(old["response_json"])
                response["duplicate"] = True
                return int(old["status"]), response

            meta = meta_snapshot.to_dict() if meta_snapshot.exists else {}
            cursor = int(meta.get("last_rev", 0))
            floor = int(meta.get("history_floor", 0))
            current_record = record_snapshot.to_dict() if record_snapshot.exists else None
            actual = int(current_record["revision"]) if current_record else 0
            if actual != expected:
                current = ({"revision": actual, "payload": json.loads(current_record["payload_json"]),
                            "deleted": bool(current_record["deleted"]),
                            "updated_utc": current_record["updated_utc"]}
                           if current_record else None)
                response = {"error": "revision_conflict", "conflict_id": conflict_id,
                            "app_id": app, "record_id": rid, "expected_revision": expected,
                            "current": current, "incoming": {"payload": payload, "deleted": deleted}}
                transaction.set(app_ref.collection("conflicts").document(conflict_id), {
                    "conflict_id": conflict_id, "record_id": rid, "expected_revision": expected,
                    "actual_revision": actual, "incoming_json": canon(response["incoming"]),
                    "current_json": canon(current) if current is not None else None,
                    "operation_id_hash": hashlib.sha256(op.encode()).hexdigest(),
                    "created_utc": now_utc(),
                })
                transaction.set(op_ref, {"digest": digest, "status": 409,
                                         "response_json": canon(response),
                                         "expires_at": datetime.now(timezone.utc) + self.operation_retention})
                return 409, response

            revision = cursor + 1
            updated = now_utc()
            response = {"ok": True, "app_id": app, "record_id": rid,
                        "revision": revision, "previous_cursor": cursor,
                        "deleted": deleted, "updated_utc": updated, "duplicate": False}
            transaction.set(meta_ref, {"schema_version": 1, "last_rev": revision,
                                       "history_floor": floor})
            transaction.set(record_ref, {"revision": revision, "payload_json": body,
                                         "deleted": deleted, "updated_utc": updated})
            transaction.set(app_ref.collection("events").document(f"{revision:020d}"), {
                "revision": revision, "previous_cursor": cursor, "record_id": rid,
                "payload_json": body, "deleted": deleted, "updated_utc": updated,
                "expires_at": datetime.now(timezone.utc) + self.event_retention,
            })
            transaction.set(op_ref, {"digest": digest, "status": 200,
                                     "response_json": canon(response),
                                     "expires_at": datetime.now(timezone.utc) + self.operation_retention})
            return 200, response

        # The revision counter makes writes for one app intentionally
        # sequential. Cloud Run staging is limited to one instance; this lock
        # additionally prevents needless intra-instance transaction conflicts.
        with self._write_lock_for(app):
            transaction = self.client.transaction(max_attempts=MAX_FIRESTORE_TRANSACTION_ATTEMPTS)
            return self.transactional(apply)(transaction)

    def subscribe(self, app, callback):
        """Watch only new durable events for one app; returns an unsubscribe function."""
        check_id(app, APP_RE)
        if not callable(callback):
            raise ValueError("callback is required")
        with self._watch_lock:
            existing = self._watchers.get(app)
            if existing:
                existing["callbacks"].add(callback)
                return lambda: self._unsubscribe(app, callback)

        start_cursor = self.current_cursor(app)
        entry = {"watcher": None, "callbacks": {callback}}
        with self._watch_lock:
            existing = self._watchers.get(app)
            if existing:
                existing["callbacks"].add(callback)
                return lambda: self._unsubscribe(app, callback)
            # Publish the callback before attaching the provider listener. Its
            # initial snapshot can contain a write committed after start_cursor.
            self._watchers[app] = entry
        query = self._after_revision(self._app(app).collection("events"), start_cursor).order_by("revision")
        seen = {"revision": start_cursor}
        seen_lock = threading.Lock()

        def on_snapshot(documents, _changes, _read_time):
            pending = []
            for document in sorted(documents, key=lambda doc: int(doc.to_dict()["revision"])):
                item = document.to_dict()
                revision = int(item["revision"])
                with seen_lock:
                    if revision <= seen["revision"]:
                        continue
                    seen["revision"] = revision
                pending.append({"type": "changed", "app_id": app,
                                "record_id": item["record_id"], "revision": revision,
                                "event": {"revision": revision,
                                          "previous_cursor": int(item.get("previous_cursor", revision - 1)),
                                          "record_id": item["record_id"],
                                          "payload": json.loads(item["payload_json"]),
                                          "deleted": bool(item["deleted"]),
                                          "updated_utc": item["updated_utc"]}})
            for event in pending:
                with self._watch_lock:
                    callbacks = tuple(entry["callbacks"])
                for current_callback in callbacks:
                    current_callback(event)

        try:
            watcher = query.on_snapshot(on_snapshot)
        except Exception:
            with self._watch_lock:
                if self._watchers.get(app) is entry:
                    self._watchers.pop(app, None)
            raise
        with self._watch_lock:
            if self._watchers.get(app) is entry and entry["callbacks"]:
                entry["watcher"] = watcher
                keep = True
            else:
                keep = False
        if not keep:
            watcher.unsubscribe()
            return lambda: None
        return lambda: self._unsubscribe(app, callback)

    def _unsubscribe(self, app, callback):
        watcher = None
        with self._watch_lock:
            existing = self._watchers.get(app)
            if not existing:
                return
            existing["callbacks"].discard(callback)
            if not existing["callbacks"]:
                watcher = existing["watcher"]
                self._watchers.pop(app, None)
        if watcher is not None:
            watcher.unsubscribe()
