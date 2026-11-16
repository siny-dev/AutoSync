"""Direct, free-tier-compatible AutoSync transport for Firebase Realtime Database.

This client uses one authenticated REST/SSE connection per instance. Record writes
are conditional ETag PUTs, so simultaneous edits of the same record become an
explicit conflict instead of a last-writer-wins overwrite. The local SQLite
outbox remains durable while the device is offline.
"""
from __future__ import annotations

import base64
import hashlib
import json
import random
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from .autosync_client import Outbox
from .autosync_firebase_auth import FirebaseAuthenticationTransientError
from .autosync_protocol import APP_RE, RECORD_RE, canon, now_utc


MAX_PAYLOAD_BYTES = 1_048_576
TOMBSTONE_PAYLOAD = {"_autosync_tombstone": True}
MAX_OPERATION_RECEIPTS = 256
MAX_SSE_EVENT_BYTES = 2 * 1_048_576
MAX_RETRY_ATTEMPTS = 3
RETRY_INITIAL_SECONDS = 0.25
RETRY_MAX_SECONDS = 5.0


class FirebaseRtdbError(RuntimeError):
    """A sanitized Firebase transport or data-contract error."""


class FirebaseAuthRequired(FirebaseRtdbError):
    pass


class FirebasePermissionDenied(FirebaseRtdbError):
    pass


class FirebaseTransientError(FirebaseRtdbError):
    pass


def _segment(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii").rstrip("=")


def _unsegment(value: str) -> str:
    padded = value + "=" * ((4 - len(value) % 4) % 4)
    return base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")


def _record_id_from_segment(key):
    if not isinstance(key, str) or not key:
        raise FirebaseRtdbError("invalid_rtdb_record_key")
    try:
        record_id = _unsegment(key)
    except (ValueError, UnicodeDecodeError):
        raise FirebaseRtdbError("invalid_rtdb_record_key") from None
    if not RECORD_RE.fullmatch(record_id) or _segment(record_id) != key:
        raise FirebaseRtdbError("invalid_rtdb_record_key")
    return record_id


def _request_digest(app_id, record_id, expected, payload, deleted):
    body = {"app_id": app_id, "record_id": record_id, "expected_revision": expected,
            "payload": payload, "deleted": deleted}
    return hashlib.sha256(canon(body).encode("utf-8")).hexdigest()


class FirebaseRtdbClient:
    """AutoSync-compatible client backed directly by Firebase Realtime Database.

    ``token_provider`` must return a current Firebase Authentication ID token.
    Keep refresh credentials in the operating system credential store; never pass
    a service-account key or a long-lived database secret to this client.
    ``allow_emulator`` only permits HTTP loopback for local emulator tests.
    """

    def __init__(self, database_url, token_provider, app_id, local_root,
                 *, allow_emulator=False, request_timeout=15.0):
        if not isinstance(app_id, str) or not APP_RE.fullmatch(app_id):
            raise ValueError("invalid app_id")
        if not callable(token_provider):
            raise ValueError("token_provider must be callable")
        if not isinstance(database_url, str):
            raise ValueError("database_url must be a URL")
        parsed = urllib.parse.urlsplit(database_url.strip())
        host = (parsed.hostname or "").lower()
        loopback = host in ("localhost", "127.0.0.1", "::1")
        production_host = host.endswith(".firebasedatabase.app") or host.endswith(".firebaseio.com")
        if parsed.path not in ("", "/") or parsed.fragment or parsed.username or parsed.password:
            raise ValueError("database_url must be a database root URL without credentials or a path")
        if loopback:
            if not allow_emulator or parsed.scheme not in ("http", "https"):
                raise ValueError("loopback URLs are only allowed for explicitly enabled emulator tests")
        elif parsed.scheme != "https" or not production_host:
            raise ValueError("database_url must use HTTPS on a Firebase Realtime Database host")
        if parsed.query and not allow_emulator:
            raise ValueError("database_url query parameters are only allowed for emulator tests")
        if request_timeout < 2 or request_timeout > 120:
            raise ValueError("request_timeout must be between 2 and 120 seconds")

        self.base = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, "", "", "")).rstrip("/")
        self._base_query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        self._token_provider = token_provider
        self.app = app_id
        self._app_key = _segment(app_id)
        self._records_path = f"autosync/apps/{self._app_key}/records"
        self._records_url_path = "/".join(urllib.parse.quote(part, safe="") for part in self._records_path.split("/"))
        self._timeout = float(request_timeout)
        self._allow_emulator = bool(allow_emulator)
        self.outbox = Outbox(Path(local_root) / "autosync-client.sqlite3")
        self.sync_lock = threading.Lock()
        self._closed = False
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._worker_lock = threading.RLock()
        self._stream_lock = threading.Lock()
        self._stream_response = None
        self._worker_threads = []
        self._status_callback = None
        self._last_callback_error = None
        self._stream_connected = False
        self._snapshot_ready = False

    def close(self):
        if not self._closed:
            if not self.stop():
                raise FirebaseRtdbError("background_workers_did_not_stop")
            self._closed = True
            self.outbox.close()

    def start(self, on_status=None):
        """Start real-time listening and immediate durable-outbox delivery workers.

        A separate sender worker is required because an SSE stream can remain idle
        indefinitely while the user queues a local edit. The wake event pushes the
        pending write without waiting for another remote change or a polling timer.
        """
        with self._worker_lock:
            if self._closed:
                raise FirebaseRtdbError("client_closed")
            if self._worker_threads and any(thread.is_alive() for thread in self._worker_threads):
                return self
            self._stop_event.clear()
            self._wake_event.clear()
            self._status_callback = on_status
            self._worker_threads = [
                threading.Thread(target=self._watch_worker, name="AutoSync-Firebase-SSE", daemon=True),
                threading.Thread(target=self._flush_worker, name="AutoSync-Firebase-Outbox", daemon=True),
            ]
            if self.outbox.pending_count(self.app):
                self._wake_event.set()
            for thread in self._worker_threads:
                thread.start()
        return self

    def stop(self, timeout=None):
        """Stop background workers and close the live SSE socket promptly."""
        self._stop_event.set()
        self._wake_event.set()
        with self._stream_lock:
            response = self._stream_response
        if response is not None:
            try:
                stream_socket = response.fp.raw._sock
                stream_socket.shutdown(socket.SHUT_RDWR)
            except (AttributeError, OSError):
                pass
        with self._worker_lock:
            threads = list(self._worker_threads)
        join_timeout = self._timeout * 3 + 5 if timeout is None else max(0.0, float(timeout))
        current = threading.current_thread()
        deadline = time.monotonic() + join_timeout
        for thread in threads:
            if thread is not current:
                thread.join(max(0.0, deadline - time.monotonic()))
        return not any(thread.is_alive() for thread in threads if thread is not current)

    def _emit_status(self, value):
        callback = self._status_callback
        if callback is None:
            return
        try:
            callback(dict(value))
            self._last_callback_error = None
        except Exception as exc:
            # A UI callback must not silently terminate the transport workers.
            self._last_callback_error = type(exc).__name__

    def _watch_worker(self):
        self.watch(self._stop_event, self._emit_status)

    def _flush_worker(self):
        retry_delay = RETRY_INITIAL_SECONDS
        while not self._stop_event.is_set():
            self._wake_event.wait()
            self._wake_event.clear()
            if self._stop_event.is_set():
                break
            if not self.outbox.pending_count(self.app):
                continue
            try:
                result = self._flush_pending()
            except (FirebaseRtdbError, OSError, TimeoutError) as exc:
                result = {"status": "offline" if isinstance(exc, FirebaseTransientError) else "blocked",
                          "detail": str(exc), "pending": self.outbox.pending_count(self.app)}
            self._emit_status(result)
            if result.get("status") in ("retry", "offline") and not self._stop_event.is_set():
                self._stop_event.wait(random.uniform(retry_delay * 0.8, retry_delay * 1.2))
                retry_delay = min(retry_delay * 2, RETRY_MAX_SECONDS)
                if not self._stop_event.is_set():
                    self._wake_event.set()
            else:
                retry_delay = RETRY_INITIAL_SECONDS

    def _token(self):
        try:
            token = self._token_provider()
        except FirebaseAuthenticationTransientError as exc:
            raise FirebaseTransientError(str(exc)) from None
        except Exception:
            raise FirebaseAuthRequired("firebase_id_token_unavailable") from None
        if not isinstance(token, str) or not token.strip():
            raise FirebaseAuthRequired("firebase_id_token_unavailable")
        return token.strip()

    def _url(self, relative_path, token, *, include_auth=True):
        path = self._records_url_path
        if relative_path:
            path += "/" + "/".join(urllib.parse.quote(part, safe="") for part in relative_path.split("/"))
        pairs = list(self._base_query)
        if include_auth:
            pairs.append(("auth", token))
        return f"{self.base}/{path}.json?{urllib.parse.urlencode(pairs)}"

    @staticmethod
    def _decode_response(raw):
        if not raw:
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None

    def _request(self, relative_path="", method="GET", payload=None, *, token=None,
                 etag=False, if_match=None, stream=False):
        if self._closed:
            raise FirebaseRtdbError("client_closed")
        current_token = token or self._token()
        headers = {"Accept": "text/event-stream" if stream else "application/json",
                   "Connection": "keep-alive" if stream else "close"}
        emulator_owner = self._allow_emulator and current_token == "owner"
        if emulator_owner:
            # The RTDB emulator's documented admin test token is accepted in an
            # Authorization header, not as the REST `auth` query parameter.
            # This is reachable only for explicitly enabled loopback clients.
            headers["Authorization"] = "Bearer owner"
        if etag:
            headers["X-Firebase-ETag"] = "true"
        if if_match is not None:
            headers["If-Match"] = if_match
        body = None
        if payload is not None:
            body = canon(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(self._url(relative_path, current_token,
                                                    include_auth=not emulator_owner), data=body,
                                         headers=headers, method=method)
        try:
            response = urllib.request.urlopen(request, timeout=self._timeout)
        except urllib.error.HTTPError as exc:
            raw = b""
            try:
                raw = exc.read(MAX_SSE_EVENT_BYTES + 1)
            finally:
                exc.close()
            if exc.code in (401, 403):
                raise FirebasePermissionDenied(f"firebase_http_{exc.code}") from None
            if exc.code in (408, 425, 429, 500, 502, 503, 504):
                return exc.code, self._decode_response(raw), exc.headers.get("ETag") if exc.headers else None, None
            return exc.code, self._decode_response(raw), exc.headers.get("ETag") if exc.headers else None, None
        except (OSError, TimeoutError, urllib.error.URLError) as exc:
            raise FirebaseTransientError(type(getattr(exc, "reason", exc)).__name__) from None
        if stream:
            return response.status, None, response.headers.get("ETag"), response
        try:
            raw = response.read(MAX_SSE_EVENT_BYTES + 1)
            if len(raw) > MAX_SSE_EVENT_BYTES:
                raise FirebaseRtdbError("firebase_response_too_large")
            return response.status, self._decode_response(raw), response.headers.get("ETag"), None
        finally:
            response.close()

    @staticmethod
    def _validate_record(key, value):
        if not isinstance(key, str) or not isinstance(value, dict):
            raise FirebaseRtdbError("invalid_rtdb_record")
        rid = _record_id_from_segment(key)
        if value.get("record_id") != rid:
            raise FirebaseRtdbError("rtdb_record_id_mismatch")
        revision = value.get("revision")
        payload = value.get("payload")
        deleted = value.get("deleted")
        updated = value.get("updated_utc")
        if (not isinstance(revision, int) or isinstance(revision, bool) or revision < 1 or
            not isinstance(payload, dict) or not isinstance(deleted, bool) or
            not isinstance(updated, str)):
            raise FirebaseRtdbError("invalid_rtdb_record_schema")
        try:
            if len(canon(payload).encode("utf-8")) > MAX_PAYLOAD_BYTES:
                raise FirebaseRtdbError("rtdb_record_payload_too_large")
        except (TypeError, ValueError):
            raise FirebaseRtdbError("invalid_rtdb_record_payload") from None
        receipts = value.get("operation_receipts", [])
        if (not isinstance(receipts, list) or len(receipts) > MAX_OPERATION_RECEIPTS or
            any(not isinstance(item, dict) or not isinstance(item.get("operation_id"), str) or
                not isinstance(item.get("request_sha256"), str) or
                not isinstance(item.get("revision"), int) for item in receipts)):
            raise FirebaseRtdbError("invalid_rtdb_operation_receipts")
        return {"record_id": rid, "revision": revision, "payload": payload,
                "deleted": deleted, "updated_utc": updated,
                "operation_receipts": receipts}

    def _pull_snapshot(self):
        status, data, _, _ = self._request()
        if status in (401, 403):
            raise FirebasePermissionDenied(f"firebase_http_{status}")
        if status != 200:
            if status >= 500 or status in (408, 425, 429):
                raise FirebaseTransientError(f"firebase_http_{status}")
            raise FirebaseRtdbError(f"firebase_http_{status}")
        records = []
        if data is not None:
            if not isinstance(data, dict):
                raise FirebaseRtdbError("invalid_rtdb_records_root")
            for key, value in data.items():
                records.append(self._validate_record(key, value))
        self.outbox.apply_rtdb_snapshot(self.app, records)
        self._snapshot_ready = True
        return len(records)

    def _get_record(self, rid):
        status, data, etag, _ = self._request(_segment(rid), etag=True)
        if status in (401, 403):
            raise FirebasePermissionDenied(f"firebase_http_{status}")
        if status != 200:
            if status >= 500 or status in (408, 425, 429):
                raise FirebaseTransientError(f"firebase_http_{status}")
            raise FirebaseRtdbError(f"firebase_http_{status}")
        current = None if data is None else self._validate_record(_segment(rid), data)
        return current, etag or ("null_etag" if current is None else None)

    def _write_one(self, operation):
        rid = operation["rid"]
        op_id = operation["op"]
        expected = int(operation["expected"])
        payload = operation["payload"]
        deleted = bool(operation["deleted"])
        if not isinstance(payload, dict) or len(canon(payload).encode("utf-8")) > MAX_PAYLOAD_BYTES:
            raise FirebaseRtdbError("invalid_or_oversized_payload")
        digest = _request_digest(self.app, rid, expected, payload, deleted)
        delay = RETRY_INITIAL_SECONDS
        for attempt in range(MAX_RETRY_ATTEMPTS):
            current, etag = self._get_record(rid)
            current_revision = int(current["revision"]) if current else 0
            if current:
                receipt = next((item for item in current["operation_receipts"]
                                if item["operation_id"] == op_id), None)
                if receipt:
                    if receipt["request_sha256"] != digest:
                        raise FirebaseRtdbError("idempotency_key_reused")
                    self.outbox.apply_rtdb_record(self.app, current)
                    self.outbox.mark_synced(op_id, current_revision, current)
                    return "synced", current
            if current_revision != expected:
                conflict = {"error": "revision_conflict", "current": current,
                            "incoming": {"record_id": rid, "expected_revision": expected,
                                         "payload": payload, "deleted": deleted,
                                         "operation_id": op_id}}
                self.outbox.mark_conflict(op_id, conflict)
                return "conflict", conflict
            receipts = list(current["operation_receipts"] if current else [])
            next_revision = expected + 1
            receipts.append({"operation_id": op_id, "request_sha256": digest,
                             "revision": next_revision})
            record_payload = TOMBSTONE_PAYLOAD if deleted and not payload else payload
            record = {"record_id": rid, "revision": next_revision,
                      "payload": record_payload, "deleted": deleted,
                      "updated_utc": now_utc(),
                      "operation_receipts": receipts[-MAX_OPERATION_RECEIPTS:]}
            try:
                status, result, _, _ = self._request(_segment(rid), "PUT", record,
                                                      if_match=etag or "null_etag")
            except FirebaseTransientError:
                if attempt + 1 >= MAX_RETRY_ATTEMPTS:
                    raise
                time.sleep(random.uniform(delay * 0.8, delay * 1.2))
                delay = min(delay * 2, RETRY_MAX_SECONDS)
                continue
            if status == 200:
                saved = self._validate_record(_segment(rid), result or record)
                self.outbox.apply_rtdb_record(self.app, saved)
                self.outbox.mark_synced(op_id, saved["revision"], saved)
                return "synced", saved
            if status == 412:
                if attempt + 1 >= MAX_RETRY_ATTEMPTS:
                    raise FirebaseTransientError("firebase_etag_contention_retry_exhausted")
                time.sleep(random.uniform(delay * 0.8, delay * 1.2))
                delay = min(delay * 2, RETRY_MAX_SECONDS)
                continue
            if status in (401, 403):
                raise FirebasePermissionDenied(f"firebase_http_{status}")
            if status in (408, 425, 429, 500, 502, 503, 504):
                if attempt + 1 >= MAX_RETRY_ATTEMPTS:
                    raise FirebaseTransientError(f"firebase_http_{status}")
                time.sleep(random.uniform(delay * 0.8, delay * 1.2))
                delay = min(delay * 2, RETRY_MAX_SECONDS)
                continue
            raise FirebaseRtdbError(f"firebase_http_{status}")
        raise FirebaseTransientError("firebase_write_retry_exhausted")

    def _flush_pending(self):
        sent = 0
        conflicts = 0
        while True:
            operation = self.outbox.claim_next(self.app)
            if operation is None:
                break
            try:
                state, _ = self._write_one(operation)
                if state == "synced":
                    sent += 1
                else:
                    conflicts += 1
            except FirebasePermissionDenied as exc:
                self.outbox.release(operation["op"])
                return {"status": "blocked", "detail": str(exc), "sent": sent,
                        "conflicts": conflicts, "pending": self.outbox.pending_count(self.app)}
            except FirebaseAuthRequired as exc:
                self.outbox.release(operation["op"])
                return {"status": "blocked", "detail": str(exc), "sent": sent,
                        "conflicts": conflicts, "pending": self.outbox.pending_count(self.app)}
            except FirebaseTransientError as exc:
                self.outbox.release(operation["op"])
                return {"status": "retry", "detail": str(exc), "sent": sent,
                        "conflicts": conflicts, "pending": self.outbox.pending_count(self.app)}
            except FirebaseRtdbError as exc:
                self.outbox.release(operation["op"])
                return {"status": "blocked", "detail": str(exc), "sent": sent,
                        "conflicts": conflicts, "pending": self.outbox.pending_count(self.app)}
        return {"status": "conflict" if conflicts else "synced", "sent": sent,
                "conflicts": conflicts, "pending": self.outbox.pending_count(self.app),
                "cursor": None}

    def sync_once(self):
        with self.sync_lock:
            try:
                # The SSE stream already carries the authoritative initial
                # snapshot and subsequent changes. Avoid downloading the entire
                # collection on every local edit while that stream is healthy.
                if not (self._stream_connected and self._snapshot_ready):
                    self._pull_snapshot()
            except FirebasePermissionDenied as exc:
                return {"status": "blocked", "detail": str(exc), "pending": self.outbox.pending_count(self.app)}
            except FirebaseAuthRequired as exc:
                return {"status": "blocked", "detail": str(exc), "pending": self.outbox.pending_count(self.app)}
            except FirebaseTransientError as exc:
                return {"status": "offline", "detail": str(exc), "pending": self.outbox.pending_count(self.app)}
            except FirebaseRtdbError as exc:
                return {"status": "blocked", "detail": str(exc), "pending": self.outbox.pending_count(self.app)}
            return self._flush_pending()

    def queue_write(self, record_id, payload, expected_revision=None, deleted=False, operation_id=None):
        operation = self.outbox.queue_write(self.app, record_id, payload, expected_revision, deleted, operation_id)
        self._wake_event.set()
        return operation

    def record_revision(self, record_id):
        return self.outbox.record_revision(self.app, record_id)

    def has_pending_record(self, record_id):
        return self.outbox.has_pending_record(self.app, record_id)

    def operation_status(self, operation_id):
        return self.outbox.operation_status(operation_id)

    def pending_count(self):
        return self.outbox.pending_count(self.app)

    def conflict_count(self):
        return self.outbox.conflict_count(self.app)

    def cached_record(self, record_id):
        return self.outbox.cached_record(self.app, record_id)

    def cached_records(self):
        return self.outbox.cached_records(self.app)

    def conflicts(self):
        return self.outbox.conflicts(self.app)

    def resolve_conflict(self, operation_id, payload, expected_revision, deleted=False,
                         resolution_operation_id=None):
        return self.outbox.resolve_conflict(operation_id, payload, expected_revision, deleted,
                                            resolution_operation_id)

    def _apply_sse_value(self, event_name, path, data):
        if event_name in ("cancel", "auth_revoked"):
            raise FirebaseAuthRequired("firebase_stream_auth_revoked")
        if event_name not in ("put", "patch"):
            return False
        if not isinstance(path, str) or not path.startswith("/"):
            raise FirebaseRtdbError("invalid_firebase_sse_event")
        relative = path.strip("/")
        if data is None:
            if event_name == "put" and not relative:
                self.outbox.apply_rtdb_snapshot(self.app, [])
                self._snapshot_ready = True
                return True
            return False
        if not isinstance(data, dict) and not (event_name == "patch" and relative and "/" in relative):
            raise FirebaseRtdbError("invalid_firebase_sse_event")
        if not relative:
            if event_name == "put":
                snapshot = []
                if data:
                    if not isinstance(data, dict):
                        raise FirebaseRtdbError("invalid_firebase_sse_snapshot")
                    snapshot = [self._validate_record(key, value) for key, value in data.items()]
                self.outbox.apply_rtdb_snapshot(self.app, snapshot)
            else:
                # A PATCH at the watched collection path may contain partial
                # subtrees for one or more records. Re-read each changed child
                # so only canonical full records enter the durable local cache.
                for key, value in data.items():
                    rid = _record_id_from_segment(key)
                    if value is not None:
                        current, _ = self._get_record(rid)
                        if current is not None:
                            self.outbox.apply_rtdb_record(self.app, current)
            return True
        parts = relative.split("/")
        key = parts[0]
        if len(parts) == 1:
            if event_name == "patch":
                # A PATCH at the record path contains only changed child fields.
                # Fetch the whole canonical record before updating the durable cache.
                rid = _record_id_from_segment(key)
                current, _ = self._get_record(rid)
                if current is not None:
                    self.outbox.apply_rtdb_record(self.app, current)
            elif data is not None:
                self.outbox.apply_rtdb_record(self.app, self._validate_record(key, data))
            return True
        # A nested event can be a partial field update. Fetch the complete record
        # before applying it so the durable cache never stores a partial object.
        rid = _record_id_from_segment(key)
        current, _ = self._get_record(rid)
        if current is not None:
            self.outbox.apply_rtdb_record(self.app, current)
        return True

    def _read_sse_event(self, response, stop_event):
        event_name = "message"
        data_lines = []
        event_size = 0
        while not stop_event.is_set():
            try:
                line = response.readline(MAX_SSE_EVENT_BYTES + 1)
            except socket.timeout:
                return None
            if not line:
                raise FirebaseTransientError("firebase_stream_closed")
            event_size += len(line)
            if event_size > MAX_SSE_EVENT_BYTES:
                raise FirebaseRtdbError("firebase_sse_event_too_large")
            if line in (b"\n", b"\r\n"):
                if not data_lines:
                    event_name = "message"
                    event_size = 0
                    continue
                raw = "\n".join(data_lines)
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    raise FirebaseRtdbError("invalid_firebase_sse_json") from None
                result = (event_name, parsed)
                event_name = "message"
                data_lines = []
                event_size = 0
                return result
            try:
                text = line.decode("utf-8").rstrip("\r\n")
            except UnicodeDecodeError:
                raise FirebaseRtdbError("invalid_firebase_sse_encoding") from None
            if not text or text.startswith(":"):
                continue
            field, sep, value = text.partition(":")
            if sep and value.startswith(" "):
                value = value[1:]
            if field == "event":
                event_name = value
            elif field == "data":
                data_lines.append(value)
        return None

    def watch(self, stop_event, on_status=None):
        """Attach to Firebase SSE, reconcile from the initial snapshot, and reconnect safely."""
        backoff = RETRY_INITIAL_SECONDS
        while not stop_event.is_set():
            response = None
            try:
                status, _, _, response = self._request(stream=True)
                if status in (401, 403):
                    raise FirebasePermissionDenied(f"firebase_http_{status}")
                if status != 200 or response is None:
                    raise FirebaseTransientError(f"firebase_stream_http_{status}")
                try:
                    response.fp.raw._sock.settimeout(min(self._timeout, 1.0))
                except (AttributeError, OSError):
                    pass
                with self._stream_lock:
                    self._stream_response = response
                self._stream_connected = True
                self._snapshot_ready = False
                backoff = RETRY_INITIAL_SECONDS
                if on_status:
                    on_status({"status": "connected", "transport": "firebase-rtdb-sse"})
                while not stop_event.is_set():
                    item = self._read_sse_event(response, stop_event)
                    if item is None:
                        continue
                    event_name, data = item
                    if event_name == "keep-alive":
                        continue
                    event_path = data.get("path") if isinstance(data, dict) else None
                    event_value = data.get("data") if isinstance(data, dict) else None
                    if self._apply_sse_value(event_name, event_path, event_value):
                        if event_name == "put" and event_path == "/":
                            self._snapshot_ready = True
                        result = self._flush_pending()
                        result["push"] = event_name
                        if on_status:
                            on_status(result)
            except FirebasePermissionDenied as exc:
                if on_status:
                    on_status({"status": "blocked", "detail": str(exc)})
                stop_event.wait(RETRY_MAX_SECONDS)
            except FirebaseAuthRequired as exc:
                if on_status:
                    on_status({"status": "blocked", "detail": str(exc)})
                stop_event.wait(RETRY_MAX_SECONDS)
            except FirebaseRtdbError as exc:
                if on_status:
                    on_status({"status": "blocked", "detail": str(exc)})
                stop_event.wait(backoff)
                backoff = min(backoff * 2, RETRY_MAX_SECONDS)
            except (FirebaseTransientError, OSError, TimeoutError, urllib.error.URLError) as exc:
                if on_status:
                    on_status({"status": "offline", "detail": str(exc),
                               "retry_in_seconds": round(backoff, 2)})
                stop_event.wait(random.uniform(backoff * 0.8, backoff * 1.2))
                backoff = min(backoff * 2, RETRY_MAX_SECONDS)
            finally:
                self._stream_connected = False
                self._snapshot_ready = False
                if response is not None:
                    with self._stream_lock:
                        if self._stream_response is response:
                            self._stream_response = None
                    try:
                        response.close()
                    except OSError:
                        pass
