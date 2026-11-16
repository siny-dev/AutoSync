"""Run a bounded, test-namespace-only Firebase Spark acceptance check.

Default mode is a no-network plan. Live mode requires --execute and only writes
two stable records below an app ID beginning with autosync-test-, keeping
repeated runs from growing the test namespace. Records are tombstoned during
cleanup because the example RTDB Rules intentionally forbid hard deletion.
Passwords are requested without echo and are never written to the
report or command line; FirebaseAuthSession persists only the refresh token in
Windows Credential Manager.
"""
from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
import tempfile
import time
import urllib.parse
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.autosync_firebase_auth import FirebaseAuthSession, FirebaseAuthenticationError
from src.autosync_firebase_rtdb import FirebaseRtdbClient


DEFAULT_APP_ID = "autosync-test-spark"
DEFAULT_CREDENTIAL_TARGET = "AutoSync/autosync-test-spark/account-1"
MAX_TIMEOUT_SECONDS = 120


def validate_live_target(database_url: str, app_id: str, timeout_seconds: float,
                         *, allow_emulator: bool = False) -> str:
    """Reject non-Firebase endpoints, production app namespaces, and unsafe waits."""
    parsed = urllib.parse.urlsplit(database_url.strip() if isinstance(database_url, str) else "")
    host = (parsed.hostname or "").lower()
    loopback = host in ("127.0.0.1", "localhost", "::1")
    if allow_emulator:
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        if (not loopback or parsed.scheme not in ("http", "https") or
                parsed.path not in ("", "/") or parsed.fragment or
                parsed.username or parsed.password or set(query) != {"ns"} or
                len(query["ns"]) != 1 or not query["ns"][0].startswith("demo-")):
            raise ValueError("emulator_url_must_be_namespaced_loopback")
    elif (parsed.scheme != "https" or
          not (host.endswith(".firebasedatabase.app") or host.endswith(".firebaseio.com")) or
          parsed.port not in (None, 443) or
          parsed.path not in ("", "/") or parsed.query or parsed.fragment or
          parsed.username or parsed.password):
        raise ValueError("database_url_must_be_firebase_https_root")
    if (not isinstance(app_id, str) or not app_id.startswith("autosync-test-") or
            not 1 <= len(app_id) <= 63 or
            any(ch not in "abcdefghijklmnopqrstuvwxyz0123456789._-" for ch in app_id) or
            app_id.endswith((".", "-", "_"))):
        raise ValueError("app_id_must_use_autosync-test-namespace")
    if (isinstance(timeout_seconds, bool) or
            not isinstance(timeout_seconds, (int, float)) or
            not 3 <= timeout_seconds <= MAX_TIMEOUT_SECONDS):
        raise ValueError("timeout_seconds_out_of_range")
    return host


def _wait_until(predicate, timeout: float, label: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.025)
    if predicate():
        return
    raise TimeoutError(label)


def _wait_operation(client, operation_id: str, timeout: float) -> dict:
    result = {}

    def terminal():
        nonlocal result
        result = client.operation_status(operation_id)
        return result.get("state") in ("synced", "conflict")

    _wait_until(terminal, timeout, "operation_timeout")
    return result


def run_acceptance(database_url: str, token_provider, app_id: str, timeout_seconds: float = 20,
                   *, allow_emulator: bool = False) -> dict:
    """Exercise two clients, conflict preservation, restart replay, and tombstones.

    allow_emulator exists only for opt-in loopback integration tests. The command
    line tool never enables it.
    """
    host = validate_live_target(database_url, app_id, timeout_seconds,
                                allow_emulator=allow_emulator)
    run_id = uuid.uuid4().hex
    # Stable record IDs keep repeated live checks bounded to two cloud records.
    # The per-run UUID remains in each payload and report for traceability.
    record_sync = "acceptance-sync"
    record_offline = "acceptance-replay"
    report = {
        "schema": 1,
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "target": "firebase-realtime-database",
        "environment": "emulator" if allow_emulator else "spark",
        "database_host": host,
        "app_id": app_id,
        "run_id": run_id,
        "physical_devices": 1,
        "checks": {},
        "cleanup": {"status": "not_started"},
        "errors": [],
    }
    temporary = tempfile.TemporaryDirectory(prefix="autosync-spark-live-")
    root = Path(temporary.name)
    clients = []
    client_a = None
    client_b = None
    replay_client = None
    cleanup_client = None
    try:
        def make_client(name):
            local_root = root / name
            local_root.mkdir(parents=True, exist_ok=True)
            client = FirebaseRtdbClient(
                database_url, token_provider, app_id, local_root,
                allow_emulator=allow_emulator,
                request_timeout=min(max(float(timeout_seconds), 3.0), 30.0),
            )
            clients.append(client)
            return client

        client_a = make_client("client-a")
        client_b = make_client("client-b")
        for client in (client_a, client_b):
            initial_read = client.sync_once()
            if initial_read.get("status") != "synced":
                raise RuntimeError("firebase_initial_read_" + str(initial_read.get("status", "failed")))
        start = time.monotonic()
        client_a.start()
        client_b.start()
        initial_revision = client_a.record_revision(record_sync)
        if client_b.record_revision(record_sync) != initial_revision:
            raise RuntimeError("clients_disagree_on_initial_revision")
        record_a = {"run_id": run_id, "phase": "initial", "writer": "client-a"}
        op_a = client_a.queue_write(record_sync, record_a, expected_revision=initial_revision)
        initial_status = _wait_operation(client_a, op_a, timeout_seconds)
        if initial_status.get("state") != "synced":
            raise RuntimeError("initial_cloud_write_not_synced")
        _wait_until(
            lambda: (client_b.cached_record(record_sync) or {}).get("payload") == record_a,
            timeout_seconds,
            "second_client_did_not_receive_initial_record",
        )
        report["checks"]["two_client_live_sync"] = {
            "status": "passed",
            "elapsed_ms": round((time.monotonic() - start) * 1000, 3),
        }

        start = time.monotonic()
        expected_race_revision = client_a.record_revision(record_sync)
        if client_b.record_revision(record_sync) != expected_race_revision:
            raise RuntimeError("clients_disagree_before_conflict_test")
        op_a = client_a.queue_write(
            record_sync, {"run_id": run_id, "phase": "race-a"},
            expected_revision=expected_race_revision,
        )
        op_b = client_b.queue_write(
            record_sync, {"run_id": run_id, "phase": "race-b"},
            expected_revision=expected_race_revision,
        )
        state_a = _wait_operation(client_a, op_a, timeout_seconds).get("state")
        state_b = _wait_operation(client_b, op_b, timeout_seconds).get("state")
        states = (state_a, state_b)
        if states.count("synced") != 1 or states.count("conflict") != 1:
            raise RuntimeError("same_revision_race_not_preserved")
        cleanup_client = client_a if state_a == "synced" else client_b
        _wait_until(
            lambda: ((client_a.cached_record(record_sync) or {}).get("revision") ==
                     expected_race_revision + 1 and
                     (client_b.cached_record(record_sync) or {}).get("revision") ==
                     expected_race_revision + 1),
            timeout_seconds,
            "conflict_winner_did_not_converge",
        )
        conflicts = client_a.conflicts() + client_b.conflicts()
        if len(conflicts) != 1 or conflicts[0].get("record_id") != record_sync:
            raise RuntimeError("conflict_record_not_preserved")
        report["checks"]["same_revision_conflict"] = {
            "status": "passed",
            "winner": "client-a" if state_a == "synced" else "client-b",
            "conflict_preserved": True,
            "elapsed_ms": round((time.monotonic() - start) * 1000, 3),
        }

        replay_root = root / "replay-client"
        replay_root.mkdir(parents=True, exist_ok=True)
        replay_client = FirebaseRtdbClient(
            database_url, token_provider, app_id, replay_root,
            allow_emulator=allow_emulator,
            request_timeout=min(max(float(timeout_seconds), 3.0), 30.0),
        )
        clients.append(replay_client)
        offline_op = replay_client.queue_write(
            record_offline, {"run_id": run_id, "phase": "durable-replay"},
            expected_revision=client_a.record_revision(record_offline),
        )
        if replay_client.operation_status(offline_op).get("state") != "pending":
            raise RuntimeError("outbox_was_not_durable_before_restart")
        replay_client.close()
        clients.remove(replay_client)
        replay_client = FirebaseRtdbClient(
            database_url, token_provider, app_id, replay_root,
            allow_emulator=allow_emulator,
            request_timeout=min(max(float(timeout_seconds), 3.0), 30.0),
        )
        clients.append(replay_client)
        start = time.monotonic()
        replay_result = replay_client.sync_once()
        if (replay_result.get("status") != "synced" or
                replay_client.operation_status(offline_op).get("state") != "synced"):
            raise RuntimeError("durable_outbox_restart_replay_failed")
        _wait_until(
            lambda: (client_b.cached_record(record_offline) or {}).get("payload") ==
                    {"run_id": run_id, "phase": "durable-replay"},
            timeout_seconds,
            "second_client_did_not_receive_replayed_record",
        )
        report["checks"]["durable_outbox_restart_replay"] = {
            "status": "passed",
            "elapsed_ms": round((time.monotonic() - start) * 1000, 3),
            "scope": "queued-before-close, reopened, then replayed to Firebase",
        }
        report["status"] = "passed"
    except Exception as exc:
        # Networking exceptions may contain request URLs. Keep only stable,
        # short error codes and exception types in the report.
        message = str(exc)
        safe_code = (message if message.isascii() and len(message) <= 80 and "://" not in message
                     else "acceptance_failed")
        report["status"] = "failed"
        report["errors"].append({"type": type(exc).__name__, "code": safe_code})
    finally:
        cleanup_ok = True
        cleanup_errors = []
        cleanup_client = cleanup_client or client_a
        if cleanup_client is not None:
            for record_id in (record_sync, record_offline):
                try:
                    record = cleanup_client.cached_record(record_id)
                    if record is None:
                        result = cleanup_client.sync_once()
                        if result.get("status") not in ("synced", "conflict"):
                            cleanup_ok = False
                            cleanup_errors.append("cleanup_snapshot_" + str(result.get("status", "failed")))
                            continue
                        record = cleanup_client.cached_record(record_id)
                    if record is None:
                        cleanup_ok = False
                        cleanup_errors.append("cleanup_record_missing")
                        continue
                    if record.get("deleted"):
                        continue
                    delete_op = cleanup_client.queue_write(
                        record_id, record.get("payload", {}),
                        expected_revision=int(record["revision"]), deleted=True,
                    )
                    delete_result = _wait_operation(cleanup_client, delete_op, timeout_seconds)
                    if delete_result.get("state") != "synced":
                        cleanup_ok = False
                        cleanup_errors.append("cleanup_delete_" + str(delete_result.get("state", "failed")))
                        continue
                    _wait_until(
                        lambda rid=record_id: all(
                            (client.cached_record(rid) or {}).get("deleted") is True
                            for client in (client_a, client_b) if client is not None
                        ),
                        timeout_seconds,
                        "test_record_tombstone_not_observed",
                    )
                except Exception as exc:
                    cleanup_ok = False
                    message = str(exc)
                    code = (message if message.isascii() and len(message) <= 80 and "://" not in message
                            else type(exc).__name__)
                    cleanup_errors.append("cleanup_" + record_id[-8:] + "_" + code)
        report["cleanup"] = {
            "status": "tombstoned" if cleanup_ok else "failed",
            "errors": cleanup_errors,
        }
        if not cleanup_ok:
            report["status"] = "failed"
            report["errors"].append({"type": "CleanupError", "code": "test_record_cleanup_failed"})
        for client in reversed(clients):
            try:
                client.close()
            except Exception:
                report["status"] = "failed"
                report["errors"].append({"type": "ClientCloseError", "code": "client_close_failed"})
        temporary.cleanup()
    return report


def _prompt_value(label: str, current: str = "") -> str:
    return current if current else input(label).strip()


def load_auth_session(api_key: str, email: str, credential_target: str,
                      *, prompt_password=None, auth_class=FirebaseAuthSession):
    """Load a saved session; prompt only when no refresh token is stored."""
    prompt = prompt_password or getpass.getpass
    try:
        auth = auth_class.from_credential_manager(
            api_key, credential_target, username=email,
        )
    except FirebaseAuthenticationError as exc:
        if str(exc) != "firebase_refresh_token_not_found":
            raise
        password = prompt("Firebase test account password (not saved): ")
        if not password:
            raise ValueError("firebase_test_password_required")
        auth = auth_class.sign_in_with_password(
            api_key, email, password, credential_target,
        )
    auth.get_id_token()
    return auth


def _write_report(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Firebase Spark live acceptance (test namespace only)")
    parser.add_argument("--database-url", default="", help="Firebase RTDB root URL; prompted if omitted")
    parser.add_argument("--api-key", default="", help="Firebase Web API key; prompted if omitted")
    parser.add_argument("--email", default="", help="Firebase Auth test account; prompted if omitted")
    parser.add_argument("--app-id", default=DEFAULT_APP_ID,
                        help="dedicated Rules member namespace; must begin autosync-test-")
    parser.add_argument("--credential-target", default=DEFAULT_CREDENTIAL_TARGET,
                        help="Windows Credential Manager target for the refresh token")
    parser.add_argument("--timeout-seconds", type=float, default=20)
    parser.add_argument("--execute", action="store_true",
                        help="perform Auth/network requests and write bounded, tombstoned test records")
    parser.add_argument("--report", default="")
    args = parser.parse_args(argv)
    if not args.execute:
        print(json.dumps({
            "status": "plan-only",
            "network_used": False,
            "writes_performed": False,
            "target": "Firebase RTDB Spark over HTTPS/SSE",
            "app_id": args.app_id,
            "required_membership": "Firebase UID must be an explicit member of this test app namespace",
            "checks": ["two-client live change delivery", "same-revision conflict preservation",
                       "durable outbox close/reopen replay",
                       "fixed test records tombstoned and reused without record growth"],
            "live_command": "python tools/autosync_firebase_spark_acceptance.py --execute",
        }, ensure_ascii=False, indent=2))
        return 0
    if os.name != "nt":
        parser.error("live mode requires Windows Credential Manager")
    database_url = _prompt_value("Firebase Realtime Database URL: ", args.database_url)
    api_key = _prompt_value("Firebase Web API key: ", args.api_key)
    email = _prompt_value("Firebase Auth test account email: ", args.email)
    try:
        host = validate_live_target(database_url, args.app_id, args.timeout_seconds)
        if not api_key or not email:
            raise ValueError("firebase_api_key_and_test_email_required")
        auth = load_auth_session(api_key, email, args.credential_target)
        report = run_acceptance(
            database_url, auth.get_id_token, args.app_id, args.timeout_seconds,
        )
        report["authentication"] = "firebase-email-password"
        report["database_host"] = host
    except Exception as exc:
        message = str(exc)
        safe_code = (message if message.isascii() and len(message) <= 80 and "://" not in message
                     else "preflight_or_auth_failed")
        report = {
            "schema": 1,
            "status": "failed",
            "target": "firebase-realtime-database",
            "database_host": urllib.parse.urlsplit(database_url).hostname if database_url else None,
            "checks": {},
            "cleanup": {"status": "not_needed"},
            "errors": [{"type": type(exc).__name__, "code": safe_code}],
        }
    report_path = Path(args.report) if args.report else (
        ROOT / "tools" / "reports" /
        f"autosync-spark-live-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    )
    _write_report(report_path, report)
    print(json.dumps({"status": report["status"], "report": str(report_path.resolve()),
                      "checks": report.get("checks", {}),
                      "cleanup": report.get("cleanup", {}),
                      "errors": report.get("errors", [])}, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
