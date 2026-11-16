"""Two-physical-PC Firebase RTDB acceptance handshake for AutoSync 0.1.29.

Run the peer role on H255 first, then the initiator role on 5600G with the same
run ID. Both processes must connect to the same Firebase Spark test namespace.
The only shared cloud record is a stable test record and successful runs
tombstone it. This tool never reads or writes Workboard files.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import socket
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.autosync_firebase_rtdb import FirebaseRtdbClient
from autosync_firebase_spark_acceptance import (
    _prompt_value,
    load_auth_session,
    validate_live_target,
)

APP_ID = "autosync-test-spark"
RECORD_ID = "cross-device-acceptance"
RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{5,63}$")


def wait_until(predicate, timeout: float, error_code: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    if predicate():
        return
    raise TimeoutError(error_code)


def local_host_id() -> str:
    raw = os.environ.get("COMPUTERNAME") or socket.gethostname()
    return re.sub(r"[^A-Za-z0-9._-]+", "-", raw.strip())[:40] or "unknown-host"


def wait_operation(client: FirebaseRtdbClient, operation_id: str, timeout: float) -> dict:
    result: dict = {}

    def terminal() -> bool:
        nonlocal result
        result = client.operation_status(operation_id)
        return result.get("state") in ("synced", "conflict")

    wait_until(terminal, timeout, "operation_timeout")
    if result.get("state") != "synced":
        raise RuntimeError("firebase_write_conflict")
    return result


def current_record(client: FirebaseRtdbClient) -> dict | None:
    return client.cached_record(RECORD_ID)


def matching_record(client: FirebaseRtdbClient, run_id: str, phase: str,
                    *, deleted: bool = False) -> dict | None:
    record = current_record(client)
    if not isinstance(record, dict):
        return None
    payload = record.get("payload")
    if not isinstance(payload, dict):
        return None
    if payload.get("run_id") != run_id or payload.get("phase") != phase:
        return None
    if bool(record.get("deleted")) != deleted:
        return None
    return record


def write_record(client: FirebaseRtdbClient, run_id: str, phase: str,
                 device_label: str, host_id: str, timeout: float,
                 *, deleted: bool = False) -> dict:
    record = current_record(client)
    revision = int(record.get("revision", 0)) if isinstance(record, dict) else 0
    payload = {
        "run_id": run_id,
        "phase": phase,
        "device": device_label[:40],
        "host_id": host_id,
        "updated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    operation_id = client.queue_write(
        RECORD_ID, payload, expected_revision=revision, deleted=deleted,
    )
    wait_operation(client, operation_id, timeout)
    result = current_record(client)
    if not isinstance(result, dict) or result.get("payload") != payload:
        raise RuntimeError("local_write_not_observed")
    if bool(result.get("deleted")) != deleted:
        raise RuntimeError("local_tombstone_not_observed")
    return result


def run(args: argparse.Namespace) -> dict:
    result = {
        "status": "running",
        "run_id": args.run_id,
        "role": args.role,
        "device": (args.device_label or os.environ.get("COMPUTERNAME") or "unknown")[:40],
        "host_id": local_host_id(),
        "database_host": None,
        "app_id": APP_ID,
        "checks": {},
        "cleanup": "not_started",
        "errors": [],
    }
    temp = None
    client = None
    try:
        if not RUN_ID_RE.fullmatch(args.run_id):
            raise ValueError("run_id_must_be_6_to_64_safe_characters")
        database_url = _prompt_value("Firebase Realtime Database URL: ", args.database_url)
        api_key = _prompt_value("Firebase Web API key: ", args.api_key)
        email = _prompt_value("Firebase Auth test account email: ", args.email)
        host = validate_live_target(database_url, APP_ID, args.timeout_seconds)
        result["database_host"] = host
        # Keep auth initialization inside the result/error boundary. Refresh-token
        # failures now produce a sanitized report instead of an unhandled traceback.
        auth = load_auth_session(api_key, email, args.credential_target)
        temp = tempfile.TemporaryDirectory(prefix="autosync-cross-pc-")
        client = FirebaseRtdbClient(
            database_url, auth.get_id_token, APP_ID, Path(temp.name),
            request_timeout=min(max(float(args.timeout_seconds), 3.0), 30.0),
        )
        snapshot = client.sync_once()
        if snapshot.get("status") != "synced":
            raise RuntimeError("firebase_initial_read_failed")
        existing = current_record(client)
        if existing and not existing.get("deleted"):
            payload = existing.get("payload")
            if not isinstance(payload, dict) or payload.get("run_id") != args.run_id:
                raise RuntimeError("cross_device_test_record_busy")
        client.start()

        if args.role == "initiator":
            current = current_record(client)
            if current and not current.get("deleted"):
                raise RuntimeError("cross_device_test_record_not_clean")
            started = time.monotonic()
            initial = write_record(
                client, args.run_id, "initiator_to_peer", result["device"], result["host_id"],
                args.timeout_seconds,
            )
            result["checks"]["initiator_write"] = "passed"
            wait_until(
                lambda: matching_record(client, args.run_id, "peer_to_initiator") is not None,
                args.timeout_seconds,
                "peer_did_not_receive_and_reply",
            )
            peer_record = matching_record(client, args.run_id, "peer_to_initiator")
            peer_device = peer_record["payload"].get("device", "unknown")
            peer_host_id = peer_record["payload"].get("host_id")
            if not isinstance(peer_host_id, str) or peer_host_id == result["host_id"]:
                raise RuntimeError("paired_roles_must_run_on_different_hosts")
            result["checks"]["cross_pc_round_trip"] = "passed"
            final = write_record(
                client, args.run_id, "peer_to_initiator", result["device"], result["host_id"],
                args.timeout_seconds, deleted=True,
            )
            result["checks"]["cloud_tombstone"] = "passed"
            result["peer_device"] = str(peer_device)[:40]
            result["peer_host_id"] = peer_host_id
            result["elapsed_ms"] = round((time.monotonic() - started) * 1000, 3)
            result["cleanup"] = "tombstoned"
            result["status"] = "passed"
        else:
            print(json.dumps({
                "status": "ready",
                "role": "peer",
                "run_id": args.run_id,
                "device": result["device"],
                "host_id": result["host_id"],
                "waiting_for": "initiator_to_peer",
            }, ensure_ascii=False), flush=True)
            started = time.monotonic()
            wait_until(
                lambda: matching_record(client, args.run_id, "initiator_to_peer") is not None,
                args.timeout_seconds,
                "initiator_message_not_received",
            )
            initiator_record = matching_record(client, args.run_id, "initiator_to_peer")
            initiator_device = initiator_record["payload"].get("device", "unknown")
            initiator_host_id = initiator_record["payload"].get("host_id")
            if not isinstance(initiator_host_id, str) or initiator_host_id == result["host_id"]:
                raise RuntimeError("paired_roles_must_run_on_different_hosts")
            result["checks"]["cross_pc_receive"] = "passed"
            write_record(
                client, args.run_id, "peer_to_initiator", result["device"], result["host_id"],
                args.timeout_seconds,
            )
            result["checks"]["peer_reply"] = "passed"
            wait_until(
                lambda: matching_record(
                    client, args.run_id, "peer_to_initiator", deleted=True,
                ) is not None,
                args.timeout_seconds,
                "peer_did_not_observe_cloud_tombstone",
            )
            result["checks"]["remote_tombstone_observed"] = "passed"
            result["initiator_device"] = str(initiator_device)[:40]
            result["initiator_host_id"] = initiator_host_id
            result["elapsed_ms"] = round((time.monotonic() - started) * 1000, 3)
            result["cleanup"] = "tombstoned"
            result["status"] = "passed"
    except Exception as exc:
        result["status"] = "failed"
        message = str(exc)
        code = message if message.isascii() and len(message) <= 80 and "://" not in message else type(exc).__name__
        result["errors"].append({"type": type(exc).__name__, "code": code})
    finally:
        if client is not None:
            try:
                # The initiator safely tombstones its own active test record after errors.
                record = current_record(client)
                payload = record.get("payload") if isinstance(record, dict) else None
                if (args.role == "initiator" and isinstance(payload, dict)
                        and payload.get("run_id") == args.run_id and not record.get("deleted")):
                    cleanup_op = client.queue_write(
                        RECORD_ID, payload,
                        expected_revision=int(record.get("revision", 0)), deleted=True,
                    )
                    wait_operation(client, cleanup_op, min(args.timeout_seconds, 30))
                    result["cleanup"] = "tombstoned_after_failure"
            except Exception:
                result["cleanup"] = "failed"
            try:
                client.close()
            except Exception:
                result["errors"].append({"type": "ClientCloseError", "code": "client_close_failed"})
                result["status"] = "failed"
        if temp is not None:
            temp.cleanup()
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Paired physical-PC AutoSync Firebase RTDB test")
    parser.add_argument("--role", choices=("peer", "initiator"), required=True)
    parser.add_argument("--run-id", required=True, help="use the exact same safe ID on both PCs")
    parser.add_argument("--device-label", default="", help="optional label shown in the sanitized result")
    parser.add_argument("--database-url", default="")
    parser.add_argument("--api-key", default="")
    parser.add_argument("--email", default="")
    parser.add_argument("--credential-target", default="")
    parser.add_argument("--report", default="", help="optional per-device JSON result path")
    parser.add_argument("--timeout-seconds", type=float, default=120)
    parser.add_argument("--execute", action="store_true", help="perform bounded Firebase test writes")
    args = parser.parse_args(argv)
    if args.timeout_seconds < 10 or args.timeout_seconds > 300:
        parser.error("--timeout-seconds must be between 10 and 300")
    if not args.execute:
        print(json.dumps({
            "status": "planned",
            "network_used": False,
            "writes_performed": False,
            "role": args.role,
            "run_id": args.run_id,
            "app_id": APP_ID,
            "credential_target": args.credential_target or (
                "AutoSync/autosync-test-spark/account-2" if args.role == "peer"
                else "AutoSync/autosync-test-spark/account-1"
            ),
            "next": "rerun with --execute on both PCs using the same --run-id",
        }, ensure_ascii=False, indent=2))
        return 0

    if not args.credential_target:
        args.credential_target = (
            "AutoSync/autosync-test-spark/account-2" if args.role == "peer"
            else "AutoSync/autosync-test-spark/account-1"
        )
    result = run(args)
    if args.report.strip():
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = report_path.with_name(report_path.name + ".tmp")
        temp_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        os.replace(temp_path, report_path)
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    return 0 if result.get("status") == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())

