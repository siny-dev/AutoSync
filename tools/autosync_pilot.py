"""Two-PC AutoSync pilot probe. Uses a dedicated namespace and per-PC local data."""
from __future__ import annotations

import argparse
import getpass
import json
import os
import re
import sys
import threading
import time
import uuid
import math
from datetime import datetime, timezone
from pathlib import Path
MODULE_ROOT = Path(__file__).resolve().parents[1]
if str(MODULE_ROOT) not in sys.path:
    sys.path.insert(0, str(MODULE_ROOT))

from src import AutoSyncClient
from src.autosync_protocol import RECORD_RE
from src.autosync_credentials import CredentialError, read_secret

APP_ID = "autosync-pilot-probe"
RUN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,35}$")
INSTANCE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,31}$")


def parse_args():
    parser = argparse.ArgumentParser(description="Two-PC AutoSync propagation probe (test data only)")
    parser.add_argument("--server", required=True, help="AutoSync HTTPS URL; HTTP is accepted only on loopback")
    parser.add_argument("--ca-file", type=Path, help="trusted CA/certificate for the AutoSync HTTPS server")
    parser.add_argument("--run-id", required=True, help="same unique ID on both PCs, e.g. pilot-20261027a")
    parser.add_argument("--instance-id", required=True, help="this PC's short ID, e.g. H255 or 5600G")
    parser.add_argument("--peer-id", required=True, help="the other PC's short ID")
    parser.add_argument("--seconds", type=int, default=30, help="probe duration (10-600 seconds)")
    parser.add_argument("--interval", type=float, default=1.0, help="write interval in seconds (0.25-10)")
    parser.add_argument("--data-dir", type=Path, help="local-only AutoSync client data directory")
    parser.add_argument("--credential-target", help="read the token from the current Windows user Credential Manager")
    args = parser.parse_args()
    if not RUN_RE.fullmatch(args.run_id):
        parser.error("--run-id must use letters, numbers, dot, underscore, or hyphen (max 36 chars)")
    if not INSTANCE_RE.fullmatch(args.instance_id) or not INSTANCE_RE.fullmatch(args.peer_id):
        parser.error("instance IDs must use letters, numbers, dot, underscore, or hyphen (max 32 chars)")
    if args.instance_id.lower() == args.peer_id.lower():
        parser.error("--instance-id and --peer-id must be different")
    if not 10 <= args.seconds <= 600:
        parser.error("--seconds must be from 10 to 600")
    if not 0.25 <= args.interval <= 10:
        parser.error("--interval must be from 0.25 to 10")
    own_id = f"pilot.{args.run_id}.{args.instance_id}"
    peer_record_id = f"pilot.{args.run_id}.{args.peer_id}"
    if not RECORD_RE.fullmatch(own_id) or not RECORD_RE.fullmatch(peer_record_id):
        parser.error("generated pilot record ID exceeds the AutoSync record ID format")
    args.own_record_id = own_id
    args.peer_record_id = peer_record_id
    if args.data_dir is None:
        local = Path(os.environ.get("LOCALAPPDATA", str(Path.home())))
        args.data_dir = local / "Siny" / "AutoSyncPilot" / args.run_id / args.instance_id
    return args


def main():
    args = parse_args()
    token = os.environ.get("AUTOSYNC_PILOT_TOKEN")
    if not token and args.credential_target:
        try:
            token = read_secret(args.credential_target)
        except CredentialError as exc:
            print(json.dumps({"result": "BLOCKED", "detail": str(exc)}, ensure_ascii=False))
            return 2
    if not token:
        token = getpass.getpass("AutoSync pilot app token: ")
    client = AutoSyncClient(args.server, token, APP_ID, args.data_dir, ca_file=args.ca_file)
    stop = threading.Event()
    watcher = threading.Thread(target=client.watch, args=(stop,), daemon=True)
    peer_seen = False
    synced_writes = 0
    highest_peer_sequence = 0
    sent_at = {}
    acknowledged = set()
    roundtrip_ms = []
    sequence = 0
    synced_writes = 0
    last_peer_sequence_published = 0
    blocked_detail = None

    def observe_peer():
        nonlocal peer_seen, highest_peer_sequence
        peer = client.cached_record(args.peer_record_id)
        if not peer or peer["payload"].get("run_id") != args.run_id:
            return
        peer_seen = True
        highest_peer_sequence = max(highest_peer_sequence, int(peer["payload"].get("sequence", 0)))
        peer_seen_sequence = int(peer["payload"].get("peer_seen_sequence", 0))
        observed_at = time.monotonic()
        for sequence, written_at in tuple(sent_at.items()):
            if sequence <= peer_seen_sequence and sequence not in acknowledged:
                acknowledged.add(sequence)
                roundtrip_ms.append((observed_at - written_at) * 1000)

    def publish_update():
        nonlocal sequence, synced_writes, last_peer_sequence_published, blocked_detail
        sequence += 1
        payload = {
            "run_id": args.run_id,
            "instance_id": args.instance_id,
            "sequence": sequence,
            "peer_seen_sequence": highest_peer_sequence,
            "written_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        }
        sent_at[sequence] = time.monotonic()
        operation = client.queue_write(
            args.own_record_id, payload,
            expected_revision=client.record_revision(args.own_record_id),
            operation_id=str(uuid.uuid4()),
        )
        result = client.sync_once()
        if result.get("status") == "blocked":
            blocked_detail = result.get("detail")
            return
        if client.operation_status(operation)["state"] == "synced":
            synced_writes += 1
            last_peer_sequence_published = highest_peer_sequence
        observe_peer()

    watcher.start()
    deadline = time.monotonic() + args.seconds
    started_at = time.monotonic()
    try:
        initial = client.sync_once()
        if initial.get("status") == "blocked":
            print(json.dumps({"result": "BLOCKED", "detail": initial.get("detail")}, ensure_ascii=False))
            return 2
        while time.monotonic() < deadline:
            observe_peer()
            publish_update()
            if blocked_detail:
                print(json.dumps({"result": "BLOCKED", "detail": blocked_detail}, ensure_ascii=False))
                return 2
            time.sleep(min(args.interval, max(0, deadline - time.monotonic())))

        # The last regular write can arrive just as the write window ends. Keep
        # a bounded drain period so each process can publish an acknowledgement
        # and observe the peer's acknowledgement of its own update.
        drain_deadline = time.monotonic() + min(15, max(10, args.seconds // 2))
        while not roundtrip_ms and time.monotonic() < drain_deadline:
            observe_peer()
            if peer_seen and highest_peer_sequence > last_peer_sequence_published:
                publish_update()
            if blocked_detail:
                print(json.dumps({"result": "BLOCKED", "detail": blocked_detail}, ensure_ascii=False))
                return 2
            if not roundtrip_ms:
                time.sleep(min(args.interval, max(0, drain_deadline - time.monotonic())))
        observe_peer()
        sorted_roundtrip = sorted(roundtrip_ms)
        p95_index = max(0, math.ceil(len(sorted_roundtrip) * 0.95) - 1)
        status = "PASS" if peer_seen and synced_writes and roundtrip_ms else "INCOMPLETE"
        print(json.dumps({
            "result": status,
            "app_id": APP_ID,
            "run_id": args.run_id,
            "instance_id": args.instance_id,
            "peer_id": args.peer_id,
            "own_synced_writes": synced_writes,
            "peer_record_seen": peer_seen,
            "highest_peer_sequence_seen": highest_peer_sequence,
            "roundtrip_acknowledgements": len(roundtrip_ms),
            "roundtrip_rtt_ms_median": round(sorted_roundtrip[len(sorted_roundtrip) // 2], 1) if sorted_roundtrip else None,
            "roundtrip_rtt_ms_p95": round(sorted_roundtrip[p95_index], 1) if sorted_roundtrip else None,
            "roundtrip_rtt_ms_max": round(sorted_roundtrip[-1], 1) if sorted_roundtrip else None,
            "elapsed_seconds": round(time.monotonic() - started_at, 2),
            "local_data_dir": str(args.data_dir),
        }, ensure_ascii=False))
        return 0 if status == "PASS" else 2
    except KeyboardInterrupt:
        print("Probe interrupted; any unacknowledged test edit remains in the local outbox.")
        return 130
    finally:
        stop.set()
        watcher.join(timeout=3)
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
