import json, pathlib, shutil, subprocess, sys, tempfile, threading, time
from unittest.mock import patch

sys.dont_write_bytecode = True

auto = pathlib.Path(r"\\H255\AI-Work\제작중인 프로그램\AutoUpdate")
workboard = pathlib.Path(r"\\H255\AI-Work\제작중인 프로그램\Workboard")
cli = auto / "tools" / "autoupdate.ps1"
module_source = auto / "outbox" / "modules"
current_pointer = module_source / "AutoSync" / "current.json"
expected_version = json.loads(current_pointer.read_text(encoding="utf-8"))["version"]
pwsh = shutil.which("pwsh.exe") or shutil.which("pwsh")
if not pwsh:
    raise SystemExit("PowerShell 7 is unavailable")

temp = tempfile.TemporaryDirectory(prefix="AutoSync-Workboard-Published-")
root = pathlib.Path(temp.name)
server = None
server_thread = None
workers = []
run_threads = []
result = {"status": "FAIL"}
try:
    manifest_path = root / "app-modules-AutoSync.json"
    manifest_path.write_text(json.dumps({
        "schema": 1,
        "contract": "autoupdate-app-modules-v1",
        "app_id": "workboard",
        "modules": [{"module_id": "AutoSync", "api_major": 1}],
    }), encoding="utf-8")
    resolve = subprocess.run([
        pwsh, "-NoLogo", "-NoProfile", "-NonInteractive", "-File", str(cli),
        "-Command", "resolve-modules", "-StateRoot", str(root / "autoupdate-state"),
        "-AppManifestPath", str(manifest_path), "-ModuleSourceRoot", str(module_source),
    ], stdin=subprocess.DEVNULL, capture_output=True, text=True, encoding="utf-8",
       errors="replace", timeout=180, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    if resolve.returncode:
        raise RuntimeError("AutoUpdate module resolution failed")
    resolved = json.loads(resolve.stdout.strip())
    rows = [row for row in resolved.get("modules", []) if row.get("module_id") == "AutoSync"]
    if (resolved.get("status") != "ready" or resolved.get("app_id") != "workboard" or len(rows) != 1
            or rows[0].get("version") != expected_version or rows[0].get("verified") is not True
            or rows[0].get("api_major") != 1):
        raise RuntimeError(f"Workboard did not resolve the verified AutoSync {expected_version} module")
    module_root = pathlib.Path(rows[0]["module_path"])
    if not module_root.is_dir():
        raise RuntimeError("verified AutoSync cache directory is missing")

    sys.path.insert(0, str(module_root))
    sys.path.insert(1, str(workboard))
    from src.autosync_server import create_server
    from tools.workboard_autosync_adapter import Worker, seed_document

    token = "published-module-integration-test-token-0123456789"
    server = create_server("127.0.0.1", 0, root / "server-data",
                           {"workboard": token}, tls_context=None, allow_lan=False)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    canonical = """# Workboard task list

| 순서 | 지시 | 상태 | 남은 일·완료 기준 |
|---|---|---|---|
| 1 | AutoSync 실제 패키지 시험 | 50% · 진행 | 두 PC 간 왕복 |
| 2 | 충돌 보존 | 20% · 진행 | 두 원본 보존 |
## 완료 업무

| 순서 | 업무 | 상태 | 완료 근거 |
|---|---|---|---|
| 완료 | 격리 테스트 준비 | 100% · 완료 | 임시 데이터만 사용 |
"""
    workers_by_name = {}
    files = {}
    for name, mode in (("h255", "seed-local"), ("5600g", "reconcile")):
        source = root / f"{name}.md"
        source.write_text(seed_document(canonical), encoding="utf-8")
        files[name] = source
        config = {
            "schema": 1,
            "app_id": "workboard",
            "server_url": f"http://127.0.0.1:{server.server_address[1]}",
            "source_path": str(source),
            "local_root": str(root / name / "state"),
            "bootstrap_mode": mode,
        }
        with patch("tools.workboard_autosync_adapter.read_credential", return_value=token):
            worker = Worker(config, module_root)
        workers.append(worker)
        workers_by_name[name] = worker

    first = workers_by_name["h255"].cycle(force=True)
    second = workers_by_name["5600g"].cycle(force=True)
    if first != "synced" or second != "synced":
        raise RuntimeError("identical Workboard bootstrap did not converge")
    if workers_by_name["h255"].client.conflicts() or workers_by_name["5600g"].client.conflicts():
        raise RuntimeError("unexpected conflict for identical initial documents")

    files["h255"].write_text(
        files["h255"].read_text(encoding="utf-8").replace("50% · 진행", "75% · 진행"),
        encoding="utf-8")
    edit_push = workers_by_name["h255"].cycle(force=True)
    edit_pull = workers_by_name["5600g"].cycle(force=True)
    if edit_push != "synced" or edit_pull != "synced":
        raise RuntimeError("Workboard edit did not round-trip through the published module")
    if "75% · 진행" not in files["5600g"].read_text(encoding="utf-8"):
        raise RuntimeError("peer Workboard document did not receive the edit")
    if files["h255"].read_bytes() != files["5600g"].read_bytes():
        raise RuntimeError("Workboard documents did not converge byte-for-byte")
    if workers_by_name["h255"].client.conflicts() or workers_by_name["5600g"].client.conflicts():
        raise RuntimeError("unexpected conflict after one-sided edit")

    files["h255"].write_text(
        files["h255"].read_text(encoding="utf-8").replace("75% · 진행", "82% · 진행", 1),
        encoding="utf-8")
    files["5600g"].write_text(
        files["5600g"].read_text(encoding="utf-8").replace("75% · 진행", "68% · 진행", 1),
        encoding="utf-8")
    if workers_by_name["h255"].cycle(force=True) != "synced":
        raise RuntimeError("first concurrent Workboard edit did not commit")
    if workers_by_name["5600g"].cycle(force=True) != "conflict":
        raise RuntimeError("same-row concurrent Workboard edit was not surfaced as a conflict")
    pending = workers_by_name["5600g"].client.conflicts()
    if len(pending) != 1 or pending[0]["record_id"] != "wb-task-000001":
        raise RuntimeError("same-row conflict journal is missing or contains unexpected rows")
    conflict = pending[0]
    if ("68% · 진행" not in conflict["payload"]["row"]
            or "82% · 진행" not in conflict["current"]["payload"]["row"]):
        raise RuntimeError("conflict journal did not preserve both Workboard versions")
    if "68% · 진행" not in files["5600g"].read_text(encoding="utf-8"):
        raise RuntimeError("local conflicting Workboard edit was overwritten")
    if "82% · 진행" not in workers_by_name["5600g"]._remote_records()["wb-task-000001"]["row"]:
        raise RuntimeError("server's committed Workboard edit was lost")

    files["5600g"].write_text(
        files["5600g"].read_text(encoding="utf-8").replace("68% · 진행", "90% · 진행", 1),
        encoding="utf-8")
    if workers_by_name["5600g"].cycle(force=True) != "synced":
        raise RuntimeError("explicit Workboard conflict resolution did not commit")
    if workers_by_name["h255"].cycle(force=True) != "synced":
        raise RuntimeError("peer did not receive explicit Workboard conflict resolution")
    if workers_by_name["5600g"].client.conflicts():
        raise RuntimeError("resolved Workboard conflict remains open")
    if files["h255"].read_bytes() != files["5600g"].read_bytes():
        raise RuntimeError("Workboard documents did not converge after explicit conflict resolution")
    if "90% · 진행" not in files["h255"].read_text(encoding="utf-8"):
        raise RuntimeError("resolved Workboard value did not reach the peer")

    # Reapply historical task values at newer server revisions. Each is a new edit,
    # even when its payload matches an earlier committed version.
    revision_before_repeats = workers_by_name["h255"].client.record_revision("wb-task-000001")
    for value in ("82% · 진행", "90% · 진행"):
        current = "90% · 진행" if value == "82% · 진행" else "82% · 진행"
        text = files["h255"].read_text(encoding="utf-8")
        if current not in text:
            raise RuntimeError("historical-payload fixture does not contain the expected current value")
        files["h255"].write_text(text.replace(current, value, 1), encoding="utf-8")
        if workers_by_name["h255"].cycle(force=True) != "synced":
            raise RuntimeError("reapplying a historical payload failed on the sender")
        if workers_by_name["5600g"].cycle(force=True) != "synced":
            raise RuntimeError("reapplying a historical payload failed on the peer")
        if value not in files["5600g"].read_text(encoding="utf-8"):
            raise RuntimeError("reapplied historical payload did not reach the peer")
        if files["h255"].read_bytes() != files["5600g"].read_bytes():
            raise RuntimeError("Workboard documents diverged after a historical-payload re-edit")
    revision_after_repeats = workers_by_name["h255"].client.record_revision("wb-task-000001")
    if revision_after_repeats != revision_before_repeats + 2:
        raise RuntimeError("both repeated payloads did not commit as distinct revisions")
    reverse_revision_before = workers_by_name["5600g"].client.record_revision("wb-task-000001")
    for value in ("68% · 진행", "90% · 진행"):
        current = "90% · 진행" if value == "68% · 진행" else "68% · 진행"
        text = files["5600g"].read_text(encoding="utf-8")
        if current not in text:
            raise RuntimeError("reverse historical-payload fixture does not contain the expected current value")
        files["5600g"].write_text(text.replace(current, value, 1), encoding="utf-8")
        if workers_by_name["5600g"].cycle(force=True) != "synced":
            raise RuntimeError("reapplying a historical payload failed on the reverse sender")
        if workers_by_name["h255"].cycle(force=True) != "synced":
            raise RuntimeError("reapplying a historical payload failed on the reverse peer")
        if value not in files["h255"].read_text(encoding="utf-8"):
            raise RuntimeError("reverse historical payload did not reach the peer")
        if files["h255"].read_bytes() != files["5600g"].read_bytes():
            raise RuntimeError("Workboard documents diverged after a reverse historical-payload re-edit")
    reverse_revision_after = workers_by_name["5600g"].client.record_revision("wb-task-000001")
    if reverse_revision_after != reverse_revision_before + 2:
        raise RuntimeError("both reverse repeated payloads did not commit as distinct revisions")
    run_errors = {}
    def run_worker(name, worker):
        try:
            worker.run(parent_pid=None, once=False)
        except BaseException as exc:
            run_errors[name] = exc
    for name in ("h255", "5600g"):
        thread = threading.Thread(target=run_worker,
                                  args=(name, workers_by_name[name]), daemon=True)
        run_threads.append(thread)
        thread.start()
    time.sleep(0.5)
    if any(not thread.is_alive() for thread in run_threads) or run_errors:
        raise RuntimeError("Workboard background workers did not stay active")
    files["h255"].write_text(
        files["h255"].read_text(encoding="utf-8").replace("90% · 진행", "95% · 진행", 1),
        encoding="utf-8")
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        if run_errors:
            raise RuntimeError("Workboard background worker failed")
        if ("95% · 진행" in files["5600g"].read_text(encoding="utf-8")
                and files["h255"].read_bytes() == files["5600g"].read_bytes()):
            break
        time.sleep(0.05)
    else:
        raise RuntimeError("background Workboard edit did not reach peer within 10 seconds")
    for worker in workers:
        worker.stop.set()
    for thread in run_threads:
        thread.join(timeout=5)
    if any(thread.is_alive() for thread in run_threads) or run_errors:
        raise RuntimeError("Workboard background workers did not stop cleanly")
    offline_config = dict(workers_by_name["h255"].config)
    offline_config["server_url"] = "http://127.0.0.1:1"
    with patch("tools.workboard_autosync_adapter.read_credential", return_value=token):
        offline_worker = Worker(offline_config, module_root)
    workers.append(offline_worker)
    workers_by_name["h255"] = offline_worker
    files["h255"].write_text(
        files["h255"].read_text(encoding="utf-8").replace("95% · 진행", "97% · 진행", 1),
        encoding="utf-8")
    if offline_worker.cycle(force=True) != "offline":
        raise RuntimeError("offline Workboard edit was not reported as queued offline work")
    if offline_worker.client.outbox.pending_count("workboard") != 1:
        raise RuntimeError("offline Workboard edit was not durably queued")
    if "97% · 진행" not in files["h255"].read_text(encoding="utf-8"):
        raise RuntimeError("offline Workboard edit was not kept visible")
    offline_worker.client.close()

    reconnect_config = dict(offline_config)
    reconnect_config["server_url"] = f"http://127.0.0.1:{server.server_address[1]}"
    with patch("tools.workboard_autosync_adapter.read_credential", return_value=token):
        reconnected = Worker(reconnect_config, module_root)
    workers.append(reconnected)
    workers_by_name["h255"] = reconnected
    if reconnected.cycle(force=True) != "synced":
        raise RuntimeError("durable offline Workboard edit did not replay after worker restart")
    if reconnected.client.outbox.pending_count("workboard") != 0:
        raise RuntimeError("replayed Workboard outbox still has pending edits")
    peer_config = dict(workers_by_name["5600g"].config)
    with patch("tools.workboard_autosync_adapter.read_credential", return_value=token):
        reconnected_peer = Worker(peer_config, module_root)
    workers.append(reconnected_peer)
    workers_by_name["5600g"] = reconnected_peer
    if reconnected_peer.cycle(force=True) != "synced":
        raise RuntimeError("peer did not recover the replayed offline Workboard edit")
    if files["h255"].read_bytes() != files["5600g"].read_bytes():
        raise RuntimeError("offline replay did not converge the two Workboard copies")
    if "97% · 진행" not in files["5600g"].read_text(encoding="utf-8"):
        raise RuntimeError("replayed offline edit did not reach the peer")
    result = {"status": "PASS", "module_version": rows[0]["version"],
              "module_verified": rows[0]["verified"], "app_id": resolved["app_id"],
              "bootstrap": "seed-local/reconcile", "initial_sync": "PASS",
              "one_sided_edit_round_trip": "PASS", "repeated_historical_payloads_both_directions": "PASS",
              "same_row_conflict": "PRESERVED_BOTH",
              "explicit_conflict_resolution": "PASS", "final_convergence": "PASS",
              "background_worker_delivery_under_10s": "PASS",
              "offline_queue_replay_after_worker_restart": "PASS",
              "isolated": True, "production_data_touched": False}
except Exception as exc:
    result = {"status": "FAIL", "reason": f"{type(exc).__name__}: {exc}"}
finally:
    for worker in workers:
        try:
            worker.stop.set()
        except Exception:
            pass
    for thread in run_threads:
        thread.join(timeout=5)
    for worker in workers:
        try:
            worker.client.close()
        except Exception:
            pass
    if server is not None:
        server.shutdown()
        server.server_close()
    if server_thread is not None:
        server_thread.join(timeout=5)
    temp.cleanup()
print(json.dumps(result, ensure_ascii=False, sort_keys=True))
raise SystemExit(0 if result["status"] == "PASS" else 1)
