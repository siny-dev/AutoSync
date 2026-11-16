"""Verify reconcile bootstrap preserves four divergent Workboard rows on published AutoSync."""
from __future__ import annotations
import argparse, json, pathlib, sys, tempfile, threading, time
from unittest.mock import patch

TOOLS = pathlib.Path(__file__).resolve().parent
AUTOSYNC = TOOLS.parent
AUTO_UPDATE = pathlib.Path(r"\\H255\AI-Work\제작중인 프로그램\AutoUpdate")
WORKBOARD = pathlib.Path(r"\\H255\AI-Work\제작중인 프로그램\Workboard")
TOKEN = "isolated-workboard-four-conflict-test-token-0123456789"
IDS = [f"wb-task-{n:06d}" for n in (1, 3, 8, 9)]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=pathlib.Path)
    args = parser.parse_args()
    sys.path.insert(0, str(TOOLS))
    import autosync_lan_pilot_server
    result = {"status": "FAIL", "production_data_touched": False, "expected_conflict_ids": IDS}
    with tempfile.TemporaryDirectory(prefix="AutoSync-Workboard-FourConflict-") as temp_name:
        root = pathlib.Path(temp_name)
        try:
            version, module_root, _ = autosync_lan_pilot_server.resolve_module(
                AUTO_UPDATE, root / "resolver-state", "workboard")
            result["module_version"] = version
            if version != "0.1.12":
                raise RuntimeError("published AutoSync module version changed; update the test expectation")
            sys.path.insert(0, str(module_root))
            sys.path.insert(1, str(WORKBOARD))
            from src.autosync_server import create_server
            from tools.workboard_autosync_adapter import Worker, seed_document

            canonical = """# Isolated Workboard bootstrap conflict fixture

| 순서 | 지시 | 상태 | 남은 일·완료 기준 |
|---|---|---|---|
| 1 | Canonical task 1 | 10% · 진행 | primary-1 |
| 3 | Canonical task 3 | 30% · 진행 | primary-3 |
| 8 | Canonical task 8 | 80% · 진행 | primary-8 |
| 9 | Canonical task 9 | 90% · 진행 | primary-9 |
"""
            stale = canonical
            for n in (1, 3, 8, 9):
                stale = stale.replace(f"primary-{n}", f"stale-local-{n}")
            server = create_server("127.0.0.1", 0, root / "server-data",
                                   {"workboard": TOKEN}, tls_context=None, allow_lan=False)
            server_thread = threading.Thread(target=server.serve_forever, daemon=True)
            server_thread.start()
            workers = []
            try:
                def worker(name: str, document: str, mode: str):
                    source = root / f"{name}.md"
                    source.write_text(seed_document(document), encoding="utf-8")
                    config = {"schema": 1, "app_id": "workboard",
                        "server_url": f"http://127.0.0.1:{server.server_address[1]}",
                        "source_path": str(source), "local_root": str(root / name / "state"),
                        "credential_target": "AutoSync/workboard", "bootstrap_mode": mode}
                    with patch("tools.workboard_autosync_adapter.read_credential", return_value=TOKEN):
                        instance = Worker(config, module_root)
                    workers.append(instance)
                    return instance, source

                primary, primary_file = worker("h255", canonical, "seed-local")
                secondary, secondary_file = worker("5600g", stale, "reconcile")
                primary_status = primary.cycle(force=True)
                secondary_status = secondary.cycle(force=True)
                conflicts = secondary.client.conflicts()
                conflict_map = {item["record_id"]: item for item in conflicts}
                local_text = secondary_file.read_text(encoding="utf-8")
                canonical_text = primary_file.read_text(encoding="utf-8")
                remote = secondary._remote_records()
                preserved = (set(conflict_map) == set(IDS)
                    and all(f"stale-local-{n}" in local_text for n in (1, 3, 8, 9))
                    and all(f"primary-{n}" in canonical_text for n in (1, 3, 8, 9))
                    and all(f"primary-{n}" in remote[rid]["row"]
                            for n, rid in zip((1, 3, 8, 9), IDS)))
                alternatives = all(
                    f"stale-local-{n}" in conflict_map[rid]["payload"]["row"]
                    and f"primary-{n}" in conflict_map[rid]["current"]["payload"]["row"]
                    for n, rid in zip((1, 3, 8, 9), IDS)
                ) if set(conflict_map) == set(IDS) else False
                result.update({"primary_status": primary_status, "reconcile_status": secondary_status,
                    "conflict_ids": sorted(conflict_map), "conflict_count": len(conflicts),
                    "all_local_and_server_versions_preserved": preserved,
                    "both_versions_in_each_conflict": alternatives})
                if primary_status != "synced" or secondary_status != "conflict" or not preserved or not alternatives:
                    raise RuntimeError("four-row first-bootstrap conflict preservation check failed")
                result["status"] = "PASS"
            finally:
                for instance in workers:
                    instance.client.close()
                server.shutdown()
                server.server_close()
                server_thread.join(timeout=3)
        except Exception as exc:
            result["error_type"] = type(exc).__name__
            result["error"] = str(exc)[:1000]
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
