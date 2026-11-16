# AutoSync application adapter contract (draft v1)

This is the boundary each program uses to connect to the shared AutoSync client module. It is a draft until one approved application connector passes the two-PC pilot.

## Module ownership

- A program references the versioned AutoSync client module; it does not copy the synchronization engine into its own package.
- Each program supplies a permanent, lowercase `app_id`. The ID is its data-isolation namespace and must not be reused by another program.
- Each installation stores its own client database/outbox under that PC user's local data directory. It never stores live synchronization state in the shared program folder.
- The server URL, app-scoped token, trusted CA, and local data directory are configuration, not source code. Current environment-variable token setup is demonstration-only.

## Record contract

- The application assigns a stable `record_id` that is identical on every PC. Do not use a PC-local row number or a timestamp as the identity.
- `payload` is one complete JSON object representing that record at a revision. Keep each record at or below the current 1 MiB limit.
- Delete by sending a tombstone (`deleted=true`); do not remove local state until the app has retained the deletion for sync.
- `revision` is assigned by the server. Client wall clocks do not decide which edit wins.

## Write and status flow

1. Persist the edit in the application's own durable store.
2. Call `queue_write(record_id, full_payload, expected_revision, deleted, operation_id=durable_journal_id)` and retain that stable operation ID until the outbox acknowledges it. One operation ID identifies one logical write: the effective app, record, payload, deleted flag, and expected revision must remain unchanged on retries. The outbox persists the original expected revision; if a caller supplies it again, it must match. An identical retry is idempotent; reuse with any changed request field (including only a newer expected revision) is rejected as `idempotency_key_reused`. A later user edit is a new logical write and must receive a new durable journal ID, even when its payload bytes happen to match an earlier edit.
3. Show `queued/offline` while it is pending. Show `synced` only after `sync_once()` reports success and the outbox has no pending edits for that record.
4. Start one `watch(stop_event, on_status)` worker per client instance while the app is active. On a `changed` hint or reconnect, the module fetches durable deltas and refreshes its cache.
5. Before editing a record, use the latest cached server revision. If the client has not reconciled yet, synchronize first.
6. On a conflict, show both the incoming edit and the latest server value. Do not silently pick one. The user or app-specific merge rule must explicitly produce a combined payload, then call `resolve_conflict(operation_id, merged_payload, current_revision, deleted, resolution_operation_id=durable_journal_id)`.

`conflicts()` returns local unresolved outbox conflicts. `operation_status(operation_id)` reports pending/sending/synced/conflict state; `cached_records()` returns the latest remote cache for the app. `resolve_conflict` accepts only the primary revision conflict and requires the exact `current_revision` shown in that response. A successful resolution creates a new idempotency operation; the original conflict remains in the local audit as resolved.
Before submitting a conflict choice, persist the chosen full payload, the displayed `current_revision`, and one generated `resolution_operation_id` in the application journal. Retry that exact choice with the same ID after a crash, timeout, or repeated click until it is acknowledged. Reusing the ID with different content is rejected; a new user choice needs a new ID. If the server revision moved after the UI loaded the choices, expect another conflict and refresh the comparison before asking again. Choosing the current server payload still creates a resolution write and revision; the current API does not provide a discard-only operation.

## Module update and activation

- Resolve each module through the verified AutoUpdate module during the app's normal update checks. Keep the last verified module version active if resolution fails.
- When a module reports activation_mode=restart, restart only the background AutoSync worker; the app UI stays open. Compare the newly resolved immutable module path/version with the worker's loaded path/version.
- Stop the old worker cleanly before starting the replacement so two workers never write through the same app instance at once. Reuse the same per-PC local root and durable outbox; do not clear pending operations or delete an old module directory while a process may still have it loaded.
- Publish loaded module version, worker PID, and activation state in local status. If the new worker fails to start, keep the prior verified package and report/retry the pending activation instead of losing queued work.
- Acceptance test: keep the app UI process running, resolve a new module version, and confirm the worker switches to that exact version without duplicate writes. Include one offline edit in the durable outbox; after the worker restart it must drain once and reach the peer. Verify rollback to the previous cached module if startup fails.
## Crash boundary that each connector must close

The current AutoSync outbox is a separate SQLite database from the application's own database. A crash after the app saves a change but before `queue_write` commits can leave an edit that is local but not queued. The sample adapter in `examples\memo_demo.py` closes this gap by committing the app data and a stable-ID change journal in one SQLite transaction, then replaying the journal idempotently on startup. Production adapters must use the same pattern or an atomic shared outbox:

- replaying a durable application change journal at startup until every edit is acknowledged; or
- using an application database transaction that also commits a durable outbox row (requires a future injectable/shared outbox adapter).

Until the connector proves recovery from this exact crash point, the app must not claim the edit is safely synchronized. This contract is why AutoSync is not yet ready to connect to Workboard or company data.

## Lifecycle and UI obligations

- Keep the client local root unique per PC and app; never point two PCs at the same client SQLite file.
- Close the client and signal the watcher to stop during orderly shutdown. Pending writes remain on disk for the next process.
- Surface at least `synced`, `queued/offline`, `retry`, `blocked/auth`, `conflict`, and `committed-refresh-pending` distinctly.
- Provide explicit conflict inspection and resolution. Do not remove the local conflict record until the user has chosen a merge.
- Do not report exact real-time latency until measured on the target LAN.

## Acceptance before first app integration

The connector must pass stable-ID mapping, save/enqueue crash recovery, app restart, offline queue replay, simultaneous conflicting edits, explicit resolution, deletion propagation, and upgrade/rollback tests on two PCs. Workboard remains under Secretary 1's ownership; this draft does not alter its source or package.
