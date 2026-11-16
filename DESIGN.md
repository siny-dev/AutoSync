# AutoSync design and review

Product name: 실시간동기화 (AutoSync). “Real-Time Sync” may appear as an internal description, not the product name.

## Purpose

AutoSync synchronizes the data of one application between its authorized instances. Every application receives a separate app_id namespace. Workboard data cannot mix with Alarm data. AutoUpdate distributes versioned program/module files; AutoSync moves app data. The AutoSync database and client outbox stay outside AutoUpdate package/version directories.

GitHub remains suitable for source and release packages, not as the live database for frequently edited app data. There is also a separate legacy AutoUpdate module named Sync (v1.0.0) that Workboard can start as an optional worker. Its GitHub/snapshot flow is not this product's real-time AutoSync transport. Do not run that writer beside an AutoSync adapter for the same records. AutoSync is the intended shared real-time module; Workboard integration must consume the verified AutoSync module rather than treating the legacy Sync module as equivalent.

## Chosen shape

- A single writer process is authoritative for a given AutoSync data directory. A process lock prevents a second server instance from opening it as another live writer. During shutdown the server closes active WebSockets, drains request handlers, closes SQLite, and releases the lock last, preventing an overlapping writer handoff.
- The writer keeps its SQLite WAL database on the host's local disk. A shared-folder SQLite database is prohibited because SMB locking and WAL behavior do not provide the desired reliable multi-PC service.
- Apps use HTTP for snapshots, ordered deltas, and conditional writes. An authenticated WebSocket carries change hints; clients then fetch durable changes by cursor. WebSocket delivery is an accelerator, never the source of truth.
- Each app has a separate bearer credential; a leaked app credential does not grant access to the other app namespaces. Each PC stores a local durable outbox and a remote cache. Edits remain queued while offline. The UI must show queued/offline separately from synced.
- A write includes the last server revision it was based on. A stale revision returns a conflict and preserves both alternatives. There is no last-writer-wins, silent overwrite, or automatic choice.
- Idempotency keys make retries safe after a dropped response. Deletions are explicit tombstones.
- If the service is unreachable, clients keep local queued work and retry with bounded backoff. On every reconnect they reconcile from the last durable cursor.
- There is no automatic writer failover. Promoting a replacement writer requires fencing the old host so two authorities cannot accept edits at once.

## Why this improves the first idea

1. Push notification alone can be missed during sleep or a network outage. Durable server revisions and reconnect pulls close that gap.
2. Comparing file timestamps can overwrite newer edits when clocks differ. Revision preconditions detect stale writes without relying on PC clocks.
3. A single active writer avoids split-brain. Replicating SQLite files through SMB is not a safe substitute.
4. Keeping each app's namespace separate lets the same reusable module serve multiple programs without blending their state.
5. A client reports “synced” only after the server acknowledges a durable transaction; a queued local edit is never mislabeled as synchronized.
6. AutoUpdate can version the AutoSync module later, while app data remains in a stable location through module upgrades.

## Phased delivery

### Phase 1 — local protocol proof (current)

Use one local writer and two separate client databases on 5600G. Prove app isolation, push followed by durable pull, idempotency, stale conflict preservation, offline queue persistence, and a single writer lock. Do not use company data. TLS 1.2+ and client CA wiring now exist; a local HTTPS/WSS handshake and sync test passed using a temporary self-signed localhost certificate generated for the test. This does not validate the eventual H255 certificate or LAN path.

### Phase 2 — fenced LAN pilot

Before enabling any LAN listener:
- choose and confirm the always-on host (H255's 2026-10-27 local preflight supports the runtime and reports no AC sleep, but actual always-on availability is not yet verified);
- confirm/reserve H255's DHCP address (192.168.50.109 at preflight), power/sleep behavior, and supported Python/runtime version;
- use the implemented TLS 1.2+ path and provision per-app tokens through the Windows Credential Manager provider; test rotation and revocation;
- confirm the host's local data directory, backup schedule, restore procedure, and free-space monitoring;
- install a supervised service and restrict its firewall rule to the intended private network and client PCs;
- run two-PC interruption, simultaneous edit, reboot, duplicate-server, and restore tests.
- Use `tools\autosync_pilot.py` from both PCs with a unique shared run ID and the dedicated `autosync-pilot-probe` token. It requires each process to see the peer and acknowledge one of its own writes; it reports round-trip acknowledgement latency using a local monotonic clock (not one-way latency and not dependent on matching PC clocks). The probe is tested using two local subprocesses but has not been run over the H255 LAN.

The Windows Credential Manager token provider and provisioning CLI are implemented, but no production token has been provisioned. No TLS listener, Windows service, firewall rule, or remote installation has been performed. LAN bind requires both a private IPv4 address and an explicit `--allow-lan` flag; plaintext LAN is refused.

### Phase 3 — module and first application connector

AutoSync 0.1.5 is published to the shared AutoUpdate module source and verified through the resolver. Its seven-file package includes the client, protocol, server, host supervisor, credential provider, and maintenance modules. The supervisor now resolves the verified cache, gates child startup, drains the old writer before a version change, backs up SQLite, and refuses overlapping writers. This is locally implemented and tested but is not installed as a production service. Keep company apps out of the module manifest until the host is provisioned and the connector contract and two-PC acceptance pass. The first approved app connector must identify stable records, persist its local change/outbox safely, consume remote deltas without erasing unsent edits, show queued/conflict/synced states, and resume after app restart.

A disposable reference app now exists in `examples\memo_demo.py`. Its manifest declares AutoSync API 1; its launcher resolves the published shared module through AutoUpdate, verifies the cached package, and runs the app with that module. It persists a note plus stable-ID change journal atomically, replays after both app-save and outbox-enqueue crash points, and consumes remote revisions. This validates the adapter mechanics locally; it is not a company app connector and does not bypass Phase 2.

Workboard remains gated because Secretary 1 owns it. Before integration, its data format, stable IDs, per-PC input paths, and shared baseline must be documented and approved. No Workboard source or package was edited here.

### Server module consumer and safe restart gate

AutoSync 0.1.5 packages the server runtime and host supervisor. examples/host-modules.json describes the host's AutoUpdate link; examples/host-config.example.json is placeholder-only. The supervisor resolves only a verified cache, stores stable data outside the version cache, and expects app credentials in the same Windows account's Credential Manager. The supervisor and child must use the selected host's approved TLS identity before any LAN bind.

The host supervisor is implemented in src/autosync_host.py. It checks the configured update interval (safe 10-minute fallback), backs up the SQLite database, asks the old child to drain, waits for lock release, starts the verified candidate behind a readiness gate, and refuses to start a second writer. The stable launcher in tools/autosync_host_launcher.py resolves the current verified module and restarts the supervisor when its script hash changes. A current-user logon task installer is available but has not been installed. A temporary Windows end-to-end test proved a 0.1.4-to-temporary-0.1.5 switch with a synthetic write preserved. AutoUpdate was exercised with temporary state: L1 resolve, child launch, L2 loaded, then L3 active. The full 0.1.5 regression passed 79/79 with TLS enabled. These are local tests only. No production service, listener, certificate, credential, firewall rule, or Workboard configuration is installed. Detailed lifecycle and acceptance cases remain in contracts/host-runtime-v1.md.


### Phase 4 — rollout and Internet option

Roll out other apps one at a time with a distinct app_id and migration/rollback plan. Internet operation needs a separately reviewed TLS relay/service and database scaling plan; it is not achieved by pointing clients at GitHub or exposing an SMB share.

## Current proof results

On 2026-10-27 14:45 KST, the 0.1.5 full TLS regression on 5600G ran 80 tests: 80 passed, 0 failed, 0 skipped. Host-control, supervisor, stable-bootstrap, and AutoUpdate module-link cases passed. The published manifest hash is f59fb68752ffebbf3df4766cbb1271993782bdc7fd110e150c68dceea74ef62a. A temporary Windows handoff test preserved a synthetic record through a controlled server restart. The stable bootstrap was rerun after config hardening: it validated isolated settings, resolved and launched the verified 0.1.5 host, served an authenticated app-state request, then the temporary credential, listener, and data were removed. This proves the local implementation path, not production deployment.

The earlier H255–5600G HTTPS pilot used AutoSync 0.1.0, synthetic data, and temporary credentials/certificate. It remains separate evidence. The H255 full 0.1.4 regression and real two-PC Workboard acceptance are still unconfirmed. No production service/listener, TLS trust, credential, firewall rule, or real Workboard data was changed.

### Host token-provider implementation

A Windows Credential Manager provider, set/delete utility, and pilot-client credential lookup are implemented. Server entries are app scoped under AutoSync/server/<app-id>; the server must run as the same Windows user that owns those entries. The 0.1.2 regression includes the Credential Manager round-trip and token-to-server-auth integration using temporary test targets that are deleted. A separate loopback check verified that server token rotation/revocation takes effect after restart; no H255 production token, service, TLS certificate, or firewall change was made.

## Event-history retention and compaction (manual and guarded)

Keep scheduled history deletion disabled during the pilot. AutoSync now stores per-app `last_rev` and `history_floor`. When a client's cursor is older than the floor, the server returns HTTP 409 `snapshot_required` with a complete snapshot; the updated client atomically replaces only its remote cache and preserves its durable local outbox. A client running older code treats the 409 as blocked and cannot silently advance past missing history. Tombstones stay in snapshots.

An explicit maintenance command takes the single-writer lock, creates and integrity-checks a SQLite backup, and only then deletes event rows through the requested revision. Idempotency receipts and unresolved conflicts are not compacted. This is a manual command, not a schedule. Regression tests cover the cursor boundary, old-client response, reset atomicity, offline outbox preservation, migration, verified backup, and refusal while the live writer holds its lock.

Still open before production: select an event-retention window and maximum offline/retry window; add a schedule only after those choices; schedule and verify off-host backups; monitor database size and backup age; perform an H255 restore drill. Treat audit-history retention separately from sync recovery: compaction is not a permanent audit log. Do not automatically compact events, operation receipts, conflicts, or tombstones during the pilot.
## Remaining design choices

- Host selection and availability target.
- A trusted TLS certificate/CA, production credential provisioning/rotation test, and supervised host service.
- How conflicts are displayed and resolved in each app.
- The crash window between an app saving locally and enqueueing into AutoSync's separate outbox; the app-specific adapter must close it with a durable journal or an atomic outbox.
- Select and test an automatic retention/offline-window policy; schedule verified off-host backups and complete an H255 restore drill before production.
- The desired cross-PC latency target. “Real-time” means push-triggered reconciliation with durable reconnect; a numeric latency guarantee requires measurement on the actual LAN.
- App-specific local file/database transaction boundaries and migration strategy.







