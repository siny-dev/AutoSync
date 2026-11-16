# 실시간동기화 (AutoSync)

**Status (2026-10-28 01:36 KST):** AutoSync 0.1.12 is published and all 7 package payload hashes verify. The latest full suite is 89 passed, 0 failed, 2 TLS-only skipped without a test certificate; host-task mock tests pass. The current Workboard adapter passed isolated integration for conflict preservation/resolution, offline replay, background delivery under 10 seconds, and two-way convergence (tools/reports/autosync-workboard-published-integration-20261028013234.json). A separate 30-write same-PC loopback push benchmark measured median 7.14 ms, p95 8.02 ms and max 11.09 ms; it is not LAN latency (tools/reports/autosync-local-push-latency-20261028013552.json). H255:8765 is TLS-reachable but its process owner/configuration is unknown; standard host configs and Siny-AutoSyncHost task are absent. Workboard is still not live-synced: current source is not built into shared 1.0.62, both PCs lack adapter configuration, and 5600G still runs the older per-PC executable.

**Historical cross-PC Workboard adapter fixture (2026-10-27, AutoSync 0.1.5):** The earlier STATUS.md narrative records one-shot synthetic task edits between H255 and 5600G, with both documents converging. No retained run-id/report for that fixture was found, so its exact scope is not independently reproducible. Preserve it as a historical record; it does not establish acceptance for the current AutoSync 0.1.12 module or current Workboard source.

AutoSync synchronizes one application's data between authorized instances. Each app gets its own app_id namespace; Workboard, Alarm, and other apps must never share records accidentally. AutoUpdate distributes code packages; AutoSync carries application data. The separate legacy AutoUpdate module named Sync (v1.0.0) is not AutoSync: Workboard can start it as an optional GitHub/snapshot worker. Keep it disabled when an AutoSync adapter is active for the same records; do not run two writers in parallel. AutoSync is the intended shared real-time data module.

The server database and each PC's durable outbox live on that PC's local disk, outside program/package folders. Do not put a live SQLite/WAL database on the shared SMB folder or use GitHub as the live data store.

## Implemented

- A single SQLite WAL writer with an OS-released per-data-directory process lock; shutdown closes active WebSockets, drains request handlers, closes SQLite, and only then releases the lock.
- Per-app namespaces, server revisions, ordered deltas, and deletion tombstones.
- Conditional writes (`If-Match`), idempotency keys, and preserved conflict alternatives; no last-write-wins overwrite.
- Per-PC SQLite outbox/cache that retains offline edits, retries safely, and resumes from durable cursors.
- WebSocket change hints followed by durable delta pulls; reconnect reconciles even if a hint was missed.
- App-scoped bearer credentials. The client rejects plain HTTP to non-loopback hosts. The server can load app tokens from Windows Credential Manager; a set/delete tool provisions the current user's per-app entries without placing tokens in command arguments or shared source files.
- TLS 1.2+ server transport and optional client CA file. LAN binds still require an explicit `--allow-lan` flag and a specific private IPv4 address.
- SQLite online backup through SQLite's backup API, including committed WAL content, integrity verification, atomic publication, and restore guarded by the single-writer lock. Restore preserves a pre-restore safety copy.
- Per-app event-history compaction is manual only: it obtains the exclusive writer lock, creates and integrity-checks a backup first, then compacts. A lagging client receives HTTP 409 `snapshot_required`; updated clients atomically reset the remote cache while preserving the offline outbox. Old clients stop safely without advancing their cursor. Tombstones, idempotency receipts, and unresolved conflicts are preserved.
- A read-only Windows host preflight script that reports OS/runtime, disk space, network interfaces, and sleep/power information without changing settings.
- An application adapter contract describing module linking, stable IDs, queued/synced/conflict states, and the unsolved crash gap between an app save and writing to the separate AutoSync outbox.
- A disposable memo reference app that closes that save-to-outbox crash gap with an app-side journal; it is a local adapter proof, not a production company-app integration.
- An AutoUpdate-backed host supervisor (src/autosync_host.py) that verifies the cached module, starts behind a readiness gate, drains the prior writer before a version switch, and protects the stable database with an online backup. Its local control API is loopback-only.
- A stable host bootstrap (tools/autosync_host_launcher.py) that resolves AutoSync from the verified AutoUpdate cache, validates every cached-file hash, and relaunches the supervisor when its own version changes. A current-user logon-task installer is provided; no production task is installed.

## Local loopback demonstration

From PowerShell:

```powershell
$root = '\\H255\AI-WORK\제작중인 프로그램\AutoSync'
$env:PYTHONPATH = $root
$env:PYTHONDONTWRITEBYTECODE = '1'
python -B -m unittest discover -s (Join-Path $root 'tests') -v

$token = python -c "import secrets; print(secrets.token_urlsafe(32))"
$env:AUTOSYNC_TOKENS = '{"test-app":"' + $token + '"}'
python (Join-Path $root 'src\autosync_server.py')
```

The demonstration server binds only to `127.0.0.1` by default. Its database defaults to `%LOCALAPPDATA%\Siny\AutoSync\data`. Do not use the demo token or environment-variable provisioning for a real service. Isolated tests cover Credential Manager lookup, rotation, and revocation; no production credential has been provisioned.

## Disposable module-link test app (not the first company rollout)

`examples\memo_demo.py` uses its own app ID (`autosync-memo-demo`), local memo database, and durable app-side change journal. `examples\app-modules.json` declares AutoSync API 1, and `examples\run-memo-demo.ps1` asks AutoUpdate to resolve and hash-verify the published module into its local cache before launching the app with that module on `PYTHONPATH`. The app replays journal rows with stable operation IDs, applies remote cache revisions only when no local edit is waiting, and surfaces queued/synced/conflict states. Its app-side journal closes the crash window between committing a note and enqueueing it in the separate AutoSync outbox. This is a disposable sample integration, not a company program rollout. AutoUpdate module releases are immutable: bump `src\__init__.py` to a new semantic version before publishing changed client code; apps request an API major and AutoUpdate selects a verified compatible version. AutoSync 0.1.5 uses `activation_mode: restart`: an already-running app keeps its loaded module until restart, and no in-process hot code reload is implemented. The host supervisor checks AutoUpdate on its configured interval and performs a controlled drain/restart for the server child when a verified module version changes. The sample launcher checks for a newer compatible client module each time it starts.

For a same-PC demonstration, start the server with a token scoped to the sample app, then run the sample from two different local data directories. Set `AUTOSYNC_DEMO_TOKEN` to the same temporary demo token used in the server's `AUTOSYNC_TOKENS` map:

```powershell
$env:AUTOSYNC_TOKENS = '{"autosync-memo-demo":"' + $token + '"}'
python (Join-Path $root 'src\autosync_server.py')

# In another PowerShell window, after the server is running:
$env:AUTOSYNC_DEMO_TOKEN = $token
python (Join-Path $root 'examples\memo_demo.py') --server 'http://127.0.0.1:8765' --data-dir "$env:LOCALAPPDATA\Siny\AutoSyncMemoDemo-PC1" put memo-1 'hello from PC1'
python (Join-Path $root 'examples\memo_demo.py') --server 'http://127.0.0.1:8765' --data-dir "$env:LOCALAPPDATA\Siny\AutoSyncMemoDemo-PC2" list
```

This is a disposable reference app, not an integration into Workboard or another company program. Its automated two-instance and crash-recovery tests use temporary databases on 5600G. The sample CLI was also run as separate processes with two isolated local data directories and synchronized edits in both directions over loopback HTTP and HTTPS/WSS; the TLS run used only a temporary self-signed localhost certificate for the test. The AutoUpdate module-link tests also resolve the published shared AutoSync package, verify its hash, and synchronize a sample memo between isolated app data directories. Separately, the published AutoSync 0.1.5 package passed an isolated H255↔5600G HTTPS pilot using the dedicated autosync-pilot-probe namespace: each client sent 29 synchronized writes, observed the peer, and received 28 round-trip acknowledgements. Median round-trip acknowledgements were 1,059.8 ms on 5600G and 1,064.4 ms on H255. The pilot used an ephemeral test certificate and random test token; all temporary processes and data were removed. This proves the published client/server can exchange synthetic data across these PCs, not that a persistent host or live Workboard integration is deployed.

## TLS/LAN pilot setup (temporary probe completed; no persistent service)

### Windows Credential Manager tokens

The server can read app tokens from the current Windows user's Credential Manager instead of AUTOSYNC_TOKENS. Store each server-side token under AutoSync/server/<app-id>. For the dedicated LAN probe, run locally on the chosen server host, in the same Windows account that will run the server (H255 is only the example candidate here; host selection is still open):

    $root = '\\H255\AI-Work\제작중인 프로그램\AutoSync'
    python (Join-Path $root 'tools\set_autosync_credential.py') set --target AutoSync/server/autosync-pilot-probe --generate

The generated token is displayed once so it can be entered, without echo, on each client PC:

    python (Join-Path $root 'tools\set_autosync_credential.py') set --target AutoSync/pilot-probe

Start the server with --credential-apps autosync-pilot-probe; it reads the server token without placing it in a command line or shared file. The server must run as the same Windows user that stored its credential. Pilot clients can use --credential-target AutoSync/pilot-probe; if omitted, the existing hidden prompt remains available. Credential Manager entries are per Windows user and machine. Never paste generated tokens into shared project documents or logs. Workboard client credentials use the target AutoSync/workboard. Server tokens are loaded into memory at server startup; after rotating or revoking a server-side token, restart the server, and restart/reconnect clients that hold the prior token in memory.

The server accepts a PEM certificate/key pair. A client can pass the trusted CA/certificate file as `ca_file` to `AutoSyncClient`. Example shape only:

```powershell
python (Join-Path $root 'src\autosync_server.py') `
  --host '<chosen-host-private-IPv4>' --port 8765 --allow-lan `
  --tls-cert '<approved-certificate-chain.pem>' --tls-key '<protected-private-key.pem>'
```

The certificate must match the host name clients use. A temporary H255 certificate/key and random pilot token were used only for the isolated test and then deleted. No production certificate/token, Credential Manager entry, persistent LAN service, firewall rule, or scheduled task was installed. H255 was reachable on the test port under the existing firewall policy; no firewall setting was changed. Production still requires selecting an always-on host, stabilizing its address, provisioning trusted TLS and per-app credentials, and installing the approved host lifecycle.

The read-only host check was run locally on H255 on 2026-10-27 09:27 KST and saved tools\reports\H255-preflight.json. H255 is Windows 10 x64 with supported Python 3.15.0/SQLite 3.53.4, about 860 GB free, DHCP IPv4 192.168.50.109, no sleep while on AC (DC sleep after four hours), and no AutoSync listener on port 8765. H255 could not connect to 5600G:8765 at the time of the check. This is a preflight snapshot only. A later temporary AutoSync 0.1.5 HTTPS probe reached H255 from 5600G, then shut the listener down; persistent service availability, address stability, production TLS trust, and credentials remain open. The script remains available for repeat checks:

```powershell
& "$env:WINDIR\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -NonInteractive -ExecutionPolicy Bypass -File '\\H255\AI-Work\제작중인 프로그램\AutoSync\tools\Test-AutoSyncHost.ps1' -ExpectedComputerName H255 -SaveReportToSharedFolder
```

Run that command on H255 itself. `-ExecutionPolicy Bypass` applies only to this PowerShell process; it does not change the machine policy. The optional switch saves `tools\reports\H255-preflight.json` to the shared project folder so the result can be reviewed from either PC. Running it from 5600G only reports 5600G.

## Two-PC pilot probe (dedicated test namespace)

After the H255 server, TLS trust, and an app-scoped token for `autosync-pilot-probe` are provisioned, run this command on each PC with the same `--run-id` and opposite instance/peer IDs. The script reads from Windows Credential Manager when --credential-target is supplied; otherwise it prompts without echo if `AUTOSYNC_PILOT_TOKEN` is not set; local outbox data stays under that PC's `%LOCALAPPDATA%\Siny\AutoSyncPilot\<run-id>\<instance-id>`. It writes only to the dedicated pilot namespace, never Workboard or another production app. `PASS` means this PC synced its own writes, received the peer's record, and observed at least one acknowledgement of its own update. The result reports median, p95, and maximum round-trip acknowledgement latency measured on that PC's monotonic clock, so the two PC clocks need not be synchronized. This is round-trip latency, not one-way delivery latency; the target-LAN measurements will determine the actual responsiveness.

Run on H255:
```powershell
$root = '\\H255\AI-WORK\제작중인 프로그램\AutoSync'
python (Join-Path $root 'tools\autosync_pilot.py') --server 'https://<H255-host>:8765' --ca-file '<trusted-ca.pem>' --run-id 'pilot-20261027a' --instance-id H255 --peer-id 5600G --seconds 60 --credential-target AutoSync/pilot-probe
```

On 5600G, use the same server, CA, run ID, and duration, with `--instance-id 5600G --peer-id H255`. Before running on either PC, store the server-generated token under AutoSync/pilot-probe in that PC account using the hidden-prompt command above. Add --credential-target AutoSync/pilot-probe to both probe commands. The published AutoSync 0.1.5 probe ran once across H255 and 5600G on 2026-10-27 with a temporary HTTPS listener, ephemeral certificate, and one-run token. The listener, token, certificate, and all probe data were removed. This does not test persistent service restart/reconnect or live Workboard data; production host setup and Workboard acceptance remain open.

## Verified server backup and restore

Backup is safe while the server is running; restore requires it to be stopped. The restore command refuses an active writer, verifies the snapshot, retains a pre-restore database copy, and moves stale WAL sidecars out of SQLite's way.

```powershell
$db = Join-Path $env:LOCALAPPDATA 'Siny\AutoSync\data\autosync.sqlite3'
python (Join-Path $root 'src\autosync_maintenance.py') backup `
  --database $db --destination 'D:\AutoSyncBackups\autosync.sqlite3'

# Stop the AutoSync writer first, then:
python (Join-Path $root 'src\autosync_maintenance.py') restore `
  --backup 'D:\AutoSyncBackups\autosync.sqlite3' --database $db
```

These commands have been tested against disposable local databases only. No production data, scheduled backup, or H255 restore has been tested. Client outboxes are PC-specific durable pending work; a separate backup/retention policy for them is still needed.

### Manual event-history compaction

Use only after selecting a cutoff under an agreed retention policy. This command refuses to run while the server owns the data directory, writes an integrity-checked backup, then compacts only the selected app's event rows. It never deletes current records/tombstones, operation receipts, or unresolved conflicts. There is no automatic compaction schedule.

```powershell
$throughRevision = 0 # safe no-op until a reviewed retention cutoff is selected
python (Join-Path $root 'src\autosync_maintenance.py') compact-events `
  --database $db `
  --backup-destination 'D:\AutoSyncBackups\before-compact.sqlite3' `
  --app 'workboard' `
  --through-revision $throughRevision
```

Clients older than the retained-history floor must support the snapshot reset protocol before compaction is operationally enabled. A client with an older module receives HTTP 409 and remains blocked until updated; it does not silently skip history.

## Before production rollout

1. Choose H255 or 5600G as the server host. Confirm/reserve its DHCP address, verify actual always-on availability, and recheck sleep behavior and free disk space during the LAN pilot.
2. Provision per-PC app-scoped tokens in Windows Credential Manager, then test rotation and revocation; do not expose tokens in logs or source files.
3. Provision a certificate whose SAN matches the chosen host name and CA trust model; verify HTTPS and WSS from both PCs.
4. Install and supervise exactly one writer on the chosen always-on host; narrowly scope the firewall, then test restart and recovery.
5. Run an actual two-PC pilot: near-live propagation, simultaneous edits/conflicts, offline queue/reconnect, lost acknowledgement, host reboot, backup and restore.
6. Add a conflict/status UI and app-specific connector. Workboard remains owned by Secretary 1; its source/package were not changed here.
7. Promote the published 0.1.12 module into a real application only after the protocol, connector contract, and two-PC pilot pass.

The draft connector contract is in contracts/adapter-v1.md. The Workboard migration and stable-ID plan is in contracts/workboard-autosync-adapter.md. No production data was changed in this AutoSync work.

## Test status

The latest TLS-enabled AutoSync source regression (2026-10-27 18:17 KST) passed **91/91 tests, 0 failures, 0 skips**. The published 0.1.12 package pointer, manifest, and all seven runtime file hashes match.

On 2026-10-27 at 19:01 KST, a temporary HTTPS LAN pilot passed between H255 and 5600G using published AutoSync 0.1.12 resolved and hash-verified through AutoUpdate on both PCs. In a 25-second run, both clients received the peer's test record; 5600G sent 20 test writes and observed 19 round-trip acknowledgements (median 1,157 ms), while H255 sent 23 writes and observed 22 acknowledgements (median 1,023 ms). These are round-trip acknowledgement measurements, not one-way latency or production Workboard acceptance.

A separate isolated Workboard `reconcile` bootstrap test passed with four divergent rows on published 0.1.12. All four IDs (`wb-task-000001`, `000003`, `000008`, `000009`) appeared as conflicts, and each local stale row and server canonical row remained preserved (`production_data_touched=false`).

Read-only runtime evidence distinguishes package cache from application activation: AutoUpdate's Workboard `current.json` selects the per-PC cached package only. H255 Workboard `app.log` confirms it applied and started version 1.0.59 at 18:30 KST. 5600G's official process still runs the old executable (SHA-256 `290641d3fc25c49b696248679a35cea17d1c37a9f94d8e061114267b37c9c442`) despite its 1.0.59 cache; its app-side cause is unresolved. The shared Workboard source, official executable, live data, configuration, and processes were not changed.

The Workboard adapter integration also passed on isolated synthetic data: seed-local/reconcile bootstrap, one-sided edit round-trip, same-row conflict preservation and explicit resolution, background delivery, offline outbox replay after worker restart, repeated historical-payload edits both directions, and convergence. Focused adapter tests pass 8/8 on shared Workboard source V1.0.059.

The pilot's temporary service, one-run TLS certificate and token, scoped firewall rule, and data roots were removed; independent checks confirmed H255 port 8765 is closed, both temp roots are absent, the remote firewall rule and local certificate-store entry are absent, and no pilot process remains. Official Workboard adapter configuration and persistent AutoSync host are still absent, so live Workboard rollout remains unverified. Workboard production files, executables, data, settings, and processes were not modified.

PowerShell host-task tests: `pwsh.exe -NoLogo -NoProfile -NonInteractive -File tests\test_host_task_installer.ps1` verifies an unrelated same-name task is never mutated, and `pwsh.exe -NoLogo -NoProfile -NonInteractive -File tests\test_host_task_install.ps1` runs the real config validator with mocked task registration to verify current-user logon, least-privilege principal, single-instance settings, and bounded restart behavior. Both are safe isolated tests; neither installs a Windows task.
