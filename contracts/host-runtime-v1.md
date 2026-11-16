# AutoSync host runtime contract v1

Status: AutoSync 0.1.5 is published and its TLS-enabled 5600G regression passes 80/80 with no skips. All seven module payload hashes match the immutable manifest. After launcher hardening, the stable bootstrap validated an isolated host config, resolved and launched the verified module, and served an authenticated app-state request; temporary credential, listener, and data were removed. The current-user logon-task installer is present but not installed. No production listener, credential, certificate, firewall rule, or host configuration is active.

## Purpose and boundary

AutoSync is a reusable module. Each program links the AutoSync client module to synchronize that program's records. A single designated host process runs the AutoSync server module for the authorized clients. The host is not copied into and started by every client program; doing so would create competing authorities. The server's database and backups stay on the host's local disk, outside AutoUpdate's versioned cache.

The server module and host supervisor are included in the verified AutoUpdate 0.1.5 package. src/autosync_host.py resolves the server from the verified cache and runs it as a child. A normal process cannot replace Python code already loaded into memory, so a newer package becomes active through a controlled drain and restart. The supervisor is implemented and locally tested, but no production service is installed.

## Components

1. **AutoUpdate** verifies and caches immutable AutoSync module versions. The host uses the existing app-module manifest and resolver contract, and accepts only a resolved path marked verified.
2. **Stable bootstrap and AutoSync host supervisor** run once on the selected host PC under the dedicated Windows account. The bootstrap resolves the verified module; the supervisor owns the server-child lifecycle and asks AutoUpdate to resolve on the configured update interval. Neither keeps a second copy of the server module.
3. **AutoSync server child** imports only from the verified, version-specific cache path. It owns the sole writer lock and the SQLite database while active.
4. **Application clients** run in their own programs and use app-scoped credentials, app_id namespaces, local caches, and durable outboxes. They never open the host database directly.

The supervisor and child must share the same configured Windows account so Credential Manager entries are available. The server's app credentials and TLS private key remain outside the module cache and source tree.

## Stable bootstrap self-update

The small entrypoint tools/autosync_host_launcher.py remains stable outside the versioned AutoSync package. On startup it calls the shared AutoUpdate CLI, accepts only the current verified AutoSync cache path, and checks every cached payload file against the immutable module manifest before launching the supervisor. The launcher holds a local single-instance lock so two logon triggers cannot create competing host supervisors.

The supervisor compares its own script hash with the verified candidate after each update check. If the supervisor code changed, it drains the server child and exits with code 75. The stable launcher then resolves AutoUpdate again and starts the new verified supervisor. A clean exit does not restart; repeated failures use bounded backoff and then stop with a manual-attention log entry.

tools/install_autosync_host_task.ps1 provides install, remove, start, stop, and status operations for a current-user, limited, interactive logon task. It verifies the AutoSync task-description marker before install, remove, start, stop, or status actions, and does not open firewall ports or elevate privileges. Installing or starting a production task still requires an approved host config, host selection, TLS trust, and credentials. The task is not installed by this package verification.

## Stable state and configuration

- Versioned code: AutoUpdate's verified cache only.
- Stable database: `%LOCALAPPDATA%\Siny\AutoSync\data\autosync.sqlite3` (or an explicitly configured local disk path); never SMB, GitHub, or a versioned cache directory.
- Host configuration: a separately ACL-protected file containing bind address, port, app IDs, Credential Manager target names, certificate paths, and database path. It contains no secret values.
- Persistent credentials: app-scoped entries in Windows Credential Manager. Application secrets and TLS private-key material are never placed in arguments, logs, manifests, or shared files. The supervisor-child control channel uses a fresh ephemeral token in the child environment; it is local-only, not persisted or logged, and is not an application credential.
- Backups: integrity-checked SQLite online backup stored outside the module cache. Retention and off-host copying are separate operational policy; a host module upgrade must not silently delete backups.
- Single-writer guard: retain the existing database writer lock and add a supervisor-instance guard so a second supervisor cannot start another server child.

## Update and restart sequence

1. Start the supervisor, obtain its single-instance guard, read the protected host configuration, and resolve AutoSync through AutoUpdate.
2. If the source is offline, use the last verified compatible cached version. Do not download unverified files or stop a healthy server just because an update check failed.
3. Verify the candidate's package signature/hash through AutoUpdate, module ID, API major, activation mode, version, and resolved cache path. If nothing changed, leave the running child alone.
4. Preflight the candidate without opening the live database for writes: import required modules, validate Python/runtime compatibility, validate config/certificate readability, verify required credential targets exist without logging their values, and check database schema compatibility. A failed preflight leaves the current child untouched.
5. If a release changes the database schema, first create and integrity-check a backup. Schema upgrades must be additive and readable by the immediately previous server version if code rollback is expected. A non-backward-compatible migration is blocked until it has a separate migration/rollback plan.
6. Ask the child to enter `draining` through the implemented control API bound only to 127.0.0.1. Requests require a fresh ephemeral bearer token passed only to that child process; the token is not persisted, logged, or put in command arguments. The endpoint does not bind to the LAN. The server stops accepting new writes, completes in-flight requests, closes WebSockets, closes SQLite, releases the writer lock last, and exits within a bounded timeout.
7. Confirm the old process exited and its writer lock is released before starting the candidate against the same stable database. If it fails to drain, keep the old version active and report a restart failure; never start a second writer over it.
8. Start the candidate in a non-serving `warming` state. It opens the database, runs integrity/schema checks, loads the configured credentials and TLS identity, and reports its exact module version and API/schema versions to the supervisor. Clients receive retryable unavailable status while the host is warming and keep their local outboxes.
9. Only after readiness succeeds does the supervisor activate serving. The supervisor records the active cached path, version, last successful health time, and last failed check without secrets.
10. If candidate startup or pre-serving readiness fails, stop it, wait for its writer lock to release, and start the previous verified cached version. Do not restore a database backup automatically after the new version has accepted writes; that could discard committed user changes. If no compatible rollback starts, leave the database intact, keep the service stopped, preserve clients' offline queues, and show `needs-attention`.
11. After activation, clients reconnect and reconcile from their durable cursors. Missed WebSocket hints are recovered through ordered delta pulls; offline outboxes retry idempotently. The UI must distinguish `offline/queued`, `syncing`, `synced`, `conflict`, and `service updating`.

## AutoUpdate runtime delivery levels

The existing AutoUpdate resolver already distinguishes package availability from a running host: a hash-verified resolved cache path is L1 and returns `runtime_state: not-reported`. The host must report the child process after import as `loaded` (L2), then report `active` (L3) only after readiness checks pass and the listener is serving. AutoUpdate exposes this through `report-module-status` / `Set-AuModuleRuntimeStatus`; the status record is checked against both process ID and process start time, so a reused PID is not mistaken for a live old process. The host supervisor must report the child PID, not its own, and remove stale status naturally by allowing the process identity check to fail after shutdown.

An isolated run of AutoUpdate/tests/test_modules.ps1 on 5600G passed the L1→L2→L3 reporting and CLI checks with a temporary source and state root. A separate real AutoUpdate CLI test resolved the published AutoSync 0.1.4 package in a temporary StateRoot, started the supervisor child, and verified L2 loaded followed by L3 active. AutoUpdate stores the latest state, so the final status record shows L3. These tests did not change production runtime.

## Visible states and logs

The supervisor status surface (service log/diagnostic command) reports: active module version and cache path, last check, update available, current lifecycle state, last successful health time, restart/rollback result, database path, backup path, and a short actionable error. It never prints tokens, private-key material, or full credential contents. Restart attempts use bounded backoff; a repeated failure stops the loop and enters `needs-attention` instead of flashing/restarting forever.

## Acceptance tests before calling the host module connected

1. Fresh host start resolves and launches the verified cached server module with a stable local database.
2. A second supervisor start is refused; a second writer cannot open the same data directory.
3. A new package is published; the next configured AutoUpdate check resolves its verified immutable cache path.
4. An invalid hash, incompatible API/runtime, missing credential, or unreadable certificate does not stop the healthy current child.
5. During a valid update, the old child drains and releases the lock before the new child starts; process logs prove no overlapping writer interval.
6. A client write made before restart survives and a waiting client write made during restart remains queued and is acknowledged once after reconnect.
7. The new server reports the expected module version, passes DB integrity/schema checks, then becomes serving; `/health` alone is insufficient unless it also reports readiness and runtime version.
8. A forced candidate startup failure before serving rolls back to the prior verified module and retains every committed record.
9. A post-activation crash restarts with bounded backoff; rollback is allowed only if database compatibility is declared. No automatic restore may erase writes accepted after activation.
10. A network outage during version check keeps the last verified version serving; no half-downloaded package is activated.
11. Power loss/reboot between drain and candidate activation leaves exactly one recoverable process and preserves SQLite/WAL integrity.
12. Logs and diagnostics contain no credentials; the loopback control API requires its ephemeral token and cannot be reached from another PC.

## Completion gates

The supervisor and server readiness/drain controls are implemented and locally tested, but this contract does not make AutoSync production-ready by itself. Production remains gated on selecting one always-on host, verifying its address and power behavior, provisioning production TLS trust and app-scoped credentials, approving the service/firewall scope, and passing the actual two-PC application acceptance test. Workboard remains under its separate owner; no production Workboard data may be changed as part of this host-runtime work without coordination.