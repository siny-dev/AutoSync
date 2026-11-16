# AutoSync protocol v1 (prototype)

Every request carries Authorization: Bearer <token>. Each token is scoped to one app_id; a token for one app cannot read or write another app namespace. Loopback HTTP is allowed for isolated tests. Non-loopback clients must use HTTPS/WSS with a trusted certificate. The server refuses LAN bind unless TLS 1.2+ and explicit `--allow-lan` are provided for a specific private IPv4 address. An approved credential store and rotation procedure are still required before deployment.

- GET /health confirms that the process responds.
- GET /v1/apps/{app_id}/state returns an authoritative snapshot and current app cursor.
- GET /v1/apps/{app_id}/state?since={cursor} returns durable ordered changes after that cursor.
- POST /v1/apps/{app_id}/records/{record_id} accepts {"payload": {...}, "deleted": false}; it requires If-Match: {record_revision} (0 for a new record) and a unique Idempotency-Key.
- GET /v1/apps/{app_id}/stream opens an authenticated WebSocket. The server sends ready, then changed hints. After a hint, fetch changes from the last durable cursor. Reconnect also fetches from that cursor.

A stale If-Match returns 409 and retains both the saved and incoming values. A client must merge explicitly and submit a new write against the current revision. A 200 response means the server committed the operation transactionally. Local queued/offline data is not described as synchronized until the server acknowledges it.

For data protection, `autosync_maintenance.py backup` uses SQLite's online backup API and validates the resulting database before an atomic publish. `restore` requires the writer lock to be free, validates the snapshot, preserves a pre-restore copy, and quarantines stale WAL/SHM sidecars before replacement. These mechanics are locally tested on disposable databases only; backup scheduling and off-host retention are not configured.

app_id is the isolation boundary. Deleted records remain as tombstones. There is no automatic conflict winner, automatic failover, or compaction policy. The client outbox is local to one PC and stores no credentials.

