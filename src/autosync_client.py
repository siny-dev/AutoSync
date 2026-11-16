"""Durable AutoSync client queue and reconnecting WebSocket watcher."""
from __future__ import annotations
import hashlib, hmac, http.client, json, random, re, select, socket, sqlite3, ssl, threading, time, urllib.error, urllib.parse, urllib.request, uuid
from pathlib import Path
from .autosync_protocol import APP_RE, OP_RE, RECORD_RE, canon, check_id

MAX_WS_TEXT_FRAME = 65_536
MAX_INLINE_PUSH_BYTES = 48 * 1024
MAX_PARTITIONED_PENDING_PUSH = 4096
WATCH_RETRY_INITIAL_SECONDS = 1.0
WATCH_RETRY_MAX_SECONDS = 30.0

def frame_client(payload, opcode=1):
    mask=__import__("os").urandom(4); n=len(payload)
    if n<126: head=bytes((0x80|opcode,0x80|n))
    elif n<=65535: head=bytes((0x80|opcode,0x80|126))+n.to_bytes(2,"big")
    else: head=bytes((0x80|opcode,0x80|127))+n.to_bytes(8,"big")
    return head+mask+bytes(b^mask[i%4] for i,b in enumerate(payload))

class SyncRejected(Exception):
    pass

class WebSocketProtocolError(ValueError):
    pass

class WebSocketHandshakeError(ConnectionError):
    def __init__(self,status):
        self.status=status
        super().__init__(f"websocket_http_{status}")

def connection_status(exc):
    reason=getattr(exc,"reason",None)
    if isinstance(exc,WebSocketHandshakeError):
        if exc.status in (401,403):
            return {"status":"blocked","detail":f"server_http_{exc.status}"}
        return {"status":"offline","detail":f"websocket_http_{exc.status}"}
    if isinstance(exc,WebSocketProtocolError):
        return {"status":"blocked","detail":"websocket_protocol_error"}
    if isinstance(exc,ssl.SSLError) or isinstance(reason,ssl.SSLError):
        return {"status":"blocked","detail":"tls_verification_or_protocol_error"}
    return {"status":"offline","detail":type(exc).__name__}

class IPv4HTTPSConnection(http.client.HTTPSConnection):
    """Prefer IPv4 for the IPv4-only AutoSync service, preserving DNS-name TLS verification."""
    def connect(self):
        addresses=socket.getaddrinfo(self.host,self.port,socket.AF_INET,socket.SOCK_STREAM,socket.IPPROTO_TCP)
        last_error=None
        for family,socktype,proto,_,sockaddr in addresses:
            raw=None
            try:
                raw=socket.create_connection(sockaddr,self.timeout,self.source_address)
                self.sock=self._context.wrap_socket(raw,server_hostname=self.host)
                return
            except OSError as exc:
                last_error=exc
                if raw is not None:
                    try:raw.close()
                    except OSError:pass
        if last_error is not None:raise last_error
        raise OSError(f"no IPv4 address available for {self.host}")

class Outbox:
    """Per-PC durable operation queue and remote cache; credentials are never persisted."""
    def __init__(self,path):
        self.path=Path(path); self.path.parent.mkdir(parents=True,exist_ok=True)
        self.lock=threading.RLock()
        self.db=sqlite3.connect(str(self.path),timeout=5,isolation_level=None,check_same_thread=False)
        self.db.row_factory=sqlite3.Row
        self.db.execute("PRAGMA busy_timeout=5000"); self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("PRAGMA journal_mode=WAL")
        version=self.db.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0,1): raise RuntimeError("unsupported client database version")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS queue(seq INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT UNIQUE, app TEXT,
          rid TEXT, expected INTEGER, payload TEXT, deleted INTEGER, state TEXT, response TEXT, created TEXT);
        CREATE INDEX IF NOT EXISTS queue_order ON queue(state,seq);
        CREATE TABLE IF NOT EXISTS cache(app TEXT,rid TEXT,rev INTEGER,payload TEXT,deleted INTEGER,updated TEXT,PRIMARY KEY(app,rid));
        CREATE TABLE IF NOT EXISTS cursors(app TEXT PRIMARY KEY,cursor INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS cursors_v2(app TEXT PRIMARY KEY,cursor_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS cursor_v2_seen(app TEXT NOT NULL,bucket INTEGER NOT NULL,seq INTEGER NOT NULL,
          PRIMARY KEY(app,bucket,seq));
        """)
        self.db.execute("PRAGMA user_version=1")
        with self.lock: self.db.execute("UPDATE queue SET state='pending' WHERE state='sending'")
    def close(self):
        with self.lock: self.db.close()
    def cursor(self,app):
        with self.lock:
            row=self.db.execute("SELECT cursor FROM cursors WHERE app=?",(app,)).fetchone()
            return int(row["cursor"]) if row else 0
    def has_cursor(self,app):
        with self.lock:
            return self.db.execute("SELECT 1 FROM cursors WHERE app=?",(app,)).fetchone() is not None
    def record_revision(self,app,rid):
        with self.lock:
            row=self.db.execute("SELECT rev FROM cache WHERE app=? AND rid=?",(app,rid)).fetchone()
            return int(row["rev"]) if row else 0
    def queue_write(self,app,rid,payload,expected=None,deleted=False,operation_id=None):
        if not isinstance(app,str) or not APP_RE.fullmatch(app) or not isinstance(rid,str) or not RECORD_RE.fullmatch(rid):
            raise ValueError("invalid app or record id")
        if not isinstance(payload,dict) or not isinstance(deleted,bool): raise ValueError("invalid payload")
        stable_op=operation_id is not None
        op=check_id(operation_id,OP_RE) if stable_op else str(uuid.uuid4())
        body=canon(payload)
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                existing=self.db.execute("SELECT app,rid,payload,deleted,expected FROM queue WHERE op=?",(op,)).fetchone()
                if existing:
                    if (existing["app"],existing["rid"],existing["payload"],bool(existing["deleted"]))!=(app,rid,body,deleted) or (expected is not None and int(existing["expected"])!=expected):
                        raise ValueError("operation_id was already used for different content")
                    self.db.execute("COMMIT"); return op
                if not stable_op:
                    pending=self.db.execute("SELECT op,expected FROM queue WHERE app=? AND rid=? AND state='pending' ORDER BY seq DESC LIMIT 1",(app,rid)).fetchone()
                    if pending:
                        self.db.execute("UPDATE queue SET payload=?,deleted=? WHERE op=?",(body,int(deleted),pending["op"]))
                        self.db.execute("COMMIT"); return pending["op"]
                conflict=self.db.execute("SELECT 1 FROM queue WHERE app=? AND rid=? AND state='conflict' LIMIT 1",(app,rid)).fetchone()
                if conflict: raise RuntimeError("record has an unresolved AutoSync conflict; resolve it explicitly first")
                sending=self.db.execute("SELECT expected FROM queue WHERE app=? AND rid=? AND state='sending' ORDER BY seq DESC LIMIT 1",(app,rid)).fetchone()
                pending=self.db.execute("SELECT expected FROM queue WHERE app=? AND rid=? AND state='pending' ORDER BY seq LIMIT 1",(app,rid)).fetchone()
                if sending: base=int(sending["expected"])
                elif pending: base=int(pending["expected"])
                else: base=self.record_revision(app,rid) if expected is None else expected
                if not isinstance(base,int) or base<0: raise ValueError("invalid expected revision")
                self.db.execute("INSERT INTO queue(op,app,rid,expected,payload,deleted,state,created) VALUES(?,?,?,?,?,?,?,?)",
                                (op,app,rid,base,body,int(deleted),"pending",time.time()))
                self.db.execute("COMMIT"); return op
            except Exception: self.db.execute("ROLLBACK"); raise
    def claim_next(self,app):
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                row=self.db.execute("SELECT * FROM queue WHERE app=? AND state='pending' ORDER BY seq LIMIT 1",(app,)).fetchone()
                if not row: self.db.execute("COMMIT"); return None
                self.db.execute("UPDATE queue SET state='sending' WHERE op=?",(row["op"],))
                self.db.execute("COMMIT")
                return {"op":row["op"],"app":row["app"],"rid":row["rid"],"expected":row["expected"],
                        "payload":json.loads(row["payload"]),"deleted":bool(row["deleted"])}
            except Exception: self.db.execute("ROLLBACK"); raise
    def mark_synced(self,op,revision,response):
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                row=self.db.execute("SELECT app,rid,seq FROM queue WHERE op=? AND state='sending'",(op,)).fetchone()
                if not row: self.db.execute("COMMIT"); return
                self.db.execute("UPDATE queue SET state='synced',response=? WHERE op=?",(canon(response),op))
                self.db.execute("UPDATE queue SET expected=? WHERE app=? AND rid=? AND state='pending' AND seq>?",
                                (revision,row["app"],row["rid"],row["seq"]))
                self.db.execute("COMMIT")
            except Exception: self.db.execute("ROLLBACK"); raise
    def mark_conflict(self,op,response):
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                row=self.db.execute("SELECT app,rid FROM queue WHERE op=?",(op,)).fetchone()
                if row:
                    self.db.execute("UPDATE queue SET state='conflict',response=? WHERE op=?",(canon(response),op))
                    blocked={"error":"blocked_by_prior_conflict","conflict_operation":op}
                    self.db.execute("UPDATE queue SET state='conflict',response=? WHERE app=? AND rid=? AND state='pending'",
                                    (canon(blocked),row["app"],row["rid"]))
                self.db.execute("COMMIT")
            except Exception: self.db.execute("ROLLBACK"); raise
    def release(self,op):
        with self.lock: self.db.execute("UPDATE queue SET state='pending' WHERE op=? AND state='sending'",(op,))
    def pending_count(self,app=None):
        with self.lock:
            if app is None: row=self.db.execute("SELECT COUNT(*) FROM queue WHERE state IN ('pending','sending')").fetchone()
            else: row=self.db.execute("SELECT COUNT(*) FROM queue WHERE app=? AND state IN ('pending','sending')",(app,)).fetchone()
            return int(row[0])
    def has_pending_record(self,app,rid):
        with self.lock:
            return self.db.execute("SELECT 1 FROM queue WHERE app=? AND rid=? AND state IN ('pending','sending','conflict') LIMIT 1",
                                   (app,rid)).fetchone() is not None
    def operation_status(self,op):
        with self.lock:
            row=self.db.execute("SELECT state,response FROM queue WHERE op=?",(op,)).fetchone()
            return {"state":"missing"} if row is None else {
                "state":row["state"],"response":json.loads(row["response"]) if row["response"] else None}
    def conflict_count(self,app=None):
        with self.lock:
            if app is None: row=self.db.execute("SELECT COUNT(*) FROM queue WHERE state='conflict'").fetchone()
            else: row=self.db.execute("SELECT COUNT(*) FROM queue WHERE app=? AND state='conflict'",(app,)).fetchone()
            return int(row[0])
    def conflicts(self,app=None):
        with self.lock:
            if app is None:
                rows=self.db.execute("SELECT * FROM queue WHERE state='conflict' ORDER BY seq").fetchall()
            else:
                rows=self.db.execute("SELECT * FROM queue WHERE app=? AND state='conflict' ORDER BY seq",(app,)).fetchall()
            results=[]
            for row in rows:
                response=json.loads(row["response"] or "{}")
                current=response.get("current")
                results.append({"operation_id":row["op"],"app_id":row["app"],"record_id":row["rid"],
                    "expected_revision":row["expected"],"payload":json.loads(row["payload"]),
                    "deleted":bool(row["deleted"]),"error":response.get("error"),
                    "current_revision":(current.get("revision",0) if current is not None else 0),
                    "current":current,"incoming":response.get("incoming"),"created":row["created"]})
            return results
    def resolve_conflict(self,op,payload,expected_revision,deleted=False,operation_id=None):
        if not isinstance(payload,dict) or not isinstance(deleted,bool) or not isinstance(expected_revision,int) or expected_revision<0:
            raise ValueError("invalid conflict resolution")
        body=canon(payload)
        if len(body.encode("utf-8"))>1_048_576: raise ValueError("payload too large")
        new_op=check_id(operation_id,OP_RE) if operation_id is not None else str(uuid.uuid4())
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                row=self.db.execute("SELECT app,rid,response,state FROM queue WHERE op=? AND state IN ('conflict','resolved')",(op,)).fetchone()
                if not row: raise ValueError("unresolved conflict operation not found")
                existing=self.db.execute("SELECT app,rid,payload,deleted,expected FROM queue WHERE op=?",(new_op,)).fetchone()
                if existing:
                    if (existing["app"],existing["rid"],existing["payload"],bool(existing["deleted"]))!=(row["app"],row["rid"],body,deleted) or int(existing["expected"])!=expected_revision:
                        raise ValueError("operation_id was already used for different content")
                    self.db.execute("COMMIT"); return new_op
                if row["state"]!="conflict": raise ValueError("conflict resolution operation is missing")
                response=json.loads(row["response"] or "{}")
                if response.get("error")!="revision_conflict":
                    raise ValueError("resolve the primary revision conflict for this record")
                current=response.get("current")
                current_revision=current.get("revision",0) if current is not None else 0
                if expected_revision!=current_revision:
                    raise ValueError("expected_revision must match the server revision shown in this conflict")
                self.db.execute("UPDATE queue SET state='resolved' WHERE app=? AND rid=? AND state='conflict'",
                                (row["app"],row["rid"]))
                self.db.execute("INSERT INTO queue(op,app,rid,expected,payload,deleted,state,created) VALUES(?,?,?,?,?,?,?,?)",
                                (new_op,row["app"],row["rid"],expected_revision,body,int(deleted),"pending",time.time()))
                self.db.execute("COMMIT")
                return new_op
            except Exception:
                self.db.execute("ROLLBACK")
                raise
    def apply_delta(self,app,result):
        if result.get("app_id")!=app or not isinstance(result.get("cursor"),int): raise ValueError("invalid server delta")
        reset=result.get("reset_required", "records" in result)
        if not isinstance(reset,bool): raise ValueError("invalid reset marker")
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                if reset:
                    records=result.get("records")
                    if not isinstance(records,list): raise ValueError("invalid reset snapshot")
                    self.db.execute("DELETE FROM cache WHERE app=?",(app,))
                    for record in records:
                        rid=record.get("record_id") if isinstance(record,dict) else None
                        rev=record.get("revision") if isinstance(record,dict) else None
                        payload=record.get("payload") if isinstance(record,dict) else None
                        deleted=record.get("deleted") if isinstance(record,dict) else None
                        updated=record.get("updated_utc") if isinstance(record,dict) else None
                        if (not isinstance(rid,str) or not RECORD_RE.fullmatch(rid) or
                            not isinstance(rev,int) or rev<0 or not isinstance(payload,dict) or
                            not isinstance(deleted,bool) or not isinstance(updated,str)):
                            raise ValueError("invalid reset snapshot record")
                        self.db.execute("INSERT INTO cache VALUES(?,?,?,?,?,?)",
                            (app,rid,rev,canon(payload),int(deleted),updated))
                else:
                    events=result.get("events",[])
                    if not isinstance(events,list): raise ValueError("invalid server events")
                    for event in events:
                        old=self.db.execute("SELECT rev FROM cache WHERE app=? AND rid=?",(app,event["record_id"])).fetchone()
                        if old is None or int(event["revision"])>int(old["rev"]):
                            self.db.execute("""INSERT INTO cache VALUES(?,?,?,?,?,?) ON CONFLICT(app,rid) DO UPDATE SET
                                rev=excluded.rev,payload=excluded.payload,deleted=excluded.deleted,updated=excluded.updated""",
                                (app,event["record_id"],event["revision"],canon(event["payload"]),int(event["deleted"]),event["updated_utc"]))
                self.db.execute("INSERT INTO cursors VALUES(?,?) ON CONFLICT(app) DO UPDATE SET cursor=excluded.cursor",
                                (app,result["cursor"]))
                self.db.execute("COMMIT")
            except Exception: self.db.execute("ROLLBACK"); raise
    def apply_push_event(self,app,event):
        """Apply an authenticated inline event only when its app cursor is contiguous."""
        if not isinstance(app,str) or not APP_RE.fullmatch(app) or not isinstance(event,dict):
            raise ValueError("invalid pushed event")
        rid=event.get("record_id"); revision=event.get("revision"); previous=event.get("previous_cursor")
        payload=event.get("payload"); deleted=event.get("deleted"); updated=event.get("updated_utc")
        if (not isinstance(rid,str) or not RECORD_RE.fullmatch(rid) or
            not isinstance(revision,int) or isinstance(revision,bool) or revision<1 or
            not isinstance(previous,int) or isinstance(previous,bool) or previous<0 or
            not isinstance(payload,dict) or not isinstance(deleted,bool) or not isinstance(updated,str)):
            raise ValueError("invalid pushed event")
        with self.lock:
            row=self.db.execute("SELECT cursor FROM cursors WHERE app=?",(app,)).fetchone()
            current=int(row["cursor"]) if row else 0
            if revision<=current:return "duplicate"
            if current!=previous:return "gap"
            self.apply_delta(app,{"app_id":app,"cursor":revision,"reset_required":False,"events":[{
                "revision":revision,"record_id":rid,"payload":payload,"deleted":deleted,"updated_utc":updated}]})
            return "applied"
    @staticmethod
    def _validate_partitioned_record(item):
        rid=item.get("record_id") if isinstance(item,dict) else None
        revision=item.get("revision") if isinstance(item,dict) else None
        payload=item.get("payload") if isinstance(item,dict) else None
        deleted=item.get("deleted") if isinstance(item,dict) else None
        updated=item.get("updated_utc") if isinstance(item,dict) else None
        if (not isinstance(rid,str) or not RECORD_RE.fullmatch(rid) or
            not isinstance(revision,int) or isinstance(revision,bool) or revision<0 or
            not isinstance(payload,dict) or not isinstance(deleted,bool) or not isinstance(updated,str)):
            raise ValueError("invalid partitioned snapshot record")
        return rid,revision,payload,deleted,updated
    @classmethod
    def _validate_partitioned_event(cls,event,bucket_count):
        rid,revision,payload,deleted,updated=cls._validate_partitioned_record(event)
        bucket=event.get("bucket_id"); sequence=event.get("bucket_seq")
        if (not isinstance(bucket,int) or isinstance(bucket,bool) or not 0<=bucket<bucket_count or
            not isinstance(sequence,int) or isinstance(sequence,bool) or sequence<1):
            raise ValueError("invalid partitioned event cursor")
        return rid,revision,payload,deleted,updated,bucket,sequence
    def partitioned_cursor(self,app):
        with self.lock:
            row=self.db.execute("SELECT cursor_json FROM cursors_v2 WHERE app=?",(app,)).fetchone()
            return json.loads(row["cursor_json"]) if row else None
    def _write_partitioned_cursor(self,app,cursor):
        self.db.execute("INSERT INTO cursors_v2 VALUES(?,?) ON CONFLICT(app) DO UPDATE SET cursor_json=excluded.cursor_json",
                        (app,canon(cursor)))
    def apply_partitioned_delta(self,app,result):
        """Apply v2 events/snapshots and a vector cursor in one local transaction.

        Per-record revisions make event ordering independent across buckets.
        A pushed event advances only its contiguous bucket prefix; missing
        sequence numbers remain eligible for durable delta recovery.
        """
        if not isinstance(app,str) or not APP_RE.fullmatch(app) or not isinstance(result,dict) or result.get("app_id")!=app:
            raise ValueError("invalid partitioned delta")
        cursor=result.get("cursor")
        if (not isinstance(cursor,dict) or cursor.get("schema_version")!=2 or
            not isinstance(cursor.get("bucket_count"),int) or isinstance(cursor.get("bucket_count"),bool) or
            not 2<=cursor["bucket_count"]<=1024 or not isinstance(cursor.get("sequences"),list) or
            len(cursor["sequences"])!=cursor["bucket_count"] or
            any(not isinstance(value,int) or isinstance(value,bool) or value<0 for value in cursor["sequences"])):
            raise ValueError("invalid partitioned cursor")
        bucket_count=cursor["bucket_count"]
        reset=result.get("reset_required",False)
        if not isinstance(reset,bool): raise ValueError("invalid partitioned reset marker")
        has_snapshot="records" in result
        if reset and not has_snapshot: raise ValueError("partitioned reset requires a full snapshot")
        raw_events=result.get("events",[])
        if not isinstance(raw_events,list): raise ValueError("invalid partitioned events")
        records=[self._validate_partitioned_record(item) for item in result.get("records",[])] if has_snapshot else []
        events=[self._validate_partitioned_event(item,bucket_count) for item in raw_events]
        if has_snapshot and raw_events:
            raise ValueError("partitioned snapshot must not mix snapshot rows and delta events")
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                old_row=self.db.execute("SELECT cursor_json FROM cursors_v2 WHERE app=?",(app,)).fetchone()
                old_cursor=json.loads(old_row["cursor_json"]) if old_row else None
                if old_cursor and (old_cursor.get("schema_version")!=2 or old_cursor.get("bucket_count")!=bucket_count):
                    if not has_snapshot:
                        raise ValueError("partitioned cursor schema changed; a full snapshot is required")
                    self.db.execute("DELETE FROM cursor_v2_seen WHERE app=?",(app,))
                    old_cursor=None
                if has_snapshot:
                    for rid,revision,payload,deleted,updated in records:
                        old=self.db.execute("SELECT rev FROM cache WHERE app=? AND rid=?",(app,rid)).fetchone()
                        if old is None or revision>int(old["rev"]):
                            self.db.execute("""INSERT INTO cache VALUES(?,?,?,?,?,?) ON CONFLICT(app,rid) DO UPDATE SET
                                rev=excluded.rev,payload=excluded.payload,deleted=excluded.deleted,updated=excluded.updated""",
                                (app,rid,revision,canon(payload),int(deleted),updated))
                    # Snapshot data includes tombstones; v2 never hard-deletes
                    # remote records, so retaining omitted local cache rows
                    # avoids losing a concurrent newer push during a snapshot.
                else:
                    for rid,revision,payload,deleted,updated,_bucket,_sequence in events:
                        old=self.db.execute("SELECT rev FROM cache WHERE app=? AND rid=?",(app,rid)).fetchone()
                        if old is None or revision>int(old["rev"]):
                            self.db.execute("""INSERT INTO cache VALUES(?,?,?,?,?,?) ON CONFLICT(app,rid) DO UPDATE SET
                                rev=excluded.rev,payload=excluded.payload,deleted=excluded.deleted,updated=excluded.updated""",
                                (app,rid,revision,canon(payload),int(deleted),updated))
                merged=list(cursor["sequences"])
                if reset:
                    self.db.execute("DELETE FROM cursor_v2_seen WHERE app=?",(app,))
                elif old_cursor:
                    merged=[max(a,b) for a,b in zip(old_cursor["sequences"],merged)]
                merged_cursor={"schema_version":2,"bucket_count":bucket_count,"sequences":merged}
                self._write_partitioned_cursor(app,merged_cursor)
                if not reset:
                    for bucket,sequence in enumerate(merged):
                        self.db.execute("DELETE FROM cursor_v2_seen WHERE app=? AND bucket=? AND seq<=?",
                                        (app,bucket,sequence))
                self.db.execute("COMMIT")
            except Exception:
                self.db.execute("ROLLBACK")
                raise
    def apply_partitioned_push_event(self,app,event):
        """Apply an unordered v2 push while keeping a contiguous vector cursor."""
        if not isinstance(app,str) or not APP_RE.fullmatch(app): raise ValueError("invalid app")
        if not isinstance(event,dict): raise ValueError("invalid partitioned event")
        bucket_count=event.get("bucket_count")
        if not isinstance(bucket_count,int) or isinstance(bucket_count,bool) or not 2<=bucket_count<=1024:
            raise ValueError("invalid partitioned event bucket count")
        rid,revision,payload,deleted,updated,bucket,sequence=self._validate_partitioned_event(event,bucket_count)
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                row=self.db.execute("SELECT cursor_json FROM cursors_v2 WHERE app=?",(app,)).fetchone()
                if row:
                    cursor=json.loads(row["cursor_json"])
                    if cursor.get("schema_version")!=2 or cursor.get("bucket_count")!=bucket_count:
                        raise ValueError("partitioned event schema mismatch; synchronize a full snapshot")
                    sequences=list(cursor["sequences"])
                else:
                    sequences=[0]*bucket_count
                old=self.db.execute("SELECT rev FROM cache WHERE app=? AND rid=?",(app,rid)).fetchone()
                applied=old is None or revision>int(old["rev"])
                if applied:
                    self.db.execute("""INSERT INTO cache VALUES(?,?,?,?,?,?) ON CONFLICT(app,rid) DO UPDATE SET
                        rev=excluded.rev,payload=excluded.payload,deleted=excluded.deleted,updated=excluded.updated""",
                        (app,rid,revision,canon(payload),int(deleted),updated))
                current=sequences[bucket]
                overflow_gap=False
                if sequence>current:
                    already_buffered=self.db.execute("SELECT 1 FROM cursor_v2_seen WHERE app=? AND bucket=? AND seq=?",
                                                     (app,bucket,sequence)).fetchone()
                    buffered_count=self.db.execute("SELECT COUNT(*) FROM cursor_v2_seen WHERE app=? AND bucket=?",
                                                   (app,bucket)).fetchone()[0]
                    if already_buffered or sequence==current+1 or buffered_count<MAX_PARTITIONED_PENDING_PUSH:
                        self.db.execute("INSERT OR IGNORE INTO cursor_v2_seen VALUES(?,?,?)",(app,bucket,sequence))
                        while self.db.execute("SELECT 1 FROM cursor_v2_seen WHERE app=? AND bucket=? AND seq=?",
                                              (app,bucket,current+1)).fetchone():
                            current+=1
                            self.db.execute("DELETE FROM cursor_v2_seen WHERE app=? AND bucket=? AND seq=?",
                                            (app,bucket,current))
                    else:
                        overflow_gap=True
                    sequences[bucket]=current
                self._write_partitioned_cursor(app,{"schema_version":2,"bucket_count":bucket_count,
                                                    "sequences":sequences})
                self.db.execute("COMMIT")
                if sequence>current or overflow_gap:
                    return "gap"
                return "applied" if applied else "duplicate"
            except Exception:
                self.db.execute("ROLLBACK")
                raise
    def cached_record(self,app,rid):
        with self.lock:
            row=self.db.execute("SELECT rev,payload,deleted,updated FROM cache WHERE app=? AND rid=?",(app,rid)).fetchone()
            return None if row is None else {"revision":row["rev"],"payload":json.loads(row["payload"]),
                                             "deleted":bool(row["deleted"]),"updated_utc":row["updated"]}
    def cached_records(self,app):
        with self.lock:
            rows=self.db.execute("SELECT rid,rev,payload,deleted,updated FROM cache WHERE app=? ORDER BY rid",(app,)).fetchall()
            return [{"record_id":row["rid"],"revision":row["rev"],"payload":json.loads(row["payload"]),
                     "deleted":bool(row["deleted"]),"updated_utc":row["updated"]} for row in rows]

    def apply_rtdb_record(self,app,record):
        """Apply one revisioned Firebase RTDB record without moving cache backwards."""
        if not isinstance(record,dict): raise ValueError("invalid RTDB record")
        rid=record.get("record_id"); rev=record.get("revision"); payload=record.get("payload")
        deleted=record.get("deleted"); updated=record.get("updated_utc")
        if (not isinstance(app,str) or not APP_RE.fullmatch(app) or
            not isinstance(rid,str) or not RECORD_RE.fullmatch(rid) or
            not isinstance(rev,int) or isinstance(rev,bool) or rev<1 or
            not isinstance(payload,dict) or not isinstance(deleted,bool) or not isinstance(updated,str)):
            raise ValueError("invalid RTDB record")
        body=canon(payload)
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                old=self.db.execute("SELECT rev,payload,deleted,updated FROM cache WHERE app=? AND rid=?",
                                    (app,rid)).fetchone()
                if old is not None and int(old["rev"])==rev:
                    if (old["payload"],bool(old["deleted"]),old["updated"])!=(body,deleted,updated):
                        raise ValueError("RTDB record changed without a revision increment")
                elif old is None or rev>int(old["rev"]):
                    self.db.execute("""INSERT INTO cache VALUES(?,?,?,?,?,?) ON CONFLICT(app,rid) DO UPDATE SET
                        rev=excluded.rev,payload=excluded.payload,deleted=excluded.deleted,updated=excluded.updated""",
                        (app,rid,rev,body,int(deleted),updated))
                self.db.execute("COMMIT")
            except Exception:
                self.db.execute("ROLLBACK")
                raise

    def apply_rtdb_snapshot(self,app,records):
        """Apply the authoritative RTDB snapshot, preserving tombstones and newer local observations."""
        if not isinstance(app,str) or not APP_RE.fullmatch(app) or not isinstance(records,list):
            raise ValueError("invalid RTDB snapshot")
        validated=[]
        seen=set()
        for record in records:
            if not isinstance(record,dict): raise ValueError("invalid RTDB snapshot record")
            rid=record.get("record_id"); rev=record.get("revision"); payload=record.get("payload")
            deleted=record.get("deleted"); updated=record.get("updated_utc")
            if (not isinstance(rid,str) or not RECORD_RE.fullmatch(rid) or rid in seen or
                not isinstance(rev,int) or isinstance(rev,bool) or rev<1 or
                not isinstance(payload,dict) or not isinstance(deleted,bool) or not isinstance(updated,str)):
                raise ValueError("invalid RTDB snapshot record")
            seen.add(rid); validated.append((rid,rev,canon(payload),int(deleted),updated))
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                for rid,rev,payload,deleted,updated in validated:
                    old=self.db.execute("SELECT rev,payload,deleted,updated FROM cache WHERE app=? AND rid=?",
                                        (app,rid)).fetchone()
                    if old is not None and int(old["rev"])==rev:
                        if (old["payload"],int(old["deleted"]),old["updated"])!=(payload,deleted,updated):
                            raise ValueError("RTDB record changed without a revision increment")
                    elif old is None or rev>int(old["rev"]):
                        self.db.execute("""INSERT INTO cache VALUES(?,?,?,?,?,?) ON CONFLICT(app,rid) DO UPDATE SET
                            rev=excluded.rev,payload=excluded.payload,deleted=excluded.deleted,updated=excluded.updated""",
                            (app,rid,rev,payload,deleted,updated))
                self.db.execute("COMMIT")
            except Exception:
                self.db.execute("ROLLBACK")
                raise

class AutoSyncClient:
    def __init__(self,base_url,token,app_id,local_root,ca_file=None,protocol="v1"):
        if protocol not in ("v1","v2"): raise ValueError("protocol must be v1 or v2")
        parsed=urllib.parse.urlsplit(base_url)
        if parsed.scheme not in ("http","https") or not parsed.hostname: raise ValueError("base_url must be http(s)")
        if parsed.scheme=="http":
            import ipaddress
            try: loopback=ipaddress.ip_address(parsed.hostname).is_loopback
            except ValueError: loopback=parsed.hostname.lower()=="localhost"
            if not loopback: raise ValueError("unencrypted non-loopback URLs are refused")
            if ca_file is not None: raise ValueError("ca_file is only valid with HTTPS")
        if not isinstance(token,str) or len(token.encode())<32: raise ValueError("token must be at least 32 bytes")
        if not isinstance(app_id,str) or not APP_RE.fullmatch(app_id): raise ValueError("invalid app_id")
        self.base=base_url.rstrip("/"); self.token=token; self.app=app_id
        self.protocol=protocol
        self._http_path=parsed.path.rstrip("/")
        self._http_host=parsed.hostname
        self._http_port=parsed.port or (443 if parsed.scheme=="https" else 80)
        self._http_secure=parsed.scheme=="https"
        self.ssl_context=ssl.create_default_context(cafile=str(ca_file) if ca_file else None) if parsed.scheme=="https" else None
        self.outbox=Outbox(Path(local_root)/"autosync-client.sqlite3")
        self.sync_lock=threading.Lock()
        self._http_lock=threading.Lock()
        self._http_connection=None
    def close(self):
        with self._http_lock:
            if self._http_connection is not None:
                self._http_connection.close()
                self._http_connection=None
        self.outbox.close()
    def queue_write(self,record_id,payload,expected_revision=None,deleted=False,operation_id=None):
        return self.outbox.queue_write(self.app,record_id,payload,expected_revision,deleted,operation_id)
    def record_revision(self,record_id):
        return self.outbox.record_revision(self.app,record_id)
    def has_pending_record(self,record_id):
        return self.outbox.has_pending_record(self.app,record_id)
    def operation_status(self,operation_id):
        return self.outbox.operation_status(operation_id)
    def cached_record(self,record_id):
        return self.outbox.cached_record(self.app,record_id)
    def cached_records(self):
        return self.outbox.cached_records(self.app)
    def conflicts(self):
        return self.outbox.conflicts(self.app)
    def resolve_conflict(self,operation_id,payload,expected_revision,deleted=False,resolution_operation_id=None):
        return self.outbox.resolve_conflict(operation_id,payload,expected_revision,deleted,resolution_operation_id)
    def _http(self,path,method="GET",payload=None,headers=None):
        h={"Authorization":"Bearer "+self.token}; h.update(headers or {})
        # The WebSocket is the long-lived channel. Keep API calls one-shot so
        # idle HTTP keepalives do not fan out health/read requests in the cloud.
        h["Connection"]="close"
        data=canon(payload).encode() if payload is not None else None
        if data is not None: h["Content-Type"]="application/json"
        target=self._http_path+path
        with self._http_lock:
            if self._http_connection is None:
                if self._http_secure:
                    self._http_connection=IPv4HTTPSConnection(
                        self._http_host,self._http_port,timeout=5,context=self.ssl_context)
                else:
                    self._http_connection=http.client.HTTPConnection(
                        self._http_host,self._http_port,timeout=5)
            connection=self._http_connection
            try:
                connection.request(method,target,body=data,headers=h)
                response=connection.getresponse()
                raw=response.read()
                status=response.status
                return status,json.loads(raw.decode("utf-8"))
            except (OSError,TimeoutError,http.client.HTTPException) as exc:
                connection.close()
                self._http_connection=None
                raise urllib.error.URLError(exc) from exc
            finally:
                connection.close()
                if self._http_connection is connection:
                    self._http_connection=None
    def _pull(self):
        if self.protocol=="v1":
            cursor=self.outbox.cursor(self.app)
            suffix=f"?since={cursor}" if self.outbox.has_cursor(self.app) else ""
            path=f"/v1/apps/{self.app}/state{suffix}"
        else:
            cursor=self.outbox.partitioned_cursor(self.app)
            if cursor is None:
                suffix=""
            else:
                encoded=__import__("base64").urlsafe_b64encode(canon(cursor).encode("utf-8")).decode("ascii").rstrip("=")
                suffix="?"+urllib.parse.urlencode({"cursor":encoded})
            path=f"/v2/apps/{self.app}/state{suffix}"
        status,result=self._http(path)
        if status==409 and result.get("error")=="snapshot_required" and result.get("reset_required") is True:
            if self.protocol=="v1": self.outbox.apply_delta(self.app,result)
            else: self.outbox.apply_partitioned_delta(self.app,result)
            return
        if status!=200: raise SyncRejected(f"server_http_{status}")
        if self.protocol=="v1": self.outbox.apply_delta(self.app,result)
        else: self.outbox.apply_partitioned_delta(self.app,result)

    def _cursor_for_status(self):
        return (self.outbox.cursor(self.app) if self.protocol=="v1"
                else self.outbox.partitioned_cursor(self.app))
    def sync_once(self):
        with self.sync_lock:
            try: self._pull()
            except SyncRejected as exc:
                return {"status":"blocked","pending":self.outbox.pending_count(self.app),"detail":str(exc)}
            except (OSError,urllib.error.URLError,TimeoutError) as exc:
                return {**connection_status(exc),"pending":self.outbox.pending_count(self.app)}
            sent=0; conflicts=0
            while True:
                op=self.outbox.claim_next(self.app)
                if op is None: break
                try:
                    path=f"/{self.protocol}/apps/{self.app}/records/{op['rid']}"
                    status,result=self._http(path,"POST",
                        {"payload":op["payload"],"deleted":op["deleted"]},
                        {"If-Match":str(op["expected"]),"Idempotency-Key":op["op"]})
                    if status==200:
                        self.outbox.mark_synced(op["op"],int(result["revision"]),result); sent+=1
                    elif status==409:
                        self.outbox.mark_conflict(op["op"],result); conflicts+=1
                    elif status in (401,403):
                        self.outbox.release(op["op"])
                        return {"status":"blocked","detail":f"server_http_{status}","sent":sent,"conflicts":conflicts,"pending":self.outbox.pending_count(self.app)}
                    else:
                        self.outbox.release(op["op"])
                        return {"status":"retry","sent":sent,"conflicts":conflicts,"pending":self.outbox.pending_count(self.app)}
                except (OSError,urllib.error.URLError,TimeoutError) as exc:
                    self.outbox.release(op["op"])
                    return {**connection_status(exc),"sent":sent,"conflicts":conflicts,"pending":self.outbox.pending_count(self.app)}
            if sent or conflicts:
                try: self._pull()
                except SyncRejected as exc:
                    return {"status":"blocked","detail":str(exc),"sent":sent,"conflicts":conflicts,
                            "pending":self.outbox.pending_count(self.app)}
                except (OSError,urllib.error.URLError,TimeoutError):
                    return {"status":"committed_refresh_pending","sent":sent,"conflicts":conflicts,
                            "pending":self.outbox.pending_count(self.app)}
            return {"status":"synced" if not conflicts else "conflict","sent":sent,"conflicts":conflicts,
                    "pending":self.outbox.pending_count(self.app),"cursor":self._cursor_for_status()}
    def _connect_websocket(self):
        parsed=urllib.parse.urlsplit(self.base); secure=parsed.scheme=="https"
        raw=socket.create_connection((parsed.hostname,parsed.port or (443 if secure else 80)),timeout=5)
        try:
            if secure: raw=self.ssl_context.wrap_socket(raw,server_hostname=parsed.hostname)
            raw.settimeout(5)
            key=__import__("base64").b64encode(__import__("os").urandom(16)).decode()
            path=f"/{self.protocol}/apps/{self.app}/stream"
            host=parsed.netloc
            request=(f"GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                     f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nAuthorization: Bearer {self.token}\r\n\r\n")
            raw.sendall(request.encode("ascii")); headers=bytearray()
            while not headers.endswith(b"\r\n\r\n"):
                part=raw.recv(1)
                if not part: raise ConnectionError("WebSocket handshake closed")
                headers.extend(part)
                if len(headers)>16_384: raise ConnectionError("invalid WebSocket handshake")
            lines=headers.decode("latin1").split("\r\n")
            status_match=re.match(r"^HTTP/\d(?:\.\d)?\s+(\d{3})(?:\s|$)",lines[0])
            if not status_match: raise ConnectionError("invalid WebSocket handshake status line")
            status=int(status_match.group(1))
            if status!=101: raise WebSocketHandshakeError(status)
            response_headers={line.split(":",1)[0].lower():line.split(":",1)[1].strip()
                              for line in lines[1:] if ":" in line}
            expected=__import__("base64").b64encode(hashlib.sha1((key+"258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
            if response_headers.get("sec-websocket-accept")!=expected: raise ConnectionError("WebSocket accept mismatch")
            raw.settimeout(None)
            return raw
        except BaseException:
            try: raw.close()
            except OSError: pass
            raise
    @staticmethod
    def _pop_frame(buffer):
        if len(buffer)<2: return None
        b0,b1=buffer[0],buffer[1]
        if b0&0x70: raise WebSocketProtocolError("reserved frame bits are set")
        if b1&0x80: raise WebSocketProtocolError("server WebSocket frames must not be masked")
        fin=bool(b0&0x80); opcode=b0&0x0f
        if opcode not in (1,8,9,10): raise WebSocketProtocolError("unsupported WebSocket opcode")
        if not fin: raise WebSocketProtocolError("fragmented WebSocket frames are unsupported")
        n=b1&127; offset=2
        if n==126:
            if len(buffer)<4:return None
            n=int.from_bytes(buffer[2:4],"big");offset=4
            if n<126: raise WebSocketProtocolError("non-canonical WebSocket frame length")
        elif n==127:
            if len(buffer)<10:return None
            n=int.from_bytes(buffer[2:10],"big");offset=10
            if n<65536 or n&(1<<63): raise WebSocketProtocolError("invalid WebSocket frame length")
        if opcode in (8,9,10) and n>125: raise WebSocketProtocolError("WebSocket control frame exceeds 125 bytes")
        if opcode==1 and n>MAX_WS_TEXT_FRAME: raise WebSocketProtocolError("WebSocket text frame exceeds size limit")
        if len(buffer)<offset+n:return None
        payload=bytes(buffer[offset:offset+n]);del buffer[:offset+n]
        return opcode,payload
    def watch(self,stop_event,on_status=None):
        """Reconnect forever until stop_event; every reconnect reconciles from the durable cursor."""
        backoff=1
        while not stop_event.is_set():
            sock=None
            try:
                sock=self._connect_websocket();sock.setblocking(False);buffer=bytearray();last_ping=time.monotonic()
                backoff=1;ready=False;retry_due=None;retry_delay=WATCH_RETRY_INITIAL_SECONDS

                def schedule_sync_retry(result):
                    nonlocal retry_due,retry_delay
                    status=result.get("status") if isinstance(result,dict) else None
                    if status in ("offline","retry","committed_refresh_pending"):
                        # Spread clients after a shared outage/contention burst while keeping
                        # durable local edits retrying even when no new push event arrives.
                        delay=random.uniform(retry_delay*0.8,retry_delay*1.2)
                        retry_due=time.monotonic()+delay
                        result["retry_in_seconds"]=round(delay,3)
                        retry_delay=min(retry_delay*2,WATCH_RETRY_MAX_SECONDS)
                    else:
                        retry_due=None;retry_delay=WATCH_RETRY_INITIAL_SECONDS
                while not stop_event.is_set():
                    readable,_,_=select.select([sock],[],[],1)
                    if readable:
                        chunk=sock.recv(65536)
                        if not chunk: raise ConnectionError("WebSocket disconnected")
                        buffer.extend(chunk)
                        while True:
                            item=self._pop_frame(buffer)
                            if item is None:break
                            opcode,payload=item
                            if opcode==8: raise ConnectionError("WebSocket closed")
                            if opcode==9:sock.sendall(frame_client(payload,10))
                            elif opcode==1:
                                message=json.loads(payload.decode("utf-8"))
                                if message.get("type")=="ready" and not ready:
                                    # Reconcile once after the listener is attached. This closes
                                    # the gap between durable pull and WebSocket connect without
                                    # paying for a second full pull on every reconnect.
                                    ready=True
                                    result=self.sync_once()
                                    schedule_sync_retry(result)
                                    if on_status:on_status(result)
                                elif message.get("type")=="changed":
                                    # Events racing ahead of the ready frame are only hints;
                                    # the post-ready durable pull includes them.
                                    if not ready: continue
                                    event=message.get("event")
                                    if isinstance(event,dict):
                                        push_state=(self.outbox.apply_push_event(self.app,event) if self.protocol=="v1"
                                                    else self.outbox.apply_partitioned_push_event(self.app,event))
                                        if push_state=="gap" or self.outbox.pending_count(self.app):
                                            result=self.sync_once()
                                        else:
                                            pending=self.outbox.pending_count(self.app)
                                            conflicts=self.outbox.conflict_count(self.app)
                                            result={"status":"conflict" if conflicts else ("queued" if pending else "synced"),
                                                    "pending":pending,"conflicts":conflicts,
                                                    "cursor":self._cursor_for_status(),"push":push_state}
                                    else:
                                        result=self.sync_once()
                                    schedule_sync_retry(result)
                                    if on_status:on_status(result)
                                elif message.get("type")=="resync_required" and ready:
                                    result=self.sync_once()
                                    schedule_sync_retry(result)
                                    if on_status:on_status(result)
                    now=time.monotonic()
                    if now-last_ping>20:
                        sock.sendall(frame_client(b"",9));last_ping=time.monotonic()
                    if ready and retry_due is not None and now>=retry_due:
                        result=self.sync_once()
                        schedule_sync_retry(result)
                        if on_status:on_status(result)
            except (OSError,ValueError,RuntimeError,urllib.error.URLError,ConnectionError) as exc:
                if on_status:on_status(connection_status(exc))
                stop_event.wait(backoff);backoff=min(backoff*2,30)
            finally:
                if sock:
                    try:sock.close()
                    except OSError:pass


