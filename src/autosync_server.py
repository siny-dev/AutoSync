#!/usr/bin/env python3
"""Loopback-only AutoSync prototype using one SQLite WAL writer."""
from __future__ import annotations
import argparse, base64, hashlib, hmac, ipaddress, json, os, re, socket, sqlite3, ssl, struct, threading, time, uuid
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
if __package__:
    from .autosync_protocol import API_VERSION, APP_RE, OP_RE, RECORD_RE, canon, check_id, now_utc
    from .autosync_credentials import CredentialError, load_server_tokens
    from .autosync_firestore_store import FirestoreStore
    from .autosync_firestore_partitioned import FirestorePartitionedStore
else:  # Support the documented `python src/autosync_server.py` entry point.
    from autosync_protocol import API_VERSION, APP_RE, OP_RE, RECORD_RE, canon, check_id, now_utc
    from autosync_credentials import CredentialError, load_server_tokens
    from autosync_firestore_store import FirestoreStore
    from autosync_firestore_partitioned import FirestorePartitionedStore

MAX_BODY = 1_048_576
MAX_APP_TOKENS = 2
MAX_INLINE_PUSH_BYTES = 48 * 1024
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def _app_token_values(value):
    """Normalize one active token or an ordered old/new rotation pair."""
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, list) and 1 <= len(value) <= MAX_APP_TOKENS:
        values = value
    else:
        raise ValueError("each app must have one token or an ordered pair of rotation tokens")
    if any(not isinstance(item, str) or len(item.encode("utf-8")) < 32 for item in values):
        raise ValueError("each app token must be at least 32 bytes")
    if len(set(values)) != len(values):
        raise ValueError("duplicate app tokens are not allowed")
    return values

class Store:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(str(self.path), timeout=5, isolation_level=None, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.execute("PRAGMA synchronous=FULL")
        if self.db.execute("PRAGMA journal_mode=WAL").fetchone()[0].lower() != "wal":
            raise RuntimeError("WAL unavailable")
        version = self.db.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, 1, 2):
            raise RuntimeError("unsupported database version")
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS records(app TEXT, id TEXT, rev INTEGER, body TEXT, deleted INTEGER, updated TEXT, PRIMARY KEY(app,id));
        CREATE TABLE IF NOT EXISTS events(rev INTEGER PRIMARY KEY AUTOINCREMENT, app TEXT, id TEXT, body TEXT, deleted INTEGER, updated TEXT, op TEXT);
        CREATE INDEX IF NOT EXISTS events_app_rev ON events(app,rev);
        CREATE TABLE IF NOT EXISTS app_state(app TEXT PRIMARY KEY,last_rev INTEGER NOT NULL,history_floor INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS ops(op TEXT PRIMARY KEY, digest TEXT, status INTEGER, response TEXT);
        CREATE TABLE IF NOT EXISTS conflicts(id TEXT PRIMARY KEY, app TEXT, record_id TEXT, expected INTEGER, actual INTEGER, incoming TEXT, current TEXT, op TEXT, created TEXT);
        """)
        self.db.execute("""INSERT OR IGNORE INTO app_state(app,last_rev,history_floor)
            SELECT app,MAX(rev),0 FROM events GROUP BY app""")
        self.db.execute("PRAGMA user_version=2")
        # Full integrity scan is a one-time startup gate, not part of frequent readiness polling.
        self.startup_integrity = self.db.execute("PRAGMA quick_check").fetchone()[0]

    def close(self):
        with self.lock:
            self.db.close()

    def health(self):
        with self.lock:
            schema_version = int(self.db.execute("PRAGMA user_version").fetchone()[0])
            return {"integrity": self.startup_integrity, "schema_version": schema_version}

    def current_cursor(self, app):
        """Return the app revision without loading records or event history."""
        check_id(app, APP_RE)
        with self.lock:
            row = self.db.execute("SELECT last_rev FROM app_state WHERE app=?", (app,)).fetchone()
            return int(row["last_rev"]) if row else 0

    def state(self, app, since=None):
        check_id(app, APP_RE)
        with self.lock:
            state = self.db.execute("SELECT last_rev,history_floor FROM app_state WHERE app=?", (app,)).fetchone()
            cursor = int(state["last_rev"]) if state else 0
            history_floor = int(state["history_floor"]) if state else 0
            if since is None or (isinstance(since, int) and since >= 0 and since < history_floor):
                rows = self.db.execute("SELECT id,rev,body,deleted,updated FROM records WHERE app=? ORDER BY id", (app,)).fetchall()
                snapshot = [{"record_id": r["id"], "revision": r["rev"], "payload": json.loads(r["body"]),
                             "deleted": bool(r["deleted"]), "updated_utc": r["updated"]} for r in rows]
                if since is None:
                    return {"api_version": API_VERSION, "app_id": app, "cursor": cursor, "records": snapshot}
                return {"api_version": API_VERSION, "app_id": app, "cursor": cursor,
                        "history_floor": history_floor, "reset_required": True,
                        "error": "snapshot_required", "records": snapshot}
            if not isinstance(since, int) or since < 0:
                raise ValueError("bad cursor")
            rows = self.db.execute("SELECT rev,id,body,deleted,updated FROM events WHERE app=? AND rev>? ORDER BY rev", (app,since)).fetchall()
            return {"api_version": API_VERSION, "app_id": app, "cursor": cursor, "reset_required": False, "events": [
                {"revision": r["rev"], "record_id": r["id"], "payload": json.loads(r["body"]),
                 "deleted": bool(r["deleted"]), "updated_utc": r["updated"]} for r in rows]}

    def compact_events(self, app, through_revision):
        """Drop old event rows after a verified backup; lagging clients must snapshot-reset."""
        check_id(app, APP_RE)
        if not isinstance(through_revision, int) or isinstance(through_revision, bool) or through_revision < 0:
            raise ValueError("invalid compaction revision")
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                row = self.db.execute("SELECT last_rev,history_floor FROM app_state WHERE app=?", (app,)).fetchone()
                last_rev = int(row["last_rev"]) if row else 0
                floor = int(row["history_floor"]) if row else 0
                doomed = self.db.execute("SELECT COUNT(*),COALESCE(MAX(rev),0) FROM events WHERE app=? AND rev<=?",
                                         (app,through_revision)).fetchone()
                count, deleted_through = int(doomed[0]), int(doomed[1])
                if count:
                    floor = max(floor, deleted_through)
                    self.db.execute("DELETE FROM events WHERE app=? AND rev<=?", (app,through_revision))
                    self.db.execute("""INSERT INTO app_state(app,last_rev,history_floor) VALUES(?,?,?)
                        ON CONFLICT(app) DO UPDATE SET history_floor=excluded.history_floor""",
                        (app,last_rev,floor))
                self.db.execute("COMMIT")
                return {"deleted_events":count,"history_floor":floor,"cursor":last_rev}
            except Exception:
                self.db.execute("ROLLBACK")
                raise

    def write(self, app, rid, payload, deleted, expected, op):
        check_id(app, APP_RE); check_id(rid, RECORD_RE); check_id(op, OP_RE)
        if not isinstance(payload, dict) or not isinstance(deleted, bool) or not isinstance(expected, int) or expected < 0:
            raise ValueError("invalid write")
        body = canon(payload)
        if len(body.encode("utf-8")) > MAX_BODY:
            raise ValueError("payload too large")
        digest = hashlib.sha256(canon([app,rid,payload,deleted,expected]).encode("utf-8")).hexdigest()
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                oldop = self.db.execute("SELECT digest,status,response FROM ops WHERE op=?", (op,)).fetchone()
                if oldop:
                    if not hmac.compare_digest(oldop["digest"], digest):
                        self.db.execute("COMMIT")
                        return 409, {"error":"idempotency_key_reused"}
                    response = json.loads(oldop["response"]); response["duplicate"] = True
                    self.db.execute("COMMIT")
                    return oldop["status"], response
                row = self.db.execute("SELECT rev,body,deleted,updated FROM records WHERE app=? AND id=?", (app,rid)).fetchone()
                actual = row["rev"] if row else 0
                if actual != expected:
                    current = ({"revision":actual,"payload":json.loads(row["body"]),
                                "deleted":bool(row["deleted"]),"updated_utc":row["updated"]} if row else None)
                    response = {"error":"revision_conflict","conflict_id":str(uuid.uuid4()),
                                "app_id":app,"record_id":rid,"expected_revision":expected,"current":current,
                                "incoming":{"payload":payload,"deleted":deleted}}
                    self.db.execute("INSERT INTO conflicts VALUES(?,?,?,?,?,?,?,?,?)",
                        (response["conflict_id"],app,rid,expected,actual,canon(response["incoming"]),
                         canon(current) if current is not None else None,op,now_utc()))
                    status = 409
                else:
                    prior = self.db.execute("SELECT last_rev FROM app_state WHERE app=?", (app,)).fetchone()
                    previous_cursor = int(prior["last_rev"]) if prior else 0
                    updated = now_utc()
                    cur = self.db.execute("INSERT INTO events(app,id,body,deleted,updated,op) VALUES(?,?,?,?,?,?)",
                                          (app,rid,body,int(deleted),updated,op))
                    rev = cur.lastrowid
                    self.db.execute("""INSERT INTO app_state(app,last_rev,history_floor) VALUES(?,?,0)
                        ON CONFLICT(app) DO UPDATE SET last_rev=excluded.last_rev""", (app,rev))
                    self.db.execute("""INSERT INTO records(app,id,rev,body,deleted,updated) VALUES(?,?,?,?,?,?)
                        ON CONFLICT(app,id) DO UPDATE SET rev=excluded.rev,body=excluded.body,
                        deleted=excluded.deleted,updated=excluded.updated""",
                        (app,rid,rev,body,int(deleted),updated))
                    response = {"ok":True,"app_id":app,"record_id":rid,"revision":rev,
                                "previous_cursor":previous_cursor,"deleted":deleted,"updated_utc":updated,"duplicate":False}
                    status = 200
                self.db.execute("INSERT INTO ops VALUES(?,?,?,?)", (op,digest,status,canon(response)))
                self.db.execute("COMMIT")
                return status,response
            except Exception:
                self.db.execute("ROLLBACK")
                raise

def frame(payload, opcode=1):
    n=len(payload)
    if n < 126: head=bytes((0x80|opcode,n))
    elif n <= 65535: head=bytes((0x80|opcode,126))+struct.pack("!H",n)
    else: head=bytes((0x80|opcode,127))+struct.pack("!Q",n)
    return head+payload

class ServerLock:
    """Cross-process, OS-released lock so only one process owns the writer and push channel."""
    def __init__(self,path):
        self.path=Path(path);self.handle=None
    def acquire(self):
        self.path.parent.mkdir(parents=True,exist_ok=True)
        self.handle=open(self.path,"a+b")
        try:
            if __import__("os").name=="nt":
                import msvcrt
                self.handle.seek(0,2)
                if self.handle.tell()==0:self.handle.write(b"0");self.handle.flush()
                self.handle.seek(0);msvcrt.locking(self.handle.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl
                fcntl.flock(self.handle.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        except (OSError,ImportError) as exc:
            self.handle.close();self.handle=None
            raise RuntimeError("another AutoSync writer already owns this data directory") from exc
    def release(self):
        if self.handle is None:return
        try:
            if __import__("os").name=="nt":
                import msvcrt
                self.handle.seek(0);msvcrt.locking(self.handle.fileno(),msvcrt.LK_UNLCK,1)
            else:
                import fcntl
                fcntl.flock(self.handle.fileno(),fcntl.LOCK_UN)
        finally:self.handle.close();self.handle=None
class Server(ThreadingHTTPServer):
    daemon_threads=False
    block_on_close=True
    allow_reuse_address=True
    def __init__(self, addr, store, tokens, instance_lock, tls_context=None, startup_gated=False, module_version=None):
        super().__init__(addr, Handler); self.store=store; self.tokens={k:tuple(x.encode("utf-8") for x in _app_token_values(v)) for k,v in tokens.items()}; self.instance_lock=instance_lock
        if tls_context is not None:
            self.socket=tls_context.wrap_socket(self.socket,server_side=True)
        self.guard=threading.RLock(); self.writers_changed=threading.Condition(self.guard)
        self.clients={}; self.connections=set(); self.active_writes=0; self.closing=False; self.closed=False
        self.draining=False; self.serving=not startup_gated
        self.module_version=module_version or os.environ.get("AUTOSYNC_MODULE_VERSION","unknown")
        self.protocol_prefix="v2" if getattr(store,"api_version",None)=="2.0" else "v1"
        self.control_start_callback=None
        self.store_watchers={}; self.last_notified_revision={}; self.last_notified_bucket_sequence={}
    def service_actions(self):
        callback=self.control_start_callback
        if callback is not None:
            self.control_start_callback=None
            callback()
    def runtime_status(self):
        state="draining" if self.draining else ("serving" if self.serving else "warming")
        health=self.store.health()
        return {"state":state,"ready":state=="serving" and health["integrity"]=="ok",
                "api_version":getattr(self.store,"api_version",API_VERSION),
                "module_version":self.module_version,**health}
    def enter_draining(self):
        with self.guard:
            first=not self.draining
            self.draining=True; self.serving=False
            return first
    def begin_write(self):
        with self.guard:
            if self.closing or self.draining or not self.serving:return False
            self.active_writes+=1
            return True
    def end_write(self):
        with self.writers_changed:
            if self.active_writes>0:self.active_writes-=1
            self.writers_changed.notify_all()
    @staticmethod
    def _client_key(app,protocol):
        return app if protocol==1 else (protocol,app)
    def activate(self):
        with self.guard:
            if self.draining:return False
            self.serving=True
            return True
    def server_close(self):
        with self.guard:
            if self.closed or self.closing:return
            self.closing=True
            clients=[(app,client) for app,group in self.clients.items() for client in tuple(group)]
        try:
            deadline=time.monotonic()+30.0
            with self.writers_changed:
                while self.active_writes:
                    remaining=deadline-time.monotonic()
                    if remaining<=0:break
                    self.writers_changed.wait(remaining)
            with self.guard:connections=tuple(self.connections)
            with self.guard:
                watchers=tuple(self.store_watchers.values()); self.store_watchers.clear()
            for unsubscribe in watchers:
                try:unsubscribe()
                except Exception:pass
            for app,client in clients:
                try:
                    with client.ws_lock:
                        client.wfile.write(frame(b"",8)); client.wfile.flush()
                except (OSError,ValueError):pass
            for connection in connections:
                try:connection.settimeout(1.0)
                except OSError:pass
                try:connection.shutdown(socket.SHUT_RDWR)
                except OSError:pass
                try:connection.close()
                except OSError:pass
            super().server_close()
        finally:
            try:self.store.close()
            finally:
                self.instance_lock.release()
                with self.guard:self.closed=True
    def notify(self, app, value, protocol=1):
        revision=value.get("revision") if isinstance(value,dict) else None
        if protocol==1 and isinstance(revision,int):
            with self.guard:
                previous=self.last_notified_revision.get(app,0)
                if revision<=previous:return
                self.last_notified_revision[app]=revision
        elif protocol==2 and isinstance(value,dict):
            event=value.get("event")
            bucket=value.get("bucket_id")
            sequence=value.get("bucket_seq")
            if isinstance(event,dict):
                bucket=event.get("bucket_id",bucket)
                sequence=event.get("bucket_seq",sequence)
            if isinstance(bucket,int) and isinstance(sequence,int):
                key=(app,bucket)
                with self.guard:
                    previous=self.last_notified_bucket_sequence.get(key,0)
                    if sequence<=previous:return
                    self.last_notified_bucket_sequence[key]=sequence
        data=frame(canon(value).encode())
        key=self._client_key(app,protocol)
        with self.guard: clients=list(self.clients.get(key,()))
        for client in clients:
            try:
                with client.ws_lock: client.wfile.write(data); client.wfile.flush()
            except (OSError,ValueError):
                self.drop(app,client,protocol)
    def add(self,app,client,protocol=1):
        key=self._client_key(app,protocol)
        with self.guard:
            if self.closing:return False
            group=self.clients.setdefault(key,set())
            first=not group
            group.add(client)
        subscribe=getattr(self.store,"subscribe",None)
        if first and callable(subscribe):
            def forward(notification):
                if len(canon(notification).encode("utf-8"))>MAX_INLINE_PUSH_BYTES:
                    notification=dict(notification);notification.pop("event",None)
                self.notify(app,notification,protocol)
            try:unsubscribe=subscribe(app,forward)
            except Exception:
                with self.guard:
                    group=self.clients.get(key)
                    if group:
                        group.discard(client)
                        if not group:self.clients.pop(key,None)
                return False
            with self.guard:
                if self.closing or not self.clients.get(key):
                    keep=False
                else:
                    self.store_watchers[key]=unsubscribe;keep=True
            if not keep:
                try:unsubscribe()
                except Exception:pass
                return False
        return True
    def drop(self,app,client,protocol=1):
        key=self._client_key(app,protocol)
        unsubscribe=None
        with self.guard:
            group=self.clients.get(key)
            if group:
                group.discard(client)
                if not group:
                    self.clients.pop(key,None)
                    unsubscribe=self.store_watchers.pop(key,None)
        if unsubscribe:
            try:unsubscribe()
            except Exception:pass

class HostControlHandler(BaseHTTPRequestHandler):
    protocol_version="HTTP/1.1"
    def log_message(self,*args): pass
    def authorized(self):
        try: loopback=ipaddress.ip_address(self.client_address[0]).is_loopback
        except ValueError: loopback=False
        value=self.headers.get("Authorization","")
        expected="Bearer "+self.server.control_token
        return loopback and hmac.compare_digest(value.encode("utf-8"),expected.encode("utf-8"))
    def send_json(self,status,value):
        data=canon(value).encode("utf-8")
        self.send_response(status); self.send_header("Content-Type","application/json; charset=utf-8")
        self.send_header("Content-Length",str(len(data))); self.send_header("Cache-Control","no-store")
        self.end_headers(); self.wfile.write(data)
    def do_GET(self):
        if not self.authorized(): return self.send_json(401,{"error":"unauthorized"})
        if urlsplit(self.path).path!="/_host/status": return self.send_json(404,{"error":"not_found"})
        try: return self.send_json(200,self.server.parent_server.runtime_status())
        except Exception: return self.send_json(503,{"error":"health_unavailable"})
    def do_POST(self):
        if not self.authorized(): return self.send_json(401,{"error":"unauthorized"})
        path=urlsplit(self.path).path
        if path=="/_host/activate":
            if not self.server.parent_server.activate(): return self.send_json(409,{"error":"server_draining"})
            return self.send_json(200,self.server.parent_server.runtime_status())
        if path=="/_host/drain":
            first=self.server.parent_server.enter_draining()
            self.send_json(202,{"state":"draining"})
            if first:
                threading.Thread(target=self.server.parent_server.shutdown,name="autosync-host-drain",daemon=True).start()
            return
        return self.send_json(404,{"error":"not_found"})


def start_host_control(server,host,port,token):
    if host!="127.0.0.1": raise ValueError("host control must bind only to 127.0.0.1")
    if not isinstance(port,int) or port<0 or port>65535: raise ValueError("invalid host-control port")
    if not isinstance(token,str) or len(token.encode("utf-8"))<32: raise ValueError("host-control token must be at least 32 bytes")
    control=ThreadingHTTPServer((host,port),HostControlHandler)
    control.control_token=token; control.parent_server=server
    thread=threading.Thread(target=control.serve_forever,name="autosync-host-control",daemon=True)
    thread.start()
    return control,thread


class Handler(BaseHTTPRequestHandler):
    protocol_version="HTTP/1.1"
    def setup(self):
        super().setup()
        self.connection.settimeout(15)
        with self.server.guard:self.server.connections.add(self.connection)
    def handle(self):
        try:super().handle()
        except OSError:
            if not self.server.closing:raise
    def finish(self):
        connection=self.connection
        try:super().finish()
        finally:
            with self.server.guard:self.server.connections.discard(connection)
    def log_message(self,*args): pass
    def authorized(self):
        value=self.headers.get("Authorization","")
        if not value.startswith("Bearer "): return False
        candidate=value[7:].encode()
        pieces=[x for x in urlsplit(self.path).path.split("/") if x]
        app=pieces[2] if len(pieces)>2 and pieces[:2]==[self.server.protocol_prefix,"apps"] else None
        if app is not None:
            expected=self.server.tokens.get(app)
            return expected is not None and any(hmac.compare_digest(candidate,secret) for secret in expected)
        return any(hmac.compare_digest(candidate,secret) for secrets in self.server.tokens.values() for secret in secrets)
    def send_json(self,status,value):
        data=canon(value).encode()
        self.send_response(status); self.send_header("Content-Type","application/json; charset=utf-8")
        self.send_header("Content-Length",str(len(data))); self.send_header("Cache-Control","no-store")
        if self.close_connection:self.send_header("Connection","close")
        self.end_headers(); self.wfile.write(data)
    def get_body(self):
        if self.headers.get_content_type()!="application/json": raise ValueError()
        rawlen=self.headers.get("Content-Length","")
        if not rawlen.isdigit() or int(rawlen)<1 or int(rawlen)>MAX_BODY: raise ValueError()
        value=json.loads(self.rfile.read(int(rawlen)).decode("utf-8"))
        if not isinstance(value,dict): raise ValueError()
        return value
    def do_GET(self):
        if not self.authorized(): return self.send_json(401,{"error":"unauthorized"})
        if self.server.draining:
            self.close_connection=True
            return self.send_json(503,{"error":"service_updating","retry_after_seconds":3})
        if not self.server.serving:
            self.close_connection=True
            return self.send_json(503,{"error":"service_warming","retry_after_seconds":3})
        p=urlsplit(self.path); parts=[x for x in p.path.split("/") if x]
        if parts==["health"]:
            try: storage=self.server.store.health()
            except Exception:return self.send_json(503,{"ok":False,"error":"storage_unavailable"})
            return self.send_json(200,{"ok":True,"api_version":getattr(self.server.store,"api_version",API_VERSION),
                                       "module_version":self.server.module_version,
                                       "storage_backend":storage.get("backend","sqlite")})
        if len(parts)==4 and parts[:2]==[self.server.protocol_prefix,"apps"] and parts[3]=="state":
            try:
                q=parse_qs(p.query,strict_parsing=True) if p.query else {}
                if self.server.protocol_prefix=="v1":
                    if set(q)-{"since"} or len(q.get("since",[]))>1: raise ValueError()
                    since=int(q["since"][0]) if "since" in q else None
                else:
                    if set(q)-{"cursor"} or len(q.get("cursor",[]))>1: raise ValueError()
                    encoded=q.get("cursor",[None])[0]
                    if encoded is None:
                        since=None
                    else:
                        if not encoded or len(encoded)>16_384: raise ValueError()
                        raw=base64.urlsafe_b64decode(encoded+"="*((4-len(encoded)%4)%4))
                        since=json.loads(raw.decode("utf-8"))
                        if not isinstance(since,dict): raise ValueError()
                result=self.server.store.state(check_id(parts[2],APP_RE),since)
                return self.send_json(409 if result.get("error")=="snapshot_required" else 200,result)
            except (ValueError,TypeError): return self.send_json(400,{"error":"invalid_request"})
            except Exception:return self.send_json(503,{"error":"storage_unavailable"})
        if len(parts)==4 and parts[:2]==[self.server.protocol_prefix,"apps"] and parts[3]=="stream":
            try: app=check_id(parts[2],APP_RE)
            except ValueError: return self.send_json(400,{"error":"invalid_request"})
            self.close_connection=True
            return self.websocket(app,2 if self.server.protocol_prefix=="v2" else 1)
        return self.send_json(404,{"error":"not_found"})
    def do_POST(self):
        if not self.authorized(): return self.send_json(401,{"error":"unauthorized"})
        if self.server.draining:
            self.close_connection=True
            return self.send_json(503,{"error":"service_updating","retry_after_seconds":3})
        if not self.server.serving:
            self.close_connection=True
            return self.send_json(503,{"error":"service_warming","retry_after_seconds":3})
        parts=[x for x in urlsplit(self.path).path.split("/") if x]
        if len(parts)!=5 or parts[:2]!=[self.server.protocol_prefix,"apps"] or parts[3]!="records":
            return self.send_json(404,{"error":"not_found"})
        try:
            match=self.headers.get("If-Match","")
            if not re.fullmatch(r"\d+",match): raise ValueError()
            body=self.get_body()
            if set(body)-{"payload","deleted"} or "payload" not in body: raise ValueError()
            if not self.server.begin_write():
                self.close_connection=True
                return self.send_json(503,{"error":"service_updating","retry_after_seconds":3})
            try:
                status,result=self.server.store.write(parts[2],parts[4],body["payload"],body.get("deleted",False),
                                                       int(match),self.headers.get("Idempotency-Key",""))
                self.send_json(status,result)
                if status==200 and not result.get("duplicate"):
                    if self.server.protocol_prefix=="v1":
                        event={"revision":result["revision"],"previous_cursor":result["previous_cursor"],
                               "record_id":parts[4],"payload":body["payload"],
                               "deleted":body.get("deleted",False),"updated_utc":result["updated_utc"]}
                        notification={"type":"changed","app_id":parts[2],"record_id":parts[4],
                                      "revision":result["revision"],"event":event}
                        if len(canon(notification).encode("utf-8"))>MAX_INLINE_PUSH_BYTES:
                            notification.pop("event")
                        self.server.notify(parts[2],notification)
                    else:
                        event={"record_id":parts[4],"revision":result["revision"],
                               "payload":body["payload"],"deleted":body.get("deleted",False),
                               "updated_utc":result["updated_utc"],"bucket_id":result["bucket_id"],
                               "bucket_seq":result["bucket_seq"],"bucket_count":self.server.store.bucket_count}
                        notification={"type":"changed","app_id":parts[2],"record_id":parts[4],
                                      "revision":result["revision"],"bucket_id":result["bucket_id"],
                                      "bucket_seq":result["bucket_seq"],"event":event}
                        if len(canon(notification).encode("utf-8"))>MAX_INLINE_PUSH_BYTES:
                            notification.pop("event")
                        self.server.notify(parts[2],notification,2)
            finally:self.server.end_write()
        except (ValueError,TypeError,json.JSONDecodeError,UnicodeDecodeError):
            self.send_json(400,{"error":"invalid_request"})
        except Exception:
            self.send_json(503,{"error":"storage_unavailable"})
    def websocket(self,app,protocol=1):
        key=self.headers.get("Sec-WebSocket-Key",""); origin=self.headers.get("Origin")
        connection_tokens={x.strip().lower() for x in self.headers.get("Connection","").split(",")}
        if self.headers.get("Upgrade","").lower()!="websocket" or "upgrade" not in connection_tokens or self.headers.get("Sec-WebSocket-Version")!="13":
            return self.send_json(400,{"error":"websocket_upgrade_required"})
        if origin and urlsplit(origin).netloc.lower()!=self.headers.get("Host","").lower():
            return self.send_json(400,{"error":"origin_mismatch"})
        try: decoded=base64.b64decode(key,validate=True)
        except ValueError: return self.send_json(400,{"error":"invalid_handshake"})
        if len(decoded)!=16: return self.send_json(400,{"error":"invalid_handshake"})
        accept=base64.b64encode(hashlib.sha1((key+WS_GUID).encode()).digest()).decode()
        self.send_response(101); self.send_header("Upgrade","websocket"); self.send_header("Connection","Upgrade")
        self.send_header("Sec-WebSocket-Accept",accept); self.end_headers(); self.wfile.flush()
        self.connection.settimeout(1.0)
        self.ws_lock=threading.Lock()
        if not self.server.add(app,self,protocol):
            try:self.wfile.write(frame(b"",8));self.wfile.flush()
            except OSError:pass
            return
        def read_exact(length):
            data=bytearray()
            while len(data)<length:
                try:chunk=self.connection.recv(length-len(data))
                except socket.timeout:
                    if self.server.closing:return None
                    continue
                except OSError:return None
                if not chunk:return None
                data.extend(chunk)
            return bytes(data)
        try:
            cursor_fn=getattr(self.server.store,"cached_cursor",self.server.store.current_cursor)
            ready={"type":"ready","app_id":app,"cursor":cursor_fn(app)}
            with self.ws_lock: self.wfile.write(frame(canon(ready).encode())); self.wfile.flush()
            while True:
                head=read_exact(2)
                if head is None:break
                opcode=head[0]&15; n=head[1]&127
                if not head[0]&128 or not head[1]&128: break
                if n==126:
                    extended=read_exact(2)
                    if extended is None:break
                    n=struct.unpack("!H",extended)[0]
                elif n==127:
                    extended=read_exact(8)
                    if extended is None:break
                    n=struct.unpack("!Q",extended)[0]
                if n>65536: break
                mask=read_exact(4); raw=read_exact(n)
                if mask is None or raw is None:break
                data=bytearray(raw)
                for i in range(n): data[i]^=mask[i%4]
                if opcode==8:
                    with self.ws_lock: self.wfile.write(frame(bytes(data),8)); self.wfile.flush()
                    break
                if opcode==9:
                    with self.ws_lock: self.wfile.write(frame(bytes(data),10)); self.wfile.flush()
        except (OSError,ValueError,ConnectionError,TypeError): pass
        finally: self.server.drop(app,self,protocol)

def create_server(host,port,data_dir,tokens,tls_context=None,allow_lan=False,startup_gated=False,module_version=None,
                  storage_backend="sqlite",firestore_project=None,firestore_collection_prefix="autosync_v1",
                  cloud_run=False,firestore_client=None,firestore_transactional=None):
    if storage_backend not in ("sqlite","firestore","firestore-v2"):
        raise ValueError("unsupported storage backend")
    if cloud_run and (storage_backend not in ("firestore","firestore-v2") or tls_context is not None):
        raise ValueError("Cloud Run mode requires Firestore storage and ingress-terminated TLS")
    try: addr=ipaddress.ip_address(host)
    except ValueError as exc: raise ValueError("host must be an IP address") from exc
    if addr.version != 4: raise ValueError("this prototype currently supports IPv4 only")
    if not addr.is_loopback:
        if cloud_run:
            if not addr.is_unspecified: raise ValueError("Cloud Run must bind to 0.0.0.0")
        else:
            if not allow_lan: raise ValueError("LAN binding requires explicit --allow-lan after host readiness checks")
            if not addr.is_private or addr.is_link_local or addr.is_multicast or addr.is_unspecified:
                raise ValueError("LAN binding is limited to a specific private IPv4 address")
            if tls_context is None: raise ValueError("LAN binding requires TLS")
    elif cloud_run:
        raise ValueError("Cloud Run must bind to 0.0.0.0")
    if tls_context is not None:
        if not isinstance(tls_context,ssl.SSLContext) or tls_context.minimum_version < ssl.TLSVersion.TLSv1_2:
            raise ValueError("TLS 1.2 or newer is required")
    if not isinstance(tokens,dict) or not tokens: raise ValueError("app-scoped tokens are required")
    for app,secret in tokens.items():
        check_id(app,APP_RE)
        _app_token_values(secret)
    root=Path(data_dir);root.mkdir(parents=True,exist_ok=True)
    instance_lock=ServerLock(root/"server.lock");instance_lock.acquire()
    store=None
    try:
        if storage_backend=="firestore":
            store=FirestoreStore(project_id=firestore_project,collection_prefix=firestore_collection_prefix,
                                 client=firestore_client,transactional=firestore_transactional)
        elif storage_backend=="firestore-v2":
            prefix=("autosync_v2" if firestore_collection_prefix=="autosync_v1" else firestore_collection_prefix)
            store=FirestorePartitionedStore(project_id=firestore_project,collection_prefix=prefix,
                                            client=firestore_client,transactional=firestore_transactional)
        else:
            store=Store(root/"autosync.sqlite3")
        return Server((host,port),store,tokens,instance_lock,tls_context,startup_gated,module_version)
    except Exception:
        if store is not None:store.close()
        instance_lock.release()
        raise

def main():
    parser=argparse.ArgumentParser(description="AutoSync service; local/LAN prototype or Firestore-backed Cloud Run mode")
    parser.add_argument("--host",default="127.0.0.1")
    parser.add_argument("--port",type=int,default=8765)
    parser.add_argument("--data-dir",type=Path,default=Path(os.environ.get("LOCALAPPDATA",str(Path.home())))/"Siny"/"AutoSync"/"data")
    parser.add_argument("--tls-cert",type=Path,help="PEM certificate chain; required for LAN mode")
    parser.add_argument("--tls-key",type=Path,help="PEM private key; required with --tls-cert")
    parser.add_argument("--allow-lan",action="store_true",help="explicitly enable a specific private IPv4 bind after host checks")
    parser.add_argument("--credential-apps",help="comma-separated app IDs whose tokens come from the current Windows user's Credential Manager")
    parser.add_argument("--credential-target-prefix",default="AutoSync/server/",help="Credential Manager target prefix; default: AutoSync/server/")
    parser.add_argument("--module-version",help="verified AutoSync module version reported by the host supervisor")
    parser.add_argument("--storage",choices=("sqlite","firestore","firestore-v2"),default="sqlite",help="server persistence backend; firestore-v2 is experimental")
    parser.add_argument("--firestore-project",default=os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCLOUD_PROJECT"))
    parser.add_argument("--firestore-collection-prefix",default="autosync_v1")
    parser.add_argument("--cloud-run",action="store_true",help="bind to Cloud Run PORT using Firestore and platform TLS")
    parser.add_argument("--startup-gated",action="store_true",help="do not serve app traffic until the local host supervisor activates this process")
    parser.add_argument("--control-host",default="127.0.0.1",help="local-only host-control address")
    parser.add_argument("--control-port",type=int,help="host-control port; requires AUTOSYNC_CONTROL_TOKEN")
    args=parser.parse_args()
    if args.cloud_run:
        if not os.environ.get("K_SERVICE"):
            parser.error("--cloud-run may only run inside a Cloud Run service (K_SERVICE is missing)")
        if args.storage not in ("firestore","firestore-v2"):parser.error("--cloud-run requires a Firestore storage backend")
        if not args.firestore_project:parser.error("Cloud Run requires GOOGLE_CLOUD_PROJECT or --firestore-project")
        if args.tls_cert or args.tls_key or args.allow_lan or args.credential_apps or args.startup_gated or args.control_port is not None:
            parser.error("--cloud-run uses ingress TLS, Secret Manager environment tokens, and no local host-control API")
        try: args.port=int(os.environ.get("PORT","8080"))
        except ValueError:parser.error("Cloud Run PORT must be an integer")
        args.host="0.0.0.0"
        if args.data_dir==Path(os.environ.get("LOCALAPPDATA",str(Path.home())))/"Siny"/"AutoSync"/"data":
            args.data_dir=Path(os.environ.get("TMPDIR","/tmp"))/"autosync-runtime"
    control_token=os.environ.get("AUTOSYNC_CONTROL_TOKEN")
    if args.startup_gated and args.control_port is None: parser.error("--startup-gated requires --control-port")
    if args.control_port is not None and (not control_token or len(control_token.encode("utf-8"))<32): parser.error("--control-port requires a 32-byte AUTOSYNC_CONTROL_TOKEN")
    if args.control_port is not None and args.control_host!="127.0.0.1": parser.error("host control must bind only to 127.0.0.1")
    if args.credential_apps:
        if os.environ.get("AUTOSYNC_TOKENS") not in (None,"","{}"):
            parser.error("use either --credential-apps or AUTOSYNC_TOKENS, not both")
        app_ids=[item.strip() for item in args.credential_apps.split(",") if item.strip()]
        try:
            tokens=load_server_tokens(app_ids,args.credential_target_prefix)
        except (CredentialError,ValueError) as exc:
            parser.error(str(exc))
    else:
        try:
            tokens=json.loads(os.environ.get("AUTOSYNC_TOKENS","{}"))
        except json.JSONDecodeError as exc:
            parser.error("AUTOSYNC_TOKENS must contain a JSON object")
    if bool(args.tls_cert) != bool(args.tls_key): parser.error("--tls-cert and --tls-key must be provided together")
    tls_context=None
    if args.tls_cert:
        tls_context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls_context.minimum_version=ssl.TLSVersion.TLSv1_2
        tls_context.load_cert_chain(args.tls_cert,args.tls_key)
    server=create_server(args.host,args.port,args.data_dir,tokens,tls_context,args.allow_lan,args.startup_gated,args.module_version,
                         storage_backend=args.storage,firestore_project=args.firestore_project,
                         firestore_collection_prefix=args.firestore_collection_prefix,cloud_run=args.cloud_run)
    control_state={"server":None,"thread":None}
    if args.control_port is not None:
        def launch_control():
            control_state["server"],control_state["thread"]=start_host_control(server,args.control_host,args.control_port,control_token)
        server.control_start_callback=launch_control
    scheme="https" if tls_context else "http"
    scope="Cloud Run ingress TLS" if args.cloud_run else ("TLS transport" if tls_context else "loopback only")
    print(f"AutoSync service listening on {scheme}://{args.host}:{args.port} ({scope})")
    print(f"Storage backend: {args.storage}" + (f"; Firestore project: {args.firestore_project}" if args.storage.startswith("firestore") else f"; SQLite data: {args.data_dir.resolve()}"))
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally:
        server.shutdown(); server.server_close()
        control=control_state["server"]; control_thread=control_state["thread"]
        if control is not None:
            control.shutdown(); control.server_close()
            if control_thread is not None: control_thread.join(timeout=2)
    return 0

if __name__=="__main__": raise SystemExit(main())

