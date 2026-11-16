import base64, concurrent.futures, json, socket, tempfile, threading, time, unittest, urllib.error, urllib.request, uuid
import ssl
from pathlib import Path
from unittest.mock import patch
from src.autosync_server import Store, create_server
from src import __version__ as AUTOSYNC_VERSION
from src.autosync_client import AutoSyncClient, Outbox, connection_status

HTTP_ONE_SHOT = tuple(int(part) for part in AUTOSYNC_VERSION.split(".")) >= (0, 1, 24)

TOKEN="test-token-at-least-32-bytes-long"
APP="test-app"

class ClientStatusTests(unittest.TestCase):
    def test_tls_errors_are_blocked_while_network_errors_are_offline(self):
        tls_error=urllib.error.URLError(ssl.SSLCertVerificationError(1,"bad certificate"))
        self.assertEqual(connection_status(tls_error)["status"],"blocked")
        self.assertEqual(connection_status(ConnectionError())["status"],"offline")

def call(url,method="GET",payload=None,headers=None,token=TOKEN):
    h={"Authorization":"Bearer "+token};h.update(headers or {})
    data=json.dumps(payload).encode() if payload is not None else None
    if data is not None:h["Content-Type"]="application/json"
    req=urllib.request.Request(url,data=data,headers=h,method=method)
    try:
        with urllib.request.urlopen(req,timeout=3) as res:return res.status,json.loads(res.read())
    except urllib.error.HTTPError as e:
        try:return e.code,json.loads(e.read())
        finally:e.close()

class StoreTests(unittest.TestCase):
    def setUp(self):self.temp=tempfile.TemporaryDirectory();self.path=Path(self.temp.name)/"db.sqlite3";self.store=Store(self.path)
    def tearDown(self):self.store.close();self.temp.cleanup()
    def write(self,app,rid,value,expected,op=None,deleted=False):
        return self.store.write(app,rid,value,deleted,expected,op or str(uuid.uuid4()))
    def test_app_namespaces_isolated(self):
        self.write("workboard","one",{"v":"A"},0);self.write("alarm","one",{"v":"B"},0)
        self.assertEqual(self.store.state("workboard")["records"][0]["payload"]["v"],"A")
        self.assertEqual(self.store.state("alarm")["records"][0]["payload"]["v"],"B")
    def test_conflict_keeps_current_and_incoming(self):
        self.write(APP,"one",{"v":"saved"},0);code,result=self.write(APP,"one",{"v":"offline"},0)
        self.assertEqual(code,409);self.assertEqual(result["current"]["payload"]["v"],"saved")
        self.assertEqual(result["incoming"]["payload"]["v"],"offline")
        self.assertEqual(self.store.state(APP)["records"][0]["payload"]["v"],"saved")
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM conflicts").fetchone()[0],1)
    def test_idempotent_retry_and_key_reuse(self):
        op="operation-key-00001";code,first=self.write(APP,"one",{"v":1},0,op)
        code2,retry=self.write(APP,"one",{"v":1},0,op);code3,reused=self.write(APP,"one",{"v":2},0,op)
        self.assertEqual((code,code2,code3),(200,200,409));self.assertTrue(retry["duplicate"])
        self.assertEqual(first["revision"],retry["revision"]);self.assertEqual(reused["error"],"idempotency_key_reused")
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM events").fetchone()[0],1)
    def test_idempotency_key_cannot_be_reused_for_same_payload_at_new_revision(self):
        op="same-payload-new-revision"
        status,first=self.write(APP,"one",{"v":1},0,op)
        status2,reused=self.write(APP,"one",{"v":1},first["revision"],op)
        self.assertEqual(status,200)
        self.assertEqual(status2,409)
        self.assertEqual(reused["error"],"idempotency_key_reused")
        self.assertEqual(self.store.state(APP)["cursor"],first["revision"])
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM events").fetchone()[0],1)

    def test_tombstones_and_reopen(self):
        _,first=self.write(APP,"one",{"text":"retained"},0)
        _,gone=self.write(APP,"one",{"text":"retained"},first["revision"],deleted=True)
        self.assertTrue(gone["deleted"]);self.store.close();self.store=Store(self.path)
        item=self.store.state(APP)["records"][0];self.assertTrue(item["deleted"]);self.assertEqual(item["payload"]["text"],"retained")

    def test_v1_database_migrates_existing_event_cursor_without_forcing_reset(self):
        _,written=self.write(APP,"existing",{"v":"pre-migration"},0)
        self.store.close()
        import sqlite3
        db=sqlite3.connect(self.path)
        try:
            db.execute("DROP TABLE app_state")
            db.execute("PRAGMA user_version=1")
            db.commit()
        finally:
            db.close()
        self.store=Store(self.path)
        self.assertEqual(self.store.db.execute("PRAGMA user_version").fetchone()[0],2)
        result=self.store.state(APP,since=0)
        self.assertFalse(result["reset_required"])
        self.assertEqual(result["cursor"],written["revision"])
        self.assertEqual(result["events"][0]["payload"]["v"],"pre-migration")

    def test_compacted_history_requires_snapshot_but_retains_tombstones_and_cursor(self):
        op1="compact-operation-0001"
        _,first=self.write(APP,"gone",{"v":1},0,op1)
        _,tombstone=self.write(APP,"gone",{"v":1},first["revision"],deleted=True)
        _,latest=self.write(APP,"live",{"v":2},0)
        result=self.store.compact_events(APP,tombstone["revision"])
        self.assertEqual(result,{"deleted_events":2,"history_floor":tombstone["revision"],"cursor":latest["revision"]})
        reset=self.store.state(APP,since=first["revision"])
        self.assertEqual(reset["error"],"snapshot_required");self.assertTrue(reset["reset_required"])
        self.assertEqual(reset["cursor"],latest["revision"])
        records={item["record_id"]:item for item in reset["records"]}
        self.assertTrue(records["gone"]["deleted"]);self.assertEqual(records["live"]["payload"]["v"],2)
        resumed=self.store.state(APP,since=tombstone["revision"])
        self.assertFalse(resumed["reset_required"])
        self.assertEqual([event["record_id"] for event in resumed["events"]],["live"])
        _,duplicate=self.write(APP,"gone",{"v":1},0,op1)
        self.assertTrue(duplicate["duplicate"],"operation receipt must survive event compaction")

    def test_concurrent_same_revision_has_one_winner(self):
        gate=threading.Barrier(3)
        def put(value):
            gate.wait();return self.write(APP,"race",{"value":value},0,str(uuid.uuid4()))[0]
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            a=pool.submit(put,"A");b=pool.submit(put,"B");gate.wait();codes=sorted([a.result(),b.result()])
        self.assertEqual(codes,[200,409]);self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM conflicts").fetchone()[0],1)
class HttpTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.srv=create_server("127.0.0.1",0,Path(self.temp.name),{APP:TOKEN})
        self.port=self.srv.server_address[1];self.base=f"http://127.0.0.1:{self.port}"
        self.thread=threading.Thread(target=self.srv.serve_forever,daemon=True);self.thread.start()
    def tearDown(self):
        if self.thread.is_alive():self.srv.shutdown()
        if not self.srv.closed and not self.srv.closing:self.srv.server_close()
        self.thread.join(timeout=2);self.temp.cleanup()

    def test_server_drain_waits_for_writes_then_closes_idle_keepalive(self):
        client=AutoSyncClient(self.base,TOKEN,APP,Path(self.temp.name)/"drain-keepalive")
        connection=None; closer=None
        try:
            self.assertEqual(client._http(f"/v1/apps/{APP}/state?since=0")[0],200)
            connection=client._http_connection
            self.assertTrue(self.srv.begin_write())
            self.srv.enter_draining()
            self.assertFalse(self.srv.begin_write(),"new writes must be rejected after drain begins")
            status,_=client._http("/health")
            self.assertEqual(status,503,"draining service should reject and close the persistent HTTP session")
            self.assertIsNone(client._http_connection,"draining response must release the client keep-alive socket")
            self.srv.shutdown()
            closer=threading.Thread(target=self.srv.server_close,daemon=True); closer.start()
            time.sleep(0.05)
            self.assertTrue(closer.is_alive(),"server closed before the active write drained")
            self.srv.end_write(); closer.join(timeout=5); self.thread.join(timeout=5)
            self.assertFalse(closer.is_alive(),"idle HTTP keep-alive connection blocked server shutdown")
            self.assertFalse(self.thread.is_alive(),"serve thread did not stop")
            self.assertEqual(len(self.srv.connections),0)
        finally:
            if closer and closer.is_alive():self.srv.end_write(); closer.join(timeout=5)
            client.close()

    def test_http_connection_lifetime_matches_module_version(self):
        client=AutoSyncClient(self.base,TOKEN,APP,Path(self.temp.name)/"keep-alive")
        connection=None
        try:
            first=client._http(f"/v1/apps/{APP}/state?since=0")
            connection=client._http_connection
            second=client._http(f"/v1/apps/{APP}/state?since=0")
            self.assertEqual((first[0],second[0]),(200,200))
            if HTTP_ONE_SHOT:
                self.assertIsNone(client._http_connection)
            else:
                self.assertIsNotNone(connection)
                self.assertIs(client._http_connection,connection)
                self.assertIsNotNone(connection.sock)
        finally:
            client.close()
        if HTTP_ONE_SHOT:
            self.assertIsNone(client._http_connection)
        else:
            self.assertIsNone(client._http_connection)
            self.assertIsNone(connection.sock)

    def test_empty_sync_skips_redundant_second_pull(self):
        client=AutoSyncClient(self.base,TOKEN,APP,Path(self.temp.name)/"single-pull")
        pulls=[];original=client._pull
        def counted_pull():
            pulls.append(1)
            return original()
        client._pull=counted_pull
        try:
            self.assertEqual(client.sync_once()["status"],"synced")
            self.assertEqual(len(pulls),1)
        finally:
            client.close()
    def test_websocket_watch_uses_transport_specific_health(self):
        client=AutoSyncClient(self.base,TOKEN,APP,Path(self.temp.name)/"watch-transport-health")
        stop=threading.Event(); requests=[]; syncs=[]
        class FakeSocket:
            def __init__(self):
                raw=json.dumps({"type":"ready"}).encode()
                self.frame=bytes((0x81,len(raw)))+raw
            def setblocking(self,value): pass
            def recv(self,size):
                frame=self.frame;self.frame=b"";return frame
            def sendall(self,data): pass
            def close(self): pass
        fake=FakeSocket()
        client.sync_once=lambda:(syncs.append(True) or {"status":"synced"})
        client._connect_websocket=lambda:fake
        def fake_http(path,method="GET",payload=None,headers=None):
            requests.append(path)
            return 200,{"ok":True}
        client._http=fake_http
        selected=False
        def fake_select(sockets,writable,exceptional,timeout):
            nonlocal selected
            if not selected:
                selected=True
                return [sockets[0]],[],[]
            time.sleep(min(timeout,0.005))
            return [],[],[]
        thread=threading.Thread(target=client.watch,args=(stop,),daemon=True)
        try:
            def run_watch():
                with patch("src.autosync_client.select.select",side_effect=fake_select):
                    thread.start()
                    deadline=time.monotonic()+1
                    target=requests if not HTTP_ONE_SHOT else syncs
                    while time.monotonic()<deadline and not target:
                        time.sleep(0.005)
                    stop.set();thread.join(timeout=2)
            if HTTP_ONE_SHOT:
                run_watch()
            else:
                with patch("src.autosync_client.HTTP_KEEPALIVE_SECONDS",0.01):
                    run_watch()
            self.assertFalse(thread.is_alive())
            self.assertTrue(syncs,"WebSocket ready should trigger a durable sync")
            if HTTP_ONE_SHOT:
                self.assertEqual(requests,[],"HTTP keepalive health must not be required")
            else:
                self.assertIn("/health",requests)
        finally:
            stop.set()
            if thread.is_alive():thread.join(timeout=2)
            client.close()

    def test_lagging_delta_request_gets_409_snapshot_not_silent_cursor_advance(self):
        _,first=self.srv.store.write(APP,"record",{"v":1},False,0,str(uuid.uuid4()))
        _,second=self.srv.store.write(APP,"record",{"v":2},False,first["revision"],str(uuid.uuid4()))
        self.srv.store.compact_events(APP,second["revision"])
        status,result=call(self.base+f"/v1/apps/{APP}/state?since={first['revision']}")
        self.assertEqual(status,409);self.assertEqual(result["error"],"snapshot_required")
        self.assertEqual(result["cursor"],second["revision"]);self.assertEqual(result["records"][0]["payload"]["v"],2)

    def test_auth_and_conditional_conflict(self):
        self.assertEqual(call(self.base+"/health",token="bad")[0],401)
        self.assertEqual(call(self.base+"/v1/apps/other-app/state",token=TOKEN)[0],401)
        url=self.base+f"/v1/apps/{APP}/records/r1"
        code,created=call(url,"POST",{"payload":{"v":"one"}},{"If-Match":"0","Idempotency-Key":str(uuid.uuid4())})
        self.assertEqual(code,200)
        code,conflict=call(url,"POST",{"payload":{"v":"two"}},{"If-Match":"0","Idempotency-Key":str(uuid.uuid4())})
        self.assertEqual(code,409);self.assertEqual(conflict["current"]["payload"]["v"],"one")
        self.assertEqual(call(self.base+f"/v1/apps/{APP}/state")[1]["records"][0]["revision"],created["revision"])
    def test_websocket_push(self):
        sock=socket.create_connection(("127.0.0.1",self.port),timeout=3);sock.settimeout(3)
        key=base64.b64encode(b"0123456789abcdef").decode()
        req=(f"GET /v1/apps/{APP}/stream HTTP/1.1\r\nHost: 127.0.0.1:{self.port}\r\n"
             f"Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
             f"Sec-WebSocket-Version: 13\r\nAuthorization: Bearer {TOKEN}\r\n\r\n")
        sock.sendall(req.encode());response=bytearray()
        while not response.endswith(b"\r\n\r\n"):response.extend(sock.recv(1))
        self.assertIn(b"101 Switching Protocols",response);self.assertEqual(self.frame_json(sock)["type"],"ready")
        code,_=call(self.base+f"/v1/apps/{APP}/records/r2","POST",{"payload":{"ok":True}},
                    {"If-Match":"0","Idempotency-Key":str(uuid.uuid4())})
        self.assertEqual(code,200);self.assertEqual(self.frame_json(sock)["record_id"],"r2");sock.close()
    def test_server_close_terminates_live_websocket_before_releasing_writer(self):
        sock=socket.create_connection(("127.0.0.1",self.port),timeout=3);sock.settimeout(3)
        key=base64.b64encode(b"0123456789abcdef").decode()
        req=(f"GET /v1/apps/{APP}/stream HTTP/1.1\r\nHost: 127.0.0.1:{self.port}\r\n"
             f"Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
             f"Sec-WebSocket-Version: 13\r\nAuthorization: Bearer {TOKEN}\r\n\r\n")
        sock.sendall(req.encode());response=bytearray()
        while not response.endswith(b"\r\n\r\n"):response.extend(sock.recv(1))
        self.assertIn(b"101 Switching Protocols",response);self.assertEqual(self.frame_json(sock)["type"],"ready")
        self.srv.shutdown();self.srv.server_close();self.thread.join(timeout=2)
        self.assertFalse(self.thread.is_alive(),"HTTP server thread did not stop")
        close_header=sock.recv(2)
        self.assertEqual(close_header[0]&15,8,"server did not send a WebSocket close frame")
        replacement=create_server("127.0.0.1",0,Path(self.temp.name),{APP:TOKEN})
        replacement.server_close()
        sock.close()
    @staticmethod
    def exact(sock,n):
        out=bytearray()
        while len(out)<n:out.extend(sock.recv(n-len(out)))
        return bytes(out)
    @classmethod
    def frame_json(cls,sock):
        _,second=cls.exact(sock,2);n=second&127
        if n==126:n=int.from_bytes(cls.exact(sock,2),"big")
        elif n==127:n=int.from_bytes(cls.exact(sock,8),"big")
        return json.loads(cls.exact(sock,n))
    def test_only_one_server_may_own_the_data_directory(self):
        with self.assertRaisesRegex(RuntimeError,"another AutoSync writer"):
            create_server("127.0.0.1",0,Path(self.temp.name),{APP:TOKEN})
    def test_server_restart_recovers_committed_records(self):
        code,_=call(self.base+f"/v1/apps/{APP}/records/persisted","POST",{"payload":{"value":"survives restart"}},
                    {"If-Match":"0","Idempotency-Key":str(uuid.uuid4())})
        self.assertEqual(code,200)
        self.srv.shutdown();self.srv.server_close();self.thread.join(timeout=2)
        self.srv=create_server("127.0.0.1",0,Path(self.temp.name),{APP:TOKEN})
        self.port=self.srv.server_address[1];self.base=f"http://127.0.0.1:{self.port}"
        self.thread=threading.Thread(target=self.srv.serve_forever,daemon=True);self.thread.start()
        result=call(self.base+f"/v1/apps/{APP}/state")[1]
        self.assertEqual(result["records"][0]["payload"]["value"],"survives restart")
    def test_reject_lan_bind(self):
        with self.assertRaisesRegex(ValueError,"explicit --allow-lan"):create_server("0.0.0.0",8765,Path(self.temp.name),{APP:TOKEN})
        with self.assertRaisesRegex(ValueError,"requires TLS"):create_server("10.0.0.5",8765,Path(self.temp.name),{APP:TOKEN},allow_lan=True)
    def test_client_refuses_plain_http_to_lan_host(self):
        with self.assertRaisesRegex(ValueError,"unencrypted non-loopback"):
            AutoSyncClient("http://10.0.0.5:8765",TOKEN,APP,Path(self.temp.name)/"client")

class ClientTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.srv=create_server("127.0.0.1",0,Path(self.temp.name),{APP:TOKEN})
        self.port=self.srv.server_address[1];self.base=f"http://127.0.0.1:{self.port}"
        self.thread=threading.Thread(target=self.srv.serve_forever,daemon=True);self.thread.start()
    def tearDown(self):
        if self.thread.is_alive():self.srv.shutdown()
        if not self.srv.closed and not self.srv.closing:self.srv.server_close()
        self.thread.join(timeout=2);self.temp.cleanup()

    def test_http_connection_lifetime_matches_module_version(self):
        client=AutoSyncClient(self.base,TOKEN,APP,Path(self.temp.name)/"keep-alive")
        connection=None
        try:
            first=client._http(f"/v1/apps/{APP}/state?since=0")
            connection=client._http_connection
            second=client._http(f"/v1/apps/{APP}/state?since=0")
            self.assertEqual((first[0],second[0]),(200,200))
            if HTTP_ONE_SHOT:
                self.assertIsNone(client._http_connection)
            else:
                self.assertIsNotNone(connection)
                self.assertIs(client._http_connection,connection)
                self.assertIsNotNone(connection.sock)
        finally:
            client.close()
        if HTTP_ONE_SHOT:
            self.assertIsNone(client._http_connection)
        else:
            self.assertIsNone(client._http_connection)
            self.assertIsNone(connection.sock)

    def test_empty_sync_skips_redundant_second_pull(self):
        client=AutoSyncClient(self.base,TOKEN,APP,Path(self.temp.name)/"single-pull")
        pulls=[];original=client._pull
        def counted_pull():
            pulls.append(1)
            return original()
        client._pull=counted_pull
        try:
            self.assertEqual(client.sync_once()["status"],"synced")
            self.assertEqual(len(pulls),1)
        finally:
            client.close()
    def test_snapshot_reset_replaces_remote_cache_and_preserves_pending_outbox(self):
        client=AutoSyncClient(self.base,TOKEN,APP,Path(self.temp.name)/"snapshot-reset")
        try:
            _,first=self.srv.store.write(APP,"remote",{"v":1},False,0,str(uuid.uuid4()))
            client._pull()
            self.assertEqual(client.cached_record("remote")["payload"]["v"],1)
            client.queue_write("local-pending",{"v":"keep offline"})
            _,second=self.srv.store.write(APP,"remote",{"v":2},False,first["revision"],str(uuid.uuid4()))
            self.srv.store.compact_events(APP,second["revision"])
            client._pull()
            self.assertEqual(client.outbox.cursor(APP),second["revision"])
            self.assertEqual(client.cached_record("remote")["payload"]["v"],2)
            self.assertEqual(client.outbox.pending_count(APP),1)
            pending=client.outbox.db.execute("SELECT rid,payload FROM queue WHERE state='pending'").fetchone()
            self.assertEqual(pending["rid"],"local-pending");self.assertEqual(json.loads(pending["payload"]),{"v":"keep offline"})
        finally:client.close()

    def test_two_clients_sync_and_preserve_stale_edit(self):
        root=Path(self.temp.name)
        one=AutoSyncClient(self.base,TOKEN,APP,root/"pc1")
        two=AutoSyncClient(self.base,TOKEN,APP,root/"pc2")
        try:
            one.queue_write("memo-1",{"text":"first"})
            self.assertEqual(one.sync_once()["status"],"synced")
            self.assertEqual(two.sync_once()["status"],"synced")
            self.assertEqual(two.outbox.cached_record(APP,"memo-1")["payload"]["text"],"first")
            one.queue_write("memo-1",{"text":"new from pc1"},expected_revision=one.outbox.record_revision(APP,"memo-1"))
            two.queue_write("memo-1",{"text":"offline from pc2"},expected_revision=two.outbox.record_revision(APP,"memo-1"))
            self.assertEqual(one.sync_once()["status"],"synced")
            result=two.sync_once()
            self.assertEqual(result["status"],"conflict")
            self.assertEqual(two.outbox.conflict_count(APP),1)
            self.assertEqual(two.outbox.cached_record(APP,"memo-1")["payload"]["text"],"new from pc1")
            conflicts=two.conflicts()
            self.assertEqual(conflicts[0]["error"],"revision_conflict")
            with self.assertRaisesRegex(RuntimeError,"unresolved AutoSync conflict"):
                two.queue_write("memo-1",{"text":"must not silently overwrite"})
            two.resolve_conflict(conflicts[0]["operation_id"],{"text":"merged by user"},conflicts[0]["current_revision"])
            self.assertEqual(two.sync_once()["status"],"synced")
            self.assertEqual(one.sync_once()["status"],"synced")
            self.assertEqual(self.srv.store.state(APP)["records"][0]["payload"]["text"],"merged by user")
            self.assertEqual(two.outbox.conflict_count(APP),0)
        finally:one.close();two.close()
    def test_conflict_resolution_retry_uses_stable_operation_id(self):
        root=Path(self.temp.name)
        one=AutoSyncClient(self.base,TOKEN,APP,root/"resolution-one")
        two=AutoSyncClient(self.base,TOKEN,APP,root/"resolution-two")
        resolution_id=str(uuid.uuid4())
        try:
            one.queue_write("memo-resolution",{"text":"server"})
            self.assertEqual(one.sync_once()["status"],"synced")
            self.assertEqual(two.sync_once()["status"],"synced")
            one.queue_write("memo-resolution",{"text":"server edit"},expected_revision=one.outbox.record_revision(APP,"memo-resolution"))
            two.queue_write("memo-resolution",{"text":"local edit"},expected_revision=two.outbox.record_revision(APP,"memo-resolution"))
            self.assertEqual(one.sync_once()["status"],"synced")
            self.assertEqual(two.sync_once()["status"],"conflict")
            conflict=two.conflicts()[0]
            args=(conflict["operation_id"],{"text":"chosen resolution"},conflict["current_revision"])
            first=two.resolve_conflict(*args,resolution_operation_id=resolution_id)
            retry=two.resolve_conflict(*args,resolution_operation_id=resolution_id)
            self.assertEqual(first,resolution_id)
            self.assertEqual(retry,first)
            self.assertEqual(two.outbox.pending_count(APP),1)
            self.assertEqual(two.sync_once()["status"],"synced")
            self.assertEqual(one.sync_once()["status"],"synced")
            self.assertEqual(self.srv.store.state(APP)["records"][0]["payload"]["text"],"chosen resolution")
            after_ack=two.resolve_conflict(*args,resolution_operation_id=resolution_id)
            self.assertEqual(after_ack,first)
            with self.assertRaisesRegex(ValueError,"already used for different content"):
                two.resolve_conflict(conflict["operation_id"],{"text":"different resolution"},conflict["current_revision"],resolution_operation_id=resolution_id)
            with self.assertRaisesRegex(ValueError,"already used for different content"):
                two.resolve_conflict(conflict["operation_id"],{"text":"chosen resolution"},conflict["current_revision"]+1,resolution_operation_id=resolution_id)
            count=self.srv.store.db.execute("SELECT COUNT(*) FROM events WHERE app=? AND id=?",(APP,"memo-resolution")).fetchone()[0]
            self.assertEqual(count,3)
        finally:one.close();two.close()
    def test_choosing_server_payload_is_a_logged_resolution_write(self):
        root=Path(self.temp.name)
        one=AutoSyncClient(self.base,TOKEN,APP,root/"server-choice-one")
        two=AutoSyncClient(self.base,TOKEN,APP,root/"server-choice-two")
        try:
            one.queue_write("memo-server-choice",{"text":"initial"})
            self.assertEqual(one.sync_once()["status"],"synced")
            self.assertEqual(two.sync_once()["status"],"synced")
            one.queue_write("memo-server-choice",{"text":"server latest"},expected_revision=one.outbox.record_revision(APP,"memo-server-choice"))
            two.queue_write("memo-server-choice",{"text":"local draft"},expected_revision=two.outbox.record_revision(APP,"memo-server-choice"))
            self.assertEqual(one.sync_once()["status"],"synced")
            self.assertEqual(two.sync_once()["status"],"conflict")
            conflict=two.conflicts()[0]
            server_payload=conflict["current"]["payload"]
            current_revision=conflict["current_revision"]
            before_events=self.srv.store.db.execute("SELECT COUNT(*) FROM events WHERE app=? AND id=?",(APP,"memo-server-choice")).fetchone()[0]
            two.resolve_conflict(conflict["operation_id"],server_payload,current_revision,resolution_operation_id=str(uuid.uuid4()))
            self.assertEqual(two.sync_once()["status"],"synced")
            self.assertEqual(one.sync_once()["status"],"synced")
            resolved=self.srv.store.state(APP)["records"][0]
            after_events=self.srv.store.db.execute("SELECT COUNT(*) FROM events WHERE app=? AND id=?",(APP,"memo-server-choice")).fetchone()[0]
            self.assertEqual(resolved["payload"],server_payload)
            self.assertEqual(resolved["revision"],current_revision+1)
            self.assertEqual(after_events,before_events+1)
        finally:one.close();two.close()
    def test_pending_edits_for_same_record_coalesce_before_send(self):
        client=AutoSyncClient(self.base,TOKEN,APP,Path(self.temp.name)/"coalesce")
        try:
            first=client.queue_write("same",{"value":1});second=client.queue_write("same",{"value":2})
            self.assertEqual(first,second);self.assertEqual(client.outbox.pending_count(APP),1)
            self.assertEqual(client.sync_once()["status"],"synced")
            self.assertEqual(client.outbox.cached_record(APP,"same")["payload"]["value"],2)
        finally:client.close()
    def test_stable_operation_id_replay_is_idempotent(self):
        client=AutoSyncClient(self.base,TOKEN,APP,Path(self.temp.name)/"stable-op")
        op=str(uuid.uuid4())
        try:
            first=client.queue_write("same",{"value":1},expected_revision=0,operation_id=op)
            replay=client.queue_write("same",{"value":1},expected_revision=0,operation_id=op)
            self.assertEqual(first,replay)
            self.assertEqual(client.sync_once()["status"],"synced")
            replay_after_ack=client.queue_write("same",{"value":1},expected_revision=0,operation_id=op)
            self.assertEqual(replay_after_ack,op)
            self.assertEqual(client.operation_status(op)["state"],"synced")
            with self.assertRaisesRegex(ValueError,"different content"):
                client.queue_write("same",{"value":2},operation_id=op)
            with self.assertRaisesRegex(ValueError,"different content"):
                client.queue_write("same",{"value":1},expected_revision=1,operation_id=op)
            count=self.srv.store.db.execute("SELECT COUNT(*) FROM events WHERE app=? AND id=?",(APP,"same")).fetchone()[0]
            self.assertEqual(count,1)
        finally:client.close()
    def test_edits_during_send_follow_acknowledged_revision_chain(self):
        box=Outbox(Path(self.temp.name)/"chain")
        try:
            op1=box.queue_write(APP,"same",{"n":1},0)
            box.claim_next(APP)
            op2=box.queue_write(APP,"same",{"n":2},0,operation_id=str(uuid.uuid4()))
            op3=box.queue_write(APP,"same",{"n":3},0,operation_id=str(uuid.uuid4()))
            box.mark_synced(op1,10,{"revision":10})
            sending=box.claim_next(APP)
            self.assertEqual(sending["op"],op2)
            self.assertEqual(sending["expected"],10)
            op4=box.queue_write(APP,"same",{"n":4},0,operation_id=str(uuid.uuid4()))
            box.mark_synced(op2,20,{"revision":20})
            next_op=box.claim_next(APP)
            self.assertEqual(next_op["op"],op3)
            self.assertEqual(next_op["expected"],20)
            box.mark_synced(op3,30,{"revision":30})
            next_op=box.claim_next(APP)
            self.assertEqual(next_op["op"],op4)
            self.assertEqual(next_op["expected"],30)
        finally:box.close()
    def test_lost_ack_retries_without_duplicate_commit(self):
        client=AutoSyncClient(self.base,TOKEN,APP,Path(self.temp.name)/"lost-ack")
        original=client._http;drop={"once":True}
        def drop_after_server_commit(path,method="GET",payload=None,headers=None):
            result=original(path,method,payload,headers)
            if method=="POST" and drop["once"]:
                drop["once"]=False
                raise ConnectionError("simulated response loss after commit")
            return result
        try:
            client.queue_write("ambiguous",{"text":"committed once"})
            client._http=drop_after_server_commit
            first=client.sync_once()
            self.assertEqual(first["status"],"offline")
            self.assertEqual(first["pending"],1)
            client._http=original
            second=client.sync_once()
            self.assertEqual(second["status"],"synced")
            count=self.srv.store.db.execute("SELECT COUNT(*) FROM events WHERE app=? AND id=?",(APP,"ambiguous")).fetchone()[0]
            self.assertEqual(count,1)
        finally:client.close()
    def test_offline_queue_survives_client_restart(self):
        root=Path(self.temp.name)/"offline"
        client=AutoSyncClient("http://127.0.0.1:1",TOKEN,APP,root)
        client.queue_write("queued",{"text":"keep"})
        result=client.sync_once()
        self.assertEqual(result["status"],"offline");self.assertEqual(result["pending"],1);client.close()
        reopened=AutoSyncClient("http://127.0.0.1:1",TOKEN,APP,root)
        self.assertEqual(reopened.outbox.pending_count(APP),1);reopened.close()
    def test_auth_failure_is_blocked_not_reported_as_offline(self):
        client=AutoSyncClient(self.base,"wrong-but-long-enough-token-123456",APP,Path(self.temp.name)/"bad-auth")
        try:self.assertEqual(client.sync_once()["status"],"blocked")
        finally:client.close()
    def test_watcher_applies_push_without_poll_interval(self):
        root=Path(self.temp.name);receiver=AutoSyncClient(self.base,TOKEN,APP,root/"receiver")
        sender=AutoSyncClient(self.base,TOKEN,APP,root/"sender")
        stop=threading.Event();watcher=threading.Thread(target=receiver.watch,args=(stop,),daemon=True);watcher.start()
        try:
            deadline=__import__("time").monotonic()+3
            while __import__("time").monotonic()<deadline:
                with self.srv.guard:
                    if self.srv.clients.get(APP):break
                __import__("time").sleep(.02)
            sender.queue_write("live",{"text":"push"})
            sender.sync_once()
            deadline=__import__("time").monotonic()+4
            while __import__("time").monotonic()<deadline:
                row=receiver.outbox.cached_record(APP,"live")
                if row and row["payload"].get("text")=="push":break
                __import__("time").sleep(.05)
            self.assertEqual(receiver.outbox.cached_record(APP,"live")["payload"]["text"],"push")
        finally:
            stop.set();watcher.join(timeout=3);receiver.close();sender.close()
    def test_watcher_reconnects_after_server_restart_and_flushes_offline_edit(self):
        receiver=AutoSyncClient(self.base,TOKEN,APP,Path(self.temp.name)/"reconnect")
        stop=threading.Event()
        watcher=threading.Thread(target=receiver.watch,args=(stop,),daemon=True)
        watcher.start()
        try:
            deadline=__import__("time").monotonic()+3
            while __import__("time").monotonic()<deadline:
                with self.srv.guard:
                    if self.srv.clients.get(APP):break
                __import__("time").sleep(.02)
            else:self.fail("watcher did not connect before restart")
            self.srv.shutdown();self.srv.server_close();self.thread.join(timeout=2)
            operation=receiver.queue_write("offline-during-restart",{"text":"replayed after restart"})
            self.srv=create_server("127.0.0.1",self.port,Path(self.temp.name),{APP:TOKEN})
            self.thread=threading.Thread(target=self.srv.serve_forever,daemon=True);self.thread.start()
            deadline=__import__("time").monotonic()+7
            cached=None
            while __import__("time").monotonic()<deadline:
                if receiver.operation_status(operation)["state"]=="synced":
                    cached=receiver.cached_record("offline-during-restart")
                    if cached and cached["payload"].get("text")=="replayed after restart":break
                __import__("time").sleep(.05)
            self.assertEqual(receiver.operation_status(operation)["state"],"synced")
            self.assertIsNotNone(cached)
            self.assertEqual(cached["payload"]["text"],"replayed after restart")
        finally:
            stop.set();watcher.join(timeout=3);receiver.close()

if __name__=="__main__":unittest.main(verbosity=2)

