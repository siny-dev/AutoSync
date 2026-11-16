"""Run an isolated TLS AutoSync pilot between 5600G and H255."""
from __future__ import annotations
import argparse, base64, json, os, secrets, shutil, socket, subprocess, sys, tempfile, time, traceback
from pathlib import Path

AUTOSYNC_ROOT=Path(__file__).resolve().parents[1]
AUTO_UPDATE_UNC=Path(r"\\H255\AI-Work\제작중인 프로그램\AutoUpdate")
AUTO_UPDATE_H255=Path(r"C:\AI-Work\제작중인 프로그램\AutoUpdate")
REMOTE_TEMP_SHARE=r"\\H255\c$\Users\Administrator\AppData\Local\Temp"
REMOTE_TEMP_NATIVE=r"C:\Users\Administrator\AppData\Local\Temp"
REMOTE_PYTHON=r"C:\Users\Administrator\AppData\Local\Programs\Python\Python315\python.exe"
H255_IP="192.168.50.109"
PC5600_IP="192.168.50.254"
PORT=8765
APP_ID="autosync-pilot-probe"
NO_WINDOW=getattr(subprocess,"CREATE_NO_WINDOW",0)


def psq(v): return "'"+str(v).replace("'","''")+"'"

def ps51():
    p=Path(os.environ.get("WINDIR",r"C:\Windows"))/"System32"/"WindowsPowerShell"/"v1.0"/"powershell.exe"
    if not p.is_file(): raise RuntimeError("Windows PowerShell unavailable")
    return str(p)

def run_ps(script,timeout=40):
    encoded=base64.b64encode(script.encode("utf-16le")).decode("ascii")
    r=subprocess.run([ps51(),"-NoLogo","-NoProfile","-NonInteractive","-ExecutionPolicy","Bypass","-EncodedCommand",encoded],capture_output=True,text=True,encoding="mbcs",errors="replace",timeout=timeout,creationflags=NO_WINDOW)
    if r.returncode: raise RuntimeError("PowerShell operation failed: "+(r.stderr or r.stdout)[-1400:])
    return r.stdout.strip()

def make_cert(root):
    cert=root/"server-cert.pem"; key=root/"server-key.pem"; psfile=root/"make-test-cert.ps1"
    psfile.write_text(f"""$ErrorActionPreference='Stop'
$certObj=$null
try {{
 $certObj=New-SelfSignedCertificate -DnsName 'H255' -CertStoreLocation 'Cert:\\CurrentUser\\My' -KeyAlgorithm RSA -KeyLength 2048 -KeyExportPolicy Exportable -HashAlgorithm SHA256 -NotBefore (Get-Date).AddMinutes(-5) -NotAfter (Get-Date).AddHours(3) -Type SSLServerAuthentication
 $rsa=[System.Security.Cryptography.X509Certificates.RSACertificateExtensions]::GetRSAPrivateKey($certObj)
 [IO.File]::WriteAllText({psq(cert)},$certObj.ExportCertificatePem(),[Text.UTF8Encoding]::new($false))
 [IO.File]::WriteAllText({psq(key)},$rsa.ExportPkcs8PrivateKeyPem(),[Text.UTF8Encoding]::new($false))
 [Console]::WriteLine($certObj.Thumbprint)
}} finally {{ if($certObj){{Remove-Item -LiteralPath ('Cert:\\CurrentUser\\My\\'+$certObj.Thumbprint) -Force -ErrorAction SilentlyContinue}} }}
""",encoding="utf-8")
    pwsh=shutil.which("pwsh.exe") or shutil.which("pwsh")
    if not pwsh: raise RuntimeError("PowerShell 7 required for temporary PEM certificate generation")
    r=subprocess.run([pwsh,"-NoLogo","-NoProfile","-NonInteractive","-File",str(psfile)],capture_output=True,text=True,encoding="mbcs",errors="replace",timeout=60,creationflags=NO_WINDOW)
    if r.returncode or not cert.is_file() or not key.is_file(): raise RuntimeError("temporary TLS certificate generation failed: "+(r.stderr or r.stdout)[-900:])
    return cert,key,r.stdout.strip()

def restrict_acl(path,account):
    icacls=Path(os.environ.get("WINDIR",r"C:\Windows"))/"System32"/"icacls.exe"
    r=subprocess.run([str(icacls),str(path),"/inheritance:r","/grant:r",account+":(OI)(CI)F","NT AUTHORITY\\SYSTEM:(OI)(CI)F","BUILTIN\\Administrators:(OI)(CI)F"],capture_output=True,text=True,encoding="mbcs",errors="replace",timeout=20,creationflags=NO_WINDOW)
    if r.returncode: raise RuntimeError("could not restrict temporary pilot folder ACL: "+(r.stderr or r.stdout)[-700:])

def remote_ps(script,timeout=40):
    prefix="""$option=New-CimSessionOption -Protocol Dcom
$session=New-CimSession -ComputerName 'H255' -SessionOption $option
try {
"""
    return run_ps(prefix+script+"\n} finally { Remove-CimSession $session }",timeout)

def firewall(action,rule):
    if action=="add":
        body=f"""
 $existing=Get-NetFirewallRule -CimSession $session -Name {psq(rule)} -ErrorAction SilentlyContinue
 if($existing){{throw 'temporary pilot rule already exists'}}
 New-NetFirewallRule -CimSession $session -Name {psq(rule)} -DisplayName 'Temporary AutoSync LAN pilot' -Direction Inbound -Action Allow -Protocol TCP -LocalPort {PORT} -LocalAddress {H255_IP} -RemoteAddress '{H255_IP},{PC5600_IP}' -Profile Private -EdgeTraversalPolicy Block | Out-Null
 [Console]::WriteLine('ADDED')
"""
    else:
        body=f"""
 $existing=Get-NetFirewallRule -CimSession $session -Name {psq(rule)} -ErrorAction SilentlyContinue
 if($existing){{Remove-NetFirewallRule -CimSession $session -Name {psq(rule)} -ErrorAction Stop}}
 [Console]::WriteLine('REMOVED')
"""
    return remote_ps(body)

def launch_remote(root_share,root_native,name,python_exe,args):
    root_share=Path(root_share); root_native=str(root_native)
    launcher_share=root_share/(name+".launcher.ps1")
    pid_share=root_share/(name+".pid")
    error_share=root_share/(name+".launch-error.txt")
    argline=subprocess.list2cmdline(args)
    launcher_share.write_text(f"""$ErrorActionPreference='Stop'
try {{
 $p=Start-Process -FilePath {psq(python_exe)} -ArgumentList {psq(argline)} -WindowStyle Hidden -RedirectStandardOutput {psq(str(Path(root_native)/(name+'.stdout.log')))} -RedirectStandardError {psq(str(Path(root_native)/(name+'.stderr.log')))} -PassThru
 [IO.File]::WriteAllText({psq(str(Path(root_native)/(name+'.pid')))},[string]$p.Id,[Text.UTF8Encoding]::new($false))
}} catch {{
 [IO.File]::WriteAllText({psq(str(Path(root_native)/(name+'.launch-error.txt')))},$_.Exception.GetType().Name,[Text.UTF8Encoding]::new($false))
 exit 2
}}
""",encoding="utf-8-sig")
    ps=ps51()
    command=subprocess.list2cmdline([ps,"-NoLogo","-NoProfile","-NonInteractive","-WindowStyle","Hidden","-ExecutionPolicy","Bypass","-File",str(Path(root_native)/(name+".launcher.ps1"))])
    body=f"""
 $r=Invoke-CimMethod -CimSession $session -ClassName Win32_Process -MethodName Create -Arguments @{{CommandLine={psq(command)};CurrentDirectory={psq(root_native)}}}
 if($r.ReturnValue -ne 0){{throw ('remote launcher returned '+$r.ReturnValue)}}
 [Console]::WriteLine('STARTED')
"""
    remote_ps(body)
    deadline=time.monotonic()+20
    while time.monotonic()<deadline:
        if pid_share.is_file(): return int(pid_share.read_text(encoding="utf-8").strip())
        if error_share.is_file(): raise RuntimeError("hidden H255 launcher failed: "+error_share.read_text(encoding="utf-8"))
        time.sleep(.25)
    raise TimeoutError("hidden H255 launcher did not write PID")

def remote_status(pid):
    return remote_ps(f"""
 $p=Get-CimInstance -CimSession $session -ClassName Win32_Process -Filter 'ProcessId = {pid}'
 if($p){{[Console]::WriteLine('ALIVE|'+$p.CommandLine)}}else{{[Console]::WriteLine('STOPPED')}}
""",timeout=20)

def remote_terminate(pid,marker):
    out=remote_ps(f"""
 $p=Get-CimInstance -CimSession $session -ClassName Win32_Process -Filter 'ProcessId = {pid}'
 if(-not $p){{[Console]::WriteLine('STOPPED');return}}
 if($p.CommandLine -notlike ('*'+{psq(marker)}+'*')){{throw 'refusing to terminate non-pilot process'}}
 $o=Invoke-CimMethod -InputObject $p -MethodName GetOwner
 if($o.User -ne 'Administrator' -or $o.Domain -ne 'H255'){{throw 'refusing to terminate another account process'}}
 Invoke-CimMethod -InputObject $p -MethodName Terminate -Arguments @{{Reason=0}} | Out-Null
 [Console]::WriteLine('TERMINATED')
""",timeout=20)
    return "TERMINATED" in out

def local_ip_to_h255():
    s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
    try:s.connect((H255_IP,PORT)); return s.getsockname()[0]
    finally:s.close()

def validate_preconditions():
    if os.environ.get("COMPUTERNAME","").upper()!="5600G": raise RuntimeError("launch this two-PC pilot from 5600G")
    if local_ip_to_h255()!=PC5600_IP: raise RuntimeError("5600G does not have the expected LAN address")
    if socket.gethostbyname("H255")!=H255_IP: raise RuntimeError("H255 no longer resolves to its checked DHCP address")
    try:
        c=socket.create_connection((H255_IP,PORT),timeout=.7); c.close()
    except OSError:return
    raise RuntimeError("H255 port 8765 is already accepting connections; refusing to collide")

def client_args(root,auto_update,source_probe,server,cert,token,run_id,instance,peer,secs):
    return [sys.executable,str(AUTOSYNC_ROOT/"tools"/"autosync_lan_pilot_client.py"),
        "--auto-update-root",str(auto_update),"--state-root",str(root/"client-state"),
        "--source-probe",str(source_probe),"--server",server,"--ca-file",str(cert),
        "--token-file",str(token),"--run-id",run_id,"--instance-id",instance,"--peer-id",peer,
        "--seconds",str(secs),"--interval","1","--data-dir",str(root/"client-data"),
        "--result-file",str(root/(instance+"-result.json"))]

def main():
    global PORT
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run",action="store_true",help="run the isolated H255↔5600G pilot")
    parser.add_argument("--port",type=int,default=PORT,help="TCP port for this isolated pilot (default: %(default)s)")
    args=parser.parse_args()
    if not 1024 <= args.port <= 65535: parser.error("--port must be between 1024 and 65535")
    PORT=args.port
    if not args.run: parser.error("refusing to start the pilot unless --run is explicit")
    stamp=time.strftime("%Y%m%d%H%M%S")
    run_id="pilot-v012-"+stamp
    rule="AutoSyncLanPilot_"+stamp
    report=AUTOSYNC_ROOT/"tools"/"reports"/("two-pc-lan-pilot-"+stamp+".json")
    result={"status":"FAIL","run_id":run_id,"expected_module_version":"0.1.12","app_id":APP_ID,
        "workboard_data_touched":False,"server_started":False,"server_stopped":False,
        "firewall_rule_added":False,"firewall_rule_removed":False,"local_temp_cleaned":False,"remote_temp_cleaned":False}
    local_root=None; remote_root=None; server_pid=None; remote_client_pid=None; local_proc=None; local_log=None; rule_maybe_added=False
    try:
        validate_preconditions()
        pointer=json.loads((AUTO_UPDATE_UNC/"outbox"/"modules"/"AutoSync"/"current.json").read_text(encoding="utf-8-sig"))
        if pointer.get("version")!="0.1.12": raise RuntimeError("pilot expects currently published AutoSync 0.1.12")
        local_root=Path(tempfile.mkdtemp(prefix="AutoSyncTwoPC_"))
        remote_root=Path(tempfile.mkdtemp(prefix="AutoSyncTwoPC_",dir=REMOTE_TEMP_SHARE))
        if not str(local_root.resolve()).startswith(str(Path(tempfile.gettempdir()).resolve()).rstrip("\\")+"\\"):
            raise RuntimeError("local pilot directory is outside system Temp")
        if not str(remote_root).startswith(REMOTE_TEMP_SHARE+"\\") or ".." in str(remote_root):
            raise RuntimeError("remote pilot directory is outside H255 Temp")
        remote_native=Path(REMOTE_TEMP_NATIVE)/remote_root.name
        restrict_acl(str(local_root),os.environ["COMPUTERNAME"]+"\\"+os.environ["USERNAME"])
        restrict_acl(str(remote_root),"*S-1-5-32-544")
        cert,key,thumb=make_cert(local_root); result["temporary_certificate_thumbprint"]=thumb
        token=secrets.token_urlsafe(32)
        token_file=local_root/"token.json"; token_file.write_text(json.dumps({APP_ID:token}),encoding="utf-8")
        shutil.copy2(cert,remote_root/"server-cert.pem")
        shutil.copy2(key,remote_root/"server-key.pem")
        shutil.copy2(token_file,remote_root/"token.json")
        rule_maybe_added=True
        firewall("add",rule); result["firewall_rule_added"]=True
        server_extra=[str(Path(r"C:\AI-Work\제작중인 프로그램\AutoSync\tools\autosync_lan_pilot_server.py")),
            "--auto-update-root",str(AUTO_UPDATE_H255),"--state-root",str(remote_native/"server-state"),
            "--data-dir",str(remote_native/"server-data"),"--tls-cert",str(remote_native/"server-cert.pem"),
            "--tls-key",str(remote_native/"server-key.pem"),"--token-file",str(remote_native/"token.json"),
            "--host",H255_IP,"--port",str(PORT),"--ready-file",str(remote_native/"server-ready.json"),
            "--stop-file",str(remote_native/"server.stop"),"--log-file",str(remote_native/"server.log")]
        server_pid=launch_remote(remote_root,remote_native,"server",REMOTE_PYTHON,server_extra)
        ready=remote_root/"server-ready.json"
        deadline=time.monotonic()+120
        while time.monotonic()<deadline and not ready.is_file(): time.sleep(.25)
        if not ready.is_file():
            try: result["server_process_status"] = remote_status(server_pid)[:1200]
            except Exception as diag_exc: result["server_process_status_error"] = type(diag_exc).__name__
            for label, path in (("stdout", remote_root/"server.stdout.log"), ("stderr", remote_root/"server.stderr.log"), ("log", remote_root/"server.log")):
                try:
                    if path.is_file(): result["server_"+label+"_tail"] = path.read_text(encoding="utf-8",errors="replace")[-1800:]
                except Exception as diag_exc: result["server_"+label+"_read_error"] = type(diag_exc).__name__
            raise TimeoutError("H255 host did not become ready")
        server_state=json.loads(ready.read_text(encoding="utf-8-sig")); result["server_readiness"]={k:v for k,v in server_state.items() if k!="module_path"}
        if server_state.get("status")!="ready" or server_state.get("module_version")!="0.1.12" or server_state.get("verified") is not True:
            raise RuntimeError("H255 did not start verified AutoSync 0.1.12")
        result["server_started"]=True
        with socket.create_connection(("H255",PORT),timeout=3): pass
        local_args=client_args(local_root,AUTO_UPDATE_UNC,AUTOSYNC_ROOT/"tools"/"autosync_pilot.py",f"https://H255:{PORT}",cert,token_file,run_id,"5600G","H255",25)
        local_log=(local_root/"5600g-wrapper.log").open("w",encoding="utf-8")
        local_proc=subprocess.Popen(local_args,stdin=subprocess.DEVNULL,stdout=local_log,stderr=subprocess.STDOUT,cwd=str(AUTOSYNC_ROOT),creationflags=NO_WINDOW)
        remote_extra=[str(Path(r"C:\AI-Work\제작중인 프로그램\AutoSync\tools\autosync_lan_pilot_client.py")),
            "--auto-update-root",str(AUTO_UPDATE_H255),"--state-root",str(remote_native/"client-state"),
            "--source-probe",r"C:\AI-Work\제작중인 프로그램\AutoSync\tools\autosync_pilot.py",
            "--server",f"https://H255:{PORT}","--ca-file",str(remote_native/"server-cert.pem"),
            "--token-file",str(remote_native/"token.json"),"--run-id",run_id,"--instance-id","H255","--peer-id","5600G",
            "--seconds","25","--interval","1","--data-dir",str(remote_native/"client-data"),
            "--result-file",str(remote_native/"H255-result.json")]
        remote_client_pid=launch_remote(remote_root,remote_native,"client",REMOTE_PYTHON,remote_extra)
        local_code=local_proc.wait(timeout=100); result["5600G_client_exit_code"]=local_code
        if local_log: local_log.flush()
        h255_result=remote_root/"H255-result.json"
        deadline=time.monotonic()+100
        while time.monotonic()<deadline and not h255_result.is_file(): time.sleep(.25)
        if not h255_result.is_file(): raise TimeoutError("H255 client did not write its result")
        result["clients"]={"5600G":json.loads((local_root/"5600G-result.json").read_text(encoding="utf-8-sig")),
                            "H255":json.loads(h255_result.read_text(encoding="utf-8-sig"))}
        if local_code!=0: raise RuntimeError("5600G client exited nonzero")
        for name,summary in result["clients"].items():
            if summary.get("result")!="PASS" or summary.get("resolved_module_version")!="0.1.12" or summary.get("module_verified") is not True or summary.get("peer_record_seen") is not True or int(summary.get("roundtrip_acknowledgements",0))<1:
                raise RuntimeError(name+" did not pass bidirectional peer and round-trip checks")
        result["status"]="PASS"
        return_code=0
    except Exception as exc:
        result["error_type"]=type(exc).__name__; result["error"]=str(exc)[:1200]; result["traceback"]=traceback.format_exc()[-2400:]
        return_code=2
    finally:
        if local_proc is not None and local_proc.poll() is None:
            local_proc.terminate()
            try:local_proc.wait(timeout=8)
            except subprocess.TimeoutExpired:local_proc.kill()
        if local_log is not None:local_log.close()
        if remote_root is not None:
            try:(remote_root/"server.stop").write_text("stop\\n",encoding="utf-8")
            except Exception:pass
        if server_pid:
            deadline=time.monotonic()+30
            while time.monotonic()<deadline:
                try: alive=remote_status(server_pid).startswith("ALIVE|")
                except Exception: alive=True
                if not alive: result["server_stopped"]=True; break
                time.sleep(.5)
            if not result["server_stopped"]:
                try:
                    if remote_terminate(server_pid,"autosync_lan_pilot_server.py"):
                        deadline=time.monotonic()+8
                        while time.monotonic()<deadline and remote_status(server_pid).startswith("ALIVE|"):time.sleep(.5)
                        result["server_stopped"]=not remote_status(server_pid).startswith("ALIVE|")
                        result["server_force_stopped"]=result["server_stopped"]
                except Exception as exc:result["server_cleanup_error"]=type(exc).__name__
        if remote_client_pid:
            try:
                if remote_status(remote_client_pid).startswith("ALIVE|"):
                    remote_terminate(remote_client_pid,"autosync_lan_pilot_client.py")
            except Exception:pass
        if rule_maybe_added:
            try:firewall("remove",rule);result["firewall_rule_removed"]=True
            except Exception as exc:result["firewall_cleanup_error"]=type(exc).__name__
        if local_root is not None:
            try:
                temp_root=str(Path(tempfile.gettempdir()).resolve()).rstrip("\\")+"\\"; actual=str(local_root.resolve())
                if not actual.startswith(temp_root) or ".." in actual:raise RuntimeError("unsafe local cleanup target")
                if local_root.exists():shutil.rmtree(local_root)
                result["local_temp_cleaned"]=not local_root.exists()
            except Exception as exc:result["local_cleanup_error"]=type(exc).__name__;result["local_temp_path_retained"]=str(local_root)
        if remote_root is not None:
            try:
                actual=str(remote_root)
                if not actual.startswith(REMOTE_TEMP_SHARE+"\\") or ".." in actual:raise RuntimeError("unsafe H255 cleanup target")
                if remote_root.exists():shutil.rmtree(str(remote_root))
                result["remote_temp_cleaned"]=not remote_root.exists()
            except Exception as exc:result["remote_cleanup_error"]=type(exc).__name__;result["remote_temp_path_retained"]=str(remote_root)
        if result["status"]=="PASS" and (not result["server_stopped"] or not result["firewall_rule_removed"] or not result["local_temp_cleaned"] or not result["remote_temp_cleaned"]):
            result["status"]="FAIL_CLEANUP";return_code=2
        report.parent.mkdir(parents=True,exist_ok=True)
        report.write_text(json.dumps(result,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
        print(json.dumps({"report":str(report),**result},ensure_ascii=False))
    return return_code

if __name__=="__main__":raise SystemExit(main())
