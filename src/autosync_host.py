"""AutoUpdate-backed, single-writer AutoSync host supervisor."""
from __future__ import annotations
import ctypes, hashlib, hmac, ipaddress, json, os, re, secrets, shutil, signal, socket, subprocess, sys, threading, time, uuid
from ctypes import wintypes
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

class HostRuntimeError(RuntimeError): pass
SUPERVISOR_RESTART_EXIT = 75
class CandidateStartError(HostRuntimeError):
    def __init__(self, message, safe_to_restore, child=None):
        super().__init__(message); self.safe_to_restore=safe_to_restore; self.child=child

def sha256(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''): h.update(block)
    return h.hexdigest()

def hidden_options():
    if os.name!='nt': return {}
    opts={'creationflags':getattr(subprocess,'CREATE_NO_WINDOW',0)}
    si=subprocess.STARTUPINFO(); si.dwFlags|=subprocess.STARTF_USESHOWWINDOW; si.wShowWindow=0
    opts['startupinfo']=si; return opts


def minimal_environment(extra=None):
    keys=('SystemRoot','SYSTEMROOT','WINDIR','PATH','TEMP','TMP','USERPROFILE','LOCALAPPDATA','APPDATA','PROGRAMDATA','PSModulePath')
    env={k:v for k,v in os.environ.items() if k in keys}
    if extra: env.update(extra)
    return env

def run_hidden(command,env=None,timeout=60,cwd=None):
    return subprocess.run([str(x) for x in command],stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
        text=True,encoding='utf-8',errors='replace',timeout=timeout,cwd=cwd,env=env,**hidden_options())

@dataclass(frozen=True)
class ModuleCandidate:
    module_id:str; version:str; api_major:int; activation_mode:str; module_path:Path; source_root:Path; manifest_sha256:str
    @classmethod
    def from_resolver_row(cls,row,state_root):
        if not isinstance(row,dict) or row.get('module_id')!='AutoSync' or row.get('verified') is not True: raise HostRuntimeError('AutoUpdate did not return a verified AutoSync module.')
        version=row.get('version'); major=row.get('api_major')
        if not isinstance(version,str) or not re.fullmatch(r'\d+\.\d+\.\d+',version) or major!=1 or row.get('activation_mode')!='restart': raise HostRuntimeError('AutoSync module version/API/activation mode is unsupported.')
        root=Path(row.get('module_path','')).resolve(strict=True)
        expected=(Path(state_root).resolve()/'modules'/'AutoSync'/'versions').resolve()
        if root.parent!=expected or root.name!=version: raise HostRuntimeError('Resolved path is outside its verified cache version.')
        return cls.from_cached_version(root,state_root)
    @classmethod
    def from_cached_version(cls,module_path,state_root):
        root=Path(module_path).resolve(strict=True); version=root.name
        expected=(Path(state_root).resolve()/'modules'/'AutoSync'/'versions').resolve()
        if root.parent!=expected or not re.fullmatch(r'\d+\.\d+\.\d+',version): raise HostRuntimeError('Saved module path is not a versioned AutoSync cache path.')
        mp=root/'module.json'
        if not mp.is_file(): raise HostRuntimeError('Cached module manifest is missing.')
        raw=mp.read_bytes(); manifest_hash=hashlib.sha256(raw).hexdigest()
        try: manifest=json.loads(raw.decode('utf-8'))
        except (UnicodeDecodeError,json.JSONDecodeError) as e: raise HostRuntimeError('Cached manifest is invalid.') from e
        if (manifest.get('schema')!=1 or manifest.get('contract')!='autoupdate-module-v1' or manifest.get('module_id')!='AutoSync'
            or manifest.get('version')!=version or manifest.get('api_major')!=1 or manifest.get('activation_mode')!='restart'):
            raise HostRuntimeError('Cached module identity or activation contract is invalid.')
        files=manifest.get('files')
        if not isinstance(files,list) or not files: raise HostRuntimeError('Cached module has no file list.')
        seen=set()
        for item in files:
            rel=item.get('path') if isinstance(item,dict) else None
            if not isinstance(rel,str) or not rel or rel.startswith(('/','\\')) or ':' in rel: raise HostRuntimeError('Unsafe cached module path.')
            rel=rel.replace('\\','/'); parts=rel.split('/')
            if any(x in ('','.','..') for x in parts) or rel in seen: raise HostRuntimeError('Duplicate or unsafe cached module path.')
            seen.add(rel); target=(root/Path(*parts)).resolve(strict=True)
            try: target.relative_to(root)
            except ValueError as e: raise HostRuntimeError('Cached file escaped module version directory.') from e
            if not target.is_file() or target.stat().st_size!=int(item.get('size',-1)) or not hmac.compare_digest(sha256(target),str(item.get('sha256','')).lower()):
                raise HostRuntimeError('Cached module file integrity check failed: '+rel)
        required={'src/autosync_server.py','src/autosync_maintenance.py','src/autosync_host.py','src/autosync_credentials.py'}
        if not required.issubset(seen): raise HostRuntimeError('Cached module omits a required host runtime file.')
        src=root/'src'
        if any(not (src/x).is_file() for x in ('autosync_server.py','autosync_maintenance.py','autosync_host.py','autosync_credentials.py')): raise HostRuntimeError('Cached host runtime is incomplete.')
        return cls('AutoSync',version,1,'restart',root,src,manifest_hash)

@dataclass
class HostConfig:
    auto_update_cli:Path; module_source_root:Path; state_root:Path; app_manifest_path:Path; update_policy_path:Path
    python_executable:str; data_dir:Path; backup_dir:Path; log_file:Path; bind_host:str; port:int; allow_lan:bool; credential_apps:tuple
    tls_cert:Path|None=None; tls_key:Path|None=None; credential_target_prefix:str='AutoSync/server/'; app_id:str='AutoSyncHost'
    startup_timeout_seconds:int=45; drain_timeout_seconds:int=45
    @classmethod
    def load(cls,path):
        data=json.loads(Path(path).read_text(encoding='utf-8'))
        if data.get('schema')!=1 or data.get('contract')!='autosync-host-config-v1': raise HostRuntimeError('Unsupported host config.')
        if type(data.get('port',8765)) is not int or type(data.get('allow_lan',False)) is not bool: raise HostRuntimeError('Host port/allow_lan types are invalid.')
        def p(name):
            v=data.get(name)
            if not isinstance(v,str) or not v.strip(): raise HostRuntimeError('Missing config field: '+name)
            return Path(v)
        apps=data.get('credential_apps')
        if not isinstance(apps,list) or not apps or any(not isinstance(x,str) for x in apps): raise HostRuntimeError('Credential Manager app IDs are required.')
        cfg=cls(p('auto_update_cli'),p('module_source_root'),p('state_root'),p('app_manifest_path'),p('update_policy_path'),
            str(data.get('python_executable') or 'python.exe'),p('data_dir'),p('backup_dir'),p('log_file'),str(data.get('bind_host') or '127.0.0.1'),
            int(data.get('port',8765)),bool(data.get('allow_lan',False)),tuple(apps),Path(data['tls_cert']) if data.get('tls_cert') else None,
            Path(data['tls_key']) if data.get('tls_key') else None,str(data.get('credential_target_prefix') or 'AutoSync/server/'),
            str(data.get('app_id') or 'AutoSyncHost'),int(data.get('startup_timeout_seconds',45)),int(data.get('drain_timeout_seconds',45)))
        cfg.validate(); return cfg
    def validate(self):
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}',self.app_id): raise HostRuntimeError('Invalid host app_id.')
        if not self.credential_apps or any(not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,63}',x) for x in self.credential_apps): raise HostRuntimeError('Invalid credential app ID.')
        if self.port<1 or self.port>65535 or self.startup_timeout_seconds<5 or self.drain_timeout_seconds<5: raise HostRuntimeError('Invalid port or lifecycle timeout.')
        if not re.fullmatch(r'[A-Za-z0-9._/-]{1,220}',self.credential_target_prefix) or not self.credential_target_prefix.endswith('/') or '..' in self.credential_target_prefix.split('/'): raise HostRuntimeError('Invalid Credential Manager target prefix.')
        if bool(self.tls_cert)!=bool(self.tls_key): raise HostRuntimeError('TLS certificate/key must be configured together.')
        try: address=ipaddress.ip_address(self.bind_host)
        except ValueError as e: raise HostRuntimeError('Host bind address must be an IP literal.') from e
        if address.version!=4: raise HostRuntimeError('Only IPv4 bind addresses are supported.')
        if not address.is_loopback and (not self.allow_lan or not address.is_private or not self.tls_cert): raise HostRuntimeError('LAN bind requires a private IPv4 address, explicit opt-in, and TLS.')
        local_paths=(self.state_root,self.data_dir,self.backup_dir,self.log_file)
        if any(not x.is_absolute() or str(x).startswith('\\\\') for x in local_paths): raise HostRuntimeError('State, database, backup, and log paths must be absolute local paths.')
        module_cache=(self.state_root/'modules').resolve()
        for stable in (self.data_dir,self.backup_dir):
            try: stable.resolve().relative_to(module_cache)
            except ValueError: continue
            raise HostRuntimeError('Database and backup paths must be outside the AutoUpdate module cache.')
    def update_interval_seconds(self,on_error=None):
        try:
            data=json.loads(self.update_policy_path.read_text(encoding='utf-8')); n=int(data['interval_minutes'])
            if data.get('schema')!=1 or n<1 or n>1440: raise ValueError('invalid update interval')
            return n*60
        except Exception as e:
            if on_error:on_error('update-policy-unavailable',{'error':str(e)})
            return 600

class PowerShellAutoUpdate:
    def __init__(self, config, executable=None, windows_powershell=None):
        self.config = config
        if executable is None:
            if os.name == "nt":
                windir = os.environ.get("WINDIR") or os.environ.get("SystemRoot") or r"C:\Windows"
                fallback = Path(windir) / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe"
                if not fallback.is_file():
                    raise HostRuntimeError(
                        "Windows PowerShell 5.1 is unavailable for AutoUpdate module resolution; "
                        "refusing an unverified PowerShell executable from PATH."
                    )
                executable = str(fallback)
                windows_powershell = True
            else:
                executable = shutil.which("pwsh")
                if not executable:
                    raise HostRuntimeError("PowerShell is unavailable for AutoUpdate module resolution.")
                windows_powershell = False
        self.executable = str(executable)
        self.windows_powershell = (
            Path(self.executable).name.casefold() == "powershell.exe"
            if windows_powershell is None else bool(windows_powershell)
        )

    def _environment(self):
        env = minimal_environment()
        if self.windows_powershell:
            windir = env.get("WINDIR") or env.get("SystemRoot") or r"C:\Windows"
            program_files = os.environ.get("ProgramFiles", r"C:\Program Files")
            env["PSModulePath"] = os.pathsep.join((
                str(Path(windir) / "System32" / "WindowsPowerShell" / "v1.0" / "Modules"),
                str(Path(program_files) / "WindowsPowerShell" / "Modules"),
            ))
        return env

    def invoke(self, command, **params):
        args = [self.executable, "-NoLogo", "-NoProfile", "-NonInteractive"]
        if self.windows_powershell:
            # Apply the bypass to this child process only; never change machine or user policy.
            args.extend(("-ExecutionPolicy", "Bypass"))
        args.extend(("-File", str(self.config.auto_update_cli), "-Command", command,
                     "-StateRoot", str(self.config.state_root)))
        for key, value in params.items():
            if value is not None:
                args.extend(("-" + key, str(value)))
        result = run_hidden(args, env=self._environment(), timeout=120)
        if result.returncode:
            raise HostRuntimeError("AutoUpdate command failed: " + (result.stderr.strip() or "nonzero exit"))
        try:
            return json.loads(result.stdout.strip())
        except json.JSONDecodeError as exc:
            raise HostRuntimeError("AutoUpdate returned invalid JSON.") from exc

    def resolve(self):
        result = self.invoke("resolve-modules", AppManifestPath=self.config.app_manifest_path,
                             ModuleSourceRoot=self.config.module_source_root)
        if result.get("status") != "ready" or result.get("app_id") != self.config.app_id:
            raise HostRuntimeError("AutoUpdate did not resolve the host manifest.")
        matches = [item for item in result.get("modules", []) if item.get("module_id") == "AutoSync"]
        if len(matches) != 1:
            raise HostRuntimeError("AutoUpdate returned no unique AutoSync module.")
        return ModuleCandidate.from_resolver_row(matches[0], self.config.state_root)

    def report(self, candidate, pid, runtime_state):
        return self.invoke("report-module-status", AppId=self.config.app_id, ModuleId="AutoSync",
                           Version=candidate.version, RuntimeState=runtime_state,
                           ApplicationProcessId=pid)


class WindowsJobObject:
    """Kills the server child if its supervisor is forcibly closed on Windows."""
    def __init__(self):
        self.handle=None
        if os.name!='nt': return
        class IO_COUNTERS(ctypes.Structure):
            _fields_=[(n,ctypes.c_ulonglong) for n in ('ReadOperationCount','WriteOperationCount','OtherOperationCount','ReadTransferCount','WriteTransferCount','OtherTransferCount')]
        class BASIC(ctypes.Structure):
            _fields_=[('PerProcessUserTimeLimit',ctypes.c_longlong),('PerJobUserTimeLimit',ctypes.c_longlong),('LimitFlags',wintypes.DWORD),
                ('MinimumWorkingSetSize',ctypes.c_size_t),('MaximumWorkingSetSize',ctypes.c_size_t),('ActiveProcessLimit',wintypes.DWORD),
                ('Affinity',ctypes.c_size_t),('PriorityClass',wintypes.DWORD),('SchedulingClass',wintypes.DWORD)]
        class EXTENDED(ctypes.Structure):
            _fields_=[('BasicLimitInformation',BASIC),('IoInfo',IO_COUNTERS),('ProcessMemoryLimit',ctypes.c_size_t),
                ('JobMemoryLimit',ctypes.c_size_t),('PeakProcessMemoryUsed',ctypes.c_size_t),('PeakJobMemoryUsed',ctypes.c_size_t)]
        k=ctypes.WinDLL('kernel32',use_last_error=True)
        k.CreateJobObjectW.argtypes=[wintypes.LPVOID,wintypes.LPCWSTR]; k.CreateJobObjectW.restype=wintypes.HANDLE
        k.SetInformationJobObject.argtypes=[wintypes.HANDLE,wintypes.INT,wintypes.LPVOID,wintypes.DWORD]; k.SetInformationJobObject.restype=wintypes.BOOL
        k.AssignProcessToJobObject.argtypes=[wintypes.HANDLE,wintypes.HANDLE]; k.AssignProcessToJobObject.restype=wintypes.BOOL
        k.CloseHandle.argtypes=[wintypes.HANDLE]; k.CloseHandle.restype=wintypes.BOOL
        h=k.CreateJobObjectW(None,None)
        if not h: raise ctypes.WinError(ctypes.get_last_error())
        info=EXTENDED(); info.BasicLimitInformation.LimitFlags=0x2000
        if not k.SetInformationJobObject(h,9,ctypes.byref(info),ctypes.sizeof(info)):
            e=ctypes.get_last_error(); k.CloseHandle(h); raise ctypes.WinError(e)
        self.handle=h; self.kernel=k
    def assign(self,process):
        if self.handle is not None and not self.kernel.AssignProcessToJobObject(self.handle,wintypes.HANDLE(int(process._handle))):
            raise ctypes.WinError(ctypes.get_last_error())
    def close(self):
        if self.handle is not None:self.kernel.CloseHandle(self.handle); self.handle=None

class ManagedServerProcess:
    def __init__(self,candidate,config):
        self.candidate=candidate; self.config=config; self.token=secrets.token_urlsafe(48)
        with socket.socket(socket.AF_INET,socket.SOCK_STREAM) as s:s.bind(('127.0.0.1',0)); self.control_port=s.getsockname()[1]
        self.control_url=f'http://127.0.0.1:{self.control_port}'; self.job=WindowsJobObject()
        cmd=[config.python_executable,'-B',str(candidate.source_root/'autosync_server.py'),'--host',config.bind_host,'--port',str(config.port),
            '--data-dir',str(config.data_dir),'--credential-apps',','.join(config.credential_apps),'--credential-target-prefix',config.credential_target_prefix,
            '--module-version',candidate.version,'--startup-gated','--control-port',str(self.control_port)]
        if config.allow_lan:cmd.append('--allow-lan')
        if config.tls_cert:cmd.extend(('--tls-cert',str(config.tls_cert),'--tls-key',str(config.tls_key)))
        env=minimal_environment({'AUTOSYNC_CONTROL_TOKEN':self.token,'AUTOSYNC_MODULE_VERSION':candidate.version,'PYTHONPATH':str(candidate.source_root),'PYTHONDONTWRITEBYTECODE':'1'})
        self.server_log_file=config.log_file.with_name('autosync-server.log')
        self.server_log_file.parent.mkdir(parents=True,exist_ok=True)
        try:
            with self.server_log_file.open('ab',buffering=0) as log:
                self.process=subprocess.Popen(cmd,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,cwd=str(candidate.source_root),env=env,
                    creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
            self.job.assign(self.process)
        except Exception:
            proc=getattr(self,'process',None)
            if proc is not None and proc.poll() is None:
                proc.terminate()
                try:proc.wait(timeout=5)
                except subprocess.TimeoutExpired:proc.kill();proc.wait(timeout=5)
            self.job.close(); raise
    @property
    def pid(self):return self.process.pid
    def is_alive(self):return self.process.poll() is None
    def request(self,method,path):
        req=Request(self.control_url+path,method=method,headers={'Authorization':'Bearer '+self.token})
        try:
            with urlopen(req,timeout=3) as response:return json.loads(response.read().decode('utf-8'))
        except HTTPError as e:
            body=e.read().decode('utf-8','replace'); code=e.code; e.close()
            raise HostRuntimeError(f'host control HTTP {code}: {body}') from e
        except (OSError,URLError,TimeoutError) as e:raise HostRuntimeError('host control unavailable') from e
    def status(self):return self.request('GET','/_host/status')
    def activate(self):return self.request('POST','/_host/activate')
    def drain(self):return self.request('POST','/_host/drain')
    def wait(self,timeout):
        try:self.process.wait(timeout=timeout); self.job.close(); return True
        except subprocess.TimeoutExpired:return False
    def stop_gated(self,timeout=5):
        if self.is_alive():
            try:self.drain()
            except HostRuntimeError:pass
            if not self.wait(timeout):
                self.job.close()
                try:self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:self.process.kill();self.process.wait(timeout=5)
        else:self.job.close()
    def force_close(self):self.job.close()

class HostInstanceLock:
    def __init__(self,path):self.path=Path(path);self.file=None
    def acquire(self):
        self.path.parent.mkdir(parents=True,exist_ok=True); self.file=self.path.open('a+b')
        try:
            if os.name=='nt':
                import msvcrt
                self.file.seek(0,2)
                if self.file.tell()==0:self.file.write(b'0');self.file.flush()
                self.file.seek(0);msvcrt.locking(self.file.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl
                fcntl.flock(self.file.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        except (OSError,ImportError) as e:
            self.file.close();self.file=None;raise HostRuntimeError('Another AutoSync supervisor owns this state root.') from e
    def release(self):
        if self.file is None:return
        try:
            if os.name=='nt':
                import msvcrt
                self.file.seek(0);msvcrt.locking(self.file.fileno(),msvcrt.LK_UNLCK,1)
            else:
                import fcntl
                fcntl.flock(self.file.fileno(),fcntl.LOCK_UN)
        finally:self.file.close();self.file=None
class HostSupervisor:
    def __init__(self,config,resolver=None,reporter=None,child_factory=None,preflight=None,backup=None,restore=None,logger=None,poll_interval=.25,supervisor_path=None):
        self.config=config; self.resolver=resolver or PowerShellAutoUpdate(config); self.reporter=reporter or self.resolver
        self.child_factory=child_factory or ManagedServerProcess; self.preflight=preflight or self._preflight
        self.backup=backup or self._backup; self.restore=restore or self._restore; self.logger=logger or self._log
        self.poll_interval=poll_interval; self.child=None; self.active_module=None; self.stop_event=threading.Event()
        self.supervisor_path=Path(supervisor_path or __file__).resolve(); self.exit_code=0
        self.host_state_dir=config.state_root/'autosync-host'; self.last_good_path=self.host_state_dir/'last-good.json'
    def _log(self,event,fields):
        self.config.log_file.parent.mkdir(parents=True,exist_ok=True)
        row={'utc':datetime.now(timezone.utc).isoformat(),'component':'AutoSyncHost','event':event,**fields}
        with self.config.log_file.open('a',encoding='utf-8',newline='\n') as f:f.write(json.dumps(row,ensure_ascii=False,sort_keys=True)+'\n')
    def _load_last_good(self):
        try:
            data=json.loads(self.last_good_path.read_text(encoding='utf-8'))
            item=ModuleCandidate.from_cached_version(data['module_path'],self.config.state_root)
            if data.get('manifest_sha256')!=item.manifest_sha256:return None
            return item
        except (OSError,ValueError,KeyError,TypeError,HostRuntimeError):return None
    def _save_last_good(self,candidate):
        self.host_state_dir.mkdir(parents=True,exist_ok=True); temp=self.last_good_path.with_name(self.last_good_path.name+'.'+uuid.uuid4().hex+'.part')
        data={'schema':1,'version':candidate.version,'module_path':str(candidate.module_path),'manifest_sha256':candidate.manifest_sha256}
        temp.write_text(json.dumps(data,ensure_ascii=False),encoding='utf-8'); os.replace(temp,self.last_good_path)
    def _python(self,candidate,args,timeout=60):
        env=minimal_environment({'PYTHONPATH':str(candidate.source_root),'PYTHONDONTWRITEBYTECODE':'1'})
        result=run_hidden([self.config.python_executable,'-B',*args],env=env,timeout=timeout,cwd=candidate.source_root)
        if result.returncode:
            detail=(result.stderr or result.stdout).strip().replace('\r',' ')[-1000:]
            raise HostRuntimeError('Host preflight/maintenance failed: '+detail)
        return result
    def _preflight(self,candidate):
        self._python(candidate,['-c','import autosync_server, autosync_maintenance, autosync_credentials, autosync_host; print("ready")'],30)
        self.config.data_dir.mkdir(parents=True,exist_ok=True)
        check='from autosync_credentials import load_server_tokens; import sys; x=load_server_tokens(sys.argv[1].split(","),sys.argv[2]); assert set(x)==set(sys.argv[1].split(","))'
        self._python(candidate,['-c',check,','.join(self.config.credential_apps),self.config.credential_target_prefix],30)
        if self.config.tls_cert:
            if not self.config.tls_cert.is_file() or not self.config.tls_key.is_file():raise HostRuntimeError('Configured TLS files are unavailable.')
            tls='import ssl,sys; c=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); c.load_cert_chain(sys.argv[1],sys.argv[2])'
            self._python(candidate,['-c',tls,str(self.config.tls_cert),str(self.config.tls_key)],30)
    def _backup(self,candidate):
        database=self.config.data_dir/'autosync.sqlite3'
        if not database.is_file():return None
        self.config.backup_dir.mkdir(parents=True,exist_ok=True); stamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        dest=self.config.backup_dir/f'autosync-pre-update-{stamp}-{uuid.uuid4().hex[:8]}.sqlite3'
        self._python(candidate,[str(candidate.source_root/'autosync_maintenance.py'),'backup','--database',str(database),'--destination',str(dest)],300)
        return dest
    def _restore(self,candidate,backup_path):
        self._python(candidate,[str(candidate.source_root/'autosync_maintenance.py'),'restore','--backup',str(backup_path),
            '--database',str(self.config.data_dir/'autosync.sqlite3')],300)
    def _wait_for(self,child,candidate,states,timeout):
        end=time.monotonic()+timeout; last=None
        while time.monotonic()<end:
            if not child.is_alive():raise HostRuntimeError('AutoSync server exited before readiness.')
            try:
                status=child.status()
                if status.get('module_version')!=candidate.version:raise HostRuntimeError('Server reported the wrong module version.')
                if status.get('state') in states:
                    if status.get('integrity')!='ok' or status.get('schema_version')!=2:raise HostRuntimeError('Database readiness check failed.')
                    return status
            except HostRuntimeError as e:last=e
            time.sleep(self.poll_interval)
        raise HostRuntimeError('AutoSync server readiness timed out.'+((' '+str(last)) if last else ''))
    def _report(self,candidate,child,state):
        try:self.reporter.report(candidate,child.pid,state)
        except Exception as e:self.logger('runtime-status-report-failed',{'state':state,'error':str(e)})
    def _start(self,candidate):
        child=self.child_factory(candidate,self.config); safe=True
        try:
            self._wait_for(child,candidate,{'warming'},self.config.startup_timeout_seconds); self._report(candidate,child,'loaded')
            safe=False  # activation may succeed even if its reply is lost
            child.activate(); status=self._wait_for(child,candidate,{'serving'},self.config.startup_timeout_seconds)
            if status.get('ready') is not True:raise HostRuntimeError('Server failed readiness after activation.')
            self._report(candidate,child,'active'); return child
        except Exception as e:
            if safe:
                child.stop_gated(); raise CandidateStartError(str(e),True) from e
            raise CandidateStartError(str(e),False,child) from e
    def _stop_current(self):
        if self.child is None:return True
        if not self.child.is_alive():self.child.wait(0); self.child=None; return True
        try:self.child.drain()
        except HostRuntimeError as e:
            self.logger('writer-drain-unconfirmed',{'version':self.active_module.version if self.active_module else None,'error':str(e)})
            return False
        if not self.child.wait(self.config.drain_timeout_seconds):
            self.logger('writer-drain-timeout',{'version':self.active_module.version if self.active_module else None}); return False
        self.child=None; return True
    def reconcile_once(self):
        try:candidate=self.resolver.resolve()
        except Exception as e:
            self.logger('resolve-failed',{'error':str(e),'serving':bool(self.child and self.child.is_alive())}); return 'resolve-failed'
        candidate_supervisor=candidate.source_root/'autosync_host.py'
        try:
            if not hmac.compare_digest(sha256(self.supervisor_path),sha256(candidate_supervisor)):
                self.logger('supervisor-update-required',{'current_path':str(self.supervisor_path),'candidate_path':str(candidate_supervisor),'version':candidate.version})
                return 'supervisor-update-required'
        except OSError as e:
            self.logger('supervisor-code-check-failed',{'error':str(e),'version':candidate.version}); return 'needs-attention'
        previous=self.active_module or self._load_last_good(); alive=bool(self.child and self.child.is_alive())
        if alive and previous and previous.manifest_sha256==candidate.manifest_sha256:
            try:
                status=self.child.status()
                if status.get('state')=='serving' and status.get('ready') is True:
                    self._report(previous,self.child,'active')
                    return 'unchanged'
                self.logger('active-health-not-ready',{'version':previous.version,'state':status.get('state')})
            except Exception as e:self.logger('active-health-check-failed',{'error':str(e)})
            return 'active-needs-attention'
        try:self.preflight(candidate)
        except Exception as e:
            self.logger('candidate-preflight-failed',{'version':candidate.version,'error':str(e)}); return 'candidate-rejected'
        backup_path=None
        if alive or (previous is None and (self.config.data_dir/'autosync.sqlite3').is_file()) or (previous and previous.manifest_sha256!=candidate.manifest_sha256 and (self.config.data_dir/'autosync.sqlite3').is_file()):
            try:backup_path=self.backup(previous or candidate)
            except Exception as e:self.logger('pre-update-backup-failed',{'error':str(e)}); return 'backup-failed'
        if alive and not self._stop_current():return 'drain-timeout'
        elif self.child is not None and not self.child.is_alive():
            self.child=None; self.logger('server-child-exited',{'previous_version':previous.version if previous else None})
        try:
            self.child=self._start(candidate); self.active_module=candidate; self._save_last_good(candidate)
            self.logger('server-active',{'version':candidate.version,'module_path':str(candidate.module_path)})
            return 'updated' if previous and previous.manifest_sha256!=candidate.manifest_sha256 else 'started'
        except CandidateStartError as e:
            if not e.safe_to_restore:
                self.child=e.child; self.active_module=candidate
                self.logger('candidate-state-uncertain',{'version':candidate.version,'error':str(e)}); return 'needs-attention'
            self.logger('candidate-start-failed',{'version':candidate.version,'error':str(e)})
            if backup_path is not None:
                try:self.restore(previous or candidate,Path(backup_path))
                except Exception as restore_error:self.logger('database-restore-failed',{'error':str(restore_error)}); return 'needs-attention'
            if previous and previous.manifest_sha256!=candidate.manifest_sha256:
                try:
                    self.preflight(previous); self.child=self._start(previous); self.active_module=previous; self._save_last_good(previous)
                    self.logger('rollback-active',{'version':previous.version}); return 'rolled-back'
                except Exception as rollback_error:
                    self.child=None; self.active_module=None; self.logger('rollback-failed',{'error':str(rollback_error)}); return 'needs-attention'
            return 'start-failed'
    def run(self):
        lock=HostInstanceLock(self.host_state_dir/'supervisor.lock')
        lock.acquire()
        failures=0
        try:
            self.config.data_dir.mkdir(parents=True,exist_ok=True)
            interval=self.config.update_interval_seconds(lambda e,f:self.logger(e,f))
            while not self.stop_event.is_set():
                try:
                    result=self.reconcile_once()
                    if result in ('updated','started','unchanged','rolled-back'):failures=0
                    elif result=='supervisor-update-required':
                        self.exit_code=SUPERVISOR_RESTART_EXIT
                        break
                    elif result in ('needs-attention','active-needs-attention'):
                        self.logger('manual-attention-required',{'result':result}); self.stop_event.wait()
                        break
                    else:failures=min(failures+1,8)
                except Exception as e:
                    failures=min(failures+1,8); self.logger('reconcile-error',{'error':str(e),'consecutive_failures':failures})
                interval=self.config.update_interval_seconds(lambda e,f:self.logger(e,f))
                delay=min(interval, min(30*(2**max(0,failures-1)),600)) if failures else interval
                self.stop_event.wait(delay)
        finally:
            try:self.close()
            finally:lock.release()
        return self.exit_code
    def close(self):
        self.stop_event.set()
        if self.child is None:return
        child=self.child
        try:
            if child.is_alive():
                try:child.drain()
                except HostRuntimeError:self.logger('shutdown-drain-unconfirmed',{'pid':child.pid})
                if not child.wait(self.config.drain_timeout_seconds):
                    self.logger('shutdown-drain-timeout',{'pid':child.pid}); child.stop_gated()
            else:child.wait(0)
        finally:
            child.force_close(); self.child=None

def main(argv=None):
    import argparse
    parser=argparse.ArgumentParser(description='AutoUpdate-backed AutoSync host supervisor')
    parser.add_argument('--config',required=True,help='ACL-protected host config JSON')
    args=parser.parse_args(argv)
    try:
        config=HostConfig.load(args.config); supervisor=HostSupervisor(config)
        def stop_signal(signum,frame):supervisor.stop_event.set()
        for sig in (getattr(signal,'SIGINT',None),getattr(signal,'SIGTERM',None),getattr(signal,'SIGBREAK',None)):
            if sig is not None:
                try:signal.signal(sig,stop_signal)
                except (ValueError,OSError):pass
        return supervisor.run()
    except HostRuntimeError as e:
        print(f'AutoSync host stopped safely: {e}',file=sys.stderr); return 2
    except Exception as e:
        print(f'AutoSync host failed: {type(e).__name__}: {e}',file=sys.stderr); return 1

if __name__=='__main__':raise SystemExit(main())
