import hashlib
import json
import os
import socket
import shutil
import subprocess
import sys
import tempfile
import unittest
import uuid
import urllib.request
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from src import autosync_host
from src import __version__ as AUTOSYNC_VERSION
from src.autosync_client import AutoSyncClient
from src.autosync_credentials import delete_secret, write_secret
from src.autosync_host import HostConfig, HostInstanceLock, HostRuntimeError, HostSupervisor, ModuleCandidate, PowerShellAutoUpdate

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = ('autosync_server.py', 'autosync_maintenance.py', 'autosync_host.py', 'autosync_credentials.py')


def make_candidate(base, version):
    state_root = base / 'state'
    package = state_root / 'modules' / 'AutoSync' / 'versions' / version
    source = package / 'src'
    source.mkdir(parents=True)
    entries = []
    for name in REQUIRED:
        source_file = ROOT / 'src' / name
        target = source / name
        shutil.copyfile(source_file, target)
        data = target.read_bytes()
        entries.append({'path': 'src/' + name, 'size': len(data), 'sha256': hashlib.sha256(data).hexdigest()})
    manifest = {'schema': 1, 'contract': 'autoupdate-module-v1', 'module_id': 'AutoSync',
                'version': version, 'api_major': 1, 'activation_mode': 'restart', 'files': entries}
    (package / 'module.json').write_text(json.dumps(manifest), encoding='utf-8')
    return ModuleCandidate.from_cached_version(package, state_root)


def make_config(base):
    return HostConfig(base / 'autoupdate.ps1', base / 'module-source', base / 'state', base / 'host-modules.json',
                      base / 'update-policy.json', 'python', base / 'data', base / 'backups', base / 'host.log',
                      '127.0.0.1', 8765, False, ('MemoApp',))


class FakeResolver:
    def __init__(self, candidate):
        self.candidate = candidate
    def resolve(self):
        return self.candidate


class FakeReporter:
    def __init__(self):
        self.calls = []
    def report(self, candidate, pid, state):
        self.calls.append((candidate.version, pid, state))


class FakeChild:
    next_pid = 1000
    def __init__(self, candidate, events, fail=None):
        self.candidate = candidate
        self.events = events
        self.fail = fail
        self.pid = FakeChild.next_pid
        FakeChild.next_pid += 1
        self.alive = fail != 'before-ready'
        self.state = 'warming'
    def is_alive(self):
        return self.alive
    def status(self):
        if self.fail == 'bad-status':
            raise HostRuntimeError('simulated readiness failure')
        return {'state': self.state, 'ready': self.state == 'serving', 'integrity': 'ok',
                'schema_version': 2, 'module_version': self.candidate.version}
    def activate(self):
        self.events.append(('activate', self.candidate.version))
        self.state = 'serving'
        if self.fail == 'activate-uncertain':
            raise HostRuntimeError('activation reply lost')
    def drain(self):
        self.events.append(('drain', self.candidate.version))
        if self.fail == 'drain':
            raise HostRuntimeError('drain reply lost')
        self.state = 'draining'
    def wait(self, timeout):
        self.events.append(('wait', self.candidate.version))
        if self.state == 'draining' or not self.alive:
            self.alive = False
            return True
        return False
    def stop_gated(self, timeout=5):
        self.events.append(('stop-gated', self.candidate.version))
        self.alive = False
    def force_close(self):
        self.alive = False


class PowerShellAutoUpdateTests(unittest.TestCase):
    def test_windows_powershell_fallback_is_process_scoped_and_sets_module_path(self):
        config = SimpleNamespace(
            auto_update_cli=Path(r"C:\AI-Work\AutoUpdate\tools\autoupdate.ps1"),
            state_root=Path(r"C:\Users\Administrator\AppData\Local\Siny\AutoUpdate"),
        )
        with mock.patch.object(autosync_host.shutil, "which", return_value=None), \
             mock.patch.object(Path, "is_file", return_value=True):
            resolver = PowerShellAutoUpdate(config)
        self.assertTrue(resolver.windows_powershell)
        self.assertTrue(resolver.executable.casefold().endswith("powershell.exe"))
        completed = __import__("subprocess").CompletedProcess([], 0, '{"status":"ready"}', "")
        with mock.patch.object(autosync_host, "run_hidden", return_value=completed) as run:
            self.assertEqual(resolver.invoke("resolve-modules"), {"status": "ready"})
        args = run.call_args.args[0]
        self.assertLess(args.index("-ExecutionPolicy"), args.index("-File"))
        self.assertEqual(args[args.index("-ExecutionPolicy") + 1], "Bypass")
        module_path = run.call_args.kwargs["env"]["PSModulePath"]
        self.assertIn("WindowsPowerShell", module_path)
        self.assertTrue(module_path.lower().endswith("modules"))


class HostSupervisorTests(unittest.TestCase):
    def setUp(self):
        FakeChild.next_pid = 1000
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.v1 = make_candidate(self.base, '0.1.3')
        self.v2 = make_candidate(self.base, '0.1.4')
        self.events = []
        self.reporter = FakeReporter()
        self.resolver = FakeResolver(self.v1)
        self.fail_versions = {}
        self.config = make_config(self.base)
        def factory(candidate, config):
            self.events.append(('start', candidate.version))
            return FakeChild(candidate, self.events, self.fail_versions.get(candidate.version))
        self.supervisor = HostSupervisor(self.config, self.resolver, self.reporter, factory,
            preflight=lambda candidate: self.events.append(('preflight', candidate.version)),
            backup=lambda candidate: self._backup(candidate), restore=lambda candidate, path: self._restore(candidate, path),
            logger=lambda event, fields: self.events.append((event, fields)), poll_interval=0)
        self.supervisor.supervisor_path = self.v1.source_root / 'autosync_host.py'
    def tearDown(self):
        self.supervisor.close()
        self.tmp.cleanup()
    def _backup(self, candidate):
        self.events.append(('backup', candidate.version))
        return self.base / 'backups' / 'pre-update.sqlite3'
    def _restore(self, candidate, path):
        self.events.append(('restore', candidate.version))
    def test_host_instance_lock_refuses_a_second_supervisor(self):
        first = HostInstanceLock(self.base / 'state' / 'autosync-host' / 'supervisor.lock')
        second = HostInstanceLock(self.base / 'state' / 'autosync-host' / 'supervisor.lock')
        first.acquire()
        try:
            with self.assertRaisesRegex(HostRuntimeError, 'Another AutoSync supervisor'):
                second.acquire()
        finally:
            first.release()
        second.acquire()
        second.release()
    def test_config_refuses_unc_state_and_non_tls_lan_bind(self):
        cfg = make_config(self.base)
        cfg.state_root = Path(r'\\H255\shared\state')
        with self.assertRaisesRegex(HostRuntimeError, 'absolute local paths'):
            cfg.validate()
        cfg = make_config(self.base)
        cfg.bind_host = '0.0.0.0'
        cfg.allow_lan = True
        with self.assertRaisesRegex(HostRuntimeError, 'private IPv4 address'):
            cfg.validate()
    def test_update_interval_reads_policy_and_uses_safe_ten_minute_fallback(self):
        policy = self.base / 'update-policy.json'
        policy.write_text(json.dumps({'schema': 1, 'interval_minutes': 5}), encoding='utf-8')
        cfg = make_config(self.base)
        cfg.update_policy_path = policy
        self.assertEqual(cfg.update_interval_seconds(), 300)
        policy.write_text('{bad json', encoding='utf-8')
        seen = []
        self.assertEqual(cfg.update_interval_seconds(lambda event, fields: seen.append(event)), 600)
        self.assertEqual(seen, ['update-policy-unavailable'])
    def test_verified_cache_requires_every_runtime_file_in_manifest(self):
        package = self.v1.module_path
        manifest_path = package / 'module.json'
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        manifest['files'] = [x for x in manifest['files'] if x['path'] != 'src/autosync_host.py']
        manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
        with self.assertRaisesRegex(HostRuntimeError, 'omits a required'):
            ModuleCandidate.from_cached_version(package, self.base / 'state')
    def test_changed_supervisor_requests_verified_bootstrap_restart(self):
        host_path = self.v2.source_root / 'autosync_host.py'
        host_path.write_bytes(host_path.read_bytes() + b'\n# candidate supervisor implementation changed\n')
        manifest_path = self.v2.module_path / 'module.json'
        manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
        entry = next(item for item in manifest['files'] if item['path'] == 'src/autosync_host.py')
        raw = host_path.read_bytes()
        entry['size'] = len(raw)
        entry['sha256'] = hashlib.sha256(raw).hexdigest()
        manifest_path.write_text(json.dumps(manifest), encoding='utf-8')
        self.resolver.candidate = ModuleCandidate.from_cached_version(self.v2.module_path, self.base / 'state')
        self.assertEqual(self.supervisor.run(), 75)
        self.assertIsNone(self.supervisor.child)
        self.assertIn('supervisor-update-required', [event[0] for event in self.events])

    def test_initial_start_and_same_version_leave_child_running(self):
        self.assertEqual(self.supervisor.reconcile_once(), 'started')
        first = self.supervisor.child
        self.assertEqual(self.supervisor.reconcile_once(), 'unchanged')
        self.assertIs(self.supervisor.child, first)
        self.assertIn(('0.1.3', first.pid, 'loaded'), self.reporter.calls)
        self.assertIn(('0.1.3', first.pid, 'active'), self.reporter.calls)
    def test_update_backs_up_then_drains_before_starting_single_writer(self):
        self.config.data_dir.mkdir(parents=True)
        (self.config.data_dir / 'autosync.sqlite3').write_bytes(b'test-db')
        self.assertEqual(self.supervisor.reconcile_once(), 'started')
        self.resolver.candidate = self.v2
        self.assertEqual(self.supervisor.reconcile_once(), 'updated')
        self.assertEqual(self.supervisor.active_module.version, '0.1.4')
        events = [x[0] for x in self.events]
        self.assertLess(events.index('backup'), events.index('drain'))
        self.assertLess(events.index('drain'), events.index('start', events.index('drain') + 1))
        self.assertFalse(any(e[0] == 'writer-drain-timeout' for e in self.events))
    def test_pre_activation_failure_restores_backup_then_restarts_previous_version(self):
        self.config.data_dir.mkdir(parents=True)
        (self.config.data_dir / 'autosync.sqlite3').write_bytes(b'test-db')
        self.assertEqual(self.supervisor.reconcile_once(), 'started')
        old = self.supervisor.child
        self.resolver.candidate = self.v2
        self.fail_versions['0.1.4'] = 'before-ready'
        self.assertEqual(self.supervisor.reconcile_once(), 'rolled-back')
        self.assertFalse(old.is_alive())
        self.assertEqual(self.supervisor.active_module.version, '0.1.3')
        names = [e[0] for e in self.events]
        self.assertLess(names.index('restore'), names.index('activate', names.index('restore')))
    def test_uncertain_activation_never_restores_database_or_starts_old_writer(self):
        self.config.data_dir.mkdir(parents=True)
        (self.config.data_dir / 'autosync.sqlite3').write_bytes(b'test-db')
        self.assertEqual(self.supervisor.reconcile_once(), 'started')
        self.resolver.candidate = self.v2
        self.fail_versions['0.1.4'] = 'activate-uncertain'
        self.assertEqual(self.supervisor.reconcile_once(), 'needs-attention')
        self.assertEqual(self.supervisor.child.candidate.version, '0.1.4')
        self.assertFalse(any(e[0] == 'restore' for e in self.events))
        self.assertEqual(sum(e == ('start', '0.1.3') for e in self.events), 1)
    def test_drain_failure_keeps_old_child_and_does_not_start_candidate(self):
        self.assertEqual(self.supervisor.reconcile_once(), 'started')
        old = self.supervisor.child
        old.fail = 'drain'
        self.resolver.candidate = self.v2
        self.assertEqual(self.supervisor.reconcile_once(), 'drain-timeout')
        self.assertTrue(old.is_alive())
        self.assertIs(self.supervisor.child, old)
        self.assertFalse(any(e == ('start', '0.1.4') for e in self.events))


@unittest.skipUnless(os.name == 'nt', 'Local AutoSync host requires Windows Credential Manager and Job Objects')
class LocalHostBootstrapIntegrationTests(unittest.TestCase):
    def test_autoupdate_local_source_starts_verified_host_and_syncs_two_clients(self):
        autoupdate_root = ROOT.parent / 'AutoUpdate'
        module_source_root = Path(os.environ['LOCALAPPDATA']) / 'Siny' / 'AutoUpdate' / 'local-module-source'
        auto_update_cli = autoupdate_root / 'tools' / 'autoupdate.ps1'
        if not auto_update_cli.is_file() or not module_source_root.is_dir():
            self.skipTest('Local AutoUpdate source or AutoSync source overlay is unavailable')

        temp = tempfile.TemporaryDirectory(prefix='autosync-local-host-e2e-')
        root = Path(temp.name)
        app_id = 'autosync-local-' + uuid.uuid4().hex[:12]
        credential_prefix = 'AutoSync/test-host/' + uuid.uuid4().hex + '/'
        token = 'local-host-test-' + uuid.uuid4().hex + '-token-material'
        credential_target = credential_prefix + app_id
        supervisor = None
        client_a = None
        client_b = None
        port = None
        try:
            write_secret(credential_target, token, username='AutoSync local host test')
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                probe.bind(('127.0.0.1', 0))
                port = probe.getsockname()[1]

            state_root = root / 'autoupdate-state'
            app_manifest = root / 'host-app-modules.json'
            app_manifest.write_text(json.dumps({
                'schema': 1,
                'contract': 'autoupdate-app-modules-v1',
                'app_id': 'AutoSyncHost',
                'modules': [{'module_id': 'AutoSync', 'api_major': 1}],
            }), encoding='utf-8')
            update_policy = root / 'update-policy.json'
            update_policy.write_text(json.dumps({'schema': 1, 'interval_minutes': 10}), encoding='utf-8')
            config = HostConfig(
                auto_update_cli=auto_update_cli,
                module_source_root=module_source_root,
                state_root=state_root,
                app_manifest_path=app_manifest,
                update_policy_path=update_policy,
                python_executable=sys.executable,
                data_dir=root / 'server-data',
                backup_dir=root / 'server-backups',
                log_file=root / 'logs' / 'host.log',
                bind_host='127.0.0.1',
                port=port,
                allow_lan=False,
                credential_apps=(app_id,),
                credential_target_prefix=credential_prefix,
            )
            config.validate()
            supervisor = HostSupervisor(
                config,
                resolver=PowerShellAutoUpdate(config),
                poll_interval=0.05,
            )
            self.assertEqual(supervisor.reconcile_once(), 'started')
            self.assertEqual(supervisor.active_module.version, AUTOSYNC_VERSION)

            base_url = f'http://127.0.0.1:{port}'
            health = urllib.request.Request(
                base_url + '/health',
                headers={'Authorization': 'Bearer ' + token},
            )
            with urllib.request.urlopen(health, timeout=5) as response:
                self.assertEqual(response.status, 200)
                self.assertEqual(json.loads(response.read())['module_version'], AUTOSYNC_VERSION)

            client_a = AutoSyncClient(base_url, token, app_id, root / 'client-a')
            client_b = AutoSyncClient(base_url, token, app_id, root / 'client-b')
            client_a.queue_write('local-e2e-record', {'from': 'client-a'})
            self.assertEqual(client_a.sync_once()['status'], 'synced')
            self.assertEqual(client_b.sync_once()['status'], 'synced')
            self.assertEqual(client_b.cached_record('local-e2e-record')['payload'], {'from': 'client-a'})

            client_b.queue_write(
                'local-e2e-record', {'from': 'client-b'},
                expected_revision=client_b.record_revision('local-e2e-record'),
            )
            self.assertEqual(client_b.sync_once()['status'], 'synced')
            self.assertEqual(client_a.sync_once()['status'], 'synced')
            self.assertEqual(client_a.cached_record('local-e2e-record')['payload'], {'from': 'client-b'})
        finally:
            try:
                if client_a is not None:
                    client_a.close()
            finally:
                try:
                    if client_b is not None:
                        client_b.close()
                finally:
                    try:
                        if supervisor is not None:
                            supervisor.close()
                    finally:
                        try:
                            delete_secret(credential_target)
                        finally:
                            temp.cleanup()
        if port is not None:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                self.assertNotEqual(probe.connect_ex(('127.0.0.1', port)), 0)

    def test_local_version_bump_restarts_host_and_preserves_synced_data(self):
        autoupdate_root = ROOT.parent / 'AutoUpdate'
        if not (autoupdate_root / 'tools' / 'sync_local_modules.py').is_file():
            self.skipTest('Local AutoUpdate module sync helper is unavailable')

        temp = tempfile.TemporaryDirectory(prefix='autosync-local-upgrade-e2e-')
        root = Path(temp.name)
        source_root = root / 'module-source' / 'AutoSync'
        module_src = source_root / 'src'
        module_src.mkdir(parents=True)
        for source in (ROOT / 'src').glob('*.py'):
            shutil.copyfile(source, module_src / source.name)

        state_root = root / 'autoupdate-state'
        source_config = root / 'local-module-sources.json'
        source_config.write_text(json.dumps({
            'schema': 1,
            'modules': [{
                'module_id': 'AutoSync',
                'api_major': 1,
                'minimum_engine': '0.1.0',
                'activation_mode': 'restart',
                'source_root': str(source_root),
            }],
        }), encoding='utf-8')
        sync_helper = autoupdate_root / 'tools' / 'sync_local_modules.py'

        def build_local_source():
            result = subprocess.run(
                [sys.executable, '-B', str(sync_helper), '--config', str(source_config),
                 '--state-root', str(state_root), '--engine-version', '0.1.1817'],
                cwd=str(autoupdate_root / 'tools'),
                env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'},
                capture_output=True, text=True, timeout=30,
                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0),
            )
            self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
            return json.loads(result.stdout)

        app_id = 'autosync-upgrade-' + uuid.uuid4().hex[:12]
        credential_prefix = 'AutoSync/test-upgrade/' + uuid.uuid4().hex + '/'
        credential_target = credential_prefix + app_id
        token = 'local-upgrade-test-' + uuid.uuid4().hex + '-token-material'
        supervisor = None
        client = None
        port = None
        try:
            first_source = build_local_source()
            self.assertEqual(first_source['status'], 'ready')
            self.assertEqual(first_source['modules'][0]['installed_version'], AUTOSYNC_VERSION)
            write_secret(credential_target, token, username='AutoSync local upgrade test')
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                probe.bind(('127.0.0.1', 0))
                port = probe.getsockname()[1]

            app_manifest = root / 'host-app-modules.json'
            app_manifest.write_text(json.dumps({
                'schema': 1,
                'contract': 'autoupdate-app-modules-v1',
                'app_id': 'AutoSyncHost',
                'modules': [{'module_id': 'AutoSync', 'api_major': 1}],
            }), encoding='utf-8')
            update_policy = root / 'update-policy.json'
            update_policy.write_text(json.dumps({'schema': 1, 'interval_minutes': 10}), encoding='utf-8')
            config = HostConfig(
                auto_update_cli=autoupdate_root / 'tools' / 'autoupdate.ps1',
                module_source_root=state_root / 'local-module-source',
                state_root=state_root,
                app_manifest_path=app_manifest,
                update_policy_path=update_policy,
                python_executable=sys.executable,
                data_dir=root / 'server-data',
                backup_dir=root / 'server-backups',
                log_file=root / 'logs' / 'host.log',
                bind_host='127.0.0.1',
                port=port,
                allow_lan=False,
                credential_apps=(app_id,),
                credential_target_prefix=credential_prefix,
            )
            config.validate()
            supervisor = HostSupervisor(config, resolver=PowerShellAutoUpdate(config), poll_interval=0.05)
            self.assertEqual(supervisor.reconcile_once(), 'started')
            self.assertEqual(supervisor.active_module.version, AUTOSYNC_VERSION)

            client = AutoSyncClient(f'http://127.0.0.1:{port}', token, app_id, root / 'client-before-update')
            client.queue_write('survives-local-update', {'value': 'before'})
            self.assertEqual(client.sync_once()['status'], 'synced')
            client.close()
            client = None

            init_path = module_src / '__init__.py'
            init_path.write_text(
                init_path.read_text(encoding='utf-8').replace(
                    f'__version__ = "{AUTOSYNC_VERSION}"', '__version__ = "0.1.32"', 1),
                encoding='utf-8',
            )
            server_path = module_src / 'autosync_server.py'
            with server_path.open('a', encoding='utf-8', newline='\n') as stream:
                stream.write('\n# isolated local upgrade fixture\n')
            upgraded_source = build_local_source()
            self.assertEqual(upgraded_source['modules'][0]['source_version'], '0.1.32')
            self.assertEqual(upgraded_source['modules'][0]['installed_version'], '0.1.32')

            self.assertEqual(supervisor.reconcile_once(), 'updated')
            self.assertEqual(supervisor.active_module.version, '0.1.32')
            self.assertEqual(len(list((root / 'server-backups').glob('autosync-pre-update-*.sqlite3'))), 1)

            client = AutoSyncClient(f'http://127.0.0.1:{port}', token, app_id, root / 'client-after-update')
            self.assertEqual(client.sync_once()['status'], 'synced')
            self.assertEqual(
                client.cached_record('survives-local-update')['payload'],
                {'value': 'before'},
            )
            self.assertTrue((state_root / 'modules' / 'AutoSync' / 'versions' / AUTOSYNC_VERSION).is_dir())
            self.assertTrue((state_root / 'modules' / 'AutoSync' / 'versions' / '0.1.32').is_dir())
        finally:
            try:
                if client is not None:
                    client.close()
            finally:
                try:
                    if supervisor is not None:
                        supervisor.close()
                finally:
                    try:
                        delete_secret(credential_target)
                    finally:
                        temp.cleanup()
        if port is not None:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                self.assertNotEqual(probe.connect_ex(('127.0.0.1', port)), 0)


if __name__ == '__main__':
    unittest.main()
