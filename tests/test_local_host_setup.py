import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from src.autosync_credentials import CredentialError
from tools import setup_local_autosync_host as setup


class LocalHostSetupTests(unittest.TestCase):
    def test_builds_loopback_only_config_from_local_autoupdate_paths(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            autoupdate = root / 'AutoUpdate'
            (autoupdate / 'tools').mkdir(parents=True)
            (autoupdate / 'config').mkdir()
            (autoupdate / 'tools' / 'autoupdate.ps1').write_text('', encoding='utf-8')
            (autoupdate / 'config' / 'update-policy.json').write_text(
                json.dumps({'schema': 1, 'interval_minutes': 10, 'local_only': True}), encoding='utf-8')
            state = root / 'Siny' / 'AutoUpdate'
            (state / 'local-module-source' / 'AutoSync').mkdir(parents=True)
            local_root = root / 'Siny' / 'AutoSync' / 'host-local'
            with mock.patch.object(setup, 'AUTOUPDATE_ROOT', autoupdate), \
                 mock.patch.dict(os.environ, {'LOCALAPPDATA': str(root)}):
                manifest, config = setup.build_configuration(['workboard'], local_root)

            self.assertEqual(manifest['app_id'], 'AutoSyncHost')
            self.assertEqual(manifest['modules'], [{'module_id': 'AutoSync', 'api_major': 1}])
            self.assertEqual(config['bind_host'], '127.0.0.1')
            self.assertEqual(config['port'], 8765)
            self.assertFalse(config['allow_lan'])
            self.assertEqual(config['credential_apps'], ['workboard'])
            self.assertEqual(config['credential_target_prefix'], 'AutoSync/local-server/')
            self.assertEqual(config['state_root'], str(state.resolve()))
            self.assertNotIn('token', json.dumps(config).casefold())

    def test_rejects_duplicate_app_ids_and_network_root(self):
        with self.assertRaises(setup.SetupError):
            setup._validate_app_ids(['Workboard', 'workboard'])
        with self.assertRaises(setup.SetupError):
            setup.build_configuration(['workboard'], Path(r'\\H255\share\host'))

    def test_creates_matching_server_and_client_credentials_only_when_both_missing(self):
        with mock.patch.object(setup, 'read_secret', side_effect=CredentialError('missing')) as read, \
             mock.patch.object(setup, 'write_secret') as write:
            status = setup._ensure_token_pair('workboard')
        self.assertEqual(status, 'created')
        self.assertEqual(read.call_args_list, [
            mock.call('AutoSync/local-server/workboard'),
            mock.call('AutoSync/local-client/workboard'),
        ])
        self.assertEqual(write.call_count, 2)
        self.assertEqual(write.call_args_list[0].args[1], write.call_args_list[1].args[1])
        self.assertGreaterEqual(len(write.call_args_list[0].args[1].encode('utf-8')), 32)

    def test_preserves_existing_credential_pairs_and_refuses_mismatches(self):
        with mock.patch.object(setup, 'read_secret', side_effect=['same-token', 'same-token']), \
             mock.patch.object(setup, 'write_secret') as write:
            self.assertEqual(setup._ensure_token_pair('workboard'), 'preserved')
        write.assert_not_called()

        with mock.patch.object(setup, 'read_secret', side_effect=['server-token', 'client-token']), \
             mock.patch.object(setup, 'write_secret') as write:
            with self.assertRaisesRegex(setup.SetupError, 'already differ'):
                setup._ensure_token_pair('workboard')
        write.assert_not_called()

    def test_existing_config_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config_path = root / 'host-config.json'
            config_path.write_text('preserve-me', encoding='utf-8')
            with mock.patch.object(setup, 'build_configuration') as build, \
                 mock.patch.object(setup, '_ensure_token_pair') as credentials:
                with self.assertRaisesRegex(setup.SetupError, 'refusing to overwrite'):
                    setup.setup_local_host(['workboard'], root)
            build.assert_not_called()
            credentials.assert_not_called()
            self.assertEqual(config_path.read_text(encoding='utf-8'), 'preserve-me')

    def test_failed_validation_removes_only_new_files_and_credentials(self):
        manifest = {'schema': 1, 'contract': 'autoupdate-app-modules-v1', 'app_id': 'AutoSyncHost', 'modules': []}
        config = {'schema': 1, 'contract': 'autosync-host-config-v1'}
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            created_targets = []

            def ensure_token(app_id):
                created_targets.extend((setup.SERVER_PREFIX + app_id, setup.CLIENT_PREFIX + app_id))
                return 'created'

            with mock.patch.object(setup, 'build_configuration', return_value=(manifest, config)), \
                 mock.patch.object(setup, '_ensure_token_pair', side_effect=ensure_token), \
                 mock.patch.object(setup, 'read_config', side_effect=setup.LauncherError('invalid')) as validate, \
                 mock.patch.object(setup, 'delete_secret') as delete:
                with self.assertRaises(setup.SetupError):
                    setup.setup_local_host(['workboard'], root)

            validate.assert_called_once()
            self.assertEqual(delete.call_args_list, [mock.call(item) for item in reversed(created_targets)])
            self.assertFalse((root / 'host-config.json').exists())
            self.assertFalse((root / 'host-app-modules.json').exists())

    def test_task_install_uses_hidden_windows_powershell_and_rejects_bad_name(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            powershell = root / 'System32' / 'WindowsPowerShell' / 'v1.0' / 'powershell.exe'
            powershell.parent.mkdir(parents=True)
            powershell.write_text('', encoding='utf-8')
            with mock.patch.dict(os.environ, {'WINDIR': str(root)}), \
                 mock.patch.object(setup.subprocess, 'run', return_value=mock.Mock(returncode=0, stdout='installed', stderr='')) as run:
                result = json.loads(setup.install_user_task(root / 'host-config.json'))
            self.assertEqual(result['mechanism'], 'scheduled-task')
            command = run.call_args.args[0]
            self.assertEqual(command[0], str(powershell))
            self.assertIn('-StartNow', command)
            self.assertEqual(run.call_args.kwargs['creationflags'], getattr(setup.subprocess, 'CREATE_NO_WINDOW', 0))

            with self.assertRaisesRegex(setup.SetupError, 'Invalid per-user task name'):
                setup.install_user_task(root / 'host-config.json', 'unrelated/name')

    def test_task_access_denied_falls_back_to_hidden_per_user_startup(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            powershell = root / 'System32' / 'WindowsPowerShell' / 'v1.0' / 'powershell.exe'
            powershell.parent.mkdir(parents=True)
            powershell.write_text('', encoding='utf-8')
            fake_python = root / 'python.exe'
            fake_pythonw = root / 'pythonw.exe'
            fake_python.touch()
            fake_pythonw.touch()
            fake_process = mock.Mock(pid=321, returncode=None)
            fake_process.poll.return_value = None
            with mock.patch.dict(os.environ, {'WINDIR': str(root), 'APPDATA': str(root / 'Roaming')}), \
                 mock.patch.object(setup.sys, 'executable', str(fake_python)), \
                 mock.patch.object(setup.subprocess, 'run', return_value=mock.Mock(
                     returncode=1, stdout='', stderr='Task Scheduler HRESULT 0x80070005')) as run, \
                 mock.patch.object(setup.subprocess, 'Popen', return_value=fake_process) as popen, \
                 mock.patch.object(setup.time, 'sleep'):
                result = json.loads(setup.install_user_task(root / 'host-config.json'))

            self.assertEqual(result['mechanism'], 'current-user-startup')
            self.assertEqual(result['pid'], 321)
            startup = Path(result['startup_file'])
            text = startup.read_text(encoding='utf-16')
            self.assertIn('shell.Run command, 0, False', text)
            self.assertIn('pythonw.exe', text)
            self.assertIn('autosync_host_launcher.py', text)
            self.assertEqual(run.call_count, 1)
            self.assertEqual(popen.call_args.kwargs['creationflags'], getattr(setup.subprocess, 'CREATE_NO_WINDOW', 0))


if __name__ == '__main__':
    unittest.main()
