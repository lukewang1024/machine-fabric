import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


class LinuxInstallRootsTests(unittest.TestCase):
    def run_install(self, home: Path, *roots: str, cwd=None) -> subprocess.CompletedProcess[str]:
        shims = home / 'shims'
        shims.mkdir(exist_ok=True)
        systemctl = shims / 'systemctl'
        systemctl.write_text('#!/bin/sh\nexit 0\n')
        systemctl.chmod(0o755)
        binary = home / 'machine-fabric-test'
        binary.write_text('''#!/bin/sh
case $* in
  *--version*) printf 'machine-fabric 0.1.33\\n' ;;
  *"executor.sock status"*) test -d "$HOME/Code" && test -d "$HOME/Workspace" ;;
  *) exit 0 ;;
esac
''')
        binary.chmod(0o755)
        env = dict(os.environ, HOME=str(home),
                   XDG_CONFIG_HOME=str(home / '.config'),
                   XDG_STATE_HOME=str(home / '.local/state'),
                   PATH=str(shims) + os.pathsep + os.environ['PATH'])
        return subprocess.run(
            ['/bin/sh', str(ROOT / 'scripts/install-linux-user.sh'), str(binary), 'demo', *roots],
            cwd=cwd or ROOT, env=env, capture_output=True, text=True, timeout=20,
        )

    def test_default_roots_are_ready_before_executor_starts(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            result = self.run_install(home)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue((home / 'Code').is_dir())
            self.assertTrue((home / 'Workspace').is_dir())

    def test_missing_explicit_root_fails_before_service_start(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            installed = home / '.local/bin/machine-fabric'
            installed.parent.mkdir(parents=True)
            installed.write_bytes(b'original installed executable')
            unit = home / '.config/systemd/user/machine-fabric-executor.service'
            unit.parent.mkdir(parents=True)
            unit.write_bytes(b'original unit')
            result = self.run_install(home, str(home / 'missing'))
            self.assertEqual(result.returncode, 2)
            self.assertIn('allow-root does not exist:', result.stderr)
            self.assertEqual(installed.read_bytes(), b'original installed executable')
            self.assertEqual(unit.read_bytes(), b'original unit')
            self.assertFalse((home / '.local/state/machine-fabric/backups').exists())

    def test_templates_resolve_from_script_when_cwd_is_unrelated(self):
        with tempfile.TemporaryDirectory() as temporary:
            result = self.run_install(Path(temporary), cwd=temporary)
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_template_preserves_installed_binary(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            package = home / 'incomplete-package'
            (package / 'scripts').mkdir(parents=True)
            (package / 'scripts/install-linux-user.sh').write_bytes(
                (ROOT / 'scripts/install-linux-user.sh').read_bytes())
            installed = home / '.local/bin/machine-fabric'
            installed.parent.mkdir(parents=True)
            installed.write_bytes(b'original binary')
            with patch.dict(self.run_install.__globals__, ROOT=package):
                result = self.run_install(home)
            self.assertEqual(result.returncode, 2)
            self.assertIn('template unavailable', result.stderr)
            self.assertEqual(installed.read_bytes(), b'original binary')
            self.assertFalse((home / '.config/systemd/user').exists())


if __name__ == '__main__':
    unittest.main()
