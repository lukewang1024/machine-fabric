import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class LinuxInstallRootsTests(unittest.TestCase):
    def run_install(self, home: Path, *roots: str) -> subprocess.CompletedProcess[str]:
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
            cwd=ROOT, env=env, capture_output=True, text=True, timeout=20,
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
            result = self.run_install(home, str(home / 'missing'))
            self.assertEqual(result.returncode, 2)
            self.assertIn('allow-root does not exist:', result.stderr)
            self.assertFalse((home / '.config/systemd/user/machine-fabric-executor.service').exists())


if __name__ == '__main__':
    unittest.main()
