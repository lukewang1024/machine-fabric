import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class LinuxARM64ReleaseTests(unittest.TestCase):
    def test_release_entrypoints_select_native_arm_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / 'fabric.yaml'
            manifest.write_text('fixture')
            (root / 'uname').write_text('#!/bin/sh\ncase $1 in -s) echo Linux;; -m) echo "$TEST_ARCH";; esac\n')
            (root / 'curl').write_text('#!/bin/sh\nprintf "%s\\n" "$@"\nexit 77\n')
            for name in ('uname', 'curl'):
                (root / name).chmod(0o755)
            for arch in ('aarch64', 'arm64'):
                for script, args in [('install-from-release.sh', ['0.1.77']),
                                     ('plan-release-fabric.sh', ['0.1.77', str(manifest)])]:
                    with self.subTest(arch=arch, script=script):
                        result = subprocess.run(['/bin/sh', str(ROOT / 'scripts' / script), *args],
                            env=dict(os.environ, PATH=str(root) + os.pathsep + os.environ['PATH'],
                                     TEST_ARCH=arch, MACHINE_FABRIC_RELEASE_BASE_URL='https://example.invalid/fabric'),
                            capture_output=True, text=True)
                        self.assertEqual(result.returncode, 77, result.stderr)
                        self.assertIn('machine-fabric-0.1.77-aarch64-unknown-linux-musl.tar.gz', result.stdout)
                        self.assertNotIn('x86_64-unknown-linux-musl', result.stdout)
