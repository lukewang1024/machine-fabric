import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class ReleaseArchitectureTests(unittest.TestCase):
    def test_intel_mac_entries_request_intel_archive(self):
        for entry in ('install-from-release.sh', 'plan-release-fabric.sh'):
            with self.subTest(entry=entry), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                shims = root / 'shims'
                shims.mkdir()
                uname = shims / 'uname'
                uname.write_text('#!/bin/sh\ncase $1 in -s) printf Darwin ;; -m) printf x86_64 ;; esac\n')
                curl = shims / 'curl'
                curl.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$REQUEST_LOG"\nexit 73\n')
                uname.chmod(0o755)
                curl.chmod(0o755)
                manifest = root / 'manifest.yaml'
                manifest.write_text('{}')
                log = root / 'request.txt'
                env = dict(os.environ, PATH=str(shims) + os.pathsep + os.environ['PATH'],
                           TERMUX_VERSION='', PREFIX='', TMPDIR=str(root), REQUEST_LOG=str(log),
                           MACHINE_FABRIC_RELEASE_BASE_URL='https://example.invalid/fabric')
                result = subprocess.run(['/bin/sh', str(ROOT / 'scripts' / entry),
                                         'v1.2.3', str(manifest)], env=env,
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 73, result.stderr)
                self.assertIn('/releases/v1.2.3/machine-fabric-1.2.3-x86_64-apple-darwin.tar.gz',
                              log.read_text())
