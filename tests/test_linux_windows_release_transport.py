import hashlib
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class WindowsReleaseTransportTests(unittest.TestCase):
    def fixture(self, root, target='x86_64-pc-windows-msvc'):
        source, cache, tools = (root / name for name in ('source', 'cache', 'tools'))
        for path in (source, cache, tools):
            path.mkdir()
        name = 'machine-fabric-1.2.3-' + target + '.zip'
        (source / name).write_bytes(b'fixture authenticated archive')
        digest = hashlib.sha256((source / name).read_bytes()).hexdigest()
        (source / 'SHA256SUMS').write_text(digest + '  ' + name + '\n')
        curl = tools / 'curl'
        curl.write_text('#!/bin/sh\nset -eu\nwhile [ "$#" -gt 0 ]; do\n'
                        'case $1 in https://*) name=${1##*/};; -o) shift; output=$1;; esac\n'
                        'shift\ndone\ncp "$FIXTURE_SOURCE/$name" "$output"\n'
                        'printf "%s\\n" "$name" >>"$FIXTURE_REQUESTS"\n')
        curl.chmod(0o700)
        env = dict(os.environ, PATH=str(tools) + os.pathsep + os.environ['PATH'],
                   FIXTURE_SOURCE=str(source), FIXTURE_REQUESTS=str(root / 'requests'),
                   MACHINE_FABRIC_RELEASE_BASE_URL='https://example.invalid/fabric')
        argv = ['sh', str(ROOT / 'scripts/prepare-windows-release.sh'), '1.2.3', target, str(cache)]
        return source, cache, name, digest, env, argv

    def test_both_windows_architectures_download_once_and_verify_cache_again(self):
        for target in ('x86_64-pc-windows-msvc', 'aarch64-pc-windows-msvc'):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                _, cache, name, digest, env, argv = self.fixture(root, target)
                for _ in range(2):
                    result = subprocess.run(argv, env=env, capture_output=True, check=True)
                    self.assertEqual(result.stdout.decode().strip(), digest)
                self.assertEqual((root / 'requests').read_text().splitlines(), ['SHA256SUMS', name])
                (cache / name).write_bytes(b'tampered')
                self.assertNotEqual(subprocess.run(argv, env=env, capture_output=True).returncode, 0)
                self.assertEqual(len((root / 'requests').read_text().splitlines()), 2)

    def test_duplicate_missing_checksum_and_redirected_archive_are_rejected(self):
        for mutation in ('duplicate', 'missing', 'redirected'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                source, cache, name, _, env, argv = self.fixture(root)
                sums = source / 'SHA256SUMS'
                if mutation == 'duplicate':
                    sums.write_text(sums.read_text() * 2)
                elif mutation == 'missing':
                    sums.write_text('0' * 64 + '  unrelated.zip\n')
                else:
                    (cache / name).symlink_to(source / name)
                self.assertNotEqual(subprocess.run(argv, env=env, capture_output=True).returncode, 0)

    def test_actual_release_archive_contains_windows_transport_helper(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            build, dist = root / 'build', root / 'dist'
            build.mkdir()
            (build / 'machine-fabric').write_bytes(b'package fixture')
            subprocess.run(['sh', 'scripts/package-release.sh', '1.2.3',
                            'x86_64-unknown-linux-musl', str(build), str(dist)], cwd=ROOT, check=True)
            with tarfile.open(next(dist.glob('*.tar.gz'))) as archive:
                prefix = 'machine-fabric-1.2.3-x86_64-unknown-linux-musl/scripts/'
                self.assertEqual(archive.extractfile(prefix + 'prepare-windows-release.sh').read(),
                                 (ROOT / 'scripts/prepare-windows-release.sh').read_bytes())


if __name__ == '__main__':
    unittest.main()
