from pathlib import Path
import plistlib
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[1]


class MacRootsTests(unittest.TestCase):
    def render(self, value):
        return subprocess.run(
            ['/bin/sh', str(ROOT / 'scripts/render-allow-roots.sh')],
            input=value, text=True, capture_output=True,
            env={'PATH': '/usr/bin:/bin'},
        )

    def test_paths_survive_launchd_xml(self):
        roots = ['/Applications/Test & Review.app', '/tmp/a<b>', '/tmp/space root']
        result = self.render('\n'.join(roots))
        self.assertEqual(result.returncode, 0, result.stderr)
        template = (ROOT / 'packaging/dev.machine-fabric.macos-agent.plist.in').read_text()
        document = plistlib.loads(template.replace('    @ALLOW_ROOTS@', result.stdout).encode())
        argv = document['ProgramArguments']
        self.assertEqual([argv[i + 1] for i, arg in enumerate(argv) if arg == '--allow-root'], roots)

    def test_empty_or_relative_roots_are_rejected(self):
        for roots in ['', '\n', 'relative/path\n', '/valid\nrelative\n']:
            self.assertNotEqual(self.render(roots).returncode, 0)


if __name__ == '__main__':
    unittest.main()
