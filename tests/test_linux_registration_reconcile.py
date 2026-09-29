import os
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/reconcile-remote-posix-registrations.sh"


class RegistrationReconcileTest(unittest.TestCase):
    def test_removes_only_executors_paired_with_unselected_peer_controllers(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            binary = home / ".local/bin/machine-fabric"
            binary.parent.mkdir(parents=True)
            binary.write_text(
                '#!/bin/sh\n'
                'case "$*" in\n'
                '  *controller.list) cat "$TEST_CONTROLLERS" ;;\n'
                '  *executor.list) printf \'{"result":{"executors":[{"id":"doubao-office-adapter-cndevbox-rust"},{"id":"old-peer-rust"}]}}\\n\' ;;\n'
                '  *) printf "%s\\n" "$*" >> "$TEST_CALLS" ;;\n'
                'esac\n'
            )
            binary.chmod(0o755)
            controllers = home / "controllers.json"
            controllers.write_text(
                '{"result":{"controllers":[\n'
                ' {"id":"MacBook-Pro"},\n'
                ' {"id":"cndevbox"},\n'
                ' {"id":"old-peer"}\n'
                ']}}\n'
            )
            calls = home / "calls"
            environment = dict(os.environ, HOME=str(home), TEST_CONTROLLERS=str(controllers), TEST_CALLS=str(calls))
            subprocess.run(
                ["sh", str(SCRIPT), "MacBook-Pro", "cndevbox", "cndevboxgui"],
                check=True, env=environment, capture_output=True, text=True,
            )
            recorded = calls.read_text()
            self.assertIn('"controllerId":"old-peer"', recorded)
            self.assertIn('"executorId":"old-peer-rust"', recorded)
            self.assertIn('"executorId":"old-peer-native"', recorded)
            self.assertNotIn("doubao-office-adapter", recorded)
            self.assertNotIn('"executorId":"cndevbox-rust"', recorded)
            self.assertNotIn('"controllerId":"MacBook-Pro"', recorded)


if __name__ == "__main__":
    unittest.main()
