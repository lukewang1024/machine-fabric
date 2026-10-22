"""Real isolated Controller/Executor regression; no desktop or helper input."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest


@unittest.skipIf(os.name == 'nt', 'Unix process fixture; Rust state tests cover all platforms')
class DesktopAvailabilityTests(unittest.TestCase):
    def test_worker_fifo_revoke_and_consume_once(self):
        root = Path(__file__).resolve().parents[1]
        binary = Path(os.environ.get('MACHINE_FABRIC_TEST_BINARY', root / 'target/debug/machine-fabric'))
        self.assertTrue(binary.is_file(), 'build the CLI before the managed runtime regression')
        with tempfile.TemporaryDirectory(prefix='mf-away-') as temporary:
            state = Path(temporary)
            controller, executor = state / 'c.sock', state / 'e.sock'
            processes, logs = [], []
            stop = threading.Event()
            provider = None
            def start(argv, label):
                log = open(state / (label + '.log'), 'w')
                logs.append(log)
                process = subprocess.Popen(argv, stdout=log, stderr=subprocess.STDOUT)
                processes.append(process)
                return process
            def call(action, params, expected_error=None):
                result = subprocess.run([str(binary), '--socket', str(controller), 'call', action, json.dumps(params)], capture_output=True, text=True, timeout=10)
                if expected_error:
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(expected_error, result.stderr)
                    return None
                self.assertEqual(result.returncode, 0, result.stderr[-1000:])
                return json.loads(result.stdout)
            def await_value(check, timeout=15):
                deadline = time.monotonic() + timeout
                while time.monotonic() < deadline:
                    value = check()
                    if value:
                        return value
                    time.sleep(.05)
                self.fail('timed out waiting for fixture condition')
            available = threading.Event()
            def publish():
                while not stop.is_set():
                    now = int(time.time() * 1000)
                    source = state / 'availability.tmp'
                    source.write_text(json.dumps({'schemaVersion': 1, 'provider': 'away-guard', 'state': 'armed', 'generation': 'fixture', 'sequence': 1, 'available': available.is_set(), 'observedAtMs': now, 'expiresAtMs': now + 10000}))
                    source.replace(state / 'desktop-availability.json')
                    stop.wait(.1)
            try:
                start([str(binary), '--socket', str(controller), 'controller', 'serve', '--id', 'fixture-controller', '--state', str(state / 'controller.json')], 'controller')
                start([str(binary), '--socket', str(executor), 'executor', 'serve', '--id', 'fixture-desktop', '--allow-root', temporary, '--state', str(state / 'fences.json')], 'executor')
                await_value(lambda: controller.exists() and executor.exists())
                self.assertTrue(call('executor.register', {'executorId': 'fixture-desktop', 'endpoint': {'transport': 'local', 'socket': str(executor)}})['ok'])
                provider = threading.Thread(target=publish)
                provider.start()
                fixture = state / 'consumer.py'
                fixture.write_text('''import json, os, pathlib, time
p=pathlib.Path(__file__).parent
name=os.environ['MACHINE_FABRIC_DESKTOP_OWNER']
with (p/'events').open('a') as f:f.write('start '+name+'\\n')
(p/(name+'.started')).touch()
if name=='one':
 while not (p/'release-one').exists():time.sleep(.05)
with (p/'events').open('a') as f:f.write('end '+name+'\\n')
''')
                deadline = str(int(time.time() * 1000) + 60000)
                def runner(name):
                    return [str(binary), '--socket', str(controller), 'computer-use', 'run', '--executor', 'fixture-desktop', '--owner', name, '--request-key', name, '--receipt', str(state / (name + '.json')), '--deadline-at', deadline, '--', sys.executable, str(fixture)]
                first = start(runner('one'), 'one-runner')
                await_value(lambda: (state / 'one.json').exists() and json.loads((state / 'one.json').read_text()).get('state') == 'queued')
                second = start(runner('two'), 'two-runner')
                await_value(lambda: (state / 'two.json').exists() and json.loads((state / 'two.json').read_text()).get('state') == 'queued')
                self.assertFalse((state / 'events').exists())
                if sys.platform == 'darwin':
                    subprocess.run(['/usr/bin/notifyutil', '-p', 'dev.machine-fabric.desktop-availability-changed'], check=True, timeout=5)
                    await_value(lambda: call('desktop.list', {'executorId': 'fixture-desktop', 'includeTerminal': False})['result']['worker']['notificationWakes'] > 0)
                    self.assertFalse((state / 'events').exists())  # Event is not permission.
                available.set()
                await_value(lambda: (state / 'one.started').exists())
                self.assertFalse((state / 'two.started').exists())
                available.clear()
                await_value(lambda: not json.loads((state / 'desktop-availability.json').read_text())['available'])
                job = json.loads((state / 'one.json').read_text())['job']
                call('executor.call', {'executorId': 'fixture-desktop', 'action': 'application.__availability_probe__', 'params': {'_desktop': {'owner': 'one', 'token': job['token']}}}, expected_error='DESKTOP_UNAVAILABLE')
                self.assertFalse((state / 'two.started').exists())
                available.set()
                (state / 'release-one').touch()
                self.assertEqual(first.wait(timeout=15), 0)
                self.assertEqual(second.wait(timeout=15), 0)
                self.assertEqual((state / 'events').read_text().splitlines(), ['start one', 'end one', 'start two', 'end two'])
                replay = subprocess.run(runner('one'), capture_output=True, text=True, timeout=10)
                self.assertEqual(replay.returncode, 0, replay.stderr)
                self.assertTrue(json.loads(replay.stdout)['alreadyConsumed'])
                self.assertEqual((state / 'events').read_text().count('start one'), 1)
                listing = call('desktop.list', {'executorId': 'fixture-desktop', 'includeTerminal': False})['result']
                self.assertFalse(listing['blocked'])
                self.assertFalse(listing['inFlight'])
                self.assertEqual(listing['jobs'], [])
                self.assertGreater(listing['worker']['pollingWakes'], 0)
                # An explicit node-local consent receipt admits only its exact
                # job while the real Away Guard provider remains unavailable.
                available.clear()
                await_value(lambda: not json.loads((state / 'desktop-availability.json').read_text())['available'])
                def submit(name):
                    return call('desktop.submit', {'executorId': 'fixture-desktop',
                        'owner': name, 'requestKey': name, 'requiresAvailability': False})['result']
                waiting, manual = submit('still-unattended'), submit('operator-authorized')
                self.assertEqual(waiting['state'], 'queued')
                self.assertEqual(manual['state'], 'queued')
                grant_command = [sys.executable, str(root / 'scripts/authorize-desktop-now.py'),
                    '--state-dir', str(state), '--job-id', manual['id'], '--owner', manual['owner'],
                    '--request-key', manual['requestKey'], '--seconds', '60',
                    '--reason', 'isolated operator-authorization regression']
                denied = subprocess.run(grant_command, capture_output=True, text=True, timeout=10)
                self.assertNotEqual(denied.returncode, 0)
                self.assertFalse((state / 'desktop-user-authorization.json').exists())
                granted = subprocess.run(grant_command + ['--confirm-user-authorized'],
                    capture_output=True, text=True, timeout=10)
                self.assertEqual(granted.returncode, 0, granted.stderr)
                authorization = json.loads(granted.stdout)
                credentials = {'executorId': 'fixture-desktop', 'owner': manual['owner'], 'token': manual['token']}
                live = await_value(lambda: (v if (v := call('desktop.get', credentials)['result'])['state'] == 'active' else None))
                self.assertTrue(live['inputAllowed'])
                self.assertTrue(live['requiresAvailability'])
                self.assertEqual(live['userAuthorization']['authorizationId'], authorization['authorizationId'])
                call('desktop.claim', dict(credentials, consumerId='one-manual-consumer'))
                listing = call('desktop.list', {'executorId': 'fixture-desktop', 'includeTerminal': False})['result']
                self.assertFalse(listing['availability']['available'])
                self.assertEqual(next(j for j in listing['jobs'] if j['id'] == waiting['id'])['state'], 'queued')
                revoked = subprocess.run([sys.executable, str(root / 'scripts/authorize-desktop-now.py'),
                    '--state-dir', str(state), '--revoke', authorization['authorizationId']], capture_output=True, text=True, timeout=10)
                self.assertEqual(revoked.returncode, 0, revoked.stderr)
                self.assertFalse(call('desktop.get', credentials)['result']['inputAllowed'])
                call('executor.call', {'executorId': 'fixture-desktop', 'action': 'application.__availability_probe__',
                    'params': {'_desktop': {'owner': manual['owner'], 'token': manual['token']}}}, expected_error='DESKTOP_UNAVAILABLE')
                call('desktop.cancel', credentials)
                call('desktop.cancel', {'executorId': 'fixture-desktop', 'owner': waiting['owner'], 'token': waiting['token']})
                await_value(lambda: not call('desktop.list', {'executorId': 'fixture-desktop', 'includeTerminal': False})['result']['jobs'])
            finally:
                (state / 'release-one').touch()
                # Release the owned child even when an assertion interrupts its runner.
                for name in ('one.json', 'two.json'):
                    receipt = state / name
                    if receipt.exists():
                        try:
                            pid = json.loads(receipt.read_text()).get('pid')
                            if pid:
                                os.kill(pid, 15)
                        except (OSError, ValueError):
                            pass
                stop.set()
                if provider:
                    provider.join(timeout=5)
                for process in reversed(processes):
                    if process.poll() is None:
                        process.terminate()
                for process in processes:
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
                for log in logs:
                    log.close()


if __name__ == '__main__':
    unittest.main()
