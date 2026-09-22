"""Execute the real staging/transaction/stop/readiness logic with fake OS edges."""
from pathlib import Path
import importlib.util
import json
import os
import plistlib
import subprocess
import tempfile
import socket
import threading
import tarfile
import shutil
import time
import hashlib
import sys
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('installer', ROOT / 'scripts/install-macos-transaction.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class Fake(m.Installer):
    def __init__(self, root):
        root = Path(root)
        source = root / 'candidate'
        source.write_text('new'); source.chmod(0o755)
        super().__init__(source, root / 'home', root / 'data', root / 'state', root / 'bin')
        self.node = 'test'
        self.timeout = .03
        self.jobs = dict(zip(m.LABELS, (800001, 800002)))
        self.live = set(self.jobs.values())
        self.next_pid = 800003
        self.calls = []
        self.fail_boot = None
        self.fail_ready = False
        self.stubborn = False
        self.delay = 0
        self.new_state = False
        self.state.mkdir(parents=True)
        self.agent.parent.mkdir(parents=True)
        self.agent.write_text('old'); self.agent.chmod(0o755)
        self.controller.parent.mkdir(parents=True)
        self.controller.write_text('old'); self.controller.chmod(0o755)
        for p, label in zip(self.plists, m.LABELS):
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(plistlib.dumps({'Label': label, 'old': True}))
        self.bins.mkdir()
        for p in self.links: p.symlink_to(self.agent)
        (self.state / 'controller.json').write_text('{"tasks":[],"leases":[],"epoch":1}')
        (self.state / 'executor-fences.json').write_text('{"fence":9}')
        for name in ('controller.json.payloads', 'task-payloads'):
            (self.state / name).mkdir()
            (self.state / name / 'payload.json').write_text('{"body":"original"}')

    def run(self, argv, check=True):
        argv = list(map(str, argv)); self.calls.append(argv)
        status = 0; output = ''
        if argv[0] == '/bin/launchctl':
            action = argv[1]
            if action in ('print', 'bootout'):
                label = argv[2].split('/', 2)[-1]
                pid = self.jobs.get(label)
                if action == 'print':
                    status = 0 if pid else 113
                    output = 'pid = %s' % pid if pid else ''
                elif pid:
                    del self.jobs[label]
                    if not self.stubborn: self.live.discard(pid)
                else: status = 113
            elif action == 'bootstrap':
                label = plistlib.loads(Path(argv[3]).read_bytes())['Label']
                if self.fail_boot == label and self.controller.read_text() == 'new':
                    status = 5
                else:
                    if label in self.jobs: raise AssertionError('duplicate bootstrap')
                    self.jobs[label] = self.next_pid; self.live.add(self.next_pid); self.next_pid += 1
            else: raise AssertionError('unexpected launchctl action ' + action)
        elif argv[0] == '/bin/ps':
            for label, executable in zip(m.LABELS, (self.controller, self.agent)):
                pid = self.jobs.get(label)
                if pid: output += '%s %s\n' % (pid, executable)
        elif argv[-1] == '--version': output = 'machine-fabric 0.1.28\n'
        elif argv[0] == '/usr/bin/dwarfdump':
            output = 'UUID: ' + ('AAAAAAAA' if Path(argv[-1]).read_text() == 'new' else 'BBBBBBBB')
        elif argv[0] == '/usr/bin/plutil': plistlib.loads(Path(argv[-1]).read_bytes())
        elif argv[0] == '/usr/bin/codesign': pass
        else: raise AssertionError(argv)
        result = subprocess.CompletedProcess(argv, status, output, '')
        if check and status: raise subprocess.CalledProcessError(status, argv)
        return result

    def alive(self, pid): return pid in self.live
    def terminate(self, pid):
        if not self.stubborn: self.live.discard(pid)

    def rpc(self, endpoint, action, params=None, max_bytes=2 * 1024 * 1024):
        if endpoint not in self.sockets:
            return m.Installer.rpc(self, endpoint, action, params, max_bytes=max_bytes)
        if action == 'executor.register': return {'ok': True}
        if self.controller.read_text() == 'new':
            if self.new_state:
                (self.state / 'controller.json').write_text('{"tasks":[],"leases":[],"epoch":2}')
                (self.state / 'executor-fences.json').write_text('{"fence":10}')
            if self.fail_ready: raise OSError('stale socket refused')
            if self.delay:
                self.delay -= 1
                raise OSError('not listening yet')
        if endpoint == self.sockets[0]: return {'controller': {'id': self.node, 'status': 'ready'}}
        return {'executorId': self.node + '-rust', 'status': 'ready', 'execution': {'active': 0, 'queued': 0}}


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.x = Fake(self.tmp.name)

    def test_success_single_bootstrap_and_private_complete_backup(self):
        self.x.install()
        boot = [a for a in self.x.calls if a[:2] == ['/bin/launchctl', 'bootstrap']]
        self.assertEqual(len(boot), 2)
        self.assertFalse(any('kickstart' in a for a in self.x.calls))
        self.assertEqual(self.x.controller.read_text(), 'new')
        for name in ('controller.json.payloads', 'task-payloads'):
            p = self.x.backup / 'state' / name / 'payload.json'
            self.assertEqual(p.read_text(), '{"body":"original"}')
            self.assertEqual(p.stat().st_mode & 0o777, 0o600)
        self.assertEqual((self.x.backup / 'manifest.json').stat().st_mode & 0o777, 0o600)

    def test_bootstrap_failure_restores_binary_plists_links(self):
        self.x.fail_boot = m.LABELS[1]
        with self.assertRaisesRegex(RuntimeError, 'previous service restored'):
            self.x.install()
        self.assertEqual(self.x.controller.read_text(), 'old')
        self.assertEqual(self.x.agent.read_text(), 'old')
        self.assertEqual(len(self.x.jobs), 2)
        self.assertTrue(os.access(self.x.controller, os.X_OK))
        self.assertEqual(os.readlink(self.x.links[0]), str(self.x.agent))
        self.assertTrue(plistlib.loads(self.x.plists[0].read_bytes())['old'])

    def test_readiness_failure_preserves_newer_state_fences_and_payloads(self):
        self.x.fail_ready = True; self.x.new_state = True
        with self.assertRaisesRegex(RuntimeError, 'previous service restored'):
            self.x.install()
        self.assertEqual(json.loads((self.x.state / 'controller.json').read_text())['epoch'], 2)
        self.assertEqual(json.loads((self.x.state / 'executor-fences.json').read_text())['fence'], 10)
        self.assertEqual(json.loads((self.x.backup / 'state/controller.json').read_text())['epoch'], 1)
        self.assertEqual(len(self.x.jobs), 2)

    def test_graceful_stop_timeout_never_replaces_binary(self):
        self.x.stubborn = True
        with self.assertRaisesRegex(RuntimeError, 'graceful stop deadline'):
            self.x.install()
        self.assertEqual(self.x.controller.read_text(), 'old')
        self.assertEqual(self.x.agent.read_text(), 'old')
        self.assertIsNone(self.x.backup)
        self.assertFalse(any(a[:2] == ['/bin/launchctl', 'bootstrap'] for a in self.x.calls))

    def test_delayed_socket_requires_successful_rpc(self):
        self.x.timeout = .5; self.x.delay = 2
        self.x.sockets[0].touch()  # Stale pathname is not successful readiness.
        self.x.install()
        self.assertEqual(self.x.delay, 0)

    def test_repeat_install_keeps_identity_and_one_boot_per_job(self):
        self.x.install(); old = m.digest(self.x.agent)
        first_signs = len([a for a in self.x.calls if '--sign' in a])
        self.x.calls.clear(); self.x.install()
        self.assertEqual(m.digest(self.x.agent), old)
        self.assertEqual(len([a for a in self.x.calls if '--sign' in a]), 0)
        self.assertEqual(first_signs, 1)
        self.assertEqual(len([a for a in self.x.calls if a[:2] == ['/bin/launchctl', 'bootstrap']]), 2)

    def test_stage_failure_stops_no_jobs(self):
        self.x.source.unlink()
        with self.assertRaisesRegex(RuntimeError, 'candidate executable missing'): self.x.install()
        self.assertFalse(any('bootout' in a for a in self.x.calls))
        self.assertEqual(len(self.x.jobs), 2)

    def test_active_queue_refuses_before_stop(self):
        (self.x.state / 'desktop-queue.json').write_text('{"inFlight":true,"jobs":[]}')
        with self.assertRaisesRegex(RuntimeError, 'not quiescent'): self.x.install()
        self.assertFalse(any('bootout' in a for a in self.x.calls))

    def test_lock_serializes_install(self):
        path = self.x.state / '.install-macos.lock'
        with path.open('a') as f:
            m.fcntl.flock(f, m.fcntl.LOCK_EX | m.fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError): self.x.install()
        self.assertFalse(self.x.calls)

    def test_identity_mismatch_rejected(self):
        original = self.x.rpc
        def wrong(endpoint, action, params=None):
            value = original(endpoint, action, params)
            if endpoint == self.x.sockets[0]: value['controller']['id'] = 'other'
            return value
        self.x.rpc = wrong
        with self.assertRaisesRegex(RuntimeError, 'identity mismatch'): self.x.health()

    def test_serde_terminal_history_and_expired_lease_do_not_block(self):
        states = ['succeeded', 'failed', 'cancelled', 'timed-out', 'outcome-unknown']
        value = {'tasks': [{'id': 'task-' + str(i), 'state': state} for i, state in enumerate(states)],
                 'leases': [{'id': 'lease-old', 'kind': 'driver', 'resource': 'workspace:w',
                             'owner': 'a', 'token': 'test', 'fence': 9, 'acquiredAt': 1,
                             'updatedAt': 1, 'expiresAt': 1, 'handoffTo': None}]}
        index = self.x.state / 'controller.json'
        index.write_text(json.dumps(value)); before = index.read_bytes()
        self.x.install()
        self.assertEqual(index.read_bytes(), before)
        self.assertEqual((self.x.state / 'executor-fences.json').read_text(), '{"fence":9}')

    def test_active_and_unknown_task_schema_refuse_without_stopping(self):
        for state in ['queued', 'running', 'timedOut', 'outcomeUnknown', 'future-state', None]:
            with self.subTest(state=state):
                (self.x.state / 'controller.json').write_text(json.dumps({'tasks': [{'state': state}], 'leases': []}))
                with self.assertRaisesRegex(RuntimeError, 'unfinished tasks'): self.x.install()
        self.assertFalse(any('bootout' in a for a in self.x.calls))

    def test_active_or_invalid_lease_blocks_without_changing_fence(self):
        for expiry in [time.time_ns() // 1_000_000 + 60000, None, '1', True, -1]:
            with self.subTest(expiry=expiry):
                (self.x.state / 'controller.json').write_text(json.dumps({'tasks': [], 'leases': [{'expiresAt': expiry}]}))
                with self.assertRaisesRegex(RuntimeError, 'lease'): self.x.install()
        self.assertFalse(any('bootout' in a for a in self.x.calls))
        self.assertEqual((self.x.state / 'executor-fences.json').read_text(), '{"fence":9}')

    def test_historical_unknown_does_not_clear_live_desktop_block(self):
        (self.x.state / 'controller.json').write_text('{"tasks":[{"state":"outcome-unknown"}],"leases":[]}')
        queue = self.x.state / 'desktop-queue.json'
        queue.write_text('{"blocked":true,"inFlight":false,"jobs":[]}')
        before = queue.read_bytes()
        with self.assertRaisesRegex(RuntimeError, 'not quiescent'): self.x.install()
        self.assertEqual(queue.read_bytes(), before)
        self.assertFalse(any('bootout' in a for a in self.x.calls))

    def test_real_release_archive_contains_independent_wrapper_and_python(self):
        root = Path(self.tmp.name)
        build = root / 'build'; build.mkdir()
        for name in ['machine-fabric', 'machine-fabric-macos-agent']:
            (build / name).write_text('test binary')
        output = root / 'dist'
        subprocess.run(['/bin/sh', str(ROOT / 'scripts/package-release.sh'), '0.1.28',
                        'aarch64-apple-darwin', str(build), str(output)], cwd=ROOT, check=True)
        unpacked = root / 'unpacked'; unpacked.mkdir()
        with tarfile.open(output / 'machine-fabric-0.1.28-aarch64-apple-darwin.tar.gz') as archive:
            archive.extractall(unpacked)
        scripts = unpacked / 'machine-fabric-0.1.28-aarch64-apple-darwin/scripts'
        helper = scripts / 'install-macos-transaction.py'
        self.assertEqual(helper.read_bytes(), (ROOT / 'scripts/install-macos-transaction.py').read_bytes())
        # argparse exits before lifecycle calls. CWD is not the source repository.
        result = subprocess.run(['/bin/sh', str(scripts / 'install-macos-app.sh'), '--help'],
                                cwd=unpacked, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('source', result.stdout)
        empty_path = root / 'minimal-bin'; empty_path.mkdir()
        (empty_path / 'dirname').symlink_to(shutil.which('dirname'))
        result = subprocess.run(['/bin/sh', str(scripts / 'install-macos-app.sh'), '--help'],
                                cwd=unpacked, env={'PATH': str(empty_path)}, capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn('Python 3 is required before', result.stderr)
        self.assertEqual(len(self.x.jobs), 2)

    def test_rollback_status_ready_but_portable_payload_rejected_never_admits_old_service(self):
        # Executed private old-binary fixture: status is ready but lazy task.get fails.
        stub = "#!" + sys.executable + "\n" + """
import json,socket,sys
endpoint=sys.argv[sys.argv.index('--socket')+1]
server=socket.socket(socket.AF_UNIX);server.bind(endpoint);server.listen(4)
while True:
 c,_=server.accept()
 with c:
  q=json.loads(c.makefile('rb').readline());print(q['action'],flush=True)
  if q['action']=='status': v={'ok':True,'result':{'controller':{'id':'test','status':'ready'}}}
  else: v={'ok':False,'error':{'code':'TASK_PAYLOAD_UNAVAILABLE','message':'old locator contract'}}
  c.sendall(json.dumps(v).encode()+b'\\n')
"""
        self.x.controller.write_text(stub); self.x.controller.chmod(0o755)
        self.x.timeout = .5
        # Candidate writes a valid new ref, then fails its live readiness.
        original = self.x.rpc
        raw = b'{"new":"payload"}'
        sha = hashlib.sha256(raw).hexdigest()
        locator = 'task-payloads/' + sha + '.json'
        def rpc(endpoint, action, params=None, **kwargs):
            if endpoint in self.x.sockets and self.x.controller.read_text() == 'new':
                (self.x.state / locator).write_bytes(raw)
                (self.x.state / 'controller.json').write_text(json.dumps({'tasks': [
                    {'id':'new-task','state':'succeeded','output':{'$taskPayloadRef':'sha256:'+sha},
                     'outputRef':{'digest':'sha256:'+sha,'bytes':len(raw),'locator':locator}}], 'leases':[]}))
                raise OSError('candidate readiness failure')
            return original(endpoint, action, params, **kwargs)
        self.x.rpc = rpc
        with self.assertRaisesRegex(RuntimeError, 'recovery failed against CURRENT state'):
            self.x.install()
        self.assertEqual(self.x.jobs, {})
        self.assertTrue((self.x.backup / 'current-state-probe/controller.json').exists())
        self.assertEqual((self.x.state / locator).read_bytes(), raw)
        self.assertFalse((self.x.backup / 'payload-probe.json').exists())
        self.assertIn('task.get', (self.x.backup / 'payload-probe.log').read_text())

    def test_real_rpc_rejects_stale_file_and_reads_actual_response(self):
        endpoint = self.x.state / 'test.sock'
        endpoint.touch()
        with self.assertRaises(OSError): m.Installer.rpc(self.x, endpoint, 'status')
        endpoint.unlink()
        ready = threading.Event()
        def serve():
            with socket.socket(socket.AF_UNIX) as server:
                server.bind(str(endpoint)); server.listen(1); ready.set()
                connection, _ = server.accept()
                with connection:
                    request = json.loads(connection.makefile('rb').readline())
                    self.assertEqual(request['action'], 'status')
                    connection.sendall(b'{"ok":true,"result":{"controller":{"id":"test","status":"ready"}}}\n')
        thread = threading.Thread(target=serve)
        thread.start(); self.assertTrue(ready.wait(1))
        result = m.Installer.rpc(self.x, endpoint, 'status')
        thread.join(1)
        self.assertEqual(result['controller']['id'], 'test')

    def test_old_binary_incompatible_retains_current_state_and_reports(self):
        original = self.x.rpc
        self.x.fail_ready = True; self.x.new_state = True
        def rpc(endpoint, action, params=None):
            if self.x.controller.read_text() == 'old' and self.x.backup:
                raise RuntimeError('old binary cannot read current schema')
            return original(endpoint, action, params)
        self.x.rpc = rpc
        with self.assertRaisesRegex(RuntimeError, 'recovery failed against CURRENT state'):
            self.x.install()
        self.assertEqual(json.loads((self.x.state / 'controller.json').read_text())['epoch'], 2)
        self.assertTrue((self.x.backup / 'manifest.json').is_file())


if __name__ == '__main__': unittest.main()
