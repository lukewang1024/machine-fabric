#!/usr/bin/env python3
"""Stage a signed Fabric app and replace quiescent launchd jobs transactionally.

Rollback restores executable/configuration identities only. Durable task data and
fences always remain CURRENT, including writes made during failed readiness.
"""
from __future__ import annotations
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import plistlib
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import uuid

LABELS = ('dev.machine-fabric.controller', 'dev.machine-fabric.macos-agent')


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


class Installer:
    def __init__(self, source, home=None, data=None, state=None, bins=None):
        self.home = Path(home or Path.home())
        self.data = Path(data or os.environ.get('XDG_DATA_HOME', self.home / '.local/share'))
        self.state = Path(state or os.environ.get('XDG_STATE_HOME', self.home / '.local/state')) / 'machine-fabric'
        self.bins = Path(bins or os.environ.get('XDG_BIN_HOME', self.home / '.local/bin'))
        self.source = Path(source).resolve()
        self.app = self.data / 'machine-fabric/Machine Fabric.app'
        self.agent = self.app / 'Contents/MacOS/machine-fabric-macos-agent'
        self.controller = self.data / 'machine-fabric/libexec/fabric-controller'
        self.plists = [self.home / 'Library/LaunchAgents' / (label + '.plist') for label in LABELS]
        self.links = [self.bins / name for name in ('machine-fabric', 'machine-fabric-macos-agent')]
        self.sockets = [self.state / name for name in ('controller.sock', 'executor.sock')]
        self.node = os.environ.get('MACHINE_FABRIC_NODE_ID') or socket.gethostname().split('.')[0]
        self.domain = 'gui/' + str(os.getuid())
        self.timeout = 20
        self.backup = None
        self.stopping_pids = {}

    def run(self, argv, check=True):
        return subprocess.run([str(x) for x in argv], check=check, capture_output=True, text=True, timeout=30)

    def launch(self, *argv, check=True):
        return self.run(['/bin/launchctl', *argv], check=check)

    def job(self, label):
        value = self.launch('print', self.domain + '/' + label, check=False)
        if value.returncode:
            return None
        pid = re.search(r'\bpid = (\d+)', value.stdout)
        return int(pid.group(1)) if pid else 0

    def alive(self, pid):
        if not pid:
            return False
        try:
            os.kill(pid, 0)
            return True
        except ProcessLookupError:
            return False

    def processes(self, executable):
        # Only this exact executable, never a bundle/title/substring match.
        value = self.run(['/bin/ps', '-axo', 'pid=,comm=']).stdout
        result = []
        for line in value.splitlines():
            parts = line.strip().split(None, 1)
            if len(parts) == 2 and parts[1] == str(executable):
                result.append(int(parts[0]))
        return result

    def terminate(self, pid):
        os.kill(pid, signal.SIGTERM)

    def stop(self, label, executable):
        pid = self.job(label)
        pids = set(self.processes(executable))
        if pid:
            pids.add(pid)
        self.stopping_pids[label] = pids
        if pid is not None:
            self.launch('bootout', self.domain + '/' + label)
        # launchd may leave a legacy LaunchServices child. Graceful TERM only.
        for orphan in self.processes(executable):
            if self.alive(orphan):
                self.terminate(orphan)
                pids.add(orphan)
        end = time.monotonic() + self.timeout
        while any(self.alive(p) for p in pids) or self.job(label) is not None:
            if time.monotonic() >= end:
                raise RuntimeError('graceful stop deadline: ' + label)
            time.sleep(.1)

    def rpc(self, endpoint, action, params=None, max_bytes=2 * 1024 * 1024):
        with socket.socket(socket.AF_UNIX) as connection:
            connection.settimeout(1)
            connection.connect(str(endpoint))
            request = {'apiVersion': 'machine-fabric.dev/v1', 'requestId': 'install-' + uuid.uuid4().hex,
                       'action': action, 'params': params or {}}
            connection.sendall(json.dumps(request).encode() + b'\n')
            with connection.makefile('rb') as stream:
                raw = stream.readline(max_bytes + 1)
            if len(raw) > max_bytes:
                raise RuntimeError('installer RPC response exceeds bound')
            value = json.loads(raw)
            if not value.get('ok'):
                raise RuntimeError('installer RPC failed: ' + str(value.get('error'))[:500])
            return value['result']

    def health(self):
        c = self.rpc(self.sockets[0], 'status')
        e = self.rpc(self.sockets[1], 'status')
        if c.get('controller', {}).get('id') != self.node or e.get('executorId') != self.node + '-rust':
            raise RuntimeError('RPC identity mismatch')
        if c['controller'].get('status') != 'ready' or e.get('status') != 'ready':
            raise RuntimeError('RPC not ready')
        # RPC plus live launchd owners. Socket existence alone is never readiness.
        for label, executable in zip(LABELS, (self.controller, self.agent)):
            pid = self.job(label)
            if not pid or pid not in self.processes(executable):
                raise RuntimeError('launchd owner/executable PID not ready')
        return c, e

    def wait_health(self):
        deadline = time.monotonic() + self.timeout
        error = None
        while time.monotonic() < deadline:
            try:
                return self.health()
            except (OSError, ValueError, RuntimeError) as exc:
                error = exc
                time.sleep(.1)
        raise RuntimeError('RPC readiness deadline: ' + str(error))

    def quiescent(self):
        if self.job(LABELS[1]) is not None:
            status = self.rpc(self.sockets[1], 'status')
            execution = status.get('execution', {})
            if execution.get('active', 0) or execution.get('queued', 0):
                raise RuntimeError('executor has active/queued work')
        # Check durable queue before and after stopping, without reconcile/promote.
        queue = self.state / 'desktop-queue.json'
        if queue.exists():
            value = json.loads(queue.read_bytes())
            # Historical outcome-unknown tasks are terminal. A live blocked
            # desktop still needs supported recovery; installation never clears it.
            if value.get('blocked') or value.get('inFlight') or any(j.get('state') in ('active', 'queued', 'draining') for j in value.get('jobs', [])):
                raise RuntimeError('desktop queue is not quiescent')
        index = self.state / 'controller.json'
        if index.exists():
            value = json.loads(index.read_bytes())
            if any(t.get('state') not in ('succeeded', 'failed', 'cancelled', 'timed-out', 'outcome-unknown') for t in value.get('tasks', [])):
                raise RuntimeError('controller has unfinished tasks')
            # Lease.expires_at is serialized camelCase and measured in ms.
            # Expired records/fences remain untouched; only admission is checked.
            now = time.time_ns() // 1_000_000
            for lease in value.get('leases', []):
                expiry = lease.get('expiresAt')
                if type(expiry) is not int or expiry < 0:
                    raise RuntimeError('unknown lease expiry schema')
                if expiry > now:
                    raise RuntimeError('controller has active driver leases')

    def stage(self, directory):
        if not self.source.is_file() or not os.access(self.source, os.X_OK):
            raise RuntimeError('candidate executable missing')
        self.version = self.run([self.source, '--version']).stdout.split()[1]
        stage_app = directory / 'app'
        if self.app.exists():
            shutil.copytree(self.app, stage_app, symlinks=True)
        target = stage_app / 'Contents/MacOS/machine-fabric-macos-agent'
        target.parent.mkdir(parents=True, exist_ok=True)
        def binary_uuid(path):
            result = self.run(['/usr/bin/dwarfdump', '--uuid', path], check=False)
            match = re.search(r'UUID: ([A-Fa-f0-9-]+)', result.stdout)
            return match.group(1) if match else None
        source_uuid = binary_uuid(self.source)
        changed = not target.exists() or not source_uuid or binary_uuid(target) != source_uuid
        if changed:
            shutil.copy2(self.source, target)
        target.chmod(0o755)
        info = {'CFBundleIdentifier': 'dev.machine-fabric.macos-agent', 'CFBundleName': 'Machine Fabric',
                'CFBundleDisplayName': 'Machine Fabric', 'CFBundleExecutable': 'machine-fabric-macos-agent',
                'CFBundlePackageType': 'APPL', 'CFBundleShortVersionString': self.version, 'LSUIElement': True}
        info_path = stage_app / 'Contents/Info.plist'
        if not info_path.exists() or plistlib.loads(info_path.read_bytes()) != info:
            info_path.write_bytes(plistlib.dumps(info)); changed = True
        if changed or self.run(['/usr/bin/codesign', '--verify', '--deep', '--strict', stage_app], check=False).returncode:
            self.run(['/usr/bin/codesign', '--force', '--sign', '-', '--identifier', 'dev.machine-fabric.macos-agent',
                      '--requirements', '=designated => identifier "dev.machine-fabric.macos-agent"', stage_app])
        self.run(['/usr/bin/codesign', '--verify', '--deep', '--strict', stage_app])
        shutil.copy2(self.source, directory / 'controller')
        (directory / 'controller').chmod(0o755)
        roots = os.environ.get('MACHINE_FABRIC_LOCAL_ALLOW_ROOTS', '\n'.join(map(str, (self.home / 'Code', self.home / 'Workspace', self.state.parent)))).splitlines()
        if not roots or any(not Path(root).is_absolute() or not root for root in roots):
            raise RuntimeError('allowed roots must be nonempty absolute paths')
        arguments = [[str(self.controller), '--socket', str(self.sockets[0]), 'controller', 'serve', '--state',
                      str(self.state / 'controller.json'), '--id', self.node],
                     [str(self.agent), '--socket', str(self.sockets[1]), 'executor', 'serve', '--id', self.node + '-rust']]
        for root in roots:
            arguments[1] += ['--allow-root', root]
        for i, label in enumerate(LABELS):
            document = {'Label': label, 'ProgramArguments': arguments[i], 'RunAtLoad': True, 'KeepAlive': True,
                        'ProcessType': 'Background' if i == 0 else 'Interactive',
                        'StandardOutPath': str(self.state / ('controller.log' if i == 0 else 'macos-agent.log')),
                        'StandardErrorPath': str(self.state / ('controller.log' if i == 0 else 'macos-agent.log'))}
            path = directory / (label + '.plist')
            path.write_bytes(plistlib.dumps(document))
            self.run(['/usr/bin/plutil', '-lint', path])
        return [stage_app, directory / 'controller'] + [directory / (label + '.plist') for label in LABELS]

    def targets(self):
        return [self.app, self.controller] + self.plists + self.links

    def capture(self):
        # This directory is intentionally outside prune-state's generic backups.
        parent = self.state / 'installer-rollbacks'
        parent.mkdir(mode=0o700, exist_ok=True)
        self.backup = parent / (time.strftime('%Y%m%dT%H%M%S') + '-' + uuid.uuid4().hex)
        self.backup.mkdir(mode=0o700)
        durable = ('controller.json', 'controller.json.payloads', 'task-payloads', 'executor-fences.json', 'desktop-queue.json')
        def state_hashes():
            files = []
            for name in durable:
                path = self.state / name
                if path.is_file(): files.append(path)
                elif path.is_dir(): files.extend(p for p in path.rglob('*') if p.is_file())
            return {str(p.relative_to(self.state)): digest(p) for p in files}
        before = state_hashes()
        records = []
        for i, path in enumerate(self.targets()):
            record = {'target': str(path), 'kind': 'absent'}
            if path.is_symlink():
                record.update(kind='link', link=os.readlink(path))
            elif path.exists():
                dest = self.backup / str(i)
                if path.is_dir(): shutil.copytree(path, dest, symlinks=True)
                else: shutil.copy2(path, dest)
                record.update(kind='directory' if path.is_dir() else 'file', copy=str(dest))
            records.append(record)
        for name in durable:
            source = self.state / name
            if source.exists():
                dest = self.backup / 'state' / name
                dest.parent.mkdir(mode=0o700, exist_ok=True)
                if source.is_dir(): shutil.copytree(source, dest)
                else: shutil.copy2(source, dest)
        if state_hashes() != before:
            raise RuntimeError('durable state changed while stopped; no replacement performed')
        for name, expected in before.items():
            if digest(self.backup / 'state' / name) != expected:
                raise RuntimeError('durable backup mismatch: ' + name)
        files = {str(p.relative_to(self.backup)): {'sha256': digest(p), 'bytes': p.stat().st_size, 'mode': p.stat().st_mode & 0o777}
                 for p in self.backup.rglob('*') if p.is_file() and not p.is_symlink()}
        (self.backup / 'manifest.json').write_text(json.dumps({'records': records, 'files': files}, indent=2))
        for p in self.backup.rglob('*'):
            if not p.is_symlink(): p.chmod(0o700 if p.is_dir() else 0o600)
        return records

    def replace(self, source, destination):
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(destination.name + '.install-' + uuid.uuid4().hex)
        if source.is_dir(): shutil.copytree(source, temporary, symlinks=True)
        else: shutil.copy2(source, temporary)
        if destination.is_dir() and not destination.is_symlink():
            displaced = destination.with_name(destination.name + '.displaced-' + uuid.uuid4().hex)
            os.replace(destination, displaced)
            try: os.replace(temporary, destination)
            except BaseException:
                os.replace(displaced, destination)
                raise
            shutil.rmtree(displaced)
        else: os.replace(temporary, destination)

    def restore(self, records):
        manifest = json.loads((self.backup / 'manifest.json').read_text())
        for name, meta in manifest['files'].items():
            if digest(self.backup / name) != meta['sha256']:
                raise RuntimeError('rollback artifact hash mismatch: ' + name)
        for record in records:
            target = Path(record['target'])
            if record['kind'] in ('directory', 'file'):
                self.replace(Path(record['copy']), target)
                # Backup is private/nonexecutable; restore original file modes.
                base = Path(record['copy']).relative_to(self.backup)
                for name, meta in manifest['files'].items():
                    rel = Path(name)
                    if rel == base: target.chmod(meta['mode'])
                    elif base in rel.parents: (target / rel.relative_to(base)).chmod(meta['mode'])
            else:
                if target.is_dir() and not target.is_symlink(): shutil.rmtree(target)
                elif target.exists() or target.is_symlink(): target.unlink()
                if record['kind'] == 'link': target.symlink_to(record['link'])

    def start(self, labels):
        for label, path in zip(LABELS, self.plists):
            if label in labels:
                if any(self.alive(p) for p in self.stopping_pids.get(label, set())):
                    raise RuntimeError('previous process has not exited: ' + label)
                self.launch('bootstrap', self.domain, path)

    def rollback_payload_probe(self):
        """Test the actual old binary on an isolated CURRENT state copy.

        Controller serve has no automatic remote dispatch; the private socket is
        never registered. Only status/task.get requests are sent. No live state,
        lease/fence or sidecar is changed by this compatibility probe.
        """
        index = self.state / 'controller.json'
        if not index.exists(): return
        document = json.loads(index.read_bytes())
        checks = []
        seen = set()
        for task in document.get('tasks', []):
            for field in ('input', 'output'):
                ref = task.get(field + 'Ref')
                marker = (task.get(field) or {})
                if ref:
                    digest_value = ref.get('digest', '')
                    if not re.fullmatch(r'sha256:[0-9a-f]{64}', digest_value):
                        raise RuntimeError('rollback probe invalid reference digest')
                    digest_hex = digest_value[7:]
                    locator = ref.get('locator')
                    if locator not in ('task-payloads/' + digest_hex + '.json',
                                       'task-payloads/' + digest_value + '.json'):
                        raise RuntimeError('rollback probe invalid reference locator')
                    size = ref.get('bytes')
                elif isinstance(marker, dict) and '$machineFabricPayload' in marker:
                    ref = marker['$machineFabricPayload']
                    digest_hex = ref.get('sha256', '')
                    if not re.fullmatch(r'[0-9a-f]{64}', digest_hex):
                        raise RuntimeError('rollback probe invalid legacy digest')
                    locator = 'controller.json.payloads/' + digest_hex + '.json'
                    size = ref.get('bytes')
                else: continue
                if locator in seen: continue
                seen.add(locator)
                payload = self.state / locator
                if payload.is_symlink() or payload.parent.is_symlink():
                    raise RuntimeError('rollback probe payload symlink refused')
                if payload.stat().st_size != size or digest(payload) != digest_hex:
                    raise RuntimeError('rollback probe payload integrity failed')
                checks.append((task['id'], field, locator, size))
        if not checks: return
        probe = self.backup / 'current-state-probe'
        probe.mkdir(mode=0o700)
        shutil.copy2(index, probe / 'controller.json')
        for name in ('controller.json.payloads', 'task-payloads'):
            source = self.state / name
            if source.exists(): shutil.copytree(source, probe / name)
        for p in probe.rglob('*'):
            p.chmod(0o700 if p.is_dir() else 0o600)
        log_path = self.backup / 'payload-probe.log'
        # A short private path respects macOS sockaddr_un's path limit.
        with tempfile.TemporaryDirectory(prefix='mf-probe-') as socket_dir, log_path.open('wb') as log:
            endpoint = Path(socket_dir) / 'c.sock'
            process = subprocess.Popen([str(self.controller), '--socket', str(endpoint), 'controller',
                                        'serve', '--state', str(probe / 'controller.json'), '--id', self.node],
                                       stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + self.timeout
                while True:
                    if process.poll() is not None:
                        raise RuntimeError('old binary payload probe exited before RPC readiness')
                    try:
                        status = self.rpc(endpoint, 'status')
                        if status.get('controller', {}).get('id') != self.node:
                            raise RuntimeError('old binary payload probe identity mismatch')
                        break
                    except (OSError, ValueError):
                        if time.monotonic() >= deadline: raise RuntimeError('old binary payload probe readiness deadline')
                        time.sleep(.1)
                for task_id, field, locator, size in checks:
                    # Full refs, not only status or one happy-path format. Bound
                    # each response independently; unusually large records fail closed.
                    limit = min(128 * 1024 * 1024, 6 * sum(
                        (t.get(k + 'Ref') or {}).get('bytes', 0) for t in document['tasks']
                        if t['id'] == task_id for k in ('input', 'output')) + 2 * size + 2 * 1024 * 1024)
                    task = self.rpc(endpoint, 'task.get', {'taskId': task_id}, max_bytes=limit)
                    expected = json.loads((probe / locator).read_bytes())
                    if task.get(field) != expected:
                        raise RuntimeError('old binary returned unresolved/different payload for ' + task_id)
                (self.backup / 'payload-probe.json').write_text(json.dumps(
                    {'ok': True, 'referencesChecked': len(checks), 'binarySha256': digest(self.controller)}))
            finally:
                if process.poll() is None:
                    process.terminate()
                    try: process.wait(timeout=self.timeout)
                    except subprocess.TimeoutExpired:
                        raise RuntimeError('isolated payload probe did not exit; PID=' + str(process.pid))
        shutil.rmtree(probe)

    def transaction(self, staged):
        was_loaded = [label for label in LABELS if self.job(label) is not None]
        self.quiescent()
        records = None
        stopping = False
        try:
            stopping = True
            self.stop(LABELS[1], self.agent)
            self.stop(LABELS[0], self.controller)
            self.quiescent()
            records = self.capture()
            for source, target in zip(staged, self.targets()[:4]): self.replace(source, target)
            self.bins.mkdir(parents=True, exist_ok=True)
            for link in self.links:
                temporary = link.with_name(link.name + '.install-' + uuid.uuid4().hex)
                temporary.symlink_to(self.agent)
                os.replace(temporary, link)
            self.start(LABELS)
            self.wait_health()
            self.rpc(self.sockets[0], 'executor.register', {'executorId': self.node + '-rust',
                     'endpoint': {'transport': 'local', 'socket': str(self.sockets[1])}})
        except BaseException as original:
            try:
                if records is not None:
                    self.stop(LABELS[1], self.agent)
                    self.stop(LABELS[0], self.controller)
                    self.restore(records)
                    self.rollback_payload_probe()
                # Partial stop failure: never replace live files; start only missing old jobs.
                if stopping:
                    self.start([label for label in was_loaded if self.job(label) is None])
                    if len(was_loaded) == 2: self.wait_health()
            except BaseException as recovery:
                raise RuntimeError('install failed: %s; recovery failed against CURRENT state: %s; retained rollback=%s (state not rewound)' %
                                   (original, recovery, self.backup)) from recovery
            raise RuntimeError('install failed; previous service restored against CURRENT state; rollback=%s; cause=%s' %
                               (self.backup, original)) from original
        print(json.dumps({'ok': True, 'version': self.version, 'rollback': str(self.backup), 'stateRestored': False}))

    def install(self):
        self.state.mkdir(parents=True, exist_ok=True)
        lock_path = self.state / '.install-macos.lock'
        with lock_path.open('a') as lock:
            os.chmod(lock_path, 0o600)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with tempfile.TemporaryDirectory(prefix='install-stage-', dir=self.state) as temporary:
                staged = self.stage(Path(temporary))
                self.transaction(staged)
        # No automatic prune: installer-rollbacks contain the only known-good identity.


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('source', nargs='?', default='target/release/machine-fabric-macos-agent')
    args = parser.parse_args()
    os.umask(0o077)
    def interrupted(signum, frame):
        raise RuntimeError('installer interrupted by signal ' + str(signum))
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGHUP, interrupted)
    Installer(args.source).install()


if __name__ == '__main__': main()
