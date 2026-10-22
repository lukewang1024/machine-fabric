#!/usr/bin/env python3
"""Node-local, exact-job operator consent; never impersonates Away Guard.

Run only after explicit user authorization. The receipt expires within fifteen
minutes and is bound to this OS boot, job, generation key and token digest.
This is administration, not a desktop RPC parameter or a consumer opt-out.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', type=Path, required=True)
    parser.add_argument('--job-id')
    parser.add_argument('--owner')
    parser.add_argument('--request-key')
    parser.add_argument('--reason')
    parser.add_argument('--seconds', type=int, default=600)
    parser.add_argument('--confirm-user-authorized', action='store_true')
    parser.add_argument('--revoke', metavar='AUTHORIZATION_ID')
    args = parser.parse_args()
    if os.name != 'posix':
        parser.error('local operator receipts currently require POSIX ownership and mode checks')
    path = args.state_dir / 'desktop-user-authorization.json'
    now = time.time_ns() // 1_000_000
    previous = None
    if path.is_symlink():
        parser.error('authorization path must not be a symlink')
    if path.exists():
        previous = json.loads(path.read_text())
    if args.revoke:
        if previous is None or previous.get('authorizationId') != args.revoke:
            parser.error('revoke requires the exact existing authorization identity')
        path.unlink()
        print(json.dumps({'revoked': True, 'authorizationId': args.revoke, 'inputSent': False}))
        return
    if not args.confirm_user_authorized or not 1 <= args.seconds <= 900:
        parser.error('explicit user confirmation and a 1..900 second lifetime are required')
    if not all((args.job_id, args.owner, args.request_key, args.reason)) or len(args.reason.encode()) > 512:
        parser.error('exact job, owner, request key and bounded reason are required')
    if previous and previous.get('expiresAtMs', 0) > now:
        parser.error('a live operator receipt exists; reconcile it instead of replacing it')
    queue = json.loads((args.state_dir / 'desktop-queue.json').read_text())
    if queue.get('blocked') or queue.get('inFlight') or queue.get('maintenanceOwner'):
        parser.error('desktop is blocked, busy or in maintenance; consent cannot clear those states')
    jobs = [j for j in queue['jobs'] if j['id'] == args.job_id]
    if len(jobs) != 1:
        parser.error('unknown or ambiguous job')
    job = jobs[0]
    if job['owner'] != args.owner or job['requestKey'] != args.request_key or job['state'] != 'queued':
        parser.error('only the exact queued job may receive initial consent')
    if any(j['state'] in ('active', 'draining') for j in queue['jobs']):
        parser.error('another desktop lease is active or draining')
    deadline = job.get('deadlineAt')
    if not queue.get('bootId') or (deadline is not None and deadline <= now):
        parser.error('verified boot identity and an unexpired request are required')
    record = {'schemaVersion': 1, 'source': 'explicit-user',
              'authorizationId': str(uuid.uuid4()), 'jobId': job['id'],
              'owner': job['owner'], 'requestKey': job['requestKey'],
              'tokenSha256': hashlib.sha256(job['token'].encode()).hexdigest(),
              'bootId': queue['bootId'], 'grantedAtMs': now,
              'expiresAtMs': now + args.seconds * 1000, 'reason': args.reason}
    descriptor, name = tempfile.mkstemp(prefix='.desktop-user-authorization-', dir=args.state_dir)
    try:
        with os.fdopen(descriptor, 'w') as stream:
            json.dump(record, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        Path(name).unlink(missing_ok=True)
    print(json.dumps({key: record[key] for key in (
        'authorizationId', 'jobId', 'source', 'grantedAtMs', 'expiresAtMs')},
        ensure_ascii=False))


if __name__ == '__main__':
    main()
