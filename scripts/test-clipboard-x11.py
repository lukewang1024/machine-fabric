#!/usr/bin/env python3
"""Bounded native clipboard test on a disposable, authenticated Xvfb display."""
import os
import pathlib
import secrets
import shutil
import subprocess
import tempfile

for tool in ('Xvfb', 'xauth', 'xclip', 'cargo'):
    if not shutil.which(tool):
        raise SystemExit(f'{tool} is required')
with tempfile.TemporaryDirectory(prefix='machine-fabric-clipboard-test-') as tmp:
    auth = pathlib.Path(tmp) / 'Xauthority'
    auth.touch(mode=0o600)
    # Pick a free display explicitly so the cookie is in place before Xvfb starts.
    for number in range(120, 200):
        if not pathlib.Path(f'/tmp/.X11-unix/X{number}').exists() and not pathlib.Path(f'/tmp/.X{number}-lock').exists():
            break
    else:
        raise SystemExit('no test display available')
    display = f':{number}'
    subprocess.run(['xauth', '-f', str(auth), 'add', display, 'MIT-MAGIC-COOKIE-1', secrets.token_hex(16)], check=True)
    read_fd, write_fd = os.pipe()
    env = dict(os.environ, DISPLAY=display, XAUTHORITY=str(auth))
    env.pop('WAYLAND_DISPLAY', None)
    server = subprocess.Popen(['Xvfb', display, '-screen', '0', '16x16x24', '-nolisten', 'tcp', '-noreset', '-auth', str(auth), '-displayfd', str(write_fd)], pass_fds=(write_fd,))
    os.close(write_fd)
    try:
        import select
        if not select.select([read_fd], [], [], 10)[0] or not os.read(read_fd, 64):
            raise RuntimeError('Xvfb did not become ready')
        subprocess.run(['cargo', 'test', '-p', 'machine-fabric-runtime', 'native_x11_test', '--', '--ignored', '--nocapture'], env=env, check=True, timeout=600)
    finally:
        os.close(read_fd)
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()

