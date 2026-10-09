#!/usr/bin/env python3
"""Reject Windows release binaries that require an external MSVC runtime."""
import argparse
from pathlib import Path
import re
import struct
import zipfile


MACHINES = {'aarch64-pc-windows-msvc': 0xAA64, 'x86_64-pc-windows-msvc': 0x8664}
EXTERNAL_CRT = re.compile(r'^(?:vcruntime|msvcp|msvcr|concrt|vcomp)\d.*\.dll$', re.I)


def imports(data, machine):
    def read(fmt, offset):
        size = struct.calcsize(fmt)
        if offset < 0 or offset + size > len(data):
            raise ValueError('truncated PE header')
        return struct.unpack_from(fmt, data, offset)

    if data[:2] != b'MZ':
        raise ValueError('missing DOS signature')
    pe, = read('<I', 0x3C)
    if data[pe:pe + 4] != b'PE\0\0':
        raise ValueError('missing PE signature')
    actual, count = read('<HH', pe + 4)
    if actual != machine or not 1 <= count <= 96:
        raise ValueError('wrong target architecture or invalid section count')
    optional_size, = read('<H', pe + 20)
    optional = pe + 24
    magic, = read('<H', optional)
    directories, = read('<I', optional + 108)
    if magic != 0x20B or optional_size < 224 or directories < 14:
        raise ValueError('invalid PE32+ optional header')
    sections = []
    for index in range(count):
        section = optional + optional_size + index * 40
        virtual_size, rva, raw_size, raw = read('<IIII', section + 8)
        if raw + raw_size > len(data):
            raise ValueError('truncated PE section')
        sections.append((rva, raw_size, raw))

    def offset(rva, size):
        for start, raw_size, raw in sections:
            if start <= rva and rva + size <= start + raw_size:
                return raw + rva - start
        raise ValueError('unmapped import RVA')

    names = []
    # Ordinary imports and delay imports must both be self contained.
    for directory, width, name_position in ((1, 20, 3), (13, 32, 1)):
        rva, size = read('<II', optional + 112 + directory * 8)
        if rva == size == 0:
            continue
        if not rva or size < width or size > 1024 * 1024:
            raise ValueError('invalid import directory')
        base = offset(rva, size)
        terminated = False
        for position in range(0, size - width + 1, width):
            entry = read('<' + 'I' * (width // 4), base + position)
            if not any(entry):
                terminated = True
                break
            if directory == 13 and entry[0] != 1:
                raise ValueError('unsupported delay import addressing')
            name = offset(entry[name_position], 1)
            end = data.find(b'\0', name, min(name + 256, len(data)))
            if end < 0:
                raise ValueError('unterminated DLL name')
            dll = data[name:end].decode('ascii')
            if not re.fullmatch(r'[A-Za-z0-9_.-]+\.dll', dll, re.I):
                raise ValueError('invalid imported DLL name')
            names.append(dll)
        if not terminated:
            raise ValueError('unterminated import directory')
    return names


def verify(archive, target):
    with zipfile.ZipFile(archive) as package:
        binaries = [item for item in package.infolist() if item.filename.lower().endswith('.exe')]
        if not binaries or not any(item.filename.endswith('/bin/machine-fabric.exe') for item in binaries):
            raise ValueError('release CLI is missing')
        for item in binaries:
            if item.file_size > 128 * 1024 * 1024:
                raise ValueError('oversized release binary')
            dlls = imports(package.read(item), MACHINES[target])
            forbidden = [dll for dll in dlls if EXTERNAL_CRT.fullmatch(dll)]
            if forbidden:
                raise ValueError(f'{item.filename}: external MSVC runtime required: {", ".join(forbidden)}')
    return len(binaries)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('archive', type=Path)
    parser.add_argument('--target', choices=MACHINES, required=True)
    args = parser.parse_args()
    print(f'Verified {verify(args.archive, args.target)} Windows release binaries: no external MSVC runtime required')
