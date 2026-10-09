"""Portable artifact checks for clean Windows installations."""
import importlib.util
from pathlib import Path
import struct
import tempfile
import unittest
import zipfile

SPEC = importlib.util.spec_from_file_location('verify_windows_release', Path(__file__).parents[1] / 'scripts/verify-windows-release.py')
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


def pe(dll='KERNEL32.dll', machine=0xAA64, delay=False):
    data = bytearray(1024)
    data[:2] = b'MZ'
    struct.pack_into('<I', data, 0x3C, 0x80)
    data[0x80:0x84] = b'PE\0\0'
    struct.pack_into('<HH', data, 0x84, machine, 1)
    struct.pack_into('<H', data, 0x94, 240)
    optional = 0x98
    struct.pack_into('<H', data, optional, 0x20B)
    struct.pack_into('<I', data, optional + 108, 16)
    directory, width = (13, 32) if delay else (1, 20)
    struct.pack_into('<II', data, optional + 112 + directory * 8, 0x1000, width * 2)
    struct.pack_into('<IIII', data, optional + 240 + 8, 512, 0x1000, 512, 512)
    if delay:
        struct.pack_into('<II', data, 512, 1, 0x1080)
    else:
        struct.pack_into('<I', data, 524, 0x1080)
    encoded = dll.encode() + b'\0'
    data[640:640 + len(encoded)] = encoded
    return bytes(data)


class WindowsReleaseRuntimeTests(unittest.TestCase):
    def verify(self, data, target='aarch64-pc-windows-msvc'):
        with tempfile.TemporaryDirectory() as root:
            archive = Path(root) / 'release.zip'
            with zipfile.ZipFile(archive, 'w') as package:
                package.writestr('machine-fabric/bin/machine-fabric.exe', data)
            return CHECK.verify(archive, target)

    def test_os_runtime_imports_and_both_architectures(self):
        for target, machine in CHECK.MACHINES.items():
            for dll in ('KERNEL32.dll', 'api-ms-win-crt-runtime-l1-1-0.dll'):
                self.assertEqual(self.verify(pe(dll, machine), target), 1)

    def test_dynamic_crt_is_rejected_in_ordinary_and_delay_imports(self):
        for dll in ('VCRUNTIME140.dll', 'vcruntime140_1.dll', 'MSVCP140_ATOMIC_WAIT.dll', 'concrt140.dll', 'vcomp140.dll', 'msvcr120.dll'):
            for delay in (False, True):
                with self.subTest(dll=dll, delay=delay):
                    with self.assertRaisesRegex(ValueError, 'external MSVC runtime'):
                        self.verify(pe(dll, delay=delay))

    def test_wrong_architecture_and_truncation_are_rejected(self):
        for data in (pe(machine=0x8664), pe()[:700], b'not a PE'):
            with self.assertRaises(ValueError):
                self.verify(data)

    def test_unmapped_import_name_and_missing_terminator_are_rejected(self):
        data = bytearray(pe())
        struct.pack_into('<I', data, 524, 0x8000)
        with self.assertRaisesRegex(ValueError, 'unmapped'):
            self.verify(data)
        data = bytearray(pe())
        struct.pack_into('<II', data, 0x98 + 120, 0x1000, 20)
        with self.assertRaisesRegex(ValueError, 'unterminated import'):
            self.verify(data)

    def test_missing_cli_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            archive = Path(root) / 'empty.zip'
            with zipfile.ZipFile(archive, 'w'):
                pass
            with self.assertRaisesRegex(ValueError, 'CLI is missing'):
                CHECK.verify(archive, 'aarch64-pc-windows-msvc')


if __name__ == '__main__':
    unittest.main()
