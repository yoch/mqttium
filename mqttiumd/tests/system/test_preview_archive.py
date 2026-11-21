"""An independently identified archive must match its executable and reception."""

import hashlib
import io
import json
from pathlib import Path
import sys
import tarfile
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))

import preview_archive as preview


def fixture(out, machine, *, wrong_receipt=False):
    elf_number = {"x86_64": 62, "aarch64": 183}[machine]
    binary = b"\x7fELF\x02\x01\x01" + b"\0" * 11 + elf_number.to_bytes(2, "little")
    sha = hashlib.sha256(binary).hexdigest()
    build = {"source_sha": "a" * 40, "machine": machine, "binary_sha256": sha, "profile": "release",
             "instrumented": False, "features": [],
             "build_options": ["--release", "--locked", "--no-default-features", "-p", "mqttiumd"],
             "elf": {"machine": machine, "minimum_glibc_symbols": "2.34", "interpreter": "/bin/sh"}}
    seal = {"format": preview.RESTART_FORMAT, "machine": machine, "broker_binary_sha256": sha,
            "broker_source_sha": "a" * 40, "package_source_sha": "b" * 40,
            "reception": {"status": "separate_receipt", "binary_sha256": "c" * 64 if wrong_receipt else sha}}
    members = {"mqttiumd": binary, "BUILD.json": json.dumps(build).encode(),
               "PACKAGE.json": json.dumps(seal).encode()}
    manifest = "".join(f"{hashlib.sha256(data).hexdigest()}  {name}\n" for name, data in members.items())
    with tarfile.open(out, "w:gz") as package:
        root = tarfile.TarInfo("fixture")
        root.type = tarfile.DIRTYPE
        package.addfile(root)
        for name, data in {**members, "SHA256SUMS": manifest.encode()}.items():
            member = tarfile.TarInfo(f"fixture/{name}")
            member.size = len(data)
            member.mode = 0o755 if name == "mqttiumd" else 0o644
            package.addfile(member, io.BytesIO(data))
    return preview.digest(out)


class ArchiveTests(unittest.TestCase):
    def test_both_native_architectures_and_actual_elf_mismatch(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            for machine in ("x86_64", "aarch64"):
                archive = root / f"{machine}.tar.gz"
                sha = fixture(archive, machine)
                other = "aarch64" if machine == "x86_64" else "x86_64"
                with (patch.object(preview.platform, "system", return_value="Linux"),
                      patch.object(preview.platform, "machine", return_value=other)):
                    with self.assertRaisesRegex(ValueError, "native_architecture_mismatch"):
                        preview.receive(archive, sha, root / "wrong")
                self.assertFalse((root / "wrong").exists())
                with (patch.object(preview.platform, "system", return_value="Linux"),
                      patch.object(preview.platform, "machine", return_value=machine),
                      patch.object(preview.platform, "libc_ver", return_value=("glibc", "2.39")),
                      patch.object(preview.shutil, "which", return_value=None)):
                    result = preview.receive(archive, sha, root / f"verified-{machine}")
                self.assertEqual(result["machine"], machine)
                self.assertEqual(result["verified_members"], 3)

    def test_altered_tar_is_refused_before_extraction(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive = root / "normal.tar.gz"
            sha = fixture(archive, "x86_64")
            with archive.open("ab") as target:
                target.write(b"changed")
            with self.assertRaisesRegex(ValueError, "archive_sha256_mismatch"):
                preview.receive(archive, sha, root / "rejected")
            self.assertFalse((root / "rejected").exists())

    def test_embedded_receipt_for_other_binary_is_refused(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive = root / "other-binary.tar.gz"
            sha = fixture(archive, "aarch64", wrong_receipt=True)
            with self.assertRaisesRegex(ValueError, "archive_reception_identity_mismatch"):
                preview.receive(archive, sha, root / "rejected")
            self.assertFalse((root / "rejected").exists())


if __name__ == "__main__":
    unittest.main()
