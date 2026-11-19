"""Gate the use of a sealed package and account for concurrent recovery copies."""

import argparse
import ipaddress
from pathlib import Path
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))

import field_preflight as preflight


class FieldPreflightTests(unittest.TestCase):
    def test_native_arm_runner_cannot_pass_for_the_sealed_x86_release(self):
        args = argparse.Namespace(archive=Path("frozen.tar.gz"), data_root=Path("data"), backup_root=Path("backup"),
                                  restore_root=None, overhead_gib=4, write_assessment=None,
                                  lan_address=None, credentials=None, smart=False)
        host = {"os": "Linux", "machine": "aarch64", "glibc": ["glibc", "2.43"],
                "uid": 1000, "paho": "2.1.0", "vm": "none", "rpi_throttled": None}
        volume = {"device": "0:27", "filesystem": "overlay", "disk": None, "mount_options": [],
                  "available_bytes": 128 * preflight.GIB, "available_inodes": 1_000_000, "private_writable": True}
        with (patch.object(preflight, "host_record", return_value=host),
              patch.object(preflight, "package_record", return_value={"elf_machine": "x86_64"}),
              patch.object(preflight, "block_inventory", return_value=[]),
              patch.object(preflight, "volume", return_value=volume)):
            result = preflight.assess(args)
        self.assertIn({"code": "native_architecture_mismatch"}, result["refusals"])
        self.assertEqual(result["preflight"], "HOLD")
        self.assertEqual(result["qualification"], "not_executed")

    def test_shared_destination_requires_three_copies_of_bytes_and_catalog_inodes(self):
        volume = {"device": "8:1", "available_bytes": 48 * preflight.GIB, "available_inodes": 500_000}
        budgets = preflight.storage_budgets(dict.fromkeys(("data", "backup", "restore"), volume), 4 * preflight.GIB)
        self.assertEqual(budgets["8:1"]["required_bytes"], 66_236_448_768)
        self.assertEqual(budgets["8:1"]["required_inodes"], 602_112)
        self.assertGreater(budgets["8:1"]["required_bytes"], budgets["8:1"]["available_bytes"])
        self.assertGreater(budgets["8:1"]["required_inodes"], budgets["8:1"]["available_inodes"])

    def test_unrelated_physical_disk_and_virtio_are_not_evidence_for_overlay(self):
        virt = {"maj:min": "254:0", "kname": "vda", "type": "disk", "tran": "virtio", "model": "QEMU"}
        nvme = {"maj:min": "259:0", "kname": "nvme0n1", "type": "disk", "tran": "nvme", "model": "Sample Drive",
                "rota": False, "size": 512 * preflight.GIB}
        partition = {"maj:min": "259:1", "kname": "nvme0n1p1", "pkname": "nvme0n1", "type": "part"}
        blocks = [nvme, partition, virt]
        self.assertIsNone(preflight.backing_disk("0:27", blocks))
        self.assertIsNone(preflight.backing_disk("254:0", blocks))
        with patch.object(preflight, "read", return_value="0 0 0 0 0 0 80 0 0 0 0"):
            mapped = preflight.backing_disk("259:1", blocks)
        self.assertEqual(mapped["device"], "259:0")
        self.assertEqual(mapped["host_block_write_bytes"], 40960)

    def test_disks_are_bound_to_an_operator_evidence_digest_and_adequate_budget(self):
        disk = {"model": "Sample Drive"}
        assessment = {"model": "Sample Drive", "projected_72h_write_bytes": 1_000_000,
                      "available_write_budget_bytes": 100, "evidence_sha256": "1" * 64,
                      "unrecognized_private_field": "value"}
        observed, errors = preflight.write_headroom({"259:0": disk}, {"259:0": assessment})
        self.assertNotIn("unrecognized_private_field", observed["259:0"])
        self.assertEqual(errors, [{"code": "insufficient_write_budget", "disk": "259:0"}])
        assessment["model"] = "Other Drive"
        self.assertEqual(preflight.write_headroom({"259:0": disk}, {"259:0": assessment}),
                         ({}, [{"code": "write_budget_unverified", "disk": "259:0"}]))

    def test_unsealed_archive_is_refused_before_interpretation(self):
        with TemporaryDirectory() as work:
            candidate = Path(work) / "candidate.tar.gz"
            candidate.write_bytes(b"not the sealed archive")
            with self.assertRaisesRegex(ValueError, "archive_sha256_mismatch"):
                preflight.package_record(candidate)
        with self.assertRaisesRegex(ValueError, "archive_must_be_a_bounded_regular_file"):
            preflight.package_record(Path("/dev/zero"))

    def test_public_and_link_local_addresses_are_not_selected_as_private_lan(self):
        for address in ("192.168.42.10", "10.1.1.1", "172.16.1.1", "fd00::10"):
            self.assertTrue(preflight.private_address(ipaddress.ip_address(address)))
        for address in ("127.0.0.1", "192.0.2.1", "169.254.10.20", "fe80::10", "8.8.8.8"):
            self.assertFalse(preflight.private_address(ipaddress.ip_address(address)))

    @unittest.skipUnless(shutil.which("openssl"), "OpenSSL provides the independent certificate checks")
    def test_certificate_must_cover_the_ip_and_its_private_key_must_be_private(self):
        with TemporaryDirectory() as work:
            pki = Path(work)
            key = pki / "server.key"
            cert = pki / "server.crt"
            subprocess.run(["openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256",
                            "-nodes", "-keyout", str(key), "-out", str(cert), "-subj", "/CN=192.168.42.11",
                            "-days", "7", "-addext", "subjectAltName=IP:192.168.42.10",
                            "-addext", "extendedKeyUsage=serverAuth"],
                           capture_output=True, check=True)
            shutil.copyfile(cert, pki / "ca.crt")
            key.chmod(0o600)
            self.assertTrue(preflight.certificate_ok(pki, ipaddress.ip_address("192.168.42.10")))
            self.assertFalse(preflight.certificate_ok(pki, ipaddress.ip_address("192.168.42.11")))
            key.chmod(0o644)
            self.assertFalse(preflight.certificate_ok(pki, ipaddress.ip_address("192.168.42.10")))


if __name__ == "__main__":
    unittest.main()
