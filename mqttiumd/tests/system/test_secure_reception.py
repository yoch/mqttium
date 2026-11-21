"""The bounded service reception also runs on hosts without cgroup v2."""

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from secure_reception import cpu_quota


class CpuQuotaTests(unittest.TestCase):
    def test_reads_cgroup_v2_limit(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "cpu.max").write_text("400000 100000\n")
            self.assertEqual(cpu_quota(root), {"source": "cgroup_v2", "limit": "400000 100000"})

    def test_reads_cgroup_v1_limit(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            controller = root / "cpu,cpuacct"
            controller.mkdir()
            (controller / "cpu.cfs_quota_us").write_text("-1\n")
            (controller / "cpu.cfs_period_us").write_text("100000\n")
            self.assertEqual(cpu_quota(root), {
                "source": "cgroup_v1", "quota_us": -1, "period_us": 100000,
            })

    def test_host_without_visible_quota_still_enters_measurement(self):
        with TemporaryDirectory() as directory:
            self.assertEqual(cpu_quota(Path(directory)), {"source": "unavailable"})
