"""Packaging keeps Cargo progress separate from machine-readable metadata."""
import json
from pathlib import Path
import subprocess
import sys
import unittest
import platform
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))
from build_preview import command
import check_preview


class PreviewCommandTests(unittest.TestCase):
    def test_execution_identity_is_distinct_from_build_identity(self):
        build={"platform":"different-build-host","machine":"different-build-machine"}
        with patch.object(platform,"platform",return_value="actual-execution-host"), patch.object(platform,"machine",return_value="actual-execution-machine"):
            observed=check_preview.platforms(build)
        self.assertEqual(observed["build_platform"],"different-build-host")
        self.assertEqual(observed["execution_platform"],"actual-execution-host","build_host_misreported_as_execution_host")
        self.assertEqual(observed["execution_machine"],"actual-execution-machine")

    def test_dependency_download_progress_cannot_prefix_metadata_json(self):
        output = command(sys.executable, "-c",
            "import sys; print('Downloading dependency',file=sys.stderr); print('{\"packages\":[]}')")
        self.assertEqual(json.loads(output), {"packages": []}, "cargo_progress_polluted_metadata")

    def test_failure_keeps_its_diagnostic_separate(self):
        with self.assertRaises(subprocess.CalledProcessError) as caught:
            command(sys.executable, "-c", "import sys; print('fetch refused',file=sys.stderr); sys.exit(7)")
        self.assertEqual(caught.exception.returncode, 7)
        self.assertIn("fetch refused", caught.exception.stderr or "", "packaging_failure_cause_lost")


if __name__ == "__main__":
    unittest.main()
