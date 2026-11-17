"""Packaging keeps Cargo progress separate from machine-readable metadata."""
import json
from pathlib import Path
import subprocess
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tools"))
from build_preview import command


class PreviewCommandTests(unittest.TestCase):
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
