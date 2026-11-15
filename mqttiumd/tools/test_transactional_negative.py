#!/usr/bin/env python3
"""The control reads a scenario path from provenance, not its scenario ID."""

import json
from pathlib import Path
import tempfile
import unittest

import check_transactional_negative as control


class ScenarioIdentity(unittest.TestCase):
    def test_the_scenario_id_is_not_a_source_path(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "campaign.json").write_text(json.dumps({"scenario": "K1"}))
            (root / "run-manifest.json").write_text(json.dumps({
                "scenario": {"path": "tests/scenarios/crash/K1.json"}
            }))
            self.assertEqual(control.scenario_file(root), "tests/scenarios/crash/K1.json")


if __name__ == "__main__":
    unittest.main()
