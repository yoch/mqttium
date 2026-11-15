"""The first lifecycle recovery cannot be selected from surviving pages."""

import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

from mqttiumd_oracle.documents import Contracts

from .support import ORACLE, ROOT


CASE = ORACLE / "cases/later-recovery-cannot-fill-first"
ASSERTION = "transactional_kill_matches_recovery"


def partial_first_inventory(records):
    """One valid, non-final page in incarnation 2, before its second kill."""
    start = next(index for index, record in enumerate(records)
                 if record["kind"] == "BrokerRecovered" and record["payload"]["run_epoch"] == "2")
    template = next(record for record in records
                    if record["kind"] == "RecoveryPageRead" and record["origin_incarnation"] == "3")
    read = copy.deepcopy(template)
    read.update(origin_incarnation="2", origin_sequence="1", causal_parents=[])
    read["payload"].update(run_epoch="2", last=False)
    read["payload"]["facts"] = read["payload"]["facts"][:2]
    read["payload"]["facts"][0]["epoch"] = "2"
    delivered = copy.deepcopy(read)
    delivered.update(kind="RecoveryPageDelivered", origin_component="broker-core", origin_sequence="2")
    delivered["causal_parents"] = [{
        "origin_component": "store-adapter", "origin_incarnation": "2", "origin_sequence": "1"
    }]
    records[start + 1:start + 1] = [read, delivered]
    for index, record in enumerate(records):
        record.update(record_sequence=str(index), time_value=str(index))
    records[-1]["payload"]["record_count"] = str(len(records))


class FirstRecovery(unittest.TestCase):
    def judge_case(self, partial=False):
        with tempfile.TemporaryDirectory(prefix="mqttiumd-first-recovery-") as temporary:
            root = Path(temporary)
            shutil.copytree(CASE, root, dirs_exist_ok=True)
            # Never copy the review bundle's historical contract snapshot.
            shutil.copytree(ROOT / "contracts", root / "contracts")
            fixture = root / "tests/fixtures/p1/three-persistent-sessions.json"
            fixture.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / "tests/fixtures/p1/three-persistent-sessions.json", fixture)
            if partial:
                path = root / "trace.jsonl"
                records = [json.loads(line) for line in path.read_text().splitlines()]
                partial_first_inventory(records)
                path.write_text("".join(json.dumps(record) + "\n" for record in records))
            result = subprocess.run(
                [sys.executable, "-m", "mqttiumd_oracle", "--root", str(root),
                 "--contracts", "contracts/v1", "judge", "--scenario", "scenario.json",
                 "--trace", "trace.jsonl"],
                env=dict(os.environ, PYTHONPATH=str(ORACLE)), capture_output=True,
                text=True, timeout=30, check=False,
            )
            self.assertIn(result.returncode, (0, 1, 2, 3), result.stdout + result.stderr)
            report = json.loads(result.stdout)
            self.assertEqual(report["contracts"], "sha256:" + Contracts.open(ROOT).digest)
            self.assertEqual(report["assertions"]["trace_complete"]["status"], "PASS")
            self.assertEqual(report["assertions"]["broker_inputs_have_provenance"]["status"], "PASS")
            return result.returncode, report

    def test_later_complete_inventory_cannot_replace_the_first_missing_inventory(self):
        code, report = self.judge_case()
        self.assertEqual(report["assertions"][ASSERTION]["status"], "INCONCLUSIVE")
        self.assertEqual((code, report["verdict"]), (2, "INCONCLUSIVE"))

    def test_later_complete_inventory_cannot_replace_the_first_partial_inventory(self):
        code, report = self.judge_case(partial=True)
        self.assertEqual(report["assertions"][ASSERTION]["status"], "INCONCLUSIVE")
        self.assertEqual((code, report["verdict"]), (2, "INCONCLUSIVE"))


if __name__ == "__main__":
    unittest.main()
