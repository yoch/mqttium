"""The oracle on the hand-made corpus of traces.

``traces/index.json`` states, for every trace, the status of each assertion.
A good trace is accepted; a faulty one is refused for its own reason and no
other.
"""

import json
import subprocess
import sys
import unittest

from mqttiumd_oracle.documents import Contracts
from mqttiumd_oracle.judge import Run, judge

from .support import ORACLE, ROOT

INDEX = json.loads((ORACLE / "traces/index.json").read_text())["traces"]


class Traces(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contracts = Contracts.open(ROOT)

    def statuses(self, case):
        run = Run.load(self.contracts, case["scenario"], case["trace"])
        return {name: status.status for name, status in judge(run).items()}, judge(run)

    def test_every_trace_gets_the_statuses_the_corpus_states(self):
        self.assertGreaterEqual(len(INDEX), 30)
        for case in INDEX:
            with self.subTest(case["name"]):
                found, detail = self.statuses(case)
                reasons = {name: status.reasons for name, status in detail.items()}
                self.assertEqual(found, case["expected"], reasons)

    def test_every_refusal_states_its_reason(self):
        for case in INDEX:
            with self.subTest(case["name"]):
                for name, status in self.statuses(case)[1].items():
                    if status.status in ("FAIL", "INCONCLUSIVE"):
                        self.assertTrue(status.reasons, name)

    def test_every_assertion_has_a_good_trace_and_a_faulty_trace_of_its_own(self):
        assertions = set(INDEX[0]["expected"])
        accepted = {name: 0 for name in assertions}
        refused_alone = {name: 0 for name in assertions}
        for case in INDEX:
            expected = case["expected"]
            for name, status in expected.items():
                accepted[name] += status == "PASS"
            refused = [
                name for name, status in expected.items() if status in ("FAIL", "INCONCLUSIVE")
            ]
            if len(refused) == 1:
                refused_alone[refused[0]] += 1
        for name in sorted(assertions):
            with self.subTest(name):
                self.assertGreater(accepted[name], 0, "no trace on which it passes")
                if name == "trace_complete":
                    # An incomplete trace concludes nothing: it cannot be
                    # refused for this assertion alone.
                    continue
                self.assertGreater(refused_alone[name], 0, "no trace refused for it alone")

    def test_an_incomplete_trace_never_passes_anything(self):
        seen = 0
        for case in INDEX:
            if case["expected"]["trace_complete"] != "INCONCLUSIVE":
                continue
            seen += 1
            with self.subTest(case["name"]):
                self.assertNotIn("PASS", self.statuses(case)[0].values())
        self.assertGreaterEqual(seen, 8)

    def test_the_corpus_is_up_to_date(self):
        outcome = subprocess.run(
            [sys.executable, str(ORACLE / "build_traces.py"), "--check"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(outcome.returncode, 0, outcome.stdout + outcome.stderr)


if __name__ == "__main__":
    unittest.main()
