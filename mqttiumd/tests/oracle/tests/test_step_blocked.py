"""What a stopped run establishes, and what it says still waits (point d)."""

import unittest

from mqttiumd_oracle import judge
from mqttiumd_oracle.documents import Contracts

from .support import ROOT


class StepBlocked(unittest.TestCase):
    def status(self, name):
        run = judge.Run.load(
            Contracts.open(ROOT),
            "tests/oracle/traces/scenarios/p1-02.json",
            f"tests/oracle/traces/{name}/trace.jsonl",
        )
        return judge.scenario_reaches_its_end(run)

    def test_the_fail_refutes_the_step_for_this_run_and_says_what_waits(self):
        status = self.status("run-blocked-with-nothing-eligible")
        self.assertEqual(status.status, judge.FAIL)
        (reason,) = status.reasons
        self.assertIn("refutes", reason)
        self.assertNotIn("stuck", reason)
        self.assertIn("0 deadlines armed and not fired", reason)
        self.assertIn("no fault holds an item", reason)

    def test_the_budget_says_what_waits_too(self):
        status = self.status("run-stopped-by-its-budget")
        self.assertEqual(status.status, judge.INCONCLUSIVE)
        self.assertIn("0 deadlines armed and not fired", status.reasons[0])


if __name__ == "__main__":
    unittest.main()
