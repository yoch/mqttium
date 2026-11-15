"""The command line exits with the code of the verdict it determined."""

import contextlib
import io
import json
import unittest
from unittest import mock

from mqttiumd_oracle.__main__ import ORACLE_DEFECT, main

from .support import ROOT

TRACES = "tests/oracle/traces"


def run(*arguments):
    output = io.StringIO()
    with contextlib.redirect_stdout(output), contextlib.redirect_stderr(io.StringIO()):
        code = main(["--root", str(ROOT), *arguments])
    return code, output.getvalue()


class CommandLine(unittest.TestCase):
    def judge(self, trace, scenario="p1-02"):
        return run(
            "judge",
            "--scenario",
            f"{TRACES}/scenarios/{scenario}.json",
            "--trace",
            f"{TRACES}/{trace}/trace.jsonl",
        )

    def test_judge_exits_with_the_code_of_the_verdict(self):
        for trace, scenario, verdict, code in [
            ("p1-02-good", "p1-02", "PASS", 0),
            ("obligation-destination-changed", "p1-02", "FAIL", 1),
            ("trace-cut-before-footer", "p1-02", "INCONCLUSIVE", 2),
            ("fault-hit-twice", "p1-02", "INCONCLUSIVE", 2),
            ("p1-02b-good", "p1-02b", "PASS", 0),
            ("core-without-bytes-against-the-full-scenario", "p1-02", "OUT_OF_SCOPE", 3),
        ]:
            with self.subTest(trace):
                found, output = self.judge(trace, scenario)
                report = json.loads(output)
                self.assertEqual((report["verdict"], found), (verdict, code))
                self.assertEqual(len(report["contracts"]), len("sha256:") + 64)

    def test_a_trace_that_does_not_exist_concludes_nothing(self):
        self.assertEqual(self.judge("absent")[0], 2)

    def test_judge_preserves_stable_failure_codes_and_context(self):
        for name, kind, code, phase in [
            ("durable-read-failure", "DurableReadFailed", "InvalidData", "Recovery"),
            ("process-control-failure", "ProcessControlFailed", "TruncatedFrame", "KillDrain"),
        ]:
            with self.subTest(name):
                found, output = self.judge(name)
                report = json.loads(output)
                self.assertEqual((found, report["verdict"]), (2, "INCONCLUSIVE"))
                self.assertEqual(report["failure_count"], "1")
                failure = report["failures"][0]
                self.assertEqual({key: failure[key] for key in ("kind", "code", "phase", "process_incarnation")}, {
                    "kind": kind, "code": code, "phase": phase, "process_incarnation": "2",
                })

    # A defect of the oracle has a code of its own: the exit status 1 means
    # FAIL, and a run that was never judged must not be recorded as one.

    def test_contracts_that_cannot_be_read_are_not_a_verdict(self):
        code, _ = run(
            "--contracts",
            "contracts/v2",
            "judge",
            "--scenario",
            f"{TRACES}/scenarios/p1-02.json",
            "--trace",
            f"{TRACES}/p1-02-good/trace.jsonl",
        )
        self.assertEqual(code, ORACLE_DEFECT)
        self.assertNotIn(ORACLE_DEFECT, (0, 1, 2, 3))

    def test_a_defect_while_judging_is_not_a_verdict(self):
        with mock.patch("mqttiumd_oracle.results._observed", side_effect=TypeError("defect")):
            self.assertEqual(self.judge("p1-02-good")[0], ORACLE_DEFECT)

    def test_a_defect_while_recomputing_is_not_a_verdict(self):
        with mock.patch("mqttiumd_oracle.results.recompute", side_effect=KeyError("defect")):
            code, _ = run("recompute", "--result", "contracts/v1/profiles.json")
        self.assertEqual(code, ORACLE_DEFECT)

    def test_a_result_that_cannot_be_recomputed_is_refused_not_failed(self):
        code, _ = run("recompute", "--result", "contracts/v1/profiles.json")
        self.assertEqual(code, 2)


if __name__ == "__main__":
    unittest.main()
