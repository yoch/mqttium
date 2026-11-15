"""The command line exits with the code of the verdict it determined."""

import contextlib
import io
import json
import unittest

from mqttiumd_oracle.__main__ import main

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
            ("p1-02b-good", "p1-02b", "OUT_OF_SCOPE", 3),
        ]:
            with self.subTest(trace):
                found, output = self.judge(trace, scenario)
                report = json.loads(output)
                self.assertEqual((report["verdict"], found), (verdict, code))
                self.assertEqual(len(report["contracts"]), len("sha256:") + 64)

    def test_a_trace_that_does_not_exist_concludes_nothing(self):
        self.assertEqual(self.judge("absent")[0], 2)


if __name__ == "__main__":
    unittest.main()
