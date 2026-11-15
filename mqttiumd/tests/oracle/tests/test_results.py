"""A result is evidence only if the oracle finds it again.

A qualifying result is produced by the oracle from the observations. One
that is offered is recomputed from the files it cites, and refused when its
statuses or its injection counts differ.
"""

import copy
import hashlib
import json
import shutil
import unittest
from unittest import mock

from mqttiumd_oracle import results
from mqttiumd_oracle.documents import Contracts

from .support import ROOT

TRACES = "tests/oracle/traces"
SCENARIO = f"{TRACES}/scenarios/p1-02.json"
ACK = "no_positive_ack_before_confirmed_commit"


class Results(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contracts = Contracts.open(ROOT)

    def setUp(self):
        self.relative = f"target/oracle-tests/{self.id().rsplit('.', 1)[-1]}"
        self.directory = ROOT / self.relative
        shutil.rmtree(self.directory, ignore_errors=True)
        self.directory.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, self.directory, ignore_errors=True)
        manifest = json.loads(
            (ROOT / "contracts/examples/v1/valid/run-manifest-content-only.json").read_text()
        )
        manifest["contracts"] = [
            {"path": path, "sha256": sha256, "bytes": str(len((ROOT / path).read_bytes()))}
            for path, sha256 in sorted(self.contracts.files.items())
        ]
        manifest["contracts_digest"] = self.contracts.digest
        self.manifest = self.write("manifest.json", manifest)

    def write(self, name, value):
        (self.directory / name).write_text(json.dumps(value))
        return f"{self.relative}/{name}"

    def produced(self, trace, scenario=SCENARIO):
        return results.produce(
            self.contracts,
            scenario,
            f"{TRACES}/{trace}/trace.jsonl",
            self.manifest,
            scope="P1-02 on the hand-made corpus",
        )

    def refusals(self, result):
        found = results.recompute(self.contracts, self.write("result.json", result))
        return sorted({(diagnostic.code, diagnostic.pointer) for diagnostic in found})

    def statuses(self, result):
        return {entry["name"]: entry["status"] for entry in result["assertions"]}

    def test_a_good_run_gives_a_qualifying_pass_that_stands(self):
        result = self.produced("p1-02-good")
        self.assertEqual(result["qualification"], "qualifying")
        self.assertEqual((result["verdict"], result["exit_code"]), ("PASS", 0))
        self.assertEqual(result["run_id"], "p1-02-good")
        self.assertEqual(set(self.statuses(result).values()), {"PASS"})
        self.assertEqual(
            result["faults"], [{"id": "lost_admission_result", "expected_hits": "1", "hits": "1"}]
        )
        self.assertEqual(self.contracts.check_file(self.write("result.json", result)), [])
        self.assertEqual(self.refusals(result), [])

    def test_a_faulty_run_gives_a_fail_with_its_reason(self):
        result = self.produced("pubrec-eligible-on-a-hidden-commit")
        self.assertEqual((result["verdict"], result["exit_code"]), ("FAIL", 1))
        self.assertEqual(self.statuses(result)[ACK], "FAIL")
        self.assertTrue(any(ACK in reason for reason in result["reasons"]), result["reasons"])
        self.assertEqual(self.refusals(result), [])

    def test_an_incomplete_trace_gives_an_inconclusive_result(self):
        result = self.produced("trace-cut-before-footer")
        self.assertEqual((result["verdict"], result["exit_code"]), ("INCONCLUSIVE", 2))
        self.assertEqual(self.refusals(result), [])

    def test_a_missed_injection_count_is_inconclusive(self):
        result = self.produced("fault-hit-twice")
        self.assertEqual(result["verdict"], "INCONCLUSIVE")
        self.assertEqual(result["faults"][0]["hits"], "2")

    def test_a_missed_injection_blocks_the_pass_even_if_no_assertion_watches_it(self):
        scenario = json.loads((ROOT / SCENARIO).read_text())
        scenario["assertions"].remove("all_required_faults_hit")
        result = self.produced("fault-hit-twice", self.write("scenario.json", scenario))
        self.assertEqual(set(self.statuses(result).values()), {"PASS"})
        self.assertEqual((result["verdict"], result["exit_code"]), ("INCONCLUSIVE", 2))
        self.assertEqual(self.contracts.check_file(self.write("result.json", result)), [])

    def test_a_result_whose_verdict_contradicts_its_statuses_is_refused_as_a_document(self):
        result = self.produced("p1-02-good")
        result["verdict"], result["exit_code"] = "FAIL", 1
        self.assertEqual(self.refusals(result), [("verdict-inconsistent", "/verdict")])

    def passing_claim(self, trace):
        """A result that cites ``trace`` and claims what a good run would."""
        result = self.produced(trace)
        claim = copy.deepcopy(self.produced("p1-02-good"))
        for member in ("trace", "run_id"):
            claim[member] = result[member]
        return claim

    def test_a_pass_claimed_on_a_faulty_trace_is_refused_where_it_differs(self):
        claim = self.passing_claim("pubrec-eligible-on-a-hidden-commit")
        # The document itself is consistent: only the recomputation tells.
        self.assertEqual(self.contracts.check_file(self.write("claim.json", claim)), [])
        index = [entry["name"] for entry in claim["assertions"]].index(ACK)
        self.assertEqual(
            self.refusals(claim), [("result-not-recomputed", f"/assertions/{index}/status")]
        )

    def test_a_pass_claimed_on_a_cut_trace_is_refused(self):
        claim = self.passing_claim("trace-cut-before-footer")
        refused = self.refusals(claim)
        self.assertEqual({code for code, _ in refused}, {"result-not-recomputed"})
        self.assertEqual(len(refused), len(claim["assertions"]))

    def test_an_injection_count_that_differs_is_refused(self):
        claim = self.passing_claim("fault-hit-twice")
        self.assertIn(("result-not-recomputed", "/faults/0/hits"), self.refusals(claim))

    def test_a_softened_failure_is_refused(self):
        # FAIL reported as INCONCLUSIVE: consistent as a document, not what
        # the observations determine.
        result = self.produced("pubrec-eligible-on-a-hidden-commit")
        for entry in result["assertions"]:
            if entry["name"] == ACK:
                entry["status"] = "INCONCLUSIVE"
        result["verdict"], result["exit_code"] = "INCONCLUSIVE", 2
        self.assertEqual({code for code, _ in self.refusals(result)}, {"result-not-recomputed"})

    def test_a_result_of_another_run_than_its_trace_is_refused(self):
        result = self.produced("p1-02-good")
        result["run_id"] = "another-run"
        self.assertEqual(self.refusals(result), [("evidence-mismatch", "/run_id")])

    def test_a_cited_trace_is_hashed_as_a_stream(self):
        # Evidence has no size bound: recomputing never loads the trace whole.
        result = self.produced("p1-02-good")
        path = self.write("result.json", result)
        calls = []
        original = self.contracts.hash_evidence

        def counting(relative):
            calls.append(relative)
            return original(relative)

        with mock.patch.object(self.contracts, "hash_evidence", side_effect=counting):
            self.assertEqual(results.recompute(self.contracts, path), [])
        self.assertEqual(calls.count(result["trace"]["path"]), 2, calls)

    def test_a_provisional_result_is_not_evidence(self):
        result = self.produced("p1-02-good")
        result["qualification"] = "provisional"
        result["verdict"], result["exit_code"] = "INCONCLUSIVE", 2
        for member in ("scenario", "manifest", "trace", "contracts_digest"):
            del result[member]
        self.assertEqual(self.refusals(result), [("result-not-qualifying", "/qualification")])

    def test_a_result_that_is_not_a_valid_document_is_refused_as_such(self):
        result = self.produced("p1-02-good")
        result["trace"]["sha256"] = hashlib.sha256(b"other").hexdigest()
        self.assertEqual(self.refusals(result), [("evidence-mismatch", "/trace")])


if __name__ == "__main__":
    unittest.main()
