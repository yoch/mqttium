"""Typed read/control observations stay inspectable without becoming business PASS."""

import copy
import hashlib
import json
import unittest

from mqttiumd_oracle import judge, results

from . import test_judge_rules as rules
from . import test_results as existing_results
from .support import ROOT

corpus = rules.corpus
ACK = "no_positive_ack_before_confirmed_commit"
READS = "durable_reads_complete"


class ReadFailures(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        existing_results.Results.setUpClass.__func__(cls)

    setUp = existing_results.Results.setUp
    write = existing_results.Results.write
    refusals = existing_results.Results.refusals

    def trace_with_failures(self, kind="DurableReadFailed", *, early_ack=False,
                            incomplete=False, count=1, missing_core_record=False,
                            partial_recovery=False):
        events = corpus.p1_02()
        if early_ack:
            corpus.early_pubrec(events)
        if partial_recovery:
            for label in ("recovery.read", "recovery.in"):
                events[corpus.at(events, label)]["payload"]["last"] = False
        for index in range(count):
            payload = {
                "code": "InvalidData" if kind == "DurableReadFailed" else "TruncatedFrame",
                "phase": "Recovery" if kind == "DurableReadFailed" else "KillDrain",
                "process_incarnation": "2",
            }
            if kind == "DurableReadFailed":
                payload["page"] = "1"
            events.append(corpus.ev(f"failure.{index}", "bench-supervisor", kind, payload))
        states = corpus.p1_02_snapshots("in-memory")
        records = corpus.render("in-memory", events, states)
        for record in records:
            if record["kind"] == "DurableStateInspected":
                name = record["payload"]["checkpoint"]
                path = self.write(f"{name}.snapshot.json", states[name])
                data = (ROOT / path).read_bytes()
                record["payload"]["snapshot"] = {
                    "path": path, "sha256": hashlib.sha256(data).hexdigest(), "bytes": str(len(data))
                }
        if missing_core_record:
            # A genuinely missing local record can still contain the result
            # that justified the acknowledgement; it is not a proven FAIL.
            missing = next(record for record in records if record["kind"] == "PacketDelivered")
            records.remove(missing)
            for index, record in enumerate(records):
                record["record_sequence"] = str(index)
                record["time_value"] = str(index)
            records[-1]["payload"]["record_count"] = str(len(records))
        if incomplete:
            records[-1]["payload"]["complete"] = False
        path = f"{self.relative}/trace.jsonl"
        (ROOT / path).write_bytes(corpus.lines(records))
        return path, records

    def run_of(self, **options):
        path, _ = self.trace_with_failures(**options)
        scenario = self.write("scenario.json", corpus.P1_02)
        run = judge.Run.load(self.contracts, scenario, path)
        self.assertEqual(run.trace.unreadable, [])
        return path, run

    def produce(self, **options):
        path, _ = self.run_of(**options)
        scenario = self.write("scenario.json", corpus.P1_02)
        return results.produce(self.contracts, scenario, path, self.manifest,
                               scope="synthetic failure observations, not a broker execution")

    def test_read_and_control_records_are_typed_schema_values(self):
        for kind in ("DurableReadFailed", "ProcessControlFailed"):
            _, records = self.trace_with_failures(kind)
            failure = records[-2]
            self.assertEqual(self.contracts.check_value(failure), [])
            wrong = copy.deepcopy(failure)
            wrong["payload"]["code"] = "a textual exception is not a stable code"
            self.assertTrue(self.contracts.check_value(wrong))
            oversized = copy.deepcopy(failure)
            oversized["payload"]["message"] = "x" * 1025
            self.assertTrue(self.contracts.check_value(oversized))

    def test_a_failure_is_inconclusive_even_with_an_incorrect_complete_footer(self):
        for kind in ("DurableReadFailed", "ProcessControlFailed"):
            _, run = self.run_of(kind=kind)
            decided = judge.judge(run)
            self.assertEqual(decided[READS].status, "INCONCLUSIVE")

    def test_an_established_unjustified_ack_stays_fail_after_read_failure(self):
        _, run = self.run_of(early_ack=True, incomplete=True)
        self.assertEqual(judge.judge(run)[ACK].status, "FAIL")

    def test_a_missing_core_record_does_not_establish_an_unjustified_ack(self):
        _, run = self.run_of(early_ack=True, incomplete=True, missing_core_record=True)
        self.assertEqual(judge.judge(run)[ACK].status, "INCONCLUSIVE")

    def test_a_partial_recovery_does_not_authorize_a_positive_ack(self):
        _, run = self.run_of(partial_recovery=True, incomplete=True)
        self.assertEqual(judge.no_positive_ack_before_confirmed_commit(run).status, "FAIL")
        self.assertEqual(judge.judge(run)[ACK].status, "FAIL")

    def test_produce_preserves_the_typed_failure_context(self):
        produced = self.produce(incomplete=True)
        self.assertEqual(produced["verdict"], "INCONCLUSIVE")
        self.assertEqual(produced["failure_count"], "1")
        failure = produced["failures"][0]
        self.assertEqual({key: failure[key] for key in ("kind", "code", "phase", "process_incarnation", "page")}, {
            "kind": "DurableReadFailed", "code": "InvalidData", "phase": "Recovery",
            "process_incarnation": "2", "page": "1",
        })
        self.assertEqual(self.contracts.check_file(self.write("result.json", produced)), [])
        self.assertEqual(self.refusals(produced), [])

    def test_recompute_refuses_omitted_or_changed_typed_failure_context(self):
        produced = self.produce(incomplete=True)
        for mutation in ("omit", "code", "phase", "incarnation"):
            with self.subTest(mutation=mutation):
                changed = copy.deepcopy(produced)
                if mutation == "omit":
                    changed.pop("failures")
                    changed.pop("failure_count")
                else:
                    key, value = {"code": ("code", "Io"), "phase": ("phase", "Snapshot"),
                                  "incarnation": ("process_incarnation", "3")}[mutation]
                    changed["failures"][0][key] = value
                self.assertIn(("result-not-recomputed", "/failures"), self.refusals(changed))

    def test_failure_result_is_bounded_and_reports_the_total_count(self):
        produced = self.produce(count=65)
        self.assertEqual(produced["failure_count"], "65")
        self.assertEqual(len(produced["failures"]), 64)
        self.assertEqual(self.contracts.check_file(self.write("result.json", produced)), [])


if __name__ == "__main__":
    unittest.main()
