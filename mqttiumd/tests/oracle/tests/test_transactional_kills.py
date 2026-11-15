"""Independent checks of deterministic transaction crash observations."""

import copy
import unittest

from mqttiumd_oracle import judge

from . import test_judge_rules as rules

corpus = rules.corpus
ASSERTION = "transactional_kill_matches_recovery"


class TransactionalKills(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        rules.Rules.setUpClass.__func__(cls)

    run_of = rules.Rules.run_of

    def run_at(self, point, edit=lambda events: None):
        def history(events):
            if point == "store.commit.applied":
                corpus.applied_then_killed(events)
            else:
                corpus.durable_without_its_record(events)
            hit = events[corpus.at(events, "hit.lost_admission_result")]
            hit["payload"]["point"] = point
            if point == "store.commit.prepared":
                for label in ("recovery.read", "recovery.in"):
                    page = events[corpus.at(events, label)]
                    page["payload"]["facts"] = [
                        fact for fact in page["payload"]["facts"]
                        if fact["kind"] not in ("Operation", "InboundExchange")
                    ]
            edit(events)

        run = self.run_of(history)
        run.scenario = copy.deepcopy(run.scenario)
        run.scenario["faults"][0].update(point=point, action="barrier")
        return run

    def decide(self, run):
        decide = judge.ASSERTIONS.get(ASSERTION)
        self.assertIsNotNone(decide, "the transaction crash assertion must be implemented")
        return decide(run)

    def test_k1_has_no_commit_of_the_interrupted_command(self):
        self.assertEqual(self.decide(self.run_at("store.commit.prepared")).status, "PASS")

    def test_k2b_recovers_commit_without_stable_record(self):
        self.assertEqual(self.decide(self.run_at("store.commit.logged")).status, "PASS")

    def test_k3_recovers_the_stable_command_before_any_result(self):
        self.assertEqual(self.decide(self.run_at("store.commit.applied")).status, "PASS")

    def test_k1_detects_an_interrupted_operation_that_was_recovered(self):
        def edit(events):
            fact = {"kind": "Operation", "operation_id": corpus.OP_ADMIT, "knowledge": "Committed"}
            for label in ("recovery.read", "recovery.in"):
                events[corpus.at(events, label)]["payload"]["facts"].append(copy.deepcopy(fact))

        result = self.decide(self.run_at("store.commit.prepared", edit))
        self.assertEqual(result.status, "FAIL")
        self.assertTrue(any("was recovered at K1" in reason for reason in result.reasons))

    def test_k2b_requires_its_committed_operation_at_recovery(self):
        def edit(events):
            for label in ("recovery.read", "recovery.in"):
                page = events[corpus.at(events, label)]
                page["payload"]["facts"] = [
                    fact for fact in page["payload"]["facts"] if fact["kind"] != "Operation"
                ]

        result = self.decide(self.run_at("store.commit.logged", edit))
        self.assertEqual(result.status, "FAIL")
        self.assertTrue(any("was not recovered Committed" in reason for reason in result.reasons))

    def test_each_point_detects_a_fault_hit_at_another_declared_point(self):
        for point in ("store.commit.prepared", "store.commit.logged", "store.commit.applied"):
            with self.subTest(point=point):
                def edit(events):
                    events[corpus.at(events, "hit.lost_admission_result")]["payload"]["point"] = "store.command.transit"

                result = self.decide(self.run_at(point, edit))
                self.assertEqual(result.status, "FAIL")
                self.assertTrue(any("declared point" in reason for reason in result.reasons))

    def test_k3_requires_the_interrupted_command_stable_record(self):
        def edit(events):
            del events[corpus.at(events, "admit.stable")]
            events[corpus.at(events, "hit.lost_admission_result")]["parents"] = ["admit.submitted"]

        result = self.decide(self.run_at("store.commit.applied", edit))
        self.assertEqual(result.status, "FAIL")
        self.assertTrue(any("no StoreCommitStable" in reason for reason in result.reasons))

    def test_missing_completed_recovery_is_inconclusive(self):
        def edit(events):
            for label in ("recovery.read", "recovery.in"):
                events[corpus.at(events, label)]["payload"]["last"] = False

        self.assertEqual(self.decide(self.run_at("store.commit.logged", edit)).status, "INCONCLUSIVE")


if __name__ == "__main__":
    unittest.main()
