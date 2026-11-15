"""Admission identities across StoreCommitStable and post-kill recovery.

These histories are hand-made from contracts, never from core decisions.
"""

import copy
import unittest

from mqttiumd_oracle import judge

from . import test_judge_rules as rules

ADMISSION, corpus = rules.ADMISSION, rules.corpus


class RecoveredAdmissions(unittest.TestCase):
    """The two evidence sources are reconciled by durable OperationId."""

    @classmethod
    def setUpClass(cls):
        rules.Rules.setUpClass.__func__(cls)

    run_of = rules.Rules.run_of

    def recovered(self, edit=lambda events: None, edit_states=lambda states: None):
        def history(events):
            corpus.durable_without_its_record(events)
            edit(events)

        return judge.judge(self.run_of(history, edit_states))[ADMISSION]

    def test_an_admission_visible_only_at_recovery_counts(self):
        self.assertEqual(self.recovered().status, "PASS")

    def test_the_same_admission_in_both_sources_counts_once(self):
        run = self.run_of(corpus.applied_then_killed)
        self.assertEqual(judge.judge(run)[ADMISSION].status, "PASS")

    def test_distinct_durable_admissions_of_one_exchange_are_a_violation(self):
        other = corpus.scoped(2, 7)

        def edit(events):
            corpus.insert_after(
                events,
                "replay.in",
                corpus.committed("second", 2, 7, other, "AdmitPublication", cause="replay.in")[:1],
            )

        def states(snapshots):
            snapshots["final"]["operations"].append(
                {
                    "operation_id": other,
                    "recipe_hex": corpus.admit_recipe(other, other, other, 1),
                    "decision": {
                        "kind": "Committed",
                        "facts": {
                            "kind": "PublicationAdmitted",
                            "publication_id": other,
                            "exchange": {"session": "1", "generation": "1"},
                        },
                    },
                }
            )

        result = self.recovered(edit, states)
        self.assertEqual(result.status, "FAIL")
        self.assertTrue(any("2 admissions" in reason for reason in result.reasons))

    def test_an_admission_of_another_exchange_cannot_supply_this_exchange(self):
        # The recipe's Packet Identifier follows the ids and session/revision.
        offset = 2 * (4 + 2 + 1 + 3 * 32 + 8 + 8)

        def states(snapshots):
            for snapshot in snapshots.values():
                for operation in snapshot["operations"]:
                    if operation["operation_id"] == corpus.OP_ADMIT:
                        recipe = operation["recipe_hex"]
                        operation["recipe_hex"] = recipe[:offset] + "0008" + recipe[offset + 4 :]

        result = self.recovered(edit_states=states)
        self.assertEqual(result.status, "FAIL")
        self.assertTrue(any("has no admission" in reason for reason in result.reasons))

    def test_missing_recovery_evidence_cannot_supply_an_admission(self):
        def edit(events):
            for label in ("recovery.read", "recovery.in"):
                payload = events[corpus.at(events, label)]["payload"]
                payload["facts"] = [fact for fact in payload["facts"] if fact["kind"] != "Operation"]

        result = self.recovered(edit)
        self.assertEqual(result.status, "FAIL")
        self.assertTrue(any("has no admission" in reason for reason in result.reasons))

    def test_recovery_commit_contradicting_durable_decision_is_not_accepted(self):
        def states(snapshots):
            for snapshot in snapshots.values():
                for operation in snapshot["operations"]:
                    if operation["operation_id"] == corpus.OP_ADMIT:
                        operation["decision"] = {"kind": "RejectedCertain", "reason": "QuotaExceeded"}

        result = self.recovered(edit_states=states)
        self.assertEqual(result.status, "FAIL")
        self.assertTrue(any("contradict" in reason for reason in result.reasons))

    def test_recovery_without_the_page_read_does_not_supply_an_admission(self):
        def edit(events):
            del events[corpus.at(events, "recovery.read")]
            events[corpus.at(events, "recovery.in")]["parents"] = []

        result = self.recovered(edit)
        self.assertEqual(result.status, "INCONCLUSIVE")
        self.assertTrue(any("page" in reason for reason in result.reasons))

    def test_repeated_recovery_observation_counts_the_identity_once(self):
        def edit(events):
            read = copy.deepcopy(events[corpus.at(events, "recovery.read")])
            delivered = copy.deepcopy(events[corpus.at(events, "recovery.in")])
            read["label"] = "again.read"
            delivered["label"] = "again.in"
            delivered["parents"] = ["again.read"]
            corpus.insert_after(events, "recovery.in", [read, delivered])

        self.assertEqual(self.recovered(edit).status, "PASS")


if __name__ == "__main__":
    unittest.main()
