"""Synthetic receipt-retirement controls, not live-maintenance qualification.

The fixture qualifier must not silently inherit the product's maintenance
semantics. A frontier describes unavailable history, never certain rejection.
"""

import copy
import json
import sys
from types import SimpleNamespace
import unittest
from unittest import mock

from mqttiumd_oracle import judge, results
from mqttiumd_oracle.documents import Contracts
from mqttiumd_oracle.strict import parse
from mqttiumd_oracle.trace import read

from .support import ORACLE, ROOT

sys.path.insert(0, str(ORACLE))
import build_traces as corpus  # noqa: E402

SCOPE = "the fixture-based qualifier does not cover live receipt retirement"


def frontier(through="1", *, owner="0"):
    return {"origin": {"incarnation": "1", "epoch": "1", "owner": owner},
            "through": through, "publication_through": through, "payload_through": through}


class ReceiptScope(unittest.TestCase):
    def unmet(self, *, snapshots=(), records=()):
        run = SimpleNamespace(
            snapshots={str(index): value for index, value in enumerate(snapshots)},
            trace=SimpleNamespace(records=records, before=lambda _left, _right: True),
        )
        with mock.patch.object(results.judge.Run, "load", return_value=run):
            return results._scope_unmet(None, None, None)

    def test_positive_snapshot_frontier_cannot_extend_fixture_qualification(self):
        snapshot = {"outbound": [], "operations": [], "receipt_frontiers": [frontier()]}
        self.assertEqual(self.unmet(snapshots=[(None, snapshot)]), [SCOPE])

    def test_positive_recovery_frontier_cannot_extend_fixture_qualification(self):
        fact = {"kind": "ReceiptFrontier", **frontier()}
        record = SimpleNamespace(kind="RecoveryPageDelivered", payload={"facts": [fact]})
        self.assertEqual(self.unmet(records=[record]), [SCOPE])

    def test_empty_or_zero_frontier_does_not_assert_live_collection(self):
        for rows in ([], [frontier("0")]):
            with self.subTest(rows=rows):
                snapshot = {"outbound": [], "operations": [], "receipt_frontiers": rows}
                self.assertEqual(self.unmet(snapshots=[(None, snapshot)]), [])

    def test_positive_identity_boundary_is_not_an_empty_registration(self):
        # A descriptive snapshot can contain inconsistent business data.
        # through=0 must not hide newly introduced identity-retirement facts.
        row = {**frontier("0"), "publication_through": "1"}
        snapshot = {"outbound": [], "operations": [], "receipt_frontiers": [row]}
        self.assertEqual(self.unmet(snapshots=[(None, snapshot)]), [SCOPE])


class HistoricalRecipe(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contracts = Contracts.open(ROOT)
        cls.fixture = parse((ROOT / "tests/fixtures/p1/three-persistent-sessions.json").read_bytes())

    def run_of(self, *, protected=True, owner="0", conflict=False):
        events = corpus.p1_02()
        states = corpus.p1_02_snapshots("receipt-history")
        # The first checkpoint independently retains the exact historical
        # recipe. Collection is represented only at the later checkpoint.
        final = states["final"]
        removed = final["operations"].pop(0)
        if protected:
            final["receipt_frontiers"] = [frontier(owner=owner)]
        if conflict:
            earlier = states["after_commit_before_crash"]["operations"][0]
            duplicate = copy.deepcopy(earlier)
            duplicate["recipe_hex"] += "00"
            final["operations"].append(duplicate)
        records = corpus.render("receipt-history", events, states)
        trace = read(self.contracts, [json.dumps(row).encode() + b"\n" for row in records])
        self.assertEqual(trace.unreadable, [])
        run = judge.Run(self.contracts, corpus.P1_02, self.fixture, trace)
        for record in trace.of_kind("DurableStateInspected"):
            name = record.payload["checkpoint"]
            run.snapshots[name] = (record, states[name])
        self.assertEqual(removed["operation_id"], corpus.OP_ADMIT)
        return run

    def test_collected_historical_recipe_is_unavailable_not_business_failure(self):
        status = judge.same_recipe_for_op_A(self.run_of())
        self.assertEqual(status.status, "INCONCLUSIVE", status.reasons)
        self.assertTrue(any("retired" in reason for reason in status.reasons), status.reasons)

    def test_another_origin_cannot_explain_a_missing_recipe(self):
        self.assertEqual(judge.same_recipe_for_op_A(self.run_of(owner="1")).status, "FAIL")

    def test_unprotected_disappearance_remains_a_failure(self):
        self.assertEqual(judge.same_recipe_for_op_A(self.run_of(protected=False)).status, "FAIL")

    def test_retirement_does_not_hide_conflicting_retained_recipes(self):
        status = judge.same_recipe_for_op_A(self.run_of(conflict=True))
        self.assertEqual(status.status, "FAIL", status.reasons)
        self.assertTrue(any("different contents" in reason for reason in status.reasons))

    def test_frontier_does_not_become_a_rejection_receipt(self):
        run = self.run_of()
        self.assertNotIn(tuple(int(corpus.OP_ADMIT[name]) for name in (
            "incarnation", "epoch", "owner", "sequence")), judge._closures(run))
