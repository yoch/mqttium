"""Finite session formats are descriptive, not new fixture qualification."""

import copy
import json
from types import SimpleNamespace
import unittest
from unittest import mock

from mqttiumd_oracle import recipe, results
from mqttiumd_oracle.documents import Contracts

from .support import ROOT
from .test_session_lifecycle import be, guard, scoped

PREFIX = b"MQDR\x00\x01\x08" + scoped(8)
OPERATION = {"incarnation": "1", "epoch": "2", "owner": "3", "sequence": "8"}


def policy(seconds):
    return bytes([2]) + be(seconds, 4)


def open_at(seconds=7, now=100):
    return PREFIX + b"\x02\x00" + policy(seconds) + b"\x01" + guard() + be(6, 2) + b"client" + be(now)


def detach(seconds=7, observed_at=100):
    return PREFIX + b"\x03" + guard() + policy(seconds) + be(observed_at)


def expire(deadline=7_000_000_100, now=7_000_000_100):
    return (PREFIX + b"\x04" + guard() + scoped(7) + bytes([deadline is not None])
            + (be(deadline) if deadline is not None else b"") + be(now))


class FiniteSessionRecipes(unittest.TestCase):
    def test_time_aware_open_has_its_own_canonical_action_and_finite_policy(self):
        for seconds in (1, 0xFFFF_FFFE):
            value = recipe.decode(open_at(seconds))
            self.assertEqual(value.action, "OpenAt")
            self.assertEqual(value.operation, (1, 2, 3, 8))
            self.assertEqual(value.expected, (9, (1, 2, 3, 4)))
            self.assertEqual(value.client_id, b"client")
            self.assertEqual(value.expiry.seconds, seconds)
            self.assertEqual(value.now, 100)

    def test_detach_preserves_its_observation_and_expire_preserves_the_occurrence(self):
        value = recipe.decode(detach())
        self.assertEqual((value.action, value.guard, value.observed_at), ("Detach", (9, (1, 2, 3, 4)), 100))
        self.assertEqual(value.expiry.seconds, 7)
        value = recipe.decode(expire())
        self.assertEqual(value.action, "Expire")
        self.assertEqual(value.detached_operation, (1, 2, 3, 7))
        self.assertEqual(value.deadline, 7_000_000_100)
        self.assertEqual(value.now, value.deadline)
        self.assertIsNone(recipe.decode(expire(None)).deadline)

    def test_new_actions_have_no_truncation_or_trailing_alias(self):
        for valid in (open_at(), detach(), expire()):
            for value in [valid[:cut] for cut in range(len(valid))] + [valid + b"\x00"]:
                with self.subTest(value=value.hex()), self.assertRaises(recipe.RecipeError):
                    recipe.decode(value)

    def test_invalid_finite_discriminants_and_deadline_overflow_are_refused(self):
        for value in (open_at(0), open_at(0xFFFF_FFFF), detach(0), detach(0xFFFF_FFFF),
                      detach(7, 0xFFFF_FFFF_FFFF_FFFF), PREFIX + b"\x05"):
            with self.subTest(value=value.hex()), self.assertRaises(recipe.RecipeError):
                recipe.decode(value)


class FiniteSessionFormats(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contracts = Contracts.open(ROOT)
        cls.snapshot = json.loads((ROOT / "contracts/examples/v1/valid/store-snapshot-after-commit.json").read_text())

    def finite_snapshot(self):
        value = copy.deepcopy(self.snapshot)
        value["sessions"][0].update(expiry={"After": 7}, opened_by=OPERATION,
            detached={"operation": OPERATION, "deadline": "7000000100"})
        value["meta"]["session_clock"] = {
            "reading": {"domain": "ab" * 32, "tick": "100"}, "logical": "100"}
        return value

    def test_finite_policy_detachment_and_one_anchor_are_readable(self):
        self.assertEqual(self.contracts.check_value(self.finite_snapshot()), [])

    def test_policy_domain_and_clock_fields_are_bounded_and_typed(self):
        for edit in (
            lambda v: v["sessions"][0].update(expiry={"After": 0}),
            lambda v: v["sessions"][0].update(expiry={"After": 0xFFFF_FFFF}),
            lambda v: v["sessions"][0].update(expiry={"After": 1, "extra": 1}),
            lambda v: v["meta"]["session_clock"]["reading"].update(domain="AB" * 32),
            lambda v: v["meta"]["session_clock"].update(utc="2026-01-01"),
        ):
            value = self.finite_snapshot()
            edit(value)
            self.assertTrue(self.contracts.check_value(value))
        for pointer, edit in (
            ("/sessions/0/detached/deadline", lambda v: v["sessions"][0]["detached"].update(deadline="18446744073709551616")),
            ("/meta/session_clock/reading/tick", lambda v: v["meta"]["session_clock"]["reading"].update(tick="18446744073709551616")),
            ("/meta/session_clock/logical", lambda v: v["meta"]["session_clock"].update(logical="18446744073709551616")),
        ):
            value = self.finite_snapshot()
            edit(value)
            self.assertEqual([(d.code, d.pointer) for d in self.contracts.check_value(value)],
                             [("u64-out-of-range", pointer)])

    def test_detached_product_result_and_checkpoint_events_have_exact_shapes(self):
        base = {"format": "mqttiumd.product-event", "schema_version": 1, "seq": "1", "pid": 1}
        values = [
            {**base, "kind": "session_changed", "operation": OPERATION,
             "details": {"action": "Detached", "session": "1", "expiry": {"After": 7},
                         "detached_by": OPERATION, "deadline": "7000000100"}},
            {**base, "kind": "session_clock_checkpoint", "details": {"logical": "100", "tick": "100"}},
            {**base, "kind": "session_clock_checkpoint", "details": {
                "logical": "100", "tick": "100", "same_domain": True, "source": "maintenance_commit"}},
            {**base, "kind": "session_clock_degraded", "details": {"code": "checkpoint_late", "age_ms": "31001"}},
            {**base, "kind": "expiry_inventory", "details": {
                "finite_timers": 128, "checkpoint_inflight": True, "queued": 4, "period_ms": 30000}},
        ]
        for value in values:
            self.assertEqual(self.contracts.check_value(value), [], value)
        invalid = copy.deepcopy(values[-1])
        invalid["details"]["finite_timers"] = 129
        self.assertTrue(self.contracts.check_value(invalid))

    def test_detached_metadata_and_clock_only_cannot_enlarge_qualification(self):
        def unmet(snapshot):
            run = SimpleNamespace(snapshots={"0": (None, snapshot)},
                trace=SimpleNamespace(records=(), before=lambda _left, _right: True))
            with mock.patch.object(results.judge.Run, "load", return_value=run):
                return results._scope_unmet(None, None, None)
        for snapshot in (
            {"sessions": [{"expiry": "Never", "detached": {"operation": OPERATION, "deadline": None}}]},
            {"meta": {"session_clock": {"reading": {"domain": "ab" * 32, "tick": "100"}, "logical": "100"}}},
        ):
            self.assertIn(results.FIXTURE_SESSION_SCOPE, unmet(snapshot))
