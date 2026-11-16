"""Descriptive lifecycle formats, never fixture-based lifecycle qualification.

Canonical kind8 is independent of the product implementation. Legacy fixture
inventories omit policy/attachment; those omissions mean Never/None only.
"""

import copy
import json
from types import SimpleNamespace
import unittest
from unittest import mock

from mqttiumd_oracle import recipe, results
from mqttiumd_oracle.documents import Contracts

from .support import ROOT


def be(value, width=8):
    return value.to_bytes(width, "big")


def scoped(sequence):
    return b"".join(be(value) for value in (1, 2, 3, sequence))


def guard(session=9, opened_by=4):
    return be(session) + bytes([opened_by is not None]) + (scoped(opened_by) if opened_by is not None else b"")


def opened(*, clean=False, expiry=0, expected=None, client=b"reusable-name"):
    return (b"MQDR\x00\x01\x08" + scoped(8) + b"\x00" + bytes([clean, expiry, expected is not None])
            + (expected or b"") + be(len(client), 2) + client)


class SessionRecipes(unittest.TestCase):
    def test_kind8_open_preserves_policy_attachment_and_exact_client(self):
        for clean in (False, True):
            for expiry, policy in ((0, "OnDisconnect"), (1, "Never")):
                for expected, expected_guard in ((None, None), (guard(opened_by=None), (9, None)),
                                                 (guard(), (9, (1, 2, 3, 4)))):
                    with self.subTest(clean=clean, expiry=expiry, expected=expected_guard):
                        decoded = recipe.decode(opened(clean=clean, expiry=expiry, expected=expected))
                        self.assertEqual(decoded.kind, "SessionLifecycle")
                        self.assertEqual(decoded.operation, (1, 2, 3, 8))
                        self.assertEqual(decoded.action, "Open")
                        self.assertEqual(decoded.client_id, b"reusable-name")
                        self.assertEqual(decoded.clean_start, clean)
                        self.assertEqual(decoded.expiry, policy)
                        self.assertEqual(decoded.expected, expected_guard)

    def test_close_retains_original_guard_and_reason_without_client_lookup(self):
        for code, reason in enumerate(("Disconnect", "TransportLost", "Replaced", "Startup")):
            with self.subTest(reason=reason):
                decoded = recipe.decode(b"MQDR\x00\x01\x08" + scoped(8) + b"\x01" + guard() + bytes([code]))
                self.assertEqual(decoded.action, "Close")
                self.assertEqual(decoded.guard, (9, (1, 2, 3, 4)))
                self.assertEqual(decoded.reason, reason)
                self.assertFalse(hasattr(decoded, "client_id"))

    def test_every_truncation_and_trailing_byte_is_rejected(self):
        valid = opened(clean=True, expiry=1, expected=guard())
        for value in [valid[:cut] for cut in range(len(valid))] + [valid + b"\x00"]:
            with self.subTest(data=value.hex()), self.assertRaises(recipe.RecipeError):
                recipe.decode(value)

    def test_policy_booleans_client_bytes_and_reason_are_closed_and_bounded(self):
        prefix = b"MQDR\x00\x01\x08" + scoped(8)
        invalid = [opened(expiry=2), opened(client=b""), opened(client=b"a\x00b"),
                   opened(client=b"\xff"), opened(client=b"x" * 1025),
                   prefix + b"\x00\x02\x00\x00\x00\x01x",
                   prefix + b"\x00\x00\x00\x02\x00\x01x",
                   prefix + b"\x01" + guard() + b"\x04"]
        for value in invalid:
            with self.subTest(data=value[:48].hex()), self.assertRaises(recipe.RecipeError):
                recipe.decode(value)


class SessionFormats(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contracts = Contracts.open(ROOT)
        cls.snapshot = json.loads((ROOT / "contracts/examples/v1/valid/store-snapshot-after-commit.json").read_text())

    def test_explicit_session_policy_and_attachment_are_descriptive(self):
        value = copy.deepcopy(self.snapshot)
        value["sessions"][0].update(expiry="OnDisconnect", opened_by={
            "incarnation": "1", "epoch": "1", "owner": "0", "sequence": "8"})
        self.assertEqual(self.contracts.check_value(value), [])

    def test_legacy_session_policy_omissions_remain_readable(self):
        legacy = json.loads((ROOT / "contracts/examples/v1/valid/store-snapshot-legacy-session-defaults.json").read_text())
        self.assertNotIn("expiry", legacy["sessions"][0])
        self.assertNotIn("opened_by", legacy["sessions"][0])
        self.assertEqual(self.contracts.check_value(legacy), [])

    def test_opening_attachment_components_remain_u64(self):
        value = copy.deepcopy(self.snapshot)
        value["sessions"][0].update(expiry="Never", opened_by={
            "incarnation": "1", "epoch": "1", "owner": "0", "sequence": "18446744073709551616"})
        diagnostics = self.contracts.check_value(value)
        self.assertEqual([(item.code, item.pointer) for item in diagnostics],
                         [("u64-out-of-range", "/sessions/0/opened_by/sequence")])


class SessionQualificationScope(unittest.TestCase):
    def unmet(self, *, snapshots=(), records=()):
        run = SimpleNamespace(snapshots={str(index): (None, value) for index, value in enumerate(snapshots)},
                              trace=SimpleNamespace(records=records, before=lambda _left, _right: True))
        with mock.patch.object(results.judge.Run, "load", return_value=run):
            return results._scope_unmet(None, None, None)

    def snapshot(self):
        return {"outbound": [], "operations": [], "sessions": [], "receipt_frontiers": []}

    def assert_scope(self, found):
        self.assertIn("the fixture-based qualifier does not cover product session lifecycle", found)

    def test_new_policy_or_attachment_cannot_extend_qualification(self):
        for session in ({"expiry": "OnDisconnect"}, {"opened_by": {"sequence": "1"}}):
            with self.subTest(session=session):
                snapshot = self.snapshot()
                snapshot["sessions"] = [session]
                self.assert_scope(self.unmet(snapshots=[snapshot]))

    def test_lifecycle_receipt_cannot_extend_qualification(self):
        snapshot = self.snapshot()
        snapshot["operations"] = [{"recipe_hex": opened().hex(), "decision": {"kind": "Committed", "facts": {
            "kind": "SessionLifecycle"}}}]
        self.assert_scope(self.unmet(snapshots=[snapshot]))

    def test_lifecycle_command_is_readable_but_cannot_extend_qualification(self):
        record = json.loads((ROOT / "contracts/examples/v1/valid/trace-record-store-commit-stable.json").read_text())
        record["payload"]["command_kind"] = "SessionLifecycle"
        self.assertEqual(Contracts.open(ROOT).check_value(record), [])
        self.assert_scope(self.unmet(records=[SimpleNamespace(kind=record["kind"], payload=record["payload"])]))

    def test_lifecycle_recovery_fact_cannot_extend_qualification(self):
        record = SimpleNamespace(kind="RecoveryPageDelivered", payload={"facts": [{
            "kind": "Session", "expiry": "OnDisconnect", "opened_by": None}]})
        self.assert_scope(self.unmet(records=[record]))

    def test_legacy_and_explicit_legacy_defaults_do_not_claim_lifecycle(self):
        for session in ({}, {"expiry": "Never", "opened_by": None}):
            snapshot = self.snapshot()
            snapshot["sessions"] = [session]
            self.assertEqual(self.unmet(snapshots=[snapshot]), [])


class SessionCorpus(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contracts = Contracts.open(ROOT)

    def example(self, name):
        return json.loads((ROOT / f"contracts/examples/v1/valid/{name}.json").read_text())

    def test_generated_open_and_close_use_the_same_descriptive_kind8_layout(self):
        opened_snapshot = self.example("store-snapshot-session-open-receipt")
        close_snapshot = self.example("store-snapshot-session-close-receipt")
        decoded = [recipe.decode(bytes.fromhex(value["operations"][-1]["recipe_hex"]))
                   for value in (opened_snapshot, close_snapshot)]
        self.assertEqual(decoded[0].client_id, b"new-client")
        self.assertEqual((decoded[0].clean_start, decoded[0].expiry, decoded[0].expected),
                         (True, "OnDisconnect", None))
        self.assertEqual((decoded[1].guard, decoded[1].reason), ((1, None), "Disconnect"))

    def test_historical_producer_identity_can_outlive_its_row_for_other_destinations(self):
        value = self.example("store-snapshot-session-close-receipt")
        self.assertEqual(self.contracts.check_value(value), [])
        self.assertNotIn("1", [row["session"] for row in value["sessions"]])
        self.assertEqual(value["publications"][0]["source_session"], "1")
        self.assertLess(int(value["publications"][0]["source_session"]), int(value["meta"]["next_session"]))
        self.assertEqual([row["session"] for row in value["obligations"]], ["2", "3"])
        self.assertEqual(value["payloads"][0]["references"], "2")

    def test_optional_expected_identity_is_bounded_and_contextual(self):
        value = self.example("product-session-open-submitted")
        self.assertEqual(self.contracts.check_value(value), [])
        value["details"]["expected_session"] = "18446744073709551616"
        self.assertEqual([(d.code, d.pointer) for d in self.contracts.check_value(value)],
                         [("u64-out-of-range", "/details/expected_session")])
        value["details"]["expected_session"] = None
        value["kind"] = "runtime_refused"
        self.assertTrue(self.contracts.check_value(value))

    def test_session_refusal_cannot_fabricate_a_durable_identity(self):
        value = self.example("product-session-refused")
        value["details"]["session"] = "1"
        self.assertTrue(self.contracts.check_value(value))

    def test_session_cardinality_requires_complete_bounded_counts(self):
        value = self.example("product-session-cardinality")
        self.assertEqual(self.contracts.check_value(value), [])
        for edit in (lambda v: v["details"].pop("retired_flights"),
                     lambda v: v["details"].update(owner_sessions=4294967296)):
            invalid = copy.deepcopy(value)
            edit(invalid)
            self.assertTrue(self.contracts.check_value(invalid))
