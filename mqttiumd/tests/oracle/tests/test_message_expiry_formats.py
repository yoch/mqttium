"""Independent lifetime readers and explicit limits of historical qualification."""
from copy import deepcopy
import json
from types import SimpleNamespace
import unittest
from unittest import mock

from mqttiumd_oracle import recipe, results
from mqttiumd_oracle.documents import Contracts

from .support import ROOT
from .test_retained_formats import admission, be, header, qos0, retained_admission, scoped, subscription


def message_time(tick=100, domain=b"\xab" * 32):
    return domain + be(tick)


def lifetime(interval=2, tick=100, domain=b"\xab" * 32):
    return message_time(tick, domain) + be(interval, 4)


def expiring(nested=None, *, interval=2, tick=100):
    nested = admission(retain=0) if nested is None else nested
    return header(13) + be(len(nested), 4) + nested + lifetime(interval, tick)


def replay_at():
    nested = subscription()
    return header(14) + be(len(nested), 4) + nested + message_time()


def started():
    return header(15) + scoped(22) + scoped(40) + be(0, 4) + be(2) + be(1) + be(7, 2) + scoped(21)


def expired(*, now=2_000_000_100, domain=b"\xab" * 32):
    return header(16) + started()[7:] + lifetime() + message_time(now, domain)


class MessageExpiryRecipes(unittest.TestCase):
    def test_new_envelopes_preserve_each_legacy_child_and_the_original_time(self):
        for nested in (admission(retain=0), retained_admission(), qos0()):
            value = recipe.decode(expiring(nested))
            self.assertEqual(value.kind, "ExpiringPublication")
            self.assertEqual(value.admission, recipe.decode(nested))
            self.assertEqual(value.operation, value.admission.operation)
            self.assertEqual(value.expiry.observed, recipe.MessageTime(b"\xab" * 32, 100))
            self.assertEqual(value.expiry.deadline, 2_000_000_100)
        value = recipe.decode(replay_at())
        self.assertEqual(value.kind, "TimedReplaySubscriptions")
        self.assertEqual(value.replay, recipe.decode(subscription()))
        self.assertEqual(value.operation, value.replay.operation)

    def test_zero_and_uint_max_are_finite_and_checked(self):
        self.assertEqual(recipe.decode(expiring(interval=0)).expiry.deadline, 100)
        self.assertEqual(recipe.decode(expiring(interval=0xFFFF_FFFF)).expiry.deadline,
                         100 + 0xFFFF_FFFF * 1_000_000_000)
        with self.assertRaisesRegex(recipe.RecipeError, "overflow"):
            recipe.decode(expiring(tick=2**64 - 1))

    def test_started_and_expired_keep_the_exact_finish_identity_shape(self):
        value = recipe.decode(started())
        self.assertEqual(value.kind, "MarkDeliveryStarted")
        self.assertEqual((value.operation, value.publication, value.claim),
                         ((1, 1, 0, 22), (1, 1, 0, 40), (1, 1, 0, 21)))
        value = recipe.decode(expired())
        self.assertEqual(value.kind, "ExpireDelivery")
        self.assertEqual(value.finish.claim, (1, 1, 0, 21))
        self.assertEqual(value.operation, value.finish.operation)
        self.assertEqual(value.observed.tick, value.expiry.deadline)

    def test_expiry_requires_established_age_and_cannot_delete_unknown_time(self):
        for data in (expired(now=99), expired(now=100), expired(now=2_000_000_099),
                     expired(domain=b"\xac" * 32)):
            with self.subTest(data=data.hex()), self.assertRaises(recipe.RecipeError):
                recipe.decode(data)

    def test_every_truncation_and_trailing_alias_is_refused(self):
        for data in (expiring(), expiring(retained_admission()), expiring(qos0()), replay_at(), started(), expired()):
            for cut in range(len(data)):
                with self.subTest(kind=data[6], cut=cut), self.assertRaises(recipe.RecipeError):
                    recipe.decode(data[:cut])
            with self.assertRaises(recipe.RecipeError):
                recipe.decode(data + b"\x00")

    def test_extension_nesting_and_base_aliases_are_bounded(self):
        for child in (expiring(), replay_at(), started(), expired()):
            with self.assertRaisesRegex(recipe.RecipeError, "nested"):
                recipe.decode(expiring(child))
        for nested in (admission(retain=1), admission(qos=0, retain=0)):
            with self.assertRaises(recipe.RecipeError):
                recipe.decode(expiring(nested))
        for tag in (1, 10, 11, 13, 14, 15, 16):
            data = replay_at()
            with self.assertRaises(recipe.RecipeError):
                recipe.decode(data[:17] + bytes([tag]) + data[18:])


class MessageExpiryFormatsAndScope(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contracts = Contracts.open(ROOT)
        cls.base = json.loads((ROOT / "contracts/examples/v1/valid/store-snapshot-after-commit.json").read_text())

    def snapshot(self):
        value = deepcopy(self.base)
        value["publications"][0]["expiry"] = {
            "observed": {"domain_hex": "ab" * 32, "tick": "100"}, "interval": 2}
        return value

    def unmet(self, snapshot=None, records=()):
        run = SimpleNamespace(snapshots={"first": (None, snapshot)} if snapshot is not None else {},
                              trace=SimpleNamespace(records=records))
        with mock.patch.object(results.judge.Run, "load", return_value=run):
            return results._scope_unmet(None, None, None)

    def test_original_lifetime_and_legacy_omission_are_both_readable(self):
        self.assertEqual(self.contracts.check_value(self.base), [])
        for interval in (0, 1, 0xFFFF_FFFF):
            value = self.snapshot()
            value["publications"][0]["expiry"]["interval"] = interval
            self.assertEqual(self.contracts.check_value(value), [])

    def test_time_shape_range_and_deadline_are_checked_independently(self):
        for edit in (
            lambda v: v["publications"][0]["expiry"].update(interval=True),
            lambda v: v["publications"][0]["expiry"].update(interval=2**32),
            lambda v: v["publications"][0]["expiry"].update(utc="2026-01-01"),
            lambda v: v["publications"][0]["expiry"]["observed"].update(domain_hex="AB" * 32),
        ):
            value = self.snapshot()
            edit(value)
            self.assertTrue(self.contracts.check_value(value))
        for tick, pointer in ((str(2**64), "/publications/0/expiry/observed/tick"),
                              (str(2**64-1), "/publications/0/expiry")):
            value = self.snapshot()
            value["publications"][0]["expiry"]["observed"]["tick"] = tick
            self.assertEqual([(d.code, d.pointer) for d in self.contracts.check_value(value)],
                             [("u64-out-of-range", pointer)])

    def test_started_claim_and_outer_fact_must_agree(self):
        value = json.loads((ROOT / "contracts/examples/v1/valid/trace-record-message-expiry-recovered-started.json").read_text())
        self.assertEqual(self.contracts.check_value(value), [])
        value["payload"]["facts"][0]["claim"]["started"] = False
        self.assertTrue(self.contracts.check_value(value))

    def test_readable_lifetime_never_extends_historical_qualification(self):
        self.assertNotIn(results.FIXTURE_MESSAGE_EXPIRY_SCOPE, self.unmet(self.base))
        self.assertIn(results.FIXTURE_MESSAGE_EXPIRY_SCOPE, self.unmet(self.snapshot()))
        for data in (expiring(), replay_at(), started(), expired()):
            value = {"outbound": [], "operations": [{"recipe_hex": data.hex(), "decision": {}}]}
            self.assertIn(results.FIXTURE_MESSAGE_EXPIRY_SCOPE, self.unmet(value))
        for kind in ("DeliveryStarted", "DeliveryExpired"):
            value = {"outbound": [], "operations": [{"recipe_hex": "", "decision": {"facts": {"kind": kind}}}]}
            self.assertIn(results.FIXTURE_MESSAGE_EXPIRY_SCOPE, self.unmet(value))
        record = SimpleNamespace(kind="CommandSubmitted", payload={"command_kind": "TimedReplaySubscriptions"})
        self.assertIn(results.FIXTURE_MESSAGE_EXPIRY_SCOPE, self.unmet(records=[record]))

    def test_wire_scope_guard_finds_lifetime_even_when_no_durable_row_remains(self):
        for qos in (0, 1, 2):
            body = b"\x00\x01t" + (b"\x00\x07" if qos else b"") + b"\x05\x02\x00\x00\x00\x00"
            packet = bytes([0x30 | qos << 1, len(body)]) + body
            record = SimpleNamespace(kind="ClientBytesReceived", payload={"bytes_hex": packet.hex()})
            self.assertTrue(results._wire_message_expiry(packet.hex()))
            self.assertIn(results.FIXTURE_MESSAGE_EXPIRY_SCOPE, self.unmet(records=[record]))
        self.assertFalse(results._wire_message_expiry("3416000a70312f6578616d706c650007007061796c6f6164"))

    def test_profile_is_explicit_and_does_not_increase_durable_quotas(self):
        profile = json.loads((ROOT / "contracts/v1/product-profile.json").read_text())
        self.assertEqual(self.contracts.check_value(profile), [])
        expected = {"id": "tcp-message-expiry-pubsub-v1", "sqlite_schema_version": 9,
            "message_expiry": True, "message_expiry_interval": {"minimum": 0, "maximum": 0xFFFF_FFFF},
            "message_expiry_batch": 4, "message_expiry_scan_ms": 100,
            "message_expiry_policy": "quarantine-on-unknown-origin", "message_expiry_time_unit": "nanoseconds",
            "terminal_input_slots": 544, "frame_registration_slots": 208,
            "max_sessions": 128, "receipt_slots": 8192}
        for key, value in expected.items():
            self.assertEqual(profile[key], value)


if __name__ == "__main__":
    unittest.main()
