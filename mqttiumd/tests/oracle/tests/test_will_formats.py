"""Will layouts are independently readable and remain outside qualification."""

import copy
import json
from types import SimpleNamespace
import unittest
from unittest import mock

from mqttiumd_oracle import recipe, results
from mqttiumd_oracle.documents import Contracts

from .support import ROOT
from .test_session_lifecycle import be, guard, scoped

PREFIX = b"MQDR\x00\x01"
OPERATION = {"incarnation": "1", "epoch": "2", "owner": "3", "sequence": "8"}
TOPIC = b"atelier/capteur-1/status"
PAYLOAD = b"offline"


def open_will(*, topic=TOPIC, payload=PAYLOAD, present=True):
    value = (PREFIX + b"\x08" + scoped(8) + b"\x05\x00\x01\x01" + guard()
             + be(9, 2) + b"capteur-1" + be(100) + bytes([present]))
    return value + (be(len(topic), 2) + topic + be(len(payload), 4) + payload if present else b"")


def end_will(*, publish=True, observed_at=100):
    return PREFIX + b"\x08" + scoped(9) + b"\x06" + guard() + b"\x01" + be(observed_at) + bytes([publish])


def publication(*, topic=TOPIC, qos=1, cause=1, target=2, count=1, payload_length=7):
    header = (PREFIX + b"\x11" + scoped(10) + scoped(8) + scoped(11) + be(1)
              + be(len(topic), 2) + topic + scoped(12) + be(payload_length)
              + be(7) + be(count) + be(0) + be(1) + bytes([count]))
    return header + b"".join(be(target + index) + be(1) + be(index, 4) + bytes([qos, cause])
                             for index in range(count))


class WillRecipes(unittest.TestCase):
    def test_open_installs_a_bounded_descriptor_and_absence_is_explicit(self):
        decoded = recipe.decode(open_will())
        self.assertEqual((decoded.kind, decoded.action), ("SessionLifecycle", "OpenWillAt"))
        self.assertEqual(decoded.operation, (1, 2, 3, 8))
        self.assertEqual(decoded.expected, (9, (1, 2, 3, 4)))
        self.assertEqual(decoded.client_id, b"capteur-1")
        self.assertEqual((decoded.now, decoded.expiry), (100, "Never"))
        self.assertEqual((decoded.will.topic, decoded.will.payload), (TOPIC, PAYLOAD))
        self.assertIsNone(recipe.decode(open_will(present=False)).will)

    def test_end_keeps_the_original_attachment_observation_and_publish_intent(self):
        for publish in (False, True):
            decoded = recipe.decode(end_will(publish=publish))
            self.assertEqual(decoded.action, "EndWill")
            self.assertEqual(decoded.guard, (9, (1, 2, 3, 4)))
            self.assertEqual(decoded.observed_at, 100)
            self.assertEqual(decoded.publish, publish)

    def test_publication_has_a_will_identity_without_a_fictitious_mqtt_packet(self):
        decoded = recipe.decode(publication())
        self.assertEqual(decoded.kind, "WillPublication")
        self.assertEqual(decoded.operation, (1, 2, 3, 10))
        self.assertEqual(decoded.will, (1, 2, 3, 8))
        self.assertEqual(decoded.publication, (1, 2, 3, 11))
        self.assertEqual(decoded.payload, (1, 2, 3, 12))
        self.assertEqual((decoded.topic, decoded.payload_length), (TOPIC, len(PAYLOAD)))
        self.assertEqual(decoded.targets, (recipe.Target(2, 1, 0, 1, 1),))
        self.assertFalse(hasattr(decoded, "packet_id"))
        self.assertFalse(hasattr(decoded, "exchange_generation"))

    def test_all_three_new_layouts_refuse_every_truncation_and_trailing_alias(self):
        for valid in (open_will(), open_will(present=False), end_will(), publication()):
            for data in [valid[:cut] for cut in range(len(valid))] + [valid + b"\x00"]:
                with self.subTest(data=data.hex()), self.assertRaises(recipe.RecipeError):
                    recipe.decode(data)

    def test_will_topic_payload_and_canonical_targets_are_bounded(self):
        for invalid in (
            open_will(topic=b""), open_will(topic=b"a/+"), open_will(topic=b"\xff"),
            open_will(topic=b"x" * 1025), open_will(payload=b"x" * 32769),
            publication(qos=2), publication(cause=2), publication(count=17),
            publication(payload_length=32769), publication(topic=b"bad/#"),
        ):
            with self.subTest(data=invalid[:48].hex()), self.assertRaises(recipe.RecipeError):
                recipe.decode(invalid)


class WillFormats(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contracts = Contracts.open(ROOT)
        cls.snapshot = json.loads((ROOT / "contracts/examples/v1/valid/store-snapshot-after-commit.json").read_text())

    def test_will_commit_facts_keep_both_causal_identities(self):
        value = copy.deepcopy(self.snapshot)
        facts = {"kind": "WillPublished", "will_id": OPERATION,
                 "publication_id": value["publications"][0]["publication_id"]}
        value["operations"][0]["decision"] = {"kind": "Committed", "facts": facts}
        self.assertEqual(self.contracts.check_value(value), [], "will_commit_facts_not_described")
        for field in ("will_id", "publication_id"):
            invalid = copy.deepcopy(value)
            del invalid["operations"][0]["decision"]["facts"][field]
            self.assertTrue(self.contracts.check_value(invalid), field)

    def test_will_format_support_cannot_extend_the_fixture_qualifier(self):
        def unmet(*, snapshot=None, record=None):
            run = SimpleNamespace(snapshots={} if snapshot is None else {"0": (None, snapshot)},
                trace=SimpleNamespace(records=() if record is None else (record,),
                                      before=lambda _left, _right: True))
            with mock.patch.object(results.judge.Run, "load", return_value=run):
                return results._scope_unmet(None, None, None)

        base = {"sessions": [], "outbound": [], "operations": []}
        for snapshot in (
            {**base, "wills": [{"state": "Armed"}]},
            {**base, "operations": [{"recipe_hex": publication().hex(),
                "decision": {"kind": "Committed", "facts": {"kind": "WillPublished"}}}]},
        ):
            self.assertIn("the fixture-based qualifier does not cover product Will publication",
                          unmet(snapshot=snapshot))
        self.assertIn("the fixture-based qualifier does not cover product Will publication",
                      unmet(record=SimpleNamespace(kind="CommandSubmitted",
                            payload={"command_kind": "WillPublication"})))
