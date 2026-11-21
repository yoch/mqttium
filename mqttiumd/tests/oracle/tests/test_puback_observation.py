"""Descriptive product PUBACK tokens, not outgoing-delivery qualification.

The schema preserves the runtime-dequeue association. A stale token remains
readable so a system test can observe that it was ignored; document validation
does not decide which claim occupied a network Packet Identifier.
"""

import copy
import json
import unittest

from mqttiumd_oracle.documents import Contracts

from .support import ROOT


def observed(*, target=True, kind="puback_observed"):
    return {
        "format": "mqttiumd.product-event", "schema_version": 1, "seq": "5", "pid": 1,
        "kind": kind, "connection": "7", "details": {"authority": "reader_dequeue"},
        "puback": {
            "connection": {"session": "2", "generation": "7"}, "packet_id": 1, "reason_code": 0,
            "target": {
                "delivery_id": {"publication_id": {
                    "incarnation": "1", "epoch": "1", "owner": "0", "sequence": "1",
                }, "index": 0},
                "claim_operation_id": {"incarnation": "1", "epoch": "2", "owner": "0", "sequence": "3"},
                "frame_effect_id": {"epoch": "3", "owner": "0", "sequence": "4"},
            } if target else None,
        },
    }


class PubackObservation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contracts = Contracts.open(ROOT)

    def diagnostics(self, value):
        return self.contracts.check_bytes(json.dumps(value).encode())

    def test_an_observed_puback_cannot_omit_its_immutable_observation(self):
        event = observed()
        del event["puback"]
        self.assertTrue(self.diagnostics(event))

    def test_matched_no_target_and_stale_ignored_tokens_remain_readable(self):
        for event in (observed(), observed(target=False), observed(kind="puback_ignored")):
            with self.subTest(event=event):
                self.assertEqual(self.diagnostics(event), [])

    def test_no_target_is_explicit_not_an_omitted_association(self):
        event = observed(target=False)
        del event["puback"]["target"]
        self.assertTrue(self.diagnostics(event))

    def test_a_partial_or_opaque_target_is_refused(self):
        for target in ({}, "1/2/3", {"claim_operation_id": observed()["puback"]["target"]["claim_operation_id"]}):
            with self.subTest(target=target):
                event = observed()
                event["puback"]["target"] = target
                self.assertTrue(self.diagnostics(event))

    def test_packet_identifier_and_puback_reason_are_bounded_for_this_packet(self):
        for member, value in (("packet_id", 0), ("packet_id", 65536), ("reason_code", 0x92)):
            with self.subTest(member=member, value=value):
                event = observed()
                event["puback"][member] = value
                self.assertTrue(self.diagnostics(event))

    def test_nested_identity_components_and_observation_sequence_are_u64(self):
        pointers = (
            ("connection", "session"), ("connection", "generation"),
            ("target", "delivery_id", "publication_id", "sequence"),
            ("target", "claim_operation_id", "sequence"),
            ("target", "frame_effect_id", "sequence"),
            ("source_observation_sequence",),
        )
        for pointer in pointers:
            with self.subTest(pointer=pointer):
                event = observed()
                at = event["puback"]
                for member in pointer[:-1]:
                    at = at[member]
                at[pointer[-1]] = "18446744073709551616"
                found = self.diagnostics(event)
                self.assertIn(("u64-out-of-range", "/puback/" + "/".join(pointer)),
                              [(d.code, d.pointer) for d in found])

    def test_optional_envelope_generation_cannot_contradict_the_observation(self):
        event = observed()
        event["connection"] = "8"
        self.assertIn(("evidence-mismatch", "/connection"),
                      [(d.code, d.pointer) for d in self.diagnostics(event)])

    def test_copy_source_sequence_does_not_claim_wire_causality(self):
        event = observed(kind="puback_ignored")
        event["puback"]["source_observation_sequence"] = "99"
        # This namespace identifies a retained runtime copy, not event.seq or
        # an MQTT wire field. No ordering relation is invented by the reader.
        self.assertEqual(self.diagnostics(event), [])

    def test_historical_claim_and_recovery_frame_need_not_share_an_epoch(self):
        event = copy.deepcopy(observed(kind="puback_ignored"))
        self.assertNotEqual(event["puback"]["target"]["claim_operation_id"]["epoch"],
                            event["puback"]["target"]["frame_effect_id"]["epoch"])
        self.assertEqual(self.diagnostics(event), [])
