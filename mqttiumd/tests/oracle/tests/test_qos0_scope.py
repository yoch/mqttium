"""Synthetic scope controls; these are not executions or qualifying campaigns."""

from types import SimpleNamespace
import unittest
from unittest import mock

from mqttiumd_oracle import results


class Qos0QualificationScope(unittest.TestCase):
    def unmet(self, *, snapshots=(), records=()):
        run = SimpleNamespace(
            snapshots={str(index): value for index, value in enumerate(snapshots)},
            trace=SimpleNamespace(records=records, before=lambda _left, _right: True),
        )
        with mock.patch.object(results.judge.Run, "load", return_value=run):
            return results._scope_unmet(None, None, None)

    def test_pure_qos0_without_a_durable_recipe_is_not_fixture_qualified(self):
        record = SimpleNamespace(kind="ClientBytesReceived", payload={
            "packet_kind": "PUBLISH", "qos": 0,
        })
        self.assertEqual(self.unmet(records=[record]), [
            "the fixture-based qualifier covers incoming QoS 2 only",
        ])

    def test_a_consumed_receipt_without_live_work_is_not_fixture_qualified(self):
        snapshot = {"outbound": [], "operations": [{
            "recipe_hex": (b"MQDR\x00\x01\x06" + bytes(84)).hex(),
            "decision": {"facts": {"kind": "DeliveryConsumed"}},
        }]}
        self.assertEqual(self.unmet(snapshots=[(None, snapshot)]), [
            "the fixture-based qualifier does not cover volatile or consumed QoS 0 delivery",
        ])

    def test_corroborated_queued_qos0_remains_outside_fixture_scope(self):
        publication = {"incarnation": "1", "epoch": "1", "owner": "0", "sequence": "1"}
        obligation = {"publication_id": publication, "index": 0, "session": "2",
                      "target_revision": "1", "effective_qos": 0}
        snapshot = {"outbound": [], "operations": [],
                    "meta": {"last_opened_epoch": "2"}, "obligations": [obligation]}
        fact = {"kind": "OutboundDelivery", "delivery_id": {
            "publication_id": publication, "index": 0,
        }, "target_session": "2", "target_revision": "1", "qos": 0, "claim": None}
        record = SimpleNamespace(kind="RecoveryPageRead", payload={"run_epoch": "2", "facts": [fact]})
        self.assertEqual(self.unmet(snapshots=[(None, snapshot)], records=[record]), [
            "the fixture-based qualifier does not cover volatile or consumed QoS 0 delivery",
        ])

    def test_corroborated_queued_qos1_keeps_existing_scope(self):
        publication = {"incarnation": "1", "epoch": "1", "owner": "0", "sequence": "1"}
        obligation = {"publication_id": publication, "index": 0, "session": "2",
                      "target_revision": "1", "effective_qos": 1}
        snapshot = {"outbound": [], "operations": [],
                    "meta": {"last_opened_epoch": "2"}, "obligations": [obligation]}
        fact = {"kind": "OutboundDelivery", "delivery_id": {
            "publication_id": publication, "index": 0,
        }, "target_session": "2", "target_revision": "1", "qos": 1, "claim": None}
        record = SimpleNamespace(kind="RecoveryPageRead", payload={"run_epoch": "2", "facts": [fact]})
        self.assertEqual(self.unmet(snapshots=[(None, snapshot)], records=[record]), [])

    def test_queued_snapshot_qos0_without_a_recovery_page_is_not_qualified(self):
        snapshot = {"outbound": [], "operations": [],
                    "obligations": [{"effective_qos": 0}]}
        self.assertEqual(self.unmet(snapshots=[(None, snapshot)]), [results.FIXTURE_QOS0_SCOPE])

    def test_recovery_cannot_relabel_a_qos0_obligation_as_qos1(self):
        publication = {"incarnation": "1", "epoch": "1", "owner": "0", "sequence": "1"}
        obligation = {"publication_id": publication, "index": 0, "session": "2",
                      "target_revision": "1", "effective_qos": 0}
        snapshot = {"outbound": [], "operations": [],
                    "meta": {"last_opened_epoch": "2"}, "obligations": [obligation]}
        fact = {"kind": "OutboundDelivery", "delivery_id": {
            "publication_id": publication, "index": 0,
        }, "target_session": "2", "target_revision": "1", "qos": 1, "claim": None}
        record = SimpleNamespace(kind="RecoveryPageRead", payload={"run_epoch": "2", "facts": [fact]})
        self.assertEqual(self.unmet(snapshots=[(None, snapshot)], records=[record]), [
            results.UNCORROBORATED_QUEUED, results.FIXTURE_QOS0_SCOPE,
        ])
