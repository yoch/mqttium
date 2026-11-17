"""Independent descriptive retained readers; synthetic cases qualify no broker run."""
from copy import deepcopy
import json
from types import SimpleNamespace
import unittest
from unittest import mock

from mqttiumd_oracle import recipe, results
from mqttiumd_oracle.documents import Contracts

from .support import ROOT


def be(number, size=8):
    return number.to_bytes(size, "big")


def scoped(sequence, *, epoch=1):
    return b"".join(be(number) for number in (1, epoch, 0, sequence))


def header(kind):
    return b"MQDR\x00\x01" + bytes([kind])


def admission(*, qos=1, retain=1):
    topic = b"atelier/temperature"
    return (header(1) + scoped(20) + scoped(30) + scoped(40)
            + be(1) + be(1) + be(7, 2) + be(1) + be(2)
            + bytes([qos, retain]) + be(len(topic), 2) + topic
            + scoped(50) + be(4) + be(4) + be(2) + be(qos == 2) + be(1)
            + be(1, 2) + be(2) + be(1) + be(0, 4) + b"\x01\x01")


def retained_admission(*, qos=1, retain=1, mask=1):
    nested = admission(qos=qos, retain=retain)
    return header(10) + be(len(nested), 4) + nested + be(mask, 2)


def qos0(*, topic=b"atelier/temperature", order=1, cause=1, mask=1):
    return (header(11) + scoped(20) + scoped(30) + scoped(40)
            + be(1) + be(1) + be(3) + be(order) + be(2)
            + be(len(topic), 2) + topic + scoped(50) + be(4)
            + be(4) + be(2) + be(0) + be(1) + b"\x01"
            + be(2) + be(1) + be(0, 4) + bytes([0, cause]) + be(mask, 2))


def subscription(*, first=60, epoch=1):
    topic = b"atelier/#"
    nested = (header(9) + scoped(21) + be(2) + be(1) + be(2)
              + b"\x02" + b"\x02\x04" + be(len(topic), 2) + topic
              + b"\x02\x20" + be(len(topic), 2) + topic)
    return header(12) + be(len(nested), 4) + nested + scoped(first, epoch=epoch)


class RetainedCanonicalReader(unittest.TestCase):
    def test_new_admission_does_not_reinterpret_legacy_retain_bytes(self):
        legacy = recipe.decode(admission())
        found = recipe.decode(retained_admission())
        self.assertEqual(legacy.kind, "AdmitPublication")
        self.assertEqual(found.kind, "RetainedPublication")
        self.assertEqual(found.admission, legacy)
        self.assertEqual(found.retain_targets, 1)
        self.assertEqual(found.operation, legacy.operation)

    def test_qos0_has_typed_origin_without_a_packet_identifier_or_exchange(self):
        found = recipe.decode(qos0())
        self.assertEqual(found.kind, "RetainedQos0")
        self.assertEqual((found.connection, found.ingress_order), (3, 1))
        self.assertEqual(found.topic, b"atelier/temperature")
        self.assertEqual(found.targets[0], recipe.Target(2, 1, 0, 0, 1))
        self.assertFalse(hasattr(found, "packet_id"))
        self.assertFalse(hasattr(found, "exchange_generation"))

    def test_published_topic_name_does_not_use_subscription_filter_restrictions(self):
        self.assertEqual(recipe.decode(qos0(topic=b"$share/literal")).topic, b"$share/literal")

    def test_replay_preserves_ordered_steps_and_the_complete_reserved_range(self):
        found = recipe.decode(subscription())
        self.assertEqual(found.kind, "ReplaySubscriptions")
        self.assertEqual(found.first_publication, (1, 1, 0, 60))
        self.assertEqual(found.last_publication, (1, 1, 0, 315))
        self.assertEqual([entry.options for entry in found.update.changes], [4, 32])
        self.assertEqual(found.operation, found.update.operation)

    def test_every_truncation_and_trailing_byte_is_refused(self):
        for data in (retained_admission(), qos0(), subscription()):
            for cut in range(len(data)):
                with self.subTest(kind=data[6], cut=cut), self.assertRaises(recipe.RecipeError):
                    recipe.decode(data[:cut])
            with self.assertRaises(recipe.RecipeError):
                recipe.decode(data + b"\x00")

    def test_nested_extensions_are_rejected_without_recursive_decoding(self):
        data = retained_admission()
        for nested_kind in (2, 10, 11, 12):
            invalid = data[:17] + bytes([nested_kind]) + data[18:]
            with self.subTest(kind=nested_kind), self.assertRaisesRegex(recipe.RecipeError, "nested"):
                recipe.decode(invalid)
        invalid = header(12) + be(len(data), 4) + data + scoped(60)
        with self.assertRaisesRegex(recipe.RecipeError, "nested"):
            recipe.decode(invalid)

    def test_flags_topics_and_identity_range_are_closed_domains(self):
        invalid = [retained_admission(qos=0), retained_admission(retain=0),
                   retained_admission(mask=2), qos0(mask=2), qos0(order=0),
                   qos0(cause=99), subscription(first=0), subscription(first=2**64-255),
                   subscription(epoch=2)]
        invalid.extend(qos0(topic=topic) for topic in (b"", b"a/+", b"a/#", b"a\x00b", b"\xff"))
        for data in invalid:
            with self.subTest(data=data.hex()), self.assertRaises(recipe.RecipeError):
                recipe.decode(data)


class RetainedSnapshotAndScope(unittest.TestCase):
    def snapshot(self):
        path = ROOT / "contracts/examples/v1/valid/store-snapshot-after-commit.json"
        return json.loads(path.read_text())

    def unmet(self, snapshot=None, records=()):
        run = SimpleNamespace(snapshots={"first": (None, snapshot)} if snapshot is not None else {},
                              trace=SimpleNamespace(records=records))
        with mock.patch.object(results.judge.Run, "load", return_value=run):
            return results._scope_unmet(None, None, None)

    def test_snapshot_reads_new_metadata_and_preserves_legacy_defaults(self):
        contracts = Contracts.open(ROOT)
        value = self.snapshot()
        self.assertEqual(contracts.check_value(value), [])
        publication = value["publications"][0]["publication_id"]
        value["retained"] = [{"topic_hex": b"atelier/temperature".hex(),
                              "publication_id": publication,
                              "producer_client_id_hex": b"old-producer".hex()}]
        value["publications"][0]["retained_version"] = publication
        value["obligations"][0]["retain"] = True
        value["obligations"][0]["cause"] = "RetainedReplay"
        self.assertEqual(contracts.check_value(value), [])
        for key, bad in [("retain", 1), ("cause", "Unknown")]:
            invalid = deepcopy(value)
            invalid["obligations"][0][key] = bad
            self.assertTrue(contracts.check_value(invalid))
        invalid = deepcopy(value)
        invalid["retained"] *= 33
        self.assertTrue(contracts.check_value(invalid))

    def test_retained_rows_are_sorted_and_unique_in_both_semantic_readers(self):
        contracts = Contracts.open(ROOT)
        value = self.snapshot()
        row = {"topic_hex": b"z".hex(), "publication_id": value["publications"][0]["publication_id"],
               "producer_client_id_hex": b"producer".hex()}
        value["retained"] = [row, dict(row)]
        diagnostics = contracts.check_value(value)
        self.assertTrue(any("retained" in problem.pointer for problem in diagnostics), diagnostics)
        value["retained"][1]["topic_hex"] = b"a".hex()
        diagnostics = contracts.check_value(value)
        self.assertTrue(any("retained" in problem.pointer for problem in diagnostics), diagnostics)

    def test_readable_new_formats_cannot_extend_fixture_qualification(self):
        for data in (retained_admission(), qos0(), subscription()):
            value = {"outbound": [], "operations": [{"recipe_hex": data.hex(), "decision": {}}]}
            self.assertIn(results.FIXTURE_RETAINED_SCOPE, self.unmet(value))
        value = {"outbound": [], "operations": [], "retained": [{"topic_hex": "61"}]}
        self.assertIn(results.FIXTURE_RETAINED_SCOPE, self.unmet(value))
        value = {"outbound": [], "operations": [], "obligations": [{"cause": "RetainedReplay"}]}
        self.assertIn(results.FIXTURE_RETAINED_SCOPE, self.unmet(value))

    def test_qos0_commit_fact_has_no_invented_exchange(self):
        contracts = Contracts.open(ROOT)
        value = self.snapshot()
        value["operations"][0]["recipe_hex"] = qos0().hex()
        value["operations"][0]["decision"] = {"kind": "Committed", "facts": {
            "kind": "RetainedQos0Stored", "publication_id": value["publications"][0]["publication_id"]}}
        self.assertEqual(contracts.check_value(value), [])
        value["operations"][0]["decision"]["facts"]["exchange"] = {"session": "1", "generation": "1"}
        self.assertTrue(contracts.check_value(value))

    def test_terminal_qos0_fact_remains_outside_scope_after_payload_disappears(self):
        value = {"outbound": [], "operations": [{"recipe_hex": "", "decision": {
            "facts": {"kind": "RetainedQos0Stored", "publication_id": {}}}}]}
        self.assertIn(results.FIXTURE_RETAINED_SCOPE, self.unmet(value))
        record = SimpleNamespace(kind="CommandSubmitted", payload={"command_kind": "RetainedQos0"})
        self.assertIn(results.FIXTURE_RETAINED_SCOPE, self.unmet(records=[record]))


if __name__ == "__main__":
    unittest.main()
