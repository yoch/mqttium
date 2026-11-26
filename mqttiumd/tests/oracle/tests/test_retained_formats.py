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
        self.assertEqual(found.last_publication, (1, 1, 0, 60 + recipe.MAX_RETAINED_REPLAYS - 1))
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
                   qos0(cause=99), subscription(first=0),
                   subscription(first=2**64 - (recipe.MAX_RETAINED_REPLAYS - 1)),
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


def wide_admission(*, count=17, qos=2, retain=1):
    topic = b"atelier/wide"
    targets = b"".join(
        be(2 + idx) + be(1) + be(idx, 4) + bytes([1, 1]) for idx in range(count)
    )
    return (
        header(1) + scoped(20) + scoped(30) + scoped(40)
        + be(1) + be(1) + be(7, 2) + be(1) + be(2)
        + bytes([qos, retain]) + be(len(topic), 2) + topic
        + scoped(50) + be(4) + be(4) + be(count + 1) + be(qos == 2) + be(1)
        + be(count, 2) + targets
    )


def wide_mask_bytes(count, mask):
    byte_len = (count + 7) // 8
    return be(byte_len, 2) + mask.to_bytes(byte_len, "little")


def wide_retained_publication(*, count=17, mask=None):
    if mask is None:
        mask = (1 << 0) | (1 << 16)
    nested = wide_admission(count=count, qos=2, retain=1)
    return header(20) + be(len(nested), 4) + nested + wide_mask_bytes(count, mask)


def wide_retained_qos0(*, count=17, mask=None):
    if mask is None:
        mask = (1 << 0) | (1 << 16)
    topic = b"atelier/wide0"
    targets = b"".join(
        be(2 + idx) + be(1) + be(idx, 4) + bytes([0, 1]) for idx in range(count)
    )
    return (
        header(21) + scoped(20) + scoped(30) + scoped(40)
        + be(1) + be(1) + be(3) + be(1) + be(2)
        + be(len(topic), 2) + topic + scoped(50) + be(4)
        + be(4) + be(count + 1) + be(0) + be(1)
        + be(count, 2) + targets + wide_mask_bytes(count, mask)
    )


def wide_will_publication(*, count=17, live_retain=None):
    topic = b"atelier/wide-will"
    targets = b"".join(
        be(2 + idx) + be(1) + be(idx, 4) + bytes([1, 1]) for idx in range(count)
    )
    body = (
        header(22) + scoped(10) + scoped(8) + scoped(11) + be(1)
        + be(len(topic), 2) + topic + scoped(12) + be(7)
        + be(7) + be(count) + be(0) + be(1)
        + be(count, 2) + targets
    )
    if live_retain is None:
        return body + b"\x00"
    return body + b"\x01" + wide_mask_bytes(count, live_retain)


def encode_publish_props(
    *,
    pfi=1,
    content_type=b"application/json",
    response_topic=b"replies/1",
    correlation_data=b"\x01\x02",
    user_properties=((b"trace", b"a"), (b"trace", b"b")),
):
    flags = 0
    out = bytearray()
    if pfi is not None:
        flags |= 0x01
        out.append(pfi)
    if content_type is not None:
        flags |= 0x02
        out += be(len(content_type), 2) + content_type
    if response_topic is not None:
        flags |= 0x04
        out += be(len(response_topic), 2) + response_topic
    if correlation_data is not None:
        flags |= 0x08
        out += be(len(correlation_data), 2) + correlation_data
    if user_properties:
        flags |= 0x10
        out += be(len(user_properties), 2)
        for k, v in user_properties:
            out += be(len(k), 2) + k + be(len(v), 2) + v
    return bytes([flags]) + bytes(out)


def annotated_publication(inner=None, props_bytes=None):
    inner = admission(qos=2, retain=0) if inner is None else inner
    props_bytes = encode_publish_props() if props_bytes is None else props_bytes
    return header(23) + be(len(props_bytes), 4) + props_bytes + be(len(inner), 4) + inner


class Recipes20To23(unittest.TestCase):
    def test_wide_retained_qos0_and_will_recipes_decode_canonically(self):
        rp = recipe.decode(wide_retained_publication())
        self.assertEqual(rp.kind, "RetainedPublication")
        self.assertEqual(len(rp.admission.targets), 17)
        self.assertEqual(rp.retain_targets, (1 << 0) | (1 << 16))

        rq = recipe.decode(wide_retained_qos0())
        self.assertEqual(rq.kind, "RetainedQos0")
        self.assertEqual(len(rq.targets), 17)
        self.assertEqual(rq.retain_targets, (1 << 0) | (1 << 16))

        wp_plain = recipe.decode(wide_will_publication(live_retain=None))
        self.assertEqual(wp_plain.kind, "WillPublication")
        self.assertEqual(len(wp_plain.targets), 17)
        self.assertIsNone(wp_plain.live_retain)

        wp_ret = recipe.decode(wide_will_publication(live_retain=(1 << 16)))
        self.assertEqual(wp_ret.live_retain, 1 << 16)

    def test_annotated_publication_decodes_all_five_properties_in_order(self):
        for inner in (admission(qos=2, retain=0), retained_admission(), wide_retained_publication(), wide_retained_qos0()):
            raw = annotated_publication(inner=inner)
            decoded = recipe.decode(raw)
            self.assertEqual(decoded.kind, "AnnotatedPublication")
            self.assertEqual(decoded.inner, recipe.decode(inner))
            self.assertEqual(decoded.operation, decoded.inner.operation)
            self.assertEqual(decoded.properties.payload_format_indicator, 1)
            self.assertEqual(decoded.properties.content_type, b"application/json")
            self.assertEqual(decoded.properties.response_topic, b"replies/1")
            self.assertEqual(decoded.properties.correlation_data, b"\x01\x02")
            self.assertEqual(decoded.properties.user_properties, ((b"trace", b"a"), (b"trace", b"b")))

    def test_recipes_20_to_23_reject_noncanonical_truncation_and_bad_domains(self):
        # Truncations & trailing bytes refused.
        for valid in (
            wide_retained_publication(),
            wide_retained_qos0(),
            wide_will_publication(),
            wide_will_publication(live_retain=1),
            annotated_publication(),
        ):
            for cut in (0, 6, 7, len(valid) - 1):
                with self.assertRaises(recipe.RecipeError):
                    recipe.decode(valid[:cut])
            with self.assertRaises(recipe.RecipeError):
                recipe.decode(valid + b"\x00")

        # Noncanonical wide tags with <= 16 targets, invalid mask bits, empty/invalid props.
        invalid = [
            wide_retained_publication(count=16, mask=1),
            wide_retained_publication(count=17, mask=1 << 17),
            wide_retained_qos0(count=16, mask=1),
            wide_retained_qos0(count=17, mask=1 << 17),
            wide_will_publication(count=16, live_retain=None),
            wide_will_publication(count=17, live_retain=1 << 17),
            annotated_publication(props_bytes=b"\x00"),
            annotated_publication(props_bytes=b"\x20"),
            annotated_publication(props_bytes=encode_publish_props(pfi=2)),
            annotated_publication(props_bytes=encode_publish_props(response_topic=b"bad/+")),
            annotated_publication(props_bytes=encode_publish_props(content_type=b"\xff")),
            annotated_publication(inner=subscription()),
        ]
        for raw in invalid:
            with self.assertRaises(recipe.RecipeError):
                recipe.decode(raw)

    def test_recipes_20_to_23_stay_outside_historical_p1_qualification(self):
        def unmet(snapshot=None, records=()):
            run = SimpleNamespace(
                snapshots={"first": (None, snapshot)} if snapshot is not None else {},
                trace=SimpleNamespace(records=records),
            )
            with mock.patch.object(results.judge.Run, "load", return_value=run):
                return results._scope_unmet(None, None, None)

        for raw, expected in (
            (wide_retained_publication(), results.FIXTURE_RETAINED_SCOPE),
            (wide_retained_qos0(), results.FIXTURE_RETAINED_SCOPE),
            (wide_will_publication(), results.FIXTURE_WILL_SCOPE),
            (annotated_publication(), results.FIXTURE_PROPERTIES_SCOPE),
        ):
            snap = {"outbound": [], "operations": [{"recipe_hex": raw.hex(), "decision": {}}]}
            self.assertIn(expected, unmet(snapshot=snap))


def claimed_publication(claims, *, targets=((2, 1), (3, 1)), retain=0, kind=None):
    """An admission to ``targets`` (session, effective QoS) with the inline
    claims ``claims`` (index of the target, Packet Identifier): kind 24 for
    one claim, kind 25 for several, unless ``kind`` says otherwise."""
    topic = b"atelier/temperature"
    body = (scoped(20) + scoped(30) + scoped(40)
            + be(1) + be(1) + be(7, 2) + be(1) + be(2)
            + bytes([1, retain]) + be(len(topic), 2) + topic
            + scoped(50) + be(4) + be(4) + be(len(targets)) + be(0) + be(1)
            + be(len(targets), 2)
            + b"".join(be(session) + be(1) + be(index, 4) + bytes([qos, 1])
                       for index, (session, qos) in enumerate(targets)))
    pairs = b"".join(be(index, 2) + be(packet_id, 2) for index, packet_id in claims)
    if kind is None:
        kind = 24 if len(claims) == 1 else 25
    if kind == 0:
        return header(1) + body
    return header(kind) + body + (pairs if kind == 24 else be(len(claims), 2) + pairs)


class Recipes24And25(unittest.TestCase):
    """An admission that claims its QoS 1/2 targets in the same commit
    (``ClaimedPublication``, ``MultiClaimedPublication``): the hot path of
    the product since lots 6 and 9. The oracle reads them, attributes them
    to the exchange of their admission, and keeps their delivery semantics
    outside the P1 fixture scope, as it does for ``ClaimDelivery``."""

    def test_a_claimed_publication_is_its_admission_and_its_claims(self):
        admission_alone = recipe.decode(claimed_publication((), kind=0))
        one = recipe.decode(claimed_publication([(1, 5)]))
        self.assertEqual(one.kind, "ClaimedPublication")
        self.assertEqual(one.admission, admission_alone)
        self.assertEqual(one.claims, (recipe.InlineClaim(1, 5),))
        self.assertEqual(one.operation, admission_alone.operation)

        several = recipe.decode(claimed_publication([(0, 5), (1, 6)]))
        self.assertEqual(several.kind, "MultiClaimedPublication")
        self.assertEqual(several.admission, admission_alone)
        self.assertEqual(several.claims, (recipe.InlineClaim(0, 5), recipe.InlineClaim(1, 6)))

        for inner in (claimed_publication([(0, 5)]), claimed_publication([(0, 5), (1, 6)])):
            wrapped = recipe.decode(annotated_publication(inner=inner))
            self.assertEqual(wrapped.kind, "AnnotatedPublication")
            self.assertEqual(wrapped.inner, recipe.decode(inner))

    def test_noncanonical_claims_are_refused(self):
        invalid = [
            claimed_publication([(2, 5)]),                               # no such target
            claimed_publication([(0, 0)]),                               # Packet Identifier 0
            claimed_publication([(0, 5)], targets=((2, 0), (3, 1))),     # a QoS 0 target
            claimed_publication([(0, 5)], retain=1),                     # a retained admission
            claimed_publication([(0, 5)]) + b"\x00",                     # trailing bytes
            claimed_publication([(0, 5)])[:-1],                          # truncated
            claimed_publication([(0, 5)], kind=25),                      # one claim is kind 24
            claimed_publication([(0, 5), (1, 6)], kind=24),              # two claims are kind 25
            claimed_publication([(0, 5), (1, 6), (1, 7)]),               # more claims than targets
            claimed_publication([(1, 5), (0, 6)]),                       # not ascending
            claimed_publication([(0, 5), (0, 6)]),                       # the same target twice
            claimed_publication([(0, 5), (1, 6)], targets=((2, 1), (2, 1))),  # one session twice
            claimed_publication([(0, 5), (1, 0)]),                       # Packet Identifier 0
            claimed_publication([(0, 5), (1, 6)], retain=1),
        ]
        for raw in invalid:
            with self.subTest(raw=raw.hex()), self.assertRaises(recipe.RecipeError):
                recipe.decode(raw)

    def test_a_claimed_admission_is_attributed_to_its_exchange(self):
        from mqttiumd_oracle import judge

        for raw in (claimed_publication([(0, 5)]), claimed_publication([(0, 5), (1, 6)]),
                    annotated_publication(inner=claimed_publication([(0, 5)]))):
            run = judge.Run.__new__(judge.Run)
            operation = "an-operation"
            with mock.patch.object(judge.Run, "recipes", return_value={operation: {raw.hex()}}):
                self.assertEqual(run.slots(), {operation: ("AdmitPublication", 1, 7)})

    def test_claimed_publications_stay_outside_the_fixture_scope(self):
        def unmet(snapshot=None, records=()):
            run = SimpleNamespace(
                snapshots={"first": (None, snapshot)} if snapshot is not None else {},
                trace=SimpleNamespace(records=records),
            )
            with mock.patch.object(results.judge.Run, "load", return_value=run):
                return results._scope_unmet(None, None, None)

        for raw in (claimed_publication([(0, 5)]), claimed_publication([(0, 5), (1, 6)])):
            snap = {"outbound": [], "operations": [{"recipe_hex": raw.hex(), "decision": {}}]}
            self.assertIn(results.FIXTURE_PUBSUB_SCOPE, unmet(snapshot=snap))
        for name in ("ClaimedPublication", "MultiClaimedPublication"):
            record = SimpleNamespace(kind="StoreCommandSubmitted", payload={"command_kind": name})
            self.assertIn(results.FIXTURE_PUBSUB_SCOPE, unmet(records=[record]))


if __name__ == "__main__":
    unittest.main()
