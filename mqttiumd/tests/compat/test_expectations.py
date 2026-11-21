"""Tests of the absolute expectations; they start no broker.

    python3 -m unittest discover -s mqttiumd/tests/compat -p "test_*.py"
"""
from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
sys.dont_write_bytecode = True
sys.path.insert(0, str(HERE))

import classify  # noqa: E402
import dsl  # noqa: E402
import expectations  # noqa: E402
from expectations import (CLOSED, FAIL, NOT_APPLICABLE, PASS, Each, at, at_most, expectation, in_any_order,  # noqa: E402
                          matches, one_of)
import harness  # noqa: E402
import mqttwire as wire  # noqa: E402
import normalize  # noqa: E402
import scenarios  # noqa: E402,F401  (fills dsl.REGISTRY)
from test_bench import FakePeer  # noqa: E402


def record(mark, *packets, op="recv", end="ok", level=5):
    return {"conn": "a", "op": op, "got": [wire.decode(packet, level) for packet in packets], "end": end, "mark": mark}


class Matching(unittest.TestCase):
    def test_a_mapping_is_matched_on_the_keys_it_names(self):
        packet = wire.decode(wire.suback(7, [0, 128]), 4)
        self.assertTrue(matches({"type": "SUBACK"}, packet))
        self.assertTrue(matches({"type": "SUBACK", "packet_id": 7, "codes": [0, 128]}, packet))
        self.assertFalse(matches({"type": "SUBACK", "packet_id": 8}, packet))
        self.assertFalse(matches({"type": "SUBACK", "reason": 0}, packet), "a key that is absent does not match")
        self.assertFalse(matches({"type": "SUBACK"}, [packet]))

    def test_a_list_is_matched_whole_and_in_order(self):
        self.assertTrue(matches([{"type": "A"}, {"type": "B"}], [{"type": "A", "x": 1}, {"type": "B"}]))
        self.assertFalse(matches([{"type": "A"}], [{"type": "A"}, {"type": "B"}]))
        self.assertFalse(matches([{"type": "B"}, {"type": "A"}], [{"type": "A"}, {"type": "B"}]))
        self.assertTrue(matches([], []))
        self.assertFalse(matches([], [{"type": "A"}]))

    def test_scalars_are_compared_with_their_type(self):
        self.assertTrue(matches(0, 0))
        self.assertFalse(matches(0, False))
        self.assertFalse(matches(1, True))
        self.assertFalse(matches("0", 0))

    def test_alternatives(self):
        closed = {"end": CLOSED, "got": one_of([], [{"type": "DISCONNECT", "reason": 0x81}])}
        self.assertTrue(matches(closed, {"end": "eof", "got": []}))
        self.assertTrue(matches(closed, {"end": "reset", "got": [{"type": "DISCONNECT", "reason": 0x81}]}))
        self.assertFalse(matches(closed, {"end": "still-open", "got": []}))
        self.assertFalse(matches(closed, {"end": "eof", "got": [{"type": "DISCONNECT", "reason": 0x82}]}))
        self.assertFalse(matches(closed, {"end": "eof", "got": [{"type": "SUBACK", "reason": 0x81}]}))

    def test_each(self):
        granted_or_refused = Each(3, (0, 1, 2, 0x80))
        self.assertTrue(matches(granted_or_refused, [0, 0x80, 2]))
        self.assertFalse(matches(granted_or_refused, [0, 0x80]))
        self.assertFalse(matches(granted_or_refused, [0, 0x80, 2, 0]))
        self.assertFalse(matches(granted_or_refused, [0, 0x97, 2]))
        self.assertFalse(matches(granted_or_refused, "abc"))

    def test_in_any_order(self):
        suback = {"type": "SUBACK", "codes": [1]}
        a, b = {"type": "PUBLISH", "topic": "a", "payload": "x"}, {"type": "PUBLISH", "topic": "b"}
        bag = in_any_order({"type": "SUBACK"}, {"type": "PUBLISH", "topic": "a"}, {"type": "PUBLISH", "topic": "b"})
        self.assertTrue(matches(bag, [suback, a, b]))
        self.assertTrue(matches(bag, [b, suback, a]))
        self.assertFalse(matches(bag, [suback, a]), "one is missing")
        self.assertFalse(matches(bag, [suback, a, a]), "one came twice and one is missing")
        self.assertFalse(matches(bag, [suback, a, b, b]), "one more than expected")
        self.assertFalse(matches(bag, [suback, a, {"type": "PUBLISH", "topic": "c"}]), "a foreign one")
        self.assertFalse(matches(bag, "sab"))
        self.assertTrue(matches(in_any_order(), []))
        # Each packet answers one pattern and each pattern one packet,
        # whichever pattern a packet is tried against first.
        loose = in_any_order({"type": "PUBLISH"}, {"type": "PUBLISH", "topic": "a"})
        self.assertTrue(matches(loose, [a, b]))
        self.assertTrue(matches(loose, [b, a]))
        self.assertFalse(matches(loose, [b, b]))

    def test_at_most(self):
        self.assertTrue(matches(at_most(64), 64))
        self.assertTrue(matches(at_most(64), 0))
        self.assertFalse(matches(at_most(64), 65))
        for other in (True, "64", None, 12.0):
            self.assertFalse(matches(at_most(64), other), other)
        self.assertTrue(matches({"type": "PUBLISH", "wire_bytes": at_most(64)}, {"type": "PUBLISH", "wire_bytes": 15}))
        self.assertFalse(matches({"type": "PUBLISH", "wire_bytes": at_most(64)}, {"type": "PUBLISH"}))


ANSWERED = expectation(
    "answered", statement="MQTT-3.8.4-1", requires="A SUBACK",
    when=[at("connack", {"got": [{"type": "CONNACK", "properties": {"shared_subscription_available": 0}}]})],
    then=[at("answer", {"got": [{"type": "SUBACK", "packet_id": 7}], "end": "ok"})])
REFUSING = wire.connack(level=5, props={"shared_subscription_available": 0, "receive_maximum": 8})


class Evaluation(unittest.TestCase):
    def test_pass_fail_and_not_applicable(self):
        answer = record("answer", wire.suback(7, [0], level=5))
        self.assertEqual(expectations.evaluate(ANSWERED, [record("connack", REFUSING), answer])[0], PASS)
        self.assertEqual(expectations.evaluate(ANSWERED, [record("connack", REFUSING),
                                                          record("answer", wire.suback(8, [0], level=5))])[0], FAIL)
        self.assertEqual(expectations.evaluate(ANSWERED, [record("connack", REFUSING),
                                                          record("answer", end="eof")])[0], FAIL)
        self.assertEqual(expectations.evaluate(ANSWERED, [record("connack", wire.connack(level=5)), answer])[0],
                         NOT_APPLICABLE)

    def test_a_record_that_is_missing_never_passes(self):
        self.assertEqual(expectations.evaluate(ANSWERED, [record("connack", REFUSING)])[0], FAIL)
        self.assertEqual(expectations.evaluate(ANSWERED, [])[0], NOT_APPLICABLE)

    def test_what_was_observed_is_one_line_about_the_records_named(self):
        _, observed = expectations.evaluate(ANSWERED, [record("connack", REFUSING),
                                                       record("answer", wire.disconnect(level=5, reason=0x83)),
                                                       record("other", wire.pingresp())])
        self.assertEqual(observed, "connack: recv [CONNACK reason=0x00 shared_subscription_available=0] ok; "
                                   "answer: recv [DISCONNECT reason=0x83] ok")
        _, observed = expectations.evaluate(ANSWERED, [record("connack", wire.connack(level=5)),
                                                       record("answer", wire.suback(7, [0, 0x80], level=5))])
        self.assertEqual(observed, "connack: recv [CONNACK reason=0x00 shared_subscription_available absent] ok")
        _, observed = expectations.evaluate(ANSWERED, [record("connack", REFUSING)])
        self.assertTrue(observed.endswith("; answer: no such record"), observed)
        _, observed = expectations.evaluate(ANSWERED, [record("connack", REFUSING),
                                                       record("answer", wire.suback(7, [0, 0x80], level=5))])
        self.assertTrue(observed.endswith("; answer: recv [SUBACK packet_id=7 codes=0x00,0x80] ok"), observed)

    def test_the_fields_a_pattern_reads_are_shown(self):
        small = expectation("small", statement="MQTT-3.1.2-24", requires="x", then=[
            at("answer", {"got": in_any_order({"type": "PUBLISH", "payload": "s", "wire_bytes": at_most(64)})})])
        delivery = {**wire.decode(wire.publish("t", "s", qos=1, packet_id=3, level=5), 5), "wire_bytes": 9}
        result, observed = expectations.evaluate(small, [{"conn": "a", "op": "recv", "got": [delivery], "end": "ok",
                                                         "mark": "answer"}])
        self.assertEqual((result, observed), (PASS, 'answer: recv [PUBLISH packet_id=3 payload="s" wire_bytes=9] ok'))
        result, observed = expectations.evaluate(small, [{"conn": "a", "op": "recv", "end": "ok", "mark": "answer",
                                                         "got": [{**delivery, "wire_bytes": 114}]}])
        self.assertEqual((result, observed), (FAIL, 'answer: recv [PUBLISH packet_id=3 payload="s" wire_bytes=114] ok'))

    def test_a_long_record_is_shown_by_its_counts(self):
        deliveries = [wire.publish(f"t/{index}", "x", qos=1, packet_id=index + 1, level=5) for index in range(9)]
        many = record("answer", wire.suback(7, [1], level=5), *deliveries, op="drain")
        whole = expectation("whole", statement="MQTT-3.3.1-6", requires="x", then=[at("answer", {"end": "ok"})])
        self.assertEqual(expectations.evaluate(whole, [many])[1], "answer: drain [10 packets: 1 SUBACK, 9 PUBLISH] ok")
        eight = record("answer", *deliveries[:8], op="drain")
        self.assertIn("PUBLISH packet_id=8] ok", expectations.evaluate(whole, [eight])[1])


def listed(identifier, statement="MQTT-3.8.4-1", observed="closes without SUBACK"):
    return {"id": identifier, "statement": statement, "observed": observed}


class Judgement(unittest.TestCase):
    known = {"a/1-v4#x": ANSWERED, "a/1-v4#y": ANSWERED, "a/2-v4#x": ANSWERED}

    def problems(self, evaluated, entries):
        return expectations.judge(evaluated, expectations.check_open(entries), self.known)

    def test_an_expectation_that_fails_must_be_listed(self):
        self.assertEqual(self.problems({"a/1-v4#x": PASS, "a/1-v4#y": NOT_APPLICABLE}, []), [])
        self.assertEqual(self.problems({"a/1-v4#x": FAIL}, []), [("a/1-v4#x", expectations.UNLISTED)])
        self.assertEqual(self.problems({"a/1-v4#x": FAIL}, [listed("a/1-v4#x")]), [])

    def test_a_listed_expectation_that_no_longer_fails_is_stale(self):
        entries = [listed("a/1-v4#x"), listed("a/1-v4#y")]
        self.assertEqual(self.problems({"a/1-v4#x": PASS, "a/1-v4#y": NOT_APPLICABLE}, entries),
                         [("a/1-v4#x", expectations.STALE.format(result=PASS)),
                          ("a/1-v4#y", expectations.STALE.format(result=NOT_APPLICABLE))])

    def test_a_listed_expectation_must_exist_and_cite_its_statement(self):
        self.assertEqual(self.problems({}, [listed("a/9-v4#x")]), [("a/9-v4#x", expectations.UNKNOWN)])
        self.assertEqual(self.problems({"a/1-v4#x": FAIL}, [listed("a/1-v4#x", statement="MQTT-3.8.4-2")]),
                         [("a/1-v4#x", expectations.OTHER_STATEMENT.format(listed="MQTT-3.8.4-2",
                                                                           declared="MQTT-3.8.4-1"))])

    def test_a_scenario_that_was_not_played_is_not_judged(self):
        self.assertEqual(self.problems({"a/1-v4#x": FAIL}, [listed("a/1-v4#x"), listed("a/2-v4#x")]), [])

    def test_the_list_is_checked(self):
        for bad in ({}, [{}], [listed("a/1-v4#x"), listed("a/1-v4#x")], [listed("a/2-v4#x"), listed("a/1-v4#x")],
                    [listed("a/1-v4")], [{**listed("a/1-v4#x"), "note": "x"}], [listed("a/1-v4#x", observed="")],
                    [listed("a/1-v4#x", observed="two\nlines")], [listed("a/1-v4#x", statement="section 3")],
                    [{"id": "a/1-v4#x", "statement": "MQTT-3.8.4-1"}]):
            with self.assertRaises(expectations.OpenError, msg=bad):
                expectations.check_open(bad)
        self.assertEqual(list(expectations.check_open([listed("a/1-v4#x"), listed("a/1-v4#y")])),
                         ["a/1-v4#x", "a/1-v4#y"])

    def test_cited_statements_must_exist(self):
        known = {"3.1.1": {"MQTT-3.8.4-1"}, "5.0": {"MQTT-4.13.1-1"}}
        declared = {"a/1-v4#x": ANSWERED, "a/2-v5#x": ANSWERED}
        problems = expectations.check_statements(declared, {"a/1-v4": "3.1.1", "a/2-v5": "5.0"}, known)
        self.assertEqual([name for name, _ in problems], ["a/2-v5#x"])
        self.assertEqual(expectations.check_statements(declared, {}, None), [])


class Marks(unittest.TestCase):
    def tearDown(self):
        for name in [name for name in dsl.REGISTRY if name.startswith("connect/test-of-")]:
            del dsl.REGISTRY[name]

    def test_a_mark_labels_the_record_of_the_step_before_it(self):
        entry = dsl.scenario("connect/test-of-marks-v4", doc="x", steps=[
            dsl.connect("a", "c"), dsl.mark("connack"), dsl.send("a", wire.pingreq()), dsl.recv("a"), dsl.mark("pong")])
        self.assertEqual([(step["op"], step.get("mark")) for step in entry.steps],
                         [("open", None), ("send", None), ("recv", "connack"), ("send", None), ("recv", "pong")])

        def answer(connection, packet):
            connection.sendall(wire.connack() if packet["type"] == "CONNECT" else wire.pingresp())

        peer = FakePeer(answer)
        self.addCleanup(peer.stop)
        raw = harness.Player(peer.port).play(entry.steps)
        self.assertEqual([step.get("mark") for step in raw], [None, "connack", None, "pong"])
        self.assertTrue(all("mark" not in step for step in normalize.normalize(raw, entry)))

    def test_marks_and_expectations_are_checked_at_registration(self):
        steps = [dsl.connect("a", "c"), dsl.mark("connack")]
        good = expectation("x", statement="MQTT-3.1.4-4", requires="x", then=[at("connack", {"end": "ok"})])
        for number, (bad_steps, declared) in enumerate((
                ([dsl.mark("first")], []),                                    # nothing before it
                ([dsl.open_("a"), dsl.mark("opened")], []),                    # an open leaves no record
                ([*steps, dsl.mark("again")], []),                             # two marks on one record
                ([*steps, dsl.send("a", wire.pingreq()), dsl.mark("connack")], []),    # one name twice
                (steps, [expectation("x", statement="MQTT-3.1.4-4", requires="x", then=[at("other", {})])]),
                (steps, [good, good]))):
            with self.assertRaises(ValueError, msg=number):
                dsl.scenario(f"connect/test-of-bad-{number}-v4", doc="x", steps=bad_steps, expectations=declared)
        for bad in ({"statement": "section 3"}, {"requires": ""}, {"then": []}, {"name": "a#b"}):
            with self.assertRaises(ValueError, msg=bad):
                expectation(**{"name": "x", "statement": "MQTT-3.1.4-4", "requires": "x",
                               "then": [at("connack", {})], **bad})


def closed_with(mark, *packets, op="until_close", end="eof"):
    return record(mark, *packets, op=op, end=end)


def disconnect(reason):
    return wire.disconnect(level=5, reason=reason)


V5 = {"shared": wire.connack(level=5, props={"shared_subscription_available": 0}),
      "subid": wire.connack(level=5, props={"subscription_identifier_available": 0}),
      "plain": wire.connack(level=5)}
TWELVE = "subscribe/twelve-filters-v4#suback"
SHARE_LITERAL = "subscribe/share-literal-filter-v4#suback"
UNSUBSCRIBE_SHARE = "subscribe/unsubscribe-share-filter-v4#unsuback"
SHARED = "subscribe/shared-subscription-v5#refusal"
SUBSCRIPTION_IDENTIFIER = "subscribe/subscription-identifier-v5#refusal"
RESERVED = "subscribe/reserved-option-bits-v5#closure"

# For each expectation checked in: transcripts that conform, transcripts
# that do not (the first one is what the independent audit of 8e4303d
# observed, or the nearest wrong answer), and transcripts to which the
# expectation does not apply.
CASES = {
    SHARE_LITERAL: {
        PASS: [[record("answer", wire.suback(0x0A01, [0]), level=4)], [record("answer", wire.suback(0x0A01, [0x80]), level=4)]],
        FAIL: [[record("answer", end="eof", level=4)], [record("answer", wire.suback(0x0A02, [0]), level=4)],
               [record("answer", wire.suback(0x0A01, [0, 0]), level=4)], [record("answer", end="timeout", level=4)]],
    },
    UNSUBSCRIBE_SHARE: {
        PASS: [[record("answer", wire.unsuback(0x0A02), level=4)]],
        FAIL: [[record("answer", end="eof", level=4)], [record("answer", wire.unsuback(0x0A03), level=4)],
               [record("answer", wire.suback(0x0A02, [0]), level=4)]],
    },
    TWELVE: {
        PASS: [[record("answer", wire.suback(1, [0] * 12), level=4)], [record("answer", wire.suback(1, [0x80] * 12), level=4)],
               [record("answer", wire.suback(1, [0] * 8 + [0x80] * 4), level=4)]],
        FAIL: [[record("answer", end="eof", level=4)], [record("answer", wire.suback(1, [0] * 8), level=4)],
               [record("answer", wire.suback(1, [0x97] * 12), level=4)]],
    },
    SHARED: {
        PASS: [[record("connack", V5["shared"]), record("answer", disconnect(0x9E)), closed_with("after", op="quiet")],
               [record("connack", V5["shared"]), record("answer", end="eof"), closed_with("after", op="quiet", end="reset")]],
        FAIL: [[record("connack", V5["shared"]), record("answer", wire.suback(1, [0x9E], level=5)),
                record("after", op="quiet", end="quiet")],
               [record("connack", V5["shared"]), record("answer", disconnect(0x83)), closed_with("after", op="quiet")],
               [record("connack", V5["shared"]), record("answer", disconnect(0x9E)), record("after", op="quiet", end="quiet")]],
        NOT_APPLICABLE: [[record("connack", V5["plain"]), record("answer", wire.suback(1, [1], level=5)),
                          record("after", wire.publish("shared/t", "x", qos=1, packet_id=1, level=5), op="quiet", end="quiet")]],
    },
    SUBSCRIPTION_IDENTIFIER: {
        PASS: [[record("connack", V5["subid"]), record("answer", disconnect(0xA1)), closed_with("after", op="quiet")],
               [record("connack", V5["subid"]), record("answer", end="reset"), closed_with("after", op="quiet", end="reset")]],
        FAIL: [[record("connack", V5["subid"]), record("answer", disconnect(0x83)), closed_with("after", op="quiet")],
               [record("connack", V5["subid"]), record("answer", wire.suback(1, [0], level=5)),
                record("after", op="quiet", end="quiet")]],
        NOT_APPLICABLE: [[record("connack", V5["plain"]), record("answer", wire.suback(1, [0], level=5)),
                          record("after", op="quiet", end="quiet")]],
    },
    RESERVED: {
        PASS: [[closed_with("closure", disconnect(0x81))], [closed_with("closure")], [closed_with("closure", end="reset")]],
        FAIL: [[closed_with("closure", wire.suback(1, [0], level=5), end="still-open")],
               [closed_with("closure", disconnect(0x82))], [closed_with("closure", disconnect(0x81), end="still-open")],
               [closed_with("closure", wire.suback(1, [0], level=5), disconnect(0x81))]],
    },
}


def kept(count, word, *, qos=1, retain=True):
    """The SUBACK and the retained messages of `count` topics, as a
    subscriber at QoS 1 reads them."""
    return [wire.suback(1, [qos]),
            *[wire.publish(f"{word}/{index:02d}", f"r{index:02d}", qos=qos, packet_id=index if qos else None,
                           retain=retain) for index in range(1, count + 1)]]


def drained(*packets, end="ok"):
    return record("retained", *packets, op="drain", end=end, level=4)


def silent(mark, *packets, end="quiet", level=4):
    return record(mark, *packets, op="quiet", end=end, level=level)


def store_cases(count, word):
    """Transcripts for the content of a retained store of `count` topics."""
    whole = kept(count, word)
    return {
        PASS: [[drained(*whole), silent("after")],
               [drained(*whole[::-1]), silent("after")],
               # A broker that grants QoS 0 owes the same messages.
               [drained(*kept(count, word, qos=0)), silent("after")]],
        FAIL: [[drained(*whole[:-8], end="idle"), silent("after")],            # eight are missing
               [drained(*whole[:-1], whole[1]), silent("after")],              # one twice, one missing
               [drained(*whole, whole[1]), silent("after")],                   # one twice
               [drained(*kept(count, word, retain=False)), silent("after")],   # without RETAIN
               [drained(*whole[:-1], wire.publish(f"{word}/{count:02d}", "other", qos=1, packet_id=count,
                                                  retain=True)), silent("after")],
               [drained(*whole[:-1], wire.publish("elsewhere/01", f"r{count:02d}", qos=1, packet_id=count,
                                                  retain=True)), silent("after")],
               [drained(*whole), silent("after", whole[1])],                   # one again afterwards
               [drained(*whole), silent("after", end="eof")],
               [drained(*whole)]],
    }


def sized(packet, wire_bytes=None, level=5):
    """A packet as the player records it, with its size on the wire."""
    return {**wire.decode(packet, level), "wire_bytes": len(packet) if wire_bytes is None else wire_bytes}


def read(mark, *packets, op="recv", end="ok"):
    return {"conn": "sub", "op": op, "got": list(packets), "end": end, "mark": mark}


def limit_cases(qos):
    """Transcripts for a delivery over the subscriber's Maximum Packet Size."""
    def delivery(payload, **more):
        return sized(wire.publish("limit/t", payload, qos=qos, packet_id=1, level=5, **more))

    large = delivery(b"x" * 100)
    small, following = delivery("small"), delivery("next")
    resumed = sized(wire.connack(True, level=5))
    discarded = {
        PASS: [[read("large", op="quiet", end="quiet"), read("small", small), read("after-small", op="quiet", end="quiet")]],
        FAIL: [
            # The frozen behaviour: the connection is closed and nothing more is delivered.
            [read("large", sized(wire.disconnect(level=5, reason=0x95)), op="quiet", end="eof"),
             read("small", end="eof"), read("after-small", op="quiet", end="eof")],
            [read("large", op="quiet", end="eof"), read("small", end="eof"), read("after-small", op="quiet", end="eof")],
            # The packet is sent although it exceeds the limit.
            [read("large", large, op="quiet", end="quiet"), read("small", small),
             read("after-small", op="quiet", end="quiet")],
            [read("large", op="quiet", end="quiet"), read("small", large), read("after-small", small, op="quiet", end="quiet")],
            # Nothing is delivered any more, or a packet over the limit is.
            [read("large", op="quiet", end="quiet"), read("small", end="timeout"),
             read("after-small", op="quiet", end="quiet")],
            [read("large", op="quiet", end="quiet"), read("small", {**small, "wire_bytes": 65}),
             read("after-small", op="quiet", end="quiet")],
            [read("large", op="quiet", end="quiet"), read("small", delivery("small", dup=True)),
             read("after-small", op="quiet", end="quiet")],
            [read("large", op="quiet", end="quiet"), read("small", small), read("after-small", large, op="quiet", end="quiet")],
        ],
    }
    kept_out = {
        PASS: [[read("resumed", resumed), read("no-redelivery", op="quiet", end="quiet"), read("next", following)]],
        FAIL: [
            # The frozen behaviour: the obligation was kept and is delivered to the larger connection.
            [read("resumed", resumed), read("no-redelivery", large, op="quiet", end="quiet"), read("next", following)],
            [read("resumed", resumed), read("no-redelivery", delivery(b"x" * 100, dup=True), op="quiet", end="quiet"),
             read("next", following)],
            [read("resumed", resumed), read("no-redelivery", op="quiet", end="quiet"), read("next", large)],
            [read("resumed", resumed), read("no-redelivery", op="quiet", end="quiet"),
             read("next", delivery("next", dup=True))],
            # The session was not kept.
            [read("resumed", sized(wire.connack(False, level=5))), read("no-redelivery", op="quiet", end="quiet"),
             read("next", end="timeout")],
            [read("resumed", resumed), read("no-redelivery", op="quiet", end="quiet"), read("next", end="timeout")],
        ],
    }
    return discarded, kept_out


STORE_AT_CAPACITY = "retained/thirty-two-topics-qos1-v4#store"
STORE_BEYOND = "retained/forty-topics-qos1-v4#store"
CASES[STORE_AT_CAPACITY] = store_cases(32, "thirty-two")
CASES[STORE_BEYOND] = store_cases(40, "forty")
# What mqttiumd does today beyond the thirty-two topics it keeps.
CASES[STORE_BEYOND][FAIL].insert(0, [drained(*kept(32, "forty"), end="idle"), silent("after")])
for level_of_service in (1, 2):
    name = f"publish/over-maximum-packet-size-qos{level_of_service}-v5#"
    CASES[name + "discarded"], CASES[name + "not-redelivered"] = limit_cases(level_of_service)
del level_of_service, name
# Requests with many filters: what the broker did before it served them as
# unit requests comes first among the failures.
for version in (4, 5):
    refused = 0x80 if version == 4 else 0x97
    name = f"subscribe/many-filters-effective-v{version}#"
    CASES[name + "granted"] = {
        PASS: [[record("answer", wire.suback(1, [0] * 12, level=version), level=version)]],
        FAIL: [[record("answer", wire.suback(1, [refused] * 12, level=version), level=version)],
               [record("answer", wire.suback(1, [0] * 8, level=version), level=version)],
               [record("answer", wire.suback(1, [0] * 11 + [refused], level=version), level=version)],
               [record("answer", end="eof", level=version)]],
    }
    CASES[name + "effective"] = {
        PASS: [[record("delivered", wire.publish("eff/11", "twelfth", level=version), level=version)]],
        FAIL: [[record("delivered", end="timeout", level=version)],
               [record("delivered", wire.publish("eff/10", "twelfth", level=version), level=version)],
               [record("delivered", end="eof", level=version)]],
    }
    name = f"subscribe/unsubscribe-nine-filters-v{version}#"
    CASES[name + "acknowledged"] = {
        PASS: [[record("answer", wire.unsuback(3, [0] * 9 if version == 5 else (), level=version), level=version)]],
        FAIL: [[record("answer", end="eof", level=version)],
               [record("answer", wire.unsuback(4, [0] * 9 if version == 5 else (), level=version), level=version)],
               [record("answer", wire.suback(3, [0] * 9, level=version), level=version)]],
    }
    if version == 5:
        CASES[name + "acknowledged"][FAIL].insert(
            0, [record("answer", wire.unsuback(3, [0x83] * 9, level=5), level=5)])
    CASES[name + "effective"] = {
        PASS: [[record("next", wire.publish("nine/9", "kept", level=version), level=version)]],
        FAIL: [[record("next", wire.publish("nine/8", "removed", level=version), level=version)],
               [record("next", end="timeout", level=version)]],
    }
del version, refused, name
LIMIT = "errors/packet-at-announced-maximum-v5#limit"
ANNOUNCING = wire.connack(level=5, props={"maximum_packet_size": 32_776})
CASES[LIMIT] = {
    PASS: [[record("connack", ANNOUNCING), record("exact", wire.puback(1, level=5)),
            closed_with("over", disconnect(0x95))],
           [record("connack", ANNOUNCING), record("exact", wire.puback(1, level=5, reason=0x10)),
            closed_with("over", disconnect(0x95), end="reset")]],
    # What the broker did before the announcement was also a limit, then
    # other wrong answers.
    FAIL: [[record("connack", ANNOUNCING), record("exact", wire.puback(1, level=5)),
            closed_with("over", wire.puback(2, level=5), end="still-open")],
           [record("connack", ANNOUNCING), closed_with("exact", disconnect(0x95), op="recv"),
            closed_with("over")],
           [record("connack", ANNOUNCING), record("exact", wire.puback(1, level=5)), closed_with("over")],
           [record("connack", ANNOUNCING), record("exact", wire.puback(1, level=5)),
            closed_with("over", disconnect(0x83))]],
    NOT_APPLICABLE: [[record("connack", V5["plain"]), record("exact", wire.puback(1, level=5)),
                      closed_with("over", wire.puback(2, level=5), end="still-open")]],
}


class Repository(unittest.TestCase):
    """The expectations checked in, whatever the brokers do."""

    declared = expectations.declared(dsl.REGISTRY)

    def test_each_expectation_has_its_cases(self):
        self.assertEqual(sorted(self.declared), sorted(CASES))
        cited = {SHARE_LITERAL: {"MQTT-3.8.4-1"}, UNSUBSCRIBE_SHARE: {"MQTT-3.10.4-5", "MQTT-3.10.4-4"},
                 TWELVE: {"MQTT-3.8.4-5"}, SHARED: {"MQTT-4.13.1-1"}, SUBSCRIPTION_IDENTIFIER: {"MQTT-4.13.1-1"},
                 RESERVED: {"MQTT-3.8.3-5", "MQTT-4.13.1-1"},
                 STORE_AT_CAPACITY: {"MQTT-3.3.1-5", "MQTT-3.3.1-6", "MQTT-3.3.1-8"},
                 STORE_BEYOND: {"MQTT-3.3.1-5", "MQTT-3.3.1-6", "MQTT-3.3.1-8"},
                 **{f"publish/over-maximum-packet-size-qos{qos}-v5#discarded": {"MQTT-3.1.2-25", "MQTT-3.1.2-24"}
                    for qos in (1, 2)},
                 **{f"publish/over-maximum-packet-size-qos{qos}-v5#not-redelivered": {"MQTT-3.1.2-25", "MQTT-3.2.2-3"}
                    for qos in (1, 2)},
                 **{f"subscribe/many-filters-effective-v{version}#{name}":
                    {"MQTT-3.8.4-4" if version == 4 else "MQTT-3.8.4-5"}
                    for version in (4, 5) for name in ("granted", "effective")},
                 **{f"subscribe/unsubscribe-nine-filters-v{version}#{name}": {"MQTT-3.10.4-6"}
                    for version in (4, 5) for name in ("acknowledged", "effective")},
                 LIMIT: {"MQTT-4.13.1-1"}}
        self.assertEqual(sorted(cited), sorted(CASES))
        for name, statements in cited.items():
            found = {match.group(0) for match in classify.STATEMENT.finditer(self.declared[name].statement)}
            self.assertLessEqual(statements, found, name)

    def test_each_expectation_tells_conformant_behaviour_from_the_rest(self):
        for name, cases in CASES.items():
            for wanted, transcripts in cases.items():
                for index, transcript in enumerate(transcripts):
                    result, observed = expectations.evaluate(self.declared[name], transcript)
                    self.assertEqual(result, wanted, (name, wanted, index, observed))
                    self.assertNotIn("\n", observed)

    def test_cited_statements_exist_in_the_version_of_the_scenario(self):
        versions = {name: scenario.version for name, scenario in dsl.REGISTRY.items()}
        known = classify.known_statements(HERE.parents[2] / "docs/spec")
        self.assertIsNotNone(known)
        self.assertEqual(expectations.check_statements(self.declared, versions, known), [])

    def test_the_list_of_open_expectations_is_coherent(self):
        listed_open = expectations.load_open(HERE / "expectations-open.json")
        self.assertEqual(expectations.judge({}, listed_open, self.declared), [])

    def test_an_unreadable_list_is_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "expectations-open.json"
            path.write_text(json.dumps([{"id": "x"}]))
            with self.assertRaises(expectations.OpenError):
                expectations.load_open(path)


if __name__ == "__main__":
    unittest.main()
