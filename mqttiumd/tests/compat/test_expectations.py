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
from expectations import CLOSED, FAIL, NOT_APPLICABLE, PASS, Each, at, expectation, matches, one_of  # noqa: E402
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


class Repository(unittest.TestCase):
    """The expectations checked in, whatever the brokers do."""

    declared = expectations.declared(dsl.REGISTRY)

    def test_the_expectations_are_the_six_of_the_audit(self):
        self.assertEqual(sorted(self.declared), sorted(CASES))
        cited = {SHARE_LITERAL: {"MQTT-3.8.4-1"}, UNSUBSCRIBE_SHARE: {"MQTT-3.10.4-5", "MQTT-3.10.4-4"},
                 TWELVE: {"MQTT-3.8.4-5"}, SHARED: {"MQTT-4.13.1-1"}, SUBSCRIPTION_IDENTIFIER: {"MQTT-4.13.1-1"},
                 RESERVED: {"MQTT-3.8.3-5", "MQTT-4.13.1-1"}}
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
