"""Tests of the bench itself; they start no broker.

    python3 -m unittest discover -s mqttiumd/tests/compat -p "test_*.py"
"""
from __future__ import annotations

from pathlib import Path
import sys
import unittest

HERE = Path(__file__).resolve().parent
sys.dont_write_bytecode = True
sys.path.insert(0, str(HERE))

import classify  # noqa: E402
import dsl  # noqa: E402
import harness  # noqa: E402
import mqttwire as wire  # noqa: E402
import normalize  # noqa: E402
import scenarios  # noqa: E402,F401  (fills dsl.REGISTRY)


class Codec(unittest.TestCase):
    def test_connect_with_will_and_properties_round_trips(self):
        data = wire.connect("c", level=5, clean=False, keepalive=7, username="u", password=b"p",
                            props={"session_expiry_interval": 9},
                            will={"topic": "w/t", "payload": "bye", "qos": 1, "retain": True,
                                  "props": {"will_delay_interval": 3}})
        self.assertEqual(wire.decode(data, 5), {
            "type": "CONNECT", "protocol_name": "MQTT", "level": 5, "clean": False, "keepalive": 7,
            "properties": {"session_expiry_interval": 9}, "client_id": "c", "username": "u", "password": "p",
            "will": {"qos": 1, "retain": True, "properties": {"will_delay_interval": 3}, "topic": "w/t",
                     "payload": "bye"}})

    def test_publish_properties_keep_user_property_order(self):
        data = wire.publish("t", b"\xff", qos=2, packet_id=3, level=5,
                            props=[("user_property", ("b", "2")), ("content_type", "x"), ("user_property", ("a", "1"))])
        self.assertEqual(wire.decode(data, 5), {
            "type": "PUBLISH", "dup": False, "qos": 2, "retain": False, "topic": "t", "packet_id": 3,
            "properties": {"user_property": [["b", "2"], ["a", "1"]], "content_type": "x"}, "payload": {"hex": "ff"}})

    def test_same_bytes_read_at_each_level(self):
        self.assertEqual(wire.decode(wire.puback(5), 4), {"type": "PUBACK", "packet_id": 5})
        self.assertEqual(wire.decode(wire.puback(5), 5), {"type": "PUBACK", "packet_id": 5, "reason": 0})
        self.assertEqual(wire.decode(wire.suback(2, [0, 128]), 4), {"type": "SUBACK", "packet_id": 2, "codes": [0, 128]})

    def test_malformed_input_is_refused(self):
        for data in (wire.raw(0x30, b"\xff\xff\xff\xff\x7f"), wire.subscribe(1, [("t", 0)], flags=0), b"\x20\x02\x00"):
            with self.assertRaises(wire.WireError):
                wire.decode(data, 4)

    def test_split_keeps_an_incomplete_packet(self):
        stream = wire.pingresp() + wire.puback(1) + wire.publish("t", "abc")[:4]
        packets, rest = wire.split_packets(stream)
        self.assertEqual(packets, [wire.pingresp(), wire.puback(1)])
        self.assertEqual(rest, wire.publish("t", "abc")[:4])


def received(conn, *packets):
    return {"conn": conn, "op": "recv", "end": "ok", "got": [wire.decode(packet, 5) for packet in packets]}


def sent(conn, packet):
    return {"conn": conn, "op": "send", "packet": wire.decode(packet, 5)}


class Normalization(unittest.TestCase):
    scenario = dsl.Scenario(name="x/y-v5", area="x", version="5.0", doc="", steps=(),
                            buckets={"message_expiry_interval": [0, 590, 600]})

    def ids(self, transcript):
        out = normalize.normalize(transcript, self.scenario)
        return [packet["packet_id"] for record in out for packet in record.get("got", [record.get("packet")])]

    def test_broker_identifiers_are_labelled_in_order_of_use(self):
        counting = [received("s", wire.publish("t", "a", qos=1, packet_id=1, level=5)), sent("s", wire.puback(1, level=5)),
                    received("s", wire.publish("t", "b", qos=1, packet_id=2, level=5)), sent("s", wire.puback(2, level=5))]
        reusing = [received("s", wire.publish("t", "a", qos=1, packet_id=7, level=5)), sent("s", wire.puback(7, level=5)),
                   received("s", wire.publish("t", "b", qos=1, packet_id=7, level=5)), sent("s", wire.puback(7, level=5))]
        self.assertEqual(self.ids(counting), ["b1", "b1", "b2", "b2"])
        self.assertEqual(self.ids(counting), self.ids(reusing))

    def test_a_redelivery_keeps_its_label_and_another_identifier_shows(self):
        first = received("s", wire.publish("t", "a", qos=1, packet_id=4, level=5))
        again = received("s", wire.publish("t", "a", qos=1, packet_id=4, dup=True, level=5))
        other = received("s", wire.publish("t", "a", qos=1, packet_id=5, dup=True, level=5))
        self.assertEqual(self.ids([first, again]), ["b1", "b1"])
        self.assertEqual(self.ids([first, other]), ["b1", "b2"])

    def test_qos2_identifier_lives_until_pubcomp(self):
        transcript = [received("s", wire.publish("t", "a", qos=2, packet_id=9, level=5)), sent("s", wire.pubrec(9, level=5)),
                      received("s", wire.pubrel(9, level=5)), sent("s", wire.pubcomp(9, level=5)),
                      received("s", wire.publish("t", "b", qos=2, packet_id=9, level=5))]
        self.assertEqual(self.ids(transcript), ["b1", "b1", "b1", "b1", "b2"])

    def test_client_identifiers_and_unknown_ones_are_kept(self):
        transcript = [received("p", wire.puback(21, level=5)), sent("p", wire.puback(77, level=5)),
                      sent("p", wire.pubrel(3, level=5))]
        self.assertEqual(self.ids(transcript), [21, 77, 3])

    def test_volatile_properties(self):
        connack = wire.connack(level=5, props={"assigned_client_identifier": "auto-1", "receive_maximum": 20,
                                               "reason_string": "hello", "server_keep_alive": 10})
        publish = wire.publish("t", "a", level=5, props={"message_expiry_interval": 599})
        out = normalize.normalize([received("s", connack, publish)], self.scenario)
        self.assertEqual(out[0]["got"][0]["properties"],
                         {"assigned_client_identifier": normalize.ASSIGNED, "server_keep_alive": 10})
        self.assertEqual(out[0]["got"][1]["properties"], {"message_expiry_interval": "591..600"})

    def test_bucket(self):
        self.assertEqual([normalize.bucket(value, [0, 5, 9, 10]) for value in (0, 3, 8, 10, 11)],
                         ["0", "1..5", "6..9", "10", ">10"])


def entry(name, kind="defect", **more):
    return {"id": name, "class": kind, "difference": "x", "fingerprint": "aaaa", **more}


class Classification(unittest.TestCase):
    def test_judge(self):
        divergences = classify.check_divergences([entry("a/1-v4"), entry("a/2-v4"), entry("a/3-v4"), entry("a/gone-v4")])
        observed = {"a/1-v4": (classify.DIFFERENT, "aaaa"), "a/2-v4": (classify.SAME, None),
                    "a/3-v4": (classify.DIFFERENT, "bbbb"), "a/4-v4": (classify.DIFFERENT, "cccc"),
                    "a/5-v4": (classify.SAME, None), "a/6-v4": (classify.ERROR, None)}
        classes, problems = classify.judge(observed, divergences, set(observed))
        self.assertEqual(classes, {"a/1-v4": "defect", "a/2-v4": "same", "a/3-v4": "defect",
                                   "a/4-v4": classify.UNCLASSIFIED, "a/5-v4": "same", "a/6-v4": "error"})
        self.assertEqual(dict(problems), {
            "a/gone-v4": classify.UNKNOWN, "a/2-v4": classify.STALE,
            "a/3-v4": classify.CHANGED.format(now="bbbb", then="aaaa"),
            "a/4-v4": classify.NOT_LISTED, "a/6-v4": classify.BROKEN})

    def test_entries_are_checked(self):
        for bad in ({}, [entry("a/1-v4"), entry("a/1-v4")], [entry("a/1-v4", kind="odd")],
                    [entry("a/1-v4", kind="reference-nonconformant")],
                    [entry("a/1-v4", kind="reference-nonconformant", statement="section 3", mqttiumd="x")],
                    [entry("a/1-v4", kind="unspecified", lot="x")], [entry("a/1-v4", note="x")],
                    [{"id": "a/1-v4", "class": "defect", "difference": "x"}]):
            with self.assertRaises(classify.DivergenceError):
                classify.check_divergences(bad)
        classify.check_divergences([entry("a/1-v4", kind="reference-nonconformant", statement="MQTT-3.1.4-2",
                                          mqttiumd="closes")])

    def test_cited_statements_must_exist(self):
        known = {"3.1.1": {"MQTT-3.1.3-9"}, "5.0": {"MQTT-4.13.1-1"}}
        divergences = classify.check_divergences([entry("a/1-v4", statement="MQTT-4.13.1-1"),
                                                  entry("a/2-v5", statement="MQTT-4.13.1-1 and MQTT-9.9.9-9")])
        problems = classify.check_statements(divergences, {"a/1-v4": "3.1.1", "a/2-v5": "5.0"}, known)
        self.assertEqual([name for name, _ in problems], ["a/1-v4", "a/2-v5"])
        self.assertEqual(classify.check_statements(divergences, {}, None), [])


def record_of(divergences, name):
    """What a run observes when `name` reproduces exactly as it is classified."""
    known = divergences.get(name)
    return (classify.DIFFERENT, known["fingerprint"]) if known else (classify.SAME, None)


class Incidents(unittest.TestCase):
    """A broker that exits, hangs or panics never hides behind a classification."""

    def problems(self, divergences, name, incidents):
        classes, problems = classify.judge({name: record_of(divergences, name)}, divergences, {name})
        self.assertEqual(problems, [], name)
        return classify.judge_incidents({name: incidents}, classes, divergences)

    def test_an_exit_recorded_in_the_fingerprint_fails_in_every_class(self):
        reference = [{"conn": "a", "op": "until_close", "got": [], "end": "still-open"}]
        broker = [{"conn": "a", "op": "until_close", "got": [], "end": "closed"}, {"op": "broker", "end": "exited"}]
        fingerprint = normalize.fingerprint(reference, broker)
        for kind in classify.CLASSES:
            divergences = classify.check_divergences([
                {**entry("a/1-v4", kind, statement="MQTT-3.1.4-2", mqttiumd="x"), "fingerprint": fingerprint}])
            self.assertEqual([name for name, _ in self.problems(divergences, "a/1-v4", {"mqttiumd": ["exited"]})],
                             ["a/1-v4"], kind)

    def test_an_incident_with_unchanged_transcripts_fails_in_every_class(self):
        for kind in (None, *classify.CLASSES):
            listed = [] if kind is None else [entry("a/1-v4", kind, statement="MQTT-3.1.4-2", mqttiumd="x")]
            divergences = classify.check_divergences(listed)
            for backend in harness.BACKENDS:
                for incident in classify.INCIDENTS:
                    self.assertEqual(len(self.problems(divergences, "a/1-v4", {backend: [incident]})), 1,
                                     (kind, backend, incident))
            self.assertEqual(len(self.problems(divergences, "a/1-v4", {"mqttiumd": ["killed", "panic"]})), 2, kind)
            self.assertEqual(self.problems(divergences, "a/1-v4", {}), [], kind)

    def test_no_entry_checked_in_hides_an_incident(self):
        divergences = classify.load_divergences(HERE / "divergences.json")
        for name, known in divergences.items():
            recorded = known.get("incident", {})
            self.assertEqual(self.problems(divergences, name, recorded), [], name)
            for backend in harness.BACKENDS:
                for incident in classify.INCIDENTS:
                    if incident in recorded.get(backend, ()):
                        continue
                    observed = {**recorded, backend: sorted([*recorded.get(backend, ()), incident])}
                    self.assertEqual(len(self.problems(divergences, name, observed)), 1, (name, backend, incident))

    def test_a_recorded_incident_is_expected_and_goes_stale(self):
        divergences = classify.check_divergences([entry("a/1-v4", incident={"mqttiumd": ["exited", "panic"]})])
        self.assertEqual(self.problems(divergences, "a/1-v4", {"mqttiumd": ["exited", "panic"]}), [])
        # Only what is recorded: another kind, another backend.
        self.assertEqual(len(self.problems(divergences, "a/1-v4", {"mqttiumd": ["exited", "killed", "panic"]})), 1)
        self.assertEqual(len(self.problems(divergences, "a/1-v4",
                                           {"mqttiumd": ["exited", "panic"], "reference": ["exited"]})), 1)
        # A recorded incident that did not happen is a stale entry.
        self.assertEqual(len(self.problems(divergences, "a/1-v4", {"mqttiumd": ["exited"]})), 1)
        self.assertEqual(len(self.problems(divergences, "a/1-v4", {})), 2)

    def test_a_scenario_that_was_not_played_is_not_judged(self):
        divergences = classify.check_divergences([entry("a/1-v4", incident={"mqttiumd": ["exited"]}), entry("a/2-v4")])
        classes, _ = classify.judge({"a/2-v4": (classify.DIFFERENT, "aaaa")}, divergences, {"a/1-v4", "a/2-v4"})
        self.assertEqual(classify.judge_incidents({"a/2-v4": {}}, classes, divergences), [])

    def test_recorded_incidents_are_checked(self):
        for kind, incident in (("defect", {}), ("defect", []), ("defect", {"other": ["exited"]}),
                               ("defect", {"mqttiumd": []}), ("defect", {"mqttiumd": "exited"}),
                               ("defect", {"mqttiumd": ["crashed"]}), ("defect", {"mqttiumd": ["panic", "exited"]}),
                               ("defect", {"mqttiumd": ["exited", "exited"]}),
                               # A crash of mqttiumd is a defect, whatever else differs.
                               ("profile-refused", {"mqttiumd": ["exited"]}),
                               ("unspecified", {"reference": ["killed"], "mqttiumd": ["killed"]})):
            with self.assertRaises(classify.DivergenceError, msg=(kind, incident)):
                classify.check_divergences([entry("a/1-v4", kind, incident=incident)])
        classify.check_divergences([entry("a/1-v4", "unspecified", incident={"reference": ["exited", "killed", "panic"]})])
        classify.check_divergences([entry("a/1-v4", incident={"mqttiumd": ["killed"], "reference": ["panic"]})])


class Repository(unittest.TestCase):
    """What is checked in is coherent, whatever the brokers do."""

    def test_scenarios(self):
        self.assertGreaterEqual(len(dsl.REGISTRY), 60)
        for name, scenario in dsl.REGISTRY.items():
            self.assertTrue(scenario.doc, name)
            self.assertIn(scenario.steps[0]["op"], ("open",), name)
            for step in scenario.steps:
                self.assertTrue(hasattr(harness.Player, "do_" + step["op"]), (name, step["op"]))

    def test_divergences(self):
        divergences = classify.load_divergences(HERE / "divergences.json")
        self.assertEqual(sorted(divergences), list(divergences), "entries are sorted by id")
        self.assertEqual(set(divergences) - set(dsl.REGISTRY), set())
        versions = {name: scenario.version for name, scenario in dsl.REGISTRY.items()}
        known = classify.known_statements(HERE.parents[2] / "docs/spec")
        self.assertEqual(classify.check_statements(divergences, versions, known), [])


if __name__ == "__main__":
    unittest.main()
