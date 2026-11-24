"""Tests of the bench itself; they start no broker.

    python3 -m unittest discover -s mqttiumd/tests/compat -p "test_*.py"
"""
from __future__ import annotations

import dataclasses
import json
from pathlib import Path
import socket
import struct
import sys
import tempfile
import threading
import time
import unittest

HERE = Path(__file__).resolve().parent
sys.dont_write_bytecode = True
sys.path.insert(0, str(HERE))

import classify  # noqa: E402
import dsl  # noqa: E402
import harness  # noqa: E402
import mqttwire as wire  # noqa: E402
import normalize  # noqa: E402
import run  # noqa: E402
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
                    [entry("a/1-v4", decision="record 49")], [entry("a/1-v4", decision=49)],
                    [{"id": "a/1-v4", "class": "defect", "difference": "x"}]):
            with self.assertRaises(classify.DivergenceError):
                classify.check_divergences(bad)
        classify.check_divergences([entry("a/1-v4", kind="reference-nonconformant", statement="MQTT-3.1.4-2",
                                          mqttiumd="closes", decision="0049")])

    def test_a_decision_names_a_record_of_the_repository(self):
        bench = Path(__file__).resolve().parent
        records = {path.name[:4] for path in (bench.parent.parent / "docs/decisions").glob("[0-9]*.md")}
        entries = json.loads((bench / "divergences.json").read_text())
        decided = {item["id"]: item["decision"] for item in entries if "decision" in item}
        self.assertTrue(decided, "no entry names its decision")
        self.assertEqual({name: number for name, number in decided.items() if number not in records}, {})

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
        classes, problems = classify.judge({name: record_of(divergences, name)}, divergences, {name, *divergences})
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


class FakePeer:
    """A scripted peer on loopback, in this process: it accepts one
    connection and hands each packet it reads to `answer(connection,
    packet)`, until the client closes or `answer` returns `STOP`."""

    STOP = object()

    def __init__(self, answer):
        self.answer = answer
        self.listener = socket.create_server((harness.HOST, 0))
        self.port = self.listener.getsockname()[1]
        self.closed = threading.Event()
        self.thread = threading.Thread(target=self.serve, daemon=True)
        self.thread.start()

    def serve(self):
        try:
            connection, _ = self.listener.accept()
        except OSError:
            return
        try:
            buffer = b""
            while True:
                chunk = connection.recv(65536)
                if not chunk:
                    return
                packets, buffer = wire.split_packets(buffer + chunk)
                for data in packets:
                    if self.answer(connection, wire.decode(data, 4)) is self.STOP:
                        return
        except OSError:
            return
        finally:
            connection.close()
            self.closed.set()

    def stop(self):
        self.listener.close()
        self.thread.join(5)


def acknowledging(order):
    """Answers twenty QoS 1 PUBLISH with their PUBACK, in the order `order`
    makes of the identifiers received."""
    pending = []

    def answer(connection, packet):
        if packet["type"] == "CONNECT":
            connection.sendall(wire.connack())
        elif packet["type"] == "PINGREQ":
            connection.sendall(wire.pingresp())
        elif packet["type"] == "PUBLISH":
            pending.append(packet["packet_id"])
            if len(pending) == 20:
                connection.sendall(b"".join(wire.puback(packet_id) for packet_id in order(pending)))
    return answer


def closing(*, reset):
    """Closes on SUBSCRIBE: by end of stream, or with `reset` by a TCP reset."""
    def answer(connection, packet):
        if packet["type"] == "CONNECT":
            connection.sendall(wire.connack())
        elif packet["type"] == "SUBSCRIBE":
            if reset:
                connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            return FakePeer.STOP
        return None
    return answer


def retained_after(connection, packet):
    """Sends the SUBACK of a subscription, then a retained message."""
    if packet["type"] == "CONNECT":
        connection.sendall(wire.connack())
    elif packet["type"] == "SUBSCRIBE":
        connection.sendall(wire.suback(packet["packet_id"], [0]) + wire.publish("t", "kept", retain=True))


def trickling(count, gap):
    """Sends the SUBACK of a subscription, then `count` retained messages
    `gap` seconds apart, and answers PINGREQ at once: the round trip of a
    `collect` ends long before the replay does."""
    def later(connection):
        for index in range(count):
            time.sleep(gap)
            try:
                connection.sendall(wire.publish(f"t/{index}", f"r{index}", retain=True))
            except OSError:
                return

    def answer(connection, packet):
        if packet["type"] == "CONNECT":
            connection.sendall(wire.connack())
        elif packet["type"] == "PINGREQ":
            connection.sendall(wire.pingresp())
        elif packet["type"] == "SUBSCRIBE":
            connection.sendall(wire.suback(packet["packet_id"], [0]))
            threading.Thread(target=later, args=(connection,), daemon=True).start()
    return answer


def acknowledged_late(delay, *, answer_subscribe=True):
    """Answers PINGREQ at once, and a SUBSCRIBE `delay` seconds later with
    its SUBACK and a retained message, as a broker does whose SUBACK waits
    for a durable commit; with `answer_subscribe=False`, never. `delay` may
    map a Packet Identifier to its own delay."""
    def later(connection, packet_id):
        time.sleep(delay[packet_id] if isinstance(delay, dict) else delay)
        try:
            connection.sendall(wire.suback(packet_id, [0]) + wire.publish("t", f"kept{packet_id}", retain=True))
        except OSError:
            return

    def answer(connection, packet):
        if packet["type"] == "CONNECT":
            connection.sendall(wire.connack())
        elif packet["type"] == "PINGREQ":
            connection.sendall(wire.pingresp())
        elif packet["type"] == "SUBSCRIBE" and answer_subscribe:
            threading.Thread(target=later, args=(connection, packet["packet_id"]), daemon=True).start()
    return answer


def one_at_a_time(payloads, *, packet_id=1):
    """Sends the SUBACK of a subscription and one retained QoS 1 message,
    and each next one only when the one before is acknowledged, all under
    the same Packet Identifier: a client that does not acknowledge gets one."""
    pending = list(payloads)

    def send_next(connection):
        if pending:
            payload = pending.pop(0)
            connection.sendall(wire.publish("t/" + payload, payload, qos=1, packet_id=packet_id, retain=True))

    def answer(connection, packet):
        if packet["type"] == "CONNECT":
            connection.sendall(wire.connack())
        elif packet["type"] == "PINGREQ":
            connection.sendall(wire.pingresp())
        elif packet["type"] == "SUBSCRIBE":
            connection.sendall(wire.suback(packet["packet_id"], [1]))
            send_next(connection)
        elif packet["type"] == "PUBACK" and packet["packet_id"] == packet_id:
            send_next(connection)
    return answer


SUBSCRIBED = [dsl.open_("a"), dsl.send("a", wire.connect("c")), dsl.recv("a"),
              dsl.send("a", wire.subscribe(1, [("t/#", 1)]))]


def measured(*payloads, sequence=("m0", "m1", "m2"), ordered=True):
    """The raw record of a `measure` step that counted these payloads."""
    return {"conn": "sub", "op": "measure", "kind": "PUBLISH", "field": "payload", "sequence": list(sequence),
            "ordered": ordered, "end": "quiet",
            "got": [wire.decode(wire.publish("burst/t", payload), 4) for payload in payloads]}


PLAIN =dsl.Scenario(name="x/y-v4", area="x", version="3.1.1", doc="", steps=())


class BlindSpots(unittest.TestCase):
    """What equal transcripts used to hide; played against a peer of this
    process, without a broker."""

    def peer(self, answer):
        peer = FakePeer(answer)
        self.addCleanup(peer.stop)
        return peer

    def play(self, answer, steps):
        return harness.Player(self.peer(answer).port).play(steps)

    def test_permuted_acknowledgements_do_not_compare_equal(self):
        scenario = dsl.REGISTRY["publish/pipelined-qos1-v4"]
        played = {name: normalize.normalize(self.play(acknowledging(order), scenario.steps), scenario)
                  for name, order in (("as sent", list), ("first two swapped", lambda ids: [ids[1], ids[0], *ids[2:]]),
                                      ("reversed", lambda ids: ids[::-1]),
                                      ("one twice", lambda ids: [*ids[:-1], ids[0]]))}
        for name in ("first two swapped", "reversed", "one twice"):
            self.assertFalse(normalize.same(played["as sent"], played[name]), name)
        self.assertEqual({name: transcript[-1]["order"] for name, transcript in played.items()},
                         {"as sent": "as-sent", "first two swapped": "permuted", "reversed": "permuted",
                          "one twice": "other"})
        # The form compared is a verdict: two different permutations are one difference.
        self.assertTrue(normalize.same(played["first two swapped"], played["reversed"]))

    def test_the_raw_transcript_keeps_the_acknowledgements_as_they_came(self):
        scenario = dsl.REGISTRY["publish/pipelined-qos1-v4"]
        raw = self.play(acknowledging(lambda ids: ids[::-1]), scenario.steps)
        self.assertEqual([packet["packet_id"] for packet in raw[-1]["got"] if packet["type"] == "PUBACK"],
                         list(range(20, 0, -1)))
        self.assertEqual(normalize.normalize(raw, scenario)[-1],
                         {"conn": "pub", "op": "tally", "kind": "PUBACK", "expected": 20, "arrived": "all",
                          "order": "permuted", "got": [], "end": "quiet"})

    def test_order_of_a_burst(self):
        self.assertEqual([normalize.order(seen, ["m0", "m1", "m2", "m3"])
                          for seen in ([], ["m0", "m1", "m2", "m3"], ["m0", "m3"], ["m3", "m0"], ["m0", "m0"],
                                       ["m0", "m9"], ["m1", "m0", "m0"])],
                         ["as-sent", "as-sent", "as-sent", "permuted", "other", "other", "other"])
        declared = {name: step for name in ("publish/pipelined-qos1-v4", "publish/burst-qos0-v4")
                    for step in dsl.REGISTRY[name].steps if step["op"] == "tally"}
        self.assertEqual(declared["publish/pipelined-qos1-v4"]["sequence"], list(range(1, 21)))
        self.assertEqual(declared["publish/burst-qos0-v4"]["sequence"], [f"m{index}" for index in range(100)])
        burst = {"conn": "sub", "op": "tally", "kind": "PUBLISH", "expected": 3, "field": "payload",
                 "sequence": ["m0", "m1", "m2"], "end": "quiet",
                 "got": [wire.decode(wire.publish("burst/t", payload), 4) for payload in ("m2", "m0")]}
        self.assertEqual(normalize.normalize([burst], PLAIN),
                         [{"conn": "sub", "op": "tally", "kind": "PUBLISH", "expected": 3, "arrived": "fewer",
                           "order": "permuted", "got": [], "end": "quiet"}])

    def stepwise(self, player, steps):
        self.addCleanup(player.close_all)
        records = [getattr(player, "do_" + step["op"])(step) for step in steps]
        return [record for record in records if record is not None]

    def close_after_subscribe(self, *, reset, then=()):
        """The transcript of a client whose peer closes on SUBSCRIBE; the
        steps `then` are played once the peer has closed."""
        peer = self.peer(closing(reset=reset))
        player = harness.Player(peer.port)
        first = self.stepwise(player, [dsl.open_("a"), dsl.send("a", wire.connect("c")), dsl.recv("a"),
                                       dsl.send("a", wire.subscribe(1, [("t", 0)]))])
        self.assertTrue(peer.closed.wait(5))
        return first + self.stepwise(player, [*then, dsl.until_close("a")])

    def test_end_of_stream_and_reset_are_told_apart(self):
        fin, rst = self.close_after_subscribe(reset=False), self.close_after_subscribe(reset=True)
        self.assertEqual([fin[-1]["end"], rst[-1]["end"]], ["eof", "reset"])
        self.assertFalse(normalize.same(normalize.normalize(fin, PLAIN), normalize.normalize(rst, PLAIN)))
        self.assertEqual(normalize.normalize(rst, PLAIN)[-1]["end"], "reset")
        either = dataclasses.replace(PLAIN, closure="any")
        self.assertTrue(normalize.same(normalize.normalize(fin, either), normalize.normalize(rst, either)))
        self.assertEqual(normalize.normalize(rst, either)[-1]["end"], "closed")

    def test_a_reset_met_by_a_write_is_still_a_reset(self):
        # The write consumes the error of the socket: the read after it
        # sees an end of stream unless the player remembers.
        ping = dsl.send("a", wire.pingreq())
        self.assertEqual(self.close_after_subscribe(reset=True, then=[ping, ping])[-1]["end"], "reset")
        # After an end of stream, the client's own writes are refused by
        # the peer's system; that is not a reset by the peer.
        self.assertEqual(self.close_after_subscribe(reset=False, then=[ping, ping, ping])[-1]["end"], "eof")

    def test_the_raw_transcript_keeps_the_wire_order_of_a_sorted_step(self):
        steps = [dsl.open_("a"), dsl.send("a", wire.connect("c")), dsl.recv("a"),
                 dsl.send("a", wire.subscribe(1, [("t", 0)])), dsl.recv("a", 2, sort=True)]
        raw = self.play(retained_after, steps)
        self.assertEqual([packet["type"] for packet in raw[-1]["got"]], ["SUBACK", "PUBLISH"])
        normalized = normalize.normalize(raw, PLAIN)
        self.assertEqual([packet["type"] for packet in normalized[-1]["got"]], ["PUBLISH", "SUBACK"])
        self.assertEqual(sorted(normalized[-1]), ["conn", "end", "got", "op"])


class Stability(unittest.TestCase):
    """What made a transcript depend on the speed of the machine; played
    against a peer of this process, without a broker."""

    def peer(self, answer):
        peer = FakePeer(answer)
        self.addCleanup(peer.stop)
        return peer

    def play(self, answer, steps):
        return harness.Player(self.peer(answer).port).play(steps)

    def payloads(self, record):
        return [packet["payload"] for packet in record["got"] if packet["type"] == "PUBLISH"]

    def test_a_replay_slower_than_the_silent_window_is_read_to_its_end(self):
        # Eight messages 0.1 s apart take longer than the window of 0.3 s
        # that follows the PINGRESP, and no gap between two of them does.
        raw = self.play(trickling(8, 0.1), [*SUBSCRIBED, dsl.collect("a", window=0.3)])
        self.assertEqual(self.payloads(raw[-1]), [f"r{index}" for index in range(8)])
        self.assertEqual(raw[-1]["end"], "quiet")

    def test_a_collect_after_a_subscribe_waits_for_its_suback(self):
        # The SUBACK comes 0.5 s after a PINGRESP that comes at once: a
        # PINGREQ round trip and a window of 0.3 s end before it.
        raw = self.play(acknowledged_late(0.5), [*SUBSCRIBED, dsl.collect("a", sort=True, suback=1)])
        self.assertEqual([packet["type"] for packet in raw[-1]["got"]], ["SUBACK", "PUBLISH"])
        self.assertEqual(raw[-1]["end"], "quiet")

    def test_a_suback_that_never_comes_is_recorded_as_such(self):
        raw = self.play(acknowledged_late(0, answer_subscribe=False),
                        [*SUBSCRIBED, dsl.collect("a", sort=True, suback=1, timeout=0.5)])
        self.assertEqual(raw[-1]["got"], [])
        self.assertEqual(raw[-1]["end"], "no-suback")
        # The comparable form keeps it: it is not a quiet step.
        self.assertEqual(normalize.normalize([raw[-1]], PLAIN)[0]["end"], "no-suback")

    def test_the_suback_of_an_earlier_subscribe_does_not_end_the_wait(self):
        # The first SUBACK comes during the wait for the second, and the
        # second a second later, past the window that would follow a wait
        # ended by the first.
        steps = [*SUBSCRIBED, dsl.quiet("a", window=0.1),
                 dsl.send("a", wire.subscribe(2, [("t/#", 1)])), dsl.collect("a", sort=True, suback=2, window=0.2)]
        raw = self.play(acknowledged_late({1: 0.5, 2: 1.5}), steps)
        identifiers = [packet["packet_id"] for packet in raw[-1]["got"] if packet["type"] == "SUBACK"]
        self.assertEqual(identifiers[-1], 2)
        self.assertEqual(raw[-1]["end"], "quiet")

    def test_a_peer_that_never_falls_silent_is_recorded_as_such(self):
        raw = self.play(trickling(200, 0.05), [*SUBSCRIBED, dsl.quiet("a", window=0.2, limit=0.6)])
        self.assertEqual(raw[-1]["end"], "still-receiving")
        self.assertLess(len(self.payloads(raw[-1])), 200)

    def test_a_drain_acknowledges_each_delivery_it_collects(self):
        raw = self.play(one_at_a_time("abcde"), [*SUBSCRIBED, dsl.drain("a", 5)])
        last = raw[-1]
        self.assertEqual((last["op"], last["expected"], last["acknowledged"], last["end"]), ("drain", 5, 5, "ok"))
        self.assertEqual([packet["type"] for packet in last["got"]], ["SUBACK", *["PUBLISH"] * 5])
        self.assertEqual(self.payloads(last), list("abcde"))
        # Each delivery was acknowledged before the next was read: the
        # identifier the peer reuses names a new delivery every time.
        normalized = normalize.normalize(raw, PLAIN)[-1]
        self.assertEqual([packet["packet_id"] for packet in normalized["got"] if packet["type"] == "PUBLISH"],
                         ["b1", "b2", "b3", "b4", "b5"])
        self.assertEqual((normalized["acknowledged"], normalized["end"]), (5, "ok"))

    def test_a_drain_that_is_owed_more_ends_idle(self):
        raw = self.play(one_at_a_time("ab"), [*SUBSCRIBED, dsl.drain("a", 3, idle=0.2)])
        self.assertEqual((raw[-1]["acknowledged"], raw[-1]["end"]), (2, "idle"))
        self.assertEqual(self.payloads(raw[-1]), ["a", "b"])
        raw = self.play(trickling(200, 0.05), [*SUBSCRIBED, dsl.drain("a", 300, idle=0.2, timeout=0.5)])
        self.assertEqual((raw[-1]["acknowledged"], raw[-1]["end"]), (0, "timeout"))

    def test_a_sorted_drain_compares_the_deliveries_and_not_their_order(self):
        def played(order, *, packet_id=1, sort=True):
            raw = self.play(one_at_a_time(order, packet_id=packet_id), [*SUBSCRIBED, dsl.drain("a", 3, sort=sort)])
            return normalize.normalize(raw, PLAIN)

        self.assertTrue(normalize.same(played("abc"), played("cab", packet_id=7)))
        self.assertFalse(normalize.same(played("abc"), played("cab", sort=False)))
        self.assertFalse(normalize.same(played("abc"), played("abb")))

    def test_a_measured_count_is_not_compared(self):
        def form(*payloads, **more):
            return normalize.normalize([measured(*payloads, **more)], PLAIN)

        self.assertEqual(form("m0", "m1", "m2"), [{"conn": "sub", "op": "measure", "kind": "PUBLISH",
                                                  "values": "distinct", "order": "as-sent", "got": [], "end": "quiet"}])
        # How many came is lawful either way: three, two or none compare equal.
        self.assertEqual(form("m0", "m1", "m2"), form("m0", "m2"))
        self.assertEqual(form("m0", "m1", "m2"), form())
        # What the protocol does guarantee is compared.
        self.assertEqual(form("m0", "m0")[0]["values"], "repeated")
        self.assertEqual(form("m0", "m9")[0]["values"], "foreign")
        self.assertEqual(form("m2", "m0")[0]["order"], "permuted")
        # Where no order is guaranteed (retained messages of several
        # topics), none is compared.
        self.assertEqual(form("m2", "m0", ordered=False), form("m0", ordered=False))
        self.assertNotIn("order", form("m2", "m0", ordered=False)[0])
        self.assertEqual(form("m2", "m2", ordered=False)[0]["values"], "repeated")

    def test_a_measure_step_records_what_it_counted(self):
        step = dsl.measure("a", [f"r{index}" for index in range(8)], ordered=False, window=0.3)
        raw = self.play(trickling(8, 0.1), [*SUBSCRIBED, step])
        last = raw[-1]
        self.assertEqual((last["op"], last["kind"], last["field"], last["ordered"], last["end"]),
                         ("measure", "PUBLISH", "payload", False, "quiet"))
        self.assertEqual(last["sequence"], [f"r{index}" for index in range(8)])
        self.assertEqual([packet["type"] for packet in last["got"]], ["SUBACK", *["PUBLISH"] * 8])
        self.assertEqual(normalize.normalize(raw, PLAIN)[-1],
                         {"conn": "a", "op": "measure", "kind": "PUBLISH", "values": "distinct", "end": "quiet",
                          "got": [{"type": "SUBACK", "packet_id": 1, "codes": [0]}]})

    def test_the_counts_of_both_brokers_are_reported_side_by_side(self):
        tallied = {"conn": "pub", "op": "tally", "kind": "PUBACK", "expected": 2, "end": "quiet",
                   "got": [wire.decode(wire.puback(1), 4), wire.decode(wire.pingresp(), 4)]}
        result = {"name": "x/y-v4", "raw": {"reference": [tallied, measured("m0", "m1", "m2")],
                                            "mqttiumd": [tallied, measured("m1")]}}
        rows = run.counted(result)
        self.assertEqual(rows, [
            {"scenario": "x/y-v4", "conn": "pub", "op": "tally", "kind": "PUBACK", "sent": 2, "reference": 1,
             "mqttiumd": 1},
            {"scenario": "x/y-v4", "conn": "sub", "op": "measure", "kind": "PUBLISH", "sent": 3, "reference": 3,
             "mqttiumd": 1}])
        self.assertIn("| `x/y-v4` | sub | PUBLISH | 3 | 3 | 1 |", run.count_lines(rows))
        # A scenario that could not be played on one broker reports what it has.
        self.assertEqual(run.counted({"name": "x/y-v4", "raw": {"reference": [tallied]}}),
                         [{"scenario": "x/y-v4", "conn": "pub", "op": "tally", "kind": "PUBACK", "sent": 2,
                           "reference": 1, "mqttiumd": None}])

    def test_the_raw_transcript_keeps_the_size_of_each_packet_on_the_wire(self):
        steps = [dsl.open_("a"), dsl.send("a", wire.connect("c")), dsl.recv("a"),
                 dsl.send("a", wire.subscribe(1, [("t", 0)])), dsl.recv("a", 2)]
        raw = self.play(retained_after, steps)
        self.assertEqual([packet["wire_bytes"] for packet in raw[-1]["got"]],
                         [len(wire.suback(1, [0])), len(wire.publish("t", "kept", retain=True))])
        self.assertEqual(raw[1]["got"][0]["wire_bytes"], len(wire.connack()))
        normalized = normalize.normalize(raw, PLAIN)
        self.assertTrue(all("wire_bytes" not in packet for record in normalized for packet in record.get("got", ())))


class Files(unittest.TestCase):
    def test_raw_and_normalized_transcripts_of_every_scenario_are_written(self):
        connack = wire.decode(wire.connack(level=5, props={"receive_maximum": 8}), 5)
        raw = {"reference": [{"conn": "a", "op": "recv", "got": [connack], "end": "ok", "mark": "connack"}],
               "mqttiumd": [{"conn": "a", "op": "recv", "got": [{**connack, "properties": {"receive_maximum": 20}}],
                             "end": "ok", "mark": "connack"}]}
        normalized = [{"conn": "a", "op": "recv", "got": [{key: value for key, value in connack.items()
                                                           if key != "properties"}], "end": "ok"}]
        result = {"name": "x/same-v5", "status": classify.SAME, "fingerprint": None, "incidents": {},
                  "reference": normalized, "mqttiumd": normalized, "raw": raw}
        with tempfile.TemporaryDirectory() as directory:
            run.write_transcripts(Path(directory), result)
            written = sorted(path.name for path in (Path(directory) / "x").iterdir())
            self.assertEqual(written, ["same-v5.json", "same-v5.mqttiumd.raw.json", "same-v5.reference.raw.json"])
            for backend in harness.BACKENDS:
                kept = json.loads((Path(directory) / f"x/same-v5.{backend}.raw.json").read_text())
                self.assertEqual(kept, {"scenario": "x/same-v5", "backend": backend, "transcript": raw[backend]})
            both = json.loads((Path(directory) / "x/same-v5.json").read_text())
            self.assertEqual(both, {"scenario": "x/same-v5", "status": "same", "reference": normalized,
                                    "mqttiumd": normalized})


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
        self.assertEqual(classify.BACKENDS, harness.BACKENDS)
        divergences = classify.load_divergences(HERE / "divergences.json")
        self.assertEqual(sorted(divergences), list(divergences), "entries are sorted by id")
        self.assertEqual(set(divergences) - set(dsl.REGISTRY), set())
        versions = {name: scenario.version for name, scenario in dsl.REGISTRY.items()}
        known = classify.known_statements(HERE.parents[2] / "docs/spec")
        self.assertEqual(classify.check_statements(divergences, versions, known), [])


if __name__ == "__main__":
    unittest.main()
