"""Use real mTLS peers while the paced QoS 0 driver is sending."""

from contextlib import ExitStack
from pathlib import Path
import ssl
import sys
import time

from run_broker_load import EXPIRING_TOPIC, retained_present

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests/system"))
from tcp_probe import Observations, Wire, database, packet, publish_packet, pubrel_packet, require, utf8


def connect(stack, address, pki, observations, name, *, resumed=False):
    role = name.split("-")[0]
    context = ssl.create_default_context(cafile=pki / "ca.crt")
    context.minimum_version = context.maximum_version = ssl.TLSVersion.TLSv1_3
    context.load_cert_chain(pki / f"{role}.crt", pki / f"{role}.key")
    wire = stack.enter_context(Wire(address, observations, name))
    wire.socket = context.wrap_socket(wire.socket, server_hostname=address[0])
    require(wire.socket.version() == "TLSv1.3", "unexpected_tls_version")
    wire.connect(name, session_present=resumed)
    return wire


def subscribe(wire, topic):
    wire.send(packet(0x82, b"\x00\x01\x00" + utf8(topic) + b"\x01"))
    require(wire.expect(0x90).body == b"\x00\x01\x00\x01", "witness_subscribe_not_granted")


def delivery(wire, topic, payload):
    frame = wire.expect(0x30)
    prefix = utf8(topic)
    require(frame.header == 0x32 and frame.body.startswith(prefix)
            and frame.body[len(prefix) + 2:] == b"\x00" + payload, "unexpected_witness_publication")
    identifier = frame.body[len(prefix):len(prefix) + 2]
    require(identifier != b"\x00\x00", "zero_witness_packet_identifier")
    return identifier


def run(directory, proof, pki, port, window):
    address = ("127.0.0.1", port)
    observations = Observations(proof / "witness-wire.jsonl")
    while time.monotonic_ns() < window["start_ns"]:
        time.sleep(0.005)
    timings = {}

    def mark(name):
        value = time.monotonic_ns()
        require(window["start_ns"] <= value <= window["end_ns"], "witness_left_qos0_window", name)
        timings[name] = value

    with ExitStack() as stack:
        receiver = connect(stack, address, pki, observations, "dashboard-4")
        subscriber_a = connect(stack, address, pki, observations, "dashboard-5")
        subscriber_b = connect(stack, address, pki, observations, "dashboard-6")
        source = connect(stack, address, pki, observations, "sensor-7")
        retry = connect(stack, address, pki, observations, "sensor-4")
        closing_topic = "atelier/load/witness/closing"
        independent_topic = "atelier/load/witness/independent"
        subscribe(receiver, independent_topic)
        for peer in (subscriber_a, subscriber_b):
            subscribe(peer, closing_topic)
        source.exchange(70, b"before-close", topic=closing_topic, qos=1)
        for peer in (subscriber_a, subscriber_b):
            delivery(peer, closing_topic, b"before-close")

        retry_topic = "atelier/load/witness/qos2"
        retry.send(publish_packet(retry_topic, b"resume", 55))
        retry.expect(0x50, 55)
        admitted = database(directory / "broker.sqlite")["admissions"]
        require(admitted, "missing_qos2_admission")
        mark("first_pubrec")
        mark("cut_started")
        for peer in (subscriber_a, subscriber_b, retry):
            peer.close()
        mark("three_transports_closed")

        resumed = connect(stack, address, pki, observations, "sensor-4", resumed=True)
        resumed.send(publish_packet(retry_topic, b"resume", 55, dup=True))
        resumed.expect(0x50, 55)
        require(database(directory / "broker.sqlite")["admissions"] == admitted, "resumed_qos2_readmitted")
        resumed.send(pubrel_packet(55))
        resumed.expect(0x70, 55)
        mark("qos2_pubcomp")

        mark("independent_publish")
        source.exchange(71, b"still-progressing", topic=independent_topic, qos=1)
        identifier = delivery(receiver, independent_topic, b"still-progressing")
        receiver.send(packet(0x40, identifier))
        mark("independent_delivery")

        expiry = b"\x05\x02\x00\x00\x00\x02"
        source.send(packet(0x33, utf8(EXPIRING_TOPIC) + b"\x00\x48" + expiry + b"expires"))
        source.expect(0x40, 72)
        require(retained_present(directory), "witness_retained_never_committed")
        mark("deadline_present")
        limit = time.monotonic() + 5
        while retained_present(directory) and time.monotonic() < limit:
            time.sleep(0.02)
        require(not retained_present(directory), "witness_retained_not_expired")
        mark("deadline_removed")
    return {"status": "pass", "timings_ns": timings, "closed_transports": 3,
            "independent_delay_ms": (timings["independent_delivery"] - timings["independent_publish"]) / 1e6,
            "qos2_packet_identifier": 55, "qos2_readmitted": False}
