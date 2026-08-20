"""Property-based fuzzing with Hypothesis generation and shrinking.

Complements the seed-reproducible ``tests/fuzz/fuzz.py`` with property-based
generation and automatic shrinking of failing inputs.

Run (default profile):
  python -m pytest tests/fuzz/test_hypothesis_fuzz.py -q

Aggressive profile (more examples):
  HYPOTHESIS_PROFILE=aggressive python -m pytest tests/fuzz/test_hypothesis_fuzz.py
"""

from __future__ import annotations

import os

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given, settings, strategies as st
from hypothesis import assume

# Register an aggressive profile for long/CI runs.
settings.register_profile(
    "aggressive",
    max_examples=3000,
    deadline=None,
)
settings.register_profile(
    "ci",
    max_examples=800,
    deadline=None,
)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "ci"))

from mqttium.codec.buffer import DEFAULT_MAX_PACKET_SIZE, IncrementalDecoder
from mqttium.codec.properties import PUBLISH, decode_properties
from mqttium.enums import MQTTProtocolVersion, OutboundQoSState, PacketType, QoS
from mqttium.errors import MQTTError
from mqttium.packets import (
    PublishPacket,
    encode_frame,
)
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.types import Properties

ALLOWED = (MQTTError, ConnectionError)

V5 = MQTTProtocolVersion.MQTTv5

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Valid-ish UTF-8 topics (plus occasional hostile chars).
_topic_text = st.text(
    alphabet=st.characters(
        blacklist_categories=("Cs",),  # no surrogates
        blacklist_characters=("\x00",),
    ),
    max_size=64,
)

_topics = st.one_of(
    _topic_text,
    st.sampled_from(["a/b", "", "x", "+", "#", "a/+", "$share/g/t", "t/0", "t/1"]),
)

_payloads = st.binary(max_size=256)

_qos = st.sampled_from([0, 1, 2])

_mid = st.integers(min_value=0, max_value=65535)  # include 0 (invalid)

_prop_kv = st.tuples(
    st.sampled_from(
        [
            "payload_format_indicator",
            "message_expiry_interval",
            "content_type",
            "response_topic",
            "correlation_data",
            "topic_alias",
            "subscription_identifier",
            "receive_maximum",
            "maximum_qos",
        ]
    ),
    st.one_of(
        st.integers(min_value=0, max_value=2**32 - 1),
        st.binary(max_size=32),
        _topic_text,
    ),
)


def _build_publish(topic: str, payload: bytes, qos: int, mid: int | None, props) -> bytes:
    try:
        pkt = PublishPacket(
            topic=topic,
            payload=payload,
            qos=QoS(qos),
            retain=False,
            dup=False,
            mid=mid if qos else None,
            properties=props,
        )
        return pkt.encode(V5)
    except MQTTError:
        return b""


# ---------------------------------------------------------------------------
# 1. Codec: mutated PUBLISH frames never crash the decoder + typed decode
# ---------------------------------------------------------------------------


@given(
    topic=_topics,
    payload=_payloads,
    qos=_qos,
    mid=_mid,
    mutations=st.integers(min_value=0, max_value=6),
    seed=st.integers(min_value=0, max_value=2**31 - 1),
)
@settings(deadline=None)
def test_publish_frame_roundtrip_or_clean_error(topic, payload, qos, mid, mutations, seed):
    import random

    props = None
    if seed % 3 == 0:
        props = Properties()
        props.set("topic_alias", (seed % 3))
    wire = _build_publish(topic, payload, qos, mid, props)
    assume(wire)
    rng = random.Random(seed)
    buf = bytearray(wire)
    for _ in range(mutations):
        if not buf:
            break
        op = rng.randrange(3)
        if op == 0:
            buf[rng.randrange(len(buf))] = rng.randrange(256)
        elif op == 1 and len(buf) > 1:
            del buf[rng.randrange(len(buf))]
        else:
            buf.insert(rng.randrange(len(buf) + 1), rng.randrange(256))
    dec = IncrementalDecoder(max_packet_size=DEFAULT_MAX_PACKET_SIZE)
    try:
        dec.feed(bytes(buf))
        while True:
            pkt = dec.next_packet()
            if pkt is None:
                break
            PublishPacket.decode(pkt.flags, pkt.remaining, V5)
    except ALLOWED:
        pass


# ---------------------------------------------------------------------------
# 2. Properties blob decode never crashes
# ---------------------------------------------------------------------------


@given(blob=st.binary(max_size=128))
@settings(deadline=None)
def test_decode_properties_never_crashes(blob):
    try:
        decode_properties(blob, 0, PUBLISH)
    except ALLOWED:
        pass


# ---------------------------------------------------------------------------
# 3. Engine: arbitrary packet sequences preserve invariants
# ---------------------------------------------------------------------------

_frame_type = st.sampled_from(
    [
        PacketType.CONNACK,
        PacketType.PUBLISH,
        PacketType.PUBACK,
        PacketType.PUBREC,
        PacketType.PUBREL,
        PacketType.PUBCOMP,
        PacketType.SUBACK,
        PacketType.UNSUBACK,
        PacketType.PINGRESP,
        PacketType.DISCONNECT,
        PacketType.AUTH,
    ]
)


def _engine_invariants(engine: ProtocolEngine) -> None:
    assert engine.flow.inflight >= 0
    assert engine.flow.inflight <= engine.flow.limit
    assert engine._inbound_inflight >= 0

    outbound = list(engine.store.out_items())
    expected_mids = {msg.mid for msg in outbound} | set(engine._pending_sub_mids)
    assert set(engine.packet_ids._used) == expected_mids

    queued_mids = {msg.mid for msg in engine._queued}
    expected_flow = sum(
        1
        for msg in outbound
        if msg.state is not OutboundQoSState.QUEUED and msg.mid not in queued_mids
    )
    assert engine.flow.inflight == expected_flow


@given(
    ops=st.lists(
        st.tuples(_frame_type, st.integers(min_value=0, max_value=255), st.binary(max_size=24)),
        max_size=60,
    ),
    proto=st.sampled_from([MQTTProtocolVersion.MQTTv311, V5]),
)
@settings(deadline=None)
def test_engine_sequence_invariants(ops, proto):
    engine = ProtocolEngine(
        EngineConfig(
            client_id="hyp", protocol=proto, local_receive_maximum=8, topic_alias_maximum=2
        )
    )
    dec = IncrementalDecoder(max_packet_size=DEFAULT_MAX_PACKET_SIZE)

    def feed(wire: bytes) -> None:
        dec.feed(wire)
        while True:
            pkt = dec.next_packet()
            if pkt is None:
                break
            engine.handle_raw(pkt)
        engine.take_effects()

    try:
        engine.begin_connect()
        engine.take_effects()
    except ALLOWED:
        pass

    for ptype, mid, extra in ops:
        try:
            if ptype is PacketType.CONNACK:
                body = bytes([mid & 1, 0]) + (b"\x00" if proto == V5 else b"")
                feed(encode_frame(ptype, 0, body))
            elif ptype is PacketType.PUBLISH:
                wire = _build_publish("a/b", extra, mid % 3, mid if mid % 3 else None, None)
                if wire:
                    feed(wire)
            elif ptype in (
                PacketType.PUBACK,
                PacketType.PUBREC,
                PacketType.PUBREL,
                PacketType.PUBCOMP,
            ):
                flags = 0x2 if ptype is PacketType.PUBREL else 0
                feed(encode_frame(ptype, flags, (mid % 65536).to_bytes(2, "big") + extra[:2]))
            elif ptype in (PacketType.SUBACK, PacketType.UNSUBACK):
                feed(encode_frame(ptype, 0, (mid % 65536).to_bytes(2, "big") + b"\x00"))
            else:
                feed(encode_frame(ptype, 0, extra[:1]))
        except ALLOWED:
            pass
        _engine_invariants(engine)


# ---------------------------------------------------------------------------
# 4. WebSocket frame parser: bounded memory
# ---------------------------------------------------------------------------


@given(header=st.binary(min_size=2, max_size=14), tail=st.binary(max_size=64))
@settings(deadline=None)
def test_ws_frame_parser_bounded(header, tail):
    from mqttium.transport.websocket import _parse_frame

    buf = bytearray(header + tail)
    before = len(buf)
    try:
        _parse_frame(buf, 1024)
    except ALLOWED:
        pass
    assert len(buf) <= before
