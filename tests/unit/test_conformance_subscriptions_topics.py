"""Executable conformance checks for subscriptions, topics, ordering and closure.

Same convention as ``test_conformance_qos_flows.py``: one test per numbered
statement and specification version, with the statement quoted from the
vendored index as its docstring. Tests drive the protocol engine as a broker
would see it; the statements that require the Network Connection to be closed
go through the client runtime and a scripted broker.
"""

from __future__ import annotations

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec import properties as property_codec
from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.codec.vbi import encode_vbi
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MalformedPacketError, MQTTError, NotConnectedError, ProtocolError
from mqttium.packets import (
    PubAckPacket,
    PublishPacket,
    PubRecPacket,
    encode_frame,
)
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Message, Properties
from tests.support import (
    ScriptedBrokerTransport,
    mark_delivered_messages,
    transport_factory,
    wait_until,
)

V311 = MQTTProtocolVersion.MQTTv311
V5 = MQTTProtocolVersion.MQTTv5
_DUP = 0x08


def _engine(
    protocol: MQTTProtocolVersion, store: MemoryInflightStore | None = None, **options: object
) -> ProtocolEngine:
    options.setdefault("client_id", "conformance")
    return ProtocolEngine(
        EngineConfig(protocol=protocol, **options),  # type: ignore[arg-type]
        store if store is not None else MemoryInflightStore(),
    )


def _feed(engine: ProtocolEngine, wire: bytes) -> list[EngineEffect]:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)
    return mark_delivered_messages(engine, engine.take_effects())


def _connack(
    protocol: MQTTProtocolVersion, *, session_present: bool = False, **properties: object
) -> bytes:
    body = bytes((int(session_present), 0))
    if protocol is V5:
        body += property_codec.encode_properties(Properties(properties), property_codec.CONNACK)
    else:
        assert not properties
    return encode_frame(PacketType.CONNACK, 0, body)


def _connected(
    protocol: MQTTProtocolVersion,
    store: MemoryInflightStore | None = None,
    *,
    connack: bytes | None = None,
    **options: object,
) -> ProtocolEngine:
    engine = _engine(protocol, store, **options)
    engine.begin_connect()
    _feed(engine, connack if connack is not None else _connack(protocol))
    return engine


def _reconnect(engine: ProtocolEngine, connack: bytes) -> list[EngineEffect]:
    engine.notify_transport_closed()
    engine.take_effects()
    engine.begin_connect()
    return _feed(engine, connack)


def _wire(effects: list[EngineEffect]) -> list[RawPacket]:
    """The packets the engine put on the wire, in order."""
    packets: list[RawPacket] = []
    for effect in effects:
        if effect.kind not in (EffectKind.SEND, EffectKind.SEND_ACK):
            continue
        data = effect.data
        decoder = IncrementalDecoder()
        decoder.feed(b"".join(data) if isinstance(data, tuple) else data)
        packets.extend(decoder.drain_packets())
    return packets


def _types(effects: list[EngineEffect]) -> list[PacketType]:
    return [packet.packet_type for packet in _wire(effects)]


def _inbound(
    protocol: MQTTProtocolVersion,
    qos: int,
    mid: int | None,
    *,
    topic: str = "in/topic",
    properties: Properties | None = None,
) -> bytes:
    return PublishPacket(
        topic=topic,
        payload=b"payload",
        qos=QoS(qos),
        retain=False,
        dup=False,
        mid=mid,
        properties=properties,
    ).encode(protocol)


def _messages(effects: list[EngineEffect]) -> list[Message]:
    return [effect.data for effect in effects if effect.kind is EffectKind.MESSAGE]


def _u16(value: int) -> bytes:
    return value.to_bytes(2, "big")


def _mid(packet: RawPacket) -> int:
    return int.from_bytes(packet.remaining[:2], "big")


def _suback(protocol: MQTTProtocolVersion, mid: int, *codes: int, unsub: bool = False) -> bytes:
    body = _u16(mid) + (b"\x00" if protocol is V5 else b"")
    if not unsub or protocol is V5:
        body += bytes(codes)
    return encode_frame(PacketType.UNSUBACK if unsub else PacketType.SUBACK, 0, body)


def _refused(effects: list[EngineEffect]) -> bool:
    return bool(effects) and effects[-1].kind is EffectKind.PROTOCOL_ERROR


def _feed_all(engine: ProtocolEngine, wire: bytes) -> list[EngineEffect]:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    for raw in decoder.drain_packets():
        engine.handle_raw(raw)
    return mark_delivered_messages(engine, engine.take_effects())


_BAD_STRINGS = ("nul\x00", "surrogate\ud800")


# --- Topic names and filters --------------------------------------------------


def _wildcards_only_in_filters(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)
    for topic in ("sport/+/player", "sport/#", "#", "+"):
        with pytest.raises(ProtocolError):
            engine.queue_publish(topic, b"payload")
        with pytest.raises(ProtocolError):
            engine.queue_publish(topic, b"payload", qos=1)
        engine.queue_subscribe(topic)
    assert _types(engine.take_effects()) == [PacketType.SUBSCRIBE] * 4
    assert engine.unacknowledged_messages == 0


def _multi_level_wildcard_placement(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)
    for accepted in ("#", "sport/#", "sport/tennis/#", "+/tennis/#"):
        engine.queue_subscribe(accepted)
    assert _types(engine.take_effects()) == [PacketType.SUBSCRIBE] * 4
    for refused in ("sport/tennis#", "sport/#/ranking", "#/sport", "sport/#x", "##"):
        with pytest.raises(ProtocolError):
            engine.queue_subscribe(refused)
        with pytest.raises(ProtocolError):
            engine.queue_unsubscribe(refused)
    assert _wire(engine.take_effects()) == []


def _single_level_wildcard_placement(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)
    for accepted in ("+", "+/tennis/#", "sport/+/player1", "sport/+", "+/+", "/+"):
        engine.queue_subscribe(accepted)
    assert _types(engine.take_effects()) == [PacketType.SUBSCRIBE] * 6
    for refused in ("sport+", "sport/+x", "sp+rt/tennis", "+sport", "sport/++"):
        with pytest.raises(ProtocolError):
            engine.queue_subscribe(refused)
        with pytest.raises(ProtocolError):
            engine.queue_unsubscribe(refused)
    assert _wire(engine.take_effects()) == []


def _topics_and_filters_are_not_empty(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)
    with pytest.raises(ProtocolError):
        engine.queue_publish("", b"payload")
    with pytest.raises(ProtocolError):
        engine.queue_subscribe("")
    with pytest.raises(ProtocolError):
        engine.queue_subscribe(["sport", ""])
    with pytest.raises(ProtocolError):
        engine.queue_unsubscribe("")
    assert _wire(engine.take_effects()) == []
    # One character is enough, including the separator alone.
    engine.queue_publish("/", b"payload")
    engine.queue_subscribe("/")
    assert _types(engine.take_effects()) == [PacketType.PUBLISH, PacketType.SUBSCRIBE]


def _topics_and_filters_fit_in_65535_bytes() -> None:
    engine = _connected(V311)
    longest = "x" * 65535
    engine.queue_publish(longest, b"")
    engine.queue_subscribe(longest)
    assert _types(engine.take_effects()) == [PacketType.PUBLISH, PacketType.SUBSCRIBE]
    # 65535 characters that encode to more bytes are too long as well.
    for too_long in ("x" * 65536, "\u00e9" * 40000):
        with pytest.raises(MQTTError):
            engine.queue_publish(too_long, b"")
        with pytest.raises(MQTTError):
            engine.queue_subscribe(too_long)
        with pytest.raises(MQTTError):
            engine.queue_unsubscribe(too_long)
    assert _wire(engine.take_effects()) == []


def _shared_subscription_filter_shape() -> None:
    engine = _connected(V5)
    for accepted in ("$share/group/sport/#", "$share/g/+", "$share/g//"):
        engine.queue_subscribe(accepted)
    assert _types(engine.take_effects()) == [PacketType.SUBSCRIBE] * 3
    for refused in (
        "$share//sport",
        "$share/group",
        "$share/group/",
        "$share/gr+up/a",
        "$share/g#/a",
    ):
        with pytest.raises(ProtocolError):
            engine.queue_subscribe(refused)
    assert _wire(engine.take_effects()) == []


def _filters_are_utf8(protocol: MQTTProtocolVersion, packet: PacketType) -> None:
    engine = _connected(protocol)
    queue = engine.queue_subscribe if packet is PacketType.SUBSCRIBE else engine.queue_unsubscribe
    filters = ["sp\u00f6rt/+", "\U0001f600/#"]
    queue(filters)
    (sent,) = _wire(engine.take_effects())
    assert sent.packet_type is packet
    body = bytes(sent.remaining)
    at = 0
    for topic_filter in filters:
        encoded = topic_filter.encode("utf-8")
        at = body.index(_u16(len(encoded)) + encoded, at) + 2 + len(encoded)
    if packet is PacketType.UNSUBSCRIBE:
        # Packed contiguously: nothing follows the last filter.
        assert at == len(body)
    for bad in _BAD_STRINGS:
        with pytest.raises(MQTTError):
            queue(bad)
    assert _wire(engine.take_effects()) == []


def _subscribe_filters_are_utf8(protocol: MQTTProtocolVersion) -> None:
    _filters_are_utf8(protocol, PacketType.SUBSCRIBE)


def _unsubscribe_filters_are_utf8(protocol: MQTTProtocolVersion) -> None:
    _filters_are_utf8(protocol, PacketType.UNSUBSCRIBE)


# --- SUBACK and UNSUBACK ------------------------------------------------------


def _suback_carries_the_subscribe_identifier(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)
    mid = engine.queue_subscribe(["a", "b"])
    (subscribe,) = _wire(engine.take_effects())
    assert _mid(subscribe) == mid
    # An acknowledgement for an identifier that was never used is refused.
    assert _refused(_feed(engine, _suback(protocol, mid + 1, 0, 0)))
    engine = _connected(protocol)
    mid = engine.queue_subscribe(["a", "b"])
    engine.take_effects()
    (acknowledged,) = _feed(engine, _suback(protocol, mid, 0, 0))
    assert acknowledged.kind is EffectKind.SUBACK and acknowledged.data.mid == mid


def _unsuback_carries_the_unsubscribe_identifier(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)
    mid = engine.queue_unsubscribe(["a", "b"])
    (unsubscribe,) = _wire(engine.take_effects())
    assert _mid(unsubscribe) == mid
    assert _refused(_feed(engine, _suback(protocol, mid + 1, 0, 0, unsub=True)))
    engine = _connected(protocol)
    mid = engine.queue_unsubscribe(["a", "b"])
    engine.take_effects()
    (acknowledged,) = _feed(engine, _suback(protocol, mid, 0, 0, unsub=True))
    assert acknowledged.kind is EffectKind.UNSUBACK and acknowledged.data.mid == mid


def _both_acknowledgements_carry_the_request_identifier(protocol: MQTTProtocolVersion) -> None:
    _suback_carries_the_subscribe_identifier(protocol)
    _unsuback_carries_the_unsubscribe_identifier(protocol)


def _suback_codes_follow_filter_order(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)
    mid = engine.queue_subscribe([("a", 0), ("b", 1), ("c", 2), ("d", 1)])
    engine.take_effects()
    (acknowledged,) = _feed(engine, _suback(protocol, mid, 0, 1, 0x80, 1))
    # Surfaced position by position: the third filter is the one that failed.
    assert acknowledged.data.reason_codes == (0, 1, 0x80, 1)
    # One code per filter: a different count cannot be matched to the filters.
    for codes in ((0,), (0, 1, 2)):
        engine = _connected(protocol)
        mid = engine.queue_subscribe(["a", "b"])
        engine.take_effects()
        assert _refused(_feed(engine, _suback(protocol, mid, *codes)))


def _unsuback_codes_follow_filter_order() -> None:
    engine = _connected(V5)
    mid = engine.queue_unsubscribe(["a", "b", "c"])
    engine.take_effects()
    (acknowledged,) = _feed(engine, _suback(V5, mid, 0, 0x11, 0x80, unsub=True))
    assert acknowledged.data.reason_codes == (0, 0x11, 0x80)
    for codes in ((0,), (0, 0, 0, 0)):
        engine = _connected(V5)
        mid = engine.queue_unsubscribe(["a", "b", "c"])
        engine.take_effects()
        assert _refused(_feed(engine, _suback(V5, mid, *codes, unsub=True)))


def _reserved_suback_codes_are_refused() -> None:
    for code in (0x00, 0x01, 0x02, 0x80):
        engine = _connected(V311)
        mid = engine.queue_subscribe("a")
        engine.take_effects()
        (acknowledged,) = _feed(engine, _suback(V311, mid, code))
        assert acknowledged.data.reason_codes == (code,)
    for code in (0x03, 0x7F, 0x81, 0xFF):
        engine = _connected(V311)
        mid = engine.queue_subscribe("a")
        engine.take_effects()
        assert _refused(_feed(engine, _suback(V311, mid, code)))


def _suback_code_grants_a_qos_or_reports_failure() -> None:
    for code in (0x00, 0x01, 0x02, 0x80, 0x87, 0x8F, 0x9E):
        engine = _connected(V5)
        mid = engine.queue_subscribe([("a", 2)])
        engine.take_effects()
        (acknowledged,) = _feed(engine, _suback(V5, mid, code))
        assert acknowledged.data.reason_codes == (code,)
    for code in (0x03, 0x10, 0x7F):
        engine = _connected(V5)
        mid = engine.queue_subscribe([("a", 2)])
        engine.take_effects()
        assert _refused(_feed(engine, _suback(V5, mid, code)))


# --- Ordering -----------------------------------------------------------------


def _acknowledgements_follow_arrival_order(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)
    arrivals = [(1, 21), (2, 22), (1, 20), (2, 19), (1, 23)]
    sent = _wire(_feed_all(engine, b"".join(_inbound(protocol, q, m) for q, m in arrivals)))
    assert [(packet.packet_type, _mid(packet)) for packet in sent] == [
        (PacketType.PUBACK if qos == 1 else PacketType.PUBREC, mid) for qos, mid in arrivals
    ]


def _pubrel_follows_pubrec_order(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)
    mids = []
    for _ in range(4):
        handle = engine.queue_publish("out/topic", b"payload", qos=2)
        assert handle.mid is not None
        mids.append(handle.mid)
    engine.take_effects()
    received = [mids[2], mids[0], mids[3], mids[1]]
    wire = b"".join(PubRecPacket(mid=mid).encode(protocol) for mid in received)
    sent = _wire(_feed_all(engine, wire))
    assert [(packet.packet_type, _mid(packet)) for packet in sent] == [
        (PacketType.PUBREL, mid) for mid in received
    ]


def _resend_keeps_the_original_order(protocol: MQTTProtocolVersion) -> None:
    options: dict[str, object] = {"clean_start": False}
    if protocol is V5:
        options["connect_properties"] = Properties({"session_expiry_interval": 3600})
    engine = _connected(protocol, **options)
    original = []
    for qos in (1, 2, 2, 1, 1, 2):
        handle = engine.queue_publish(f"out/{len(original)}", b"payload", qos=qos)
        original.append((handle.mid, f"out/{len(original)}", qos))
    engine.take_effects()
    # Acknowledging one in the middle must not disturb the order of the rest.
    _feed(engine, PubAckPacket(mid=original[3][0]).encode(protocol))
    del original[3]
    replayed = _wire(_reconnect(engine, _connack(protocol, session_present=True)))
    publishes = [PublishPacket.decode(p.flags, p.remaining, protocol) for p in replayed]
    assert [(p.mid, p.topic, int(p.qos)) for p in publishes] == original
    assert all(packet.flags & _DUP for packet in replayed)


def _every_publish_is_acknowledged(protocol: MQTTProtocolVersion) -> None:
    # No subscription was made and nothing consumes these messages: the
    # acknowledgement does not depend on what the application does with them.
    engine = _connected(protocol)
    for qos, expected in ((1, PacketType.PUBACK), (2, PacketType.PUBREC)):
        for mid in (30 + qos, 40 + qos):
            decoder = IncrementalDecoder()
            decoder.feed(_inbound(protocol, qos, mid, topic="never/subscribed"))
            raw = decoder.next_packet()
            assert raw is not None
            engine.handle_raw(raw)
            (sent,) = _wire(engine.take_effects())
            assert sent.packet_type is expected and _mid(sent) == mid


# --- Fixed header flags and closure ------------------------------------------


def _reserved_flags_are_sent_as_specified() -> None:
    engine = _connected(V311)
    engine.queue_subscribe("a")
    engine.queue_unsubscribe("a")
    engine.queue_ping()
    handle = engine.queue_publish("out/topic", b"payload", qos=2)
    assert handle.mid is not None
    sent = _wire(engine.take_effects())
    sent += _wire(_feed(engine, PubRecPacket(mid=handle.mid).encode(V311)))
    sent += _wire(_feed(engine, _inbound(V311, 1, 5)))
    sent += _wire(_feed(engine, _inbound(V311, 2, 6)))
    sent += _wire(_feed(engine, encode_frame(PacketType.PUBREL, 2, _u16(6))))
    reserved = {
        PacketType.SUBSCRIBE: 0b0010,
        PacketType.UNSUBSCRIBE: 0b0010,
        PacketType.PUBREL: 0b0010,
        PacketType.PINGREQ: 0,
        PacketType.PUBACK: 0,
        PacketType.PUBREC: 0,
        PacketType.PUBCOMP: 0,
    }
    seen = {packet.packet_type: packet.flags for packet in sent if packet.packet_type in reserved}
    assert seen == reserved
    assert _engine(V311).begin_connect()[0] == 0x10
    disconnect = _connected(V311).begin_disconnect()
    assert disconnect == bytes((0xE0, 0x00))


class _ViolatingBroker(ScriptedBrokerTransport):
    """A broker that answers the first PINGREQ-free moment with a bad packet."""

    def __init__(self, protocol: MQTTProtocolVersion, violation: bytes) -> None:
        super().__init__(protocol=protocol)
        self.violation = violation

    def handle_packet(self, raw: RawPacket) -> None:
        super().handle_packet(raw)
        if raw.packet_type is PacketType.CONNECT:
            self.push_rx(self.violation)


async def _closes_on(protocol: MQTTProtocolVersion, violation: bytes) -> ScriptedBrokerTransport:
    broker = _ViolatingBroker(protocol, violation)
    client = AsyncClient(client_id="strict", protocol=protocol, keepalive=0)
    client._transport_factory = transport_factory(broker)
    try:
        await client.connect("broker", 1883, timeout=2.0)
    except MQTTError:
        pass
    await wait_until(lambda: broker.is_closing() and not client.is_connected, timeout=2.0)
    return broker


async def _invalid_flags_close_the_connection() -> None:
    # PUBACK, SUBACK and PINGRESP with a reserved flag bit set.
    for violation in (
        encode_frame(PacketType.PUBACK, 0b0001, _u16(1)),
        encode_frame(PacketType.SUBACK, 0b0010, _u16(1) + b"\x00"),
        bytes((0xD1, 0x00)),
    ):
        await _closes_on(V311, violation)


async def _protocol_violation_closes_the_connection() -> None:
    for violation in (
        _connack(V311),  # a second CONNACK
        encode_frame(PacketType.PUBREL, 0b0010, _u16(0)),  # Packet Identifier 0
        _inbound(V311, 1, 9, topic="sport/#"),  # wildcard in a Topic Name
        encode_frame(PacketType.SUBACK, 0, _u16(9) + b"\x00"),  # nothing was subscribed
    ):
        await _closes_on(V311, violation)


async def _disconnect_reserved_bits_are_validated() -> None:
    broker = await _closes_on(V5, encode_frame(PacketType.DISCONNECT, 0b0001, b""))
    decoder = IncrementalDecoder()
    decoder.feed(b"".join(broker.written))
    last = decoder.drain_packets()[-1]
    assert last.packet_type is PacketType.DISCONNECT
    assert last.remaining[0] == 0x81  # Malformed Packet


def _nothing_is_sent_after_disconnect(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)
    handle = engine.queue_publish("out/topic", b"payload", qos=1)
    engine.take_effects()
    assert engine.begin_disconnect()[0] == 0xE0
    with pytest.raises(NotConnectedError):
        engine.queue_publish("out/topic", b"payload")
    with pytest.raises(NotConnectedError):
        engine.queue_publish("out/topic", b"payload", qos=1)
    with pytest.raises(NotConnectedError):
        engine.queue_subscribe("a")
    with pytest.raises(NotConnectedError):
        engine.queue_unsubscribe("a")
    with pytest.raises(NotConnectedError):
        engine.queue_ping()
    # Packets still arriving are not answered either.
    assert handle.mid is not None
    engine.handle_raw(_one(_inbound(protocol, 1, 9)))
    engine.handle_raw(_one(_inbound(protocol, 2, 10)))
    assert _wire(engine.take_effects()) == []


def _one(wire: bytes) -> RawPacket:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    return raw


async def _client_closes_after_disconnect(protocol: MQTTProtocolVersion) -> None:
    broker = ScriptedBrokerTransport(protocol=protocol)
    client = AsyncClient(client_id="closing", protocol=protocol, keepalive=0)
    client._transport_factory = transport_factory(broker)
    await client.connect("broker", 1883, timeout=2.0)
    await client.publish("out/topic", b"payload", qos=1, timeout=2.0)
    await client.disconnect()
    assert broker.is_closing()
    decoder = IncrementalDecoder()
    decoder.feed(b"".join(broker.written))
    written = [packet.packet_type for packet in decoder.drain_packets()]
    # DISCONNECT is the last packet on the connection, and there is only one.
    assert written[-1] is PacketType.DISCONNECT
    assert written.count(PacketType.DISCONNECT) == 1


def _disconnect_reason_codes_are_defined() -> None:
    for code in (0x00, 0x04, 0x80, 0x81, 0x82, 0x93, 0x94, 0x95, 0x98):
        wire = _connected(V5).begin_disconnect(reason_code=code)
        assert wire[0] == 0xE0
        assert code == 0x00 or wire[2] == code
    # Undefined values, and values only a Server may send.
    for code in (0x01, 0x03, 0x05, 0x7F, 0x86, 0x9F, 0xFF):
        engine = _connected(V5)
        with pytest.raises(ProtocolError):
            engine.begin_disconnect(reason_code=code)
        # A refused DISCONNECT leaves the connection usable.
        engine.queue_ping()
        assert _types(engine.take_effects()) == [PacketType.PINGREQ]


# --- Data representation ------------------------------------------------------


def _byte_order_mark_is_kept(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)
    topic = "\ufeffsport/\ufefftennis"
    assert topic.encode("utf-8").startswith(b"\xef\xbb\xbf")
    (received,) = _messages(_feed(engine, _inbound(protocol, 0, None, topic=topic)))
    assert received.topic == topic
    engine.queue_publish(topic, b"payload")
    (sent,) = _wire(engine.take_effects())
    assert bytes(sent.remaining)[2:5] == b"\xef\xbb\xbf"


def _raw_publish(protocol: MQTTProtocolVersion, topic: bytes) -> bytes:
    body = _u16(len(topic)) + topic + (b"\x00" if protocol is V5 else b"") + b"payload"
    return encode_frame(PacketType.PUBLISH, 0, body)


def _strings_are_well_formed_utf8(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)
    for bad in _BAD_STRINGS:
        with pytest.raises(MQTTError):
            engine.queue_publish(bad, b"payload")
    assert _wire(engine.take_effects()) == []
    # Received: ill-formed sequences, an encoded surrogate (U+D800) and U+0000.
    for bad_bytes in (b"a\xff", b"a\xc3", b"\xed\xa0\x80", b"a\x00b", b"\xc0\x80"):
        engine = _connected(protocol)
        effects = _feed(engine, _raw_publish(protocol, bad_bytes))
        assert _messages(effects) == []
        assert _refused(effects)
        assert isinstance(effects[-1].data, MalformedPacketError)


def _variable_byte_integers_use_the_fewest_bytes() -> None:
    for value, size in (
        (0, 1),
        (127, 1),
        (128, 2),
        (16383, 2),
        (16384, 3),
        (2097151, 3),
        (2097152, 4),
        (268435455, 4),
    ):
        assert len(encode_vbi(value)) == size
    # A sent packet uses it for its Remaining Length.
    engine = _connected(V5)
    engine.queue_publish("t", b"x" * 124)  # 3 + 1 + 124 = 128 bytes of body
    (effect,) = (e for e in engine.take_effects() if e.kind is EffectKind.SEND)
    data = effect.data
    wire = b"".join(data) if isinstance(data, tuple) else data
    assert wire[1:3] == bytes((0x80, 0x01))
    # Received: a padded encoding of the same length is refused.
    padded = bytes((0x30, 0x85, 0x00)) + _u16(1) + b"t" + b"\x00" + b"x"
    decoder = IncrementalDecoder()
    decoder.feed(padded)
    with pytest.raises(MalformedPacketError):
        decoder.next_packet()


def _no_properties_is_a_zero_property_length() -> None:
    engine = _connected(V5)
    engine.queue_publish("t", b"payload")
    engine.queue_subscribe("a")
    engine.queue_unsubscribe("a")
    publish, subscribe, unsubscribe = _wire(engine.take_effects())
    assert bytes(publish.remaining) == _u16(1) + b"t" + b"\x00" + b"payload"
    assert bytes(subscribe.remaining)[2] == 0
    assert bytes(unsubscribe.remaining)[2] == 0
    connect = _engine(V5, connect_properties=None, max_inbound_inflight=65535).begin_connect()
    assert connect[0] == 0x10


def _user_property_strings_are_utf8() -> None:
    engine = _connected(V5)
    engine.queue_publish(
        "t", b"payload", properties=Properties({"user_property": [("cl\u00e9", "val\u00fcr")]})
    )
    (sent,) = _wire(engine.take_effects())
    key, value = "cl\u00e9".encode(), "val\u00fcr".encode()
    assert bytes((0x26,)) + _u16(len(key)) + key + _u16(len(value)) + value in bytes(sent.remaining)
    for pair in (("nul\x00", "v"), ("k", "nul\x00"), ("surrogate\ud800", "v"), ("k", "\ud800")):
        with pytest.raises(MQTTError):
            engine.queue_publish("t", b"p", properties=Properties({"user_property": [pair]}))
    assert _wire(engine.take_effects()) == []


def _connack_reserved_flag_bits_are_validated() -> None:
    for flags in (0b0000_0010, 0b1000_0000, 0b0000_0011):
        engine = _engine(V5, clean_start=False)
        engine.begin_connect()
        effects = _feed(engine, encode_frame(PacketType.CONNACK, 0, bytes((flags, 0, 0))))
        assert _refused(effects)
        assert not any(effect.kind is EffectKind.CONNACK for effect in effects)


# --- Flow control and authentication -----------------------------------------


def _send_quota_starts_at_receive_maximum_and_stops_at_zero() -> None:
    engine = _connected(V5, connack=_connack(V5, receive_maximum=3))
    for index in range(5):
        engine.queue_publish(f"out/{index}", b"payload", qos=1 + index % 2)
    sent = _wire(engine.take_effects())
    # Three leave: the quota was three, and at zero no QoS > 0 PUBLISH is sent.
    assert len(sent) == 3
    engine.queue_publish("out/qos0", b"payload")
    assert _types(engine.take_effects()) == [PacketType.PUBLISH]
    # Without the property the quota is the protocol default, not zero.
    engine = _connected(V5)
    engine.queue_publish("out/topic", b"payload", qos=1)
    assert _types(engine.take_effects()) == [PacketType.PUBLISH]


def _no_auth_without_an_authentication_method() -> None:
    engine = _connected(V5)
    for code in (0x18, 0x19):
        with pytest.raises(ProtocolError):
            engine.queue_auth(code)
    assert _wire(engine.take_effects()) == []


def _authenticated(method: str = "SCRAM-SHA-1") -> ProtocolEngine:
    engine = _engine(
        V5, connect_properties=Properties({"authentication_method": method}), accept_auth=True
    )
    engine.begin_connect()
    _feed(engine, _connack(V5, authentication_method=method))
    return engine


def _reauthentication_starts_with_reason_0x19() -> None:
    engine = _authenticated()
    engine.queue_auth(0x19, Properties({"authentication_data": b"again"}))
    (sent,) = _wire(engine.take_effects())
    assert sent.packet_type is PacketType.AUTH
    assert sent.remaining[0] == 0x19
    # It carries the Authentication Method of the CONNECT.
    assert b"SCRAM-SHA-1" in bytes(sent.remaining)


def _auth_packets_repeat_the_connect_method() -> None:
    engine = _authenticated()
    engine.queue_auth(0x19)
    (sent,) = _wire(engine.take_effects())
    method = b"SCRAM-SHA-1"
    assert bytes((0x15,)) + _u16(len(method)) + method in bytes(sent.remaining)
    # A Server AUTH naming another method is refused.
    other = property_codec.encode_properties(
        Properties({"authentication_method": "OTHER"}), property_codec.AUTH
    )
    assert _refused(_feed(engine, encode_frame(PacketType.AUTH, 0, b"\x18" + other)))
    # So is a successful CONNACK that names another method, or none.
    for properties in ({"authentication_method": "OTHER"}, {}):
        engine = _engine(
            V5,
            connect_properties=Properties({"authentication_method": "SCRAM-SHA-1"}),
            accept_auth=True,
        )
        engine.begin_connect()
        effects = _feed(engine, _connack(V5, **properties))
        assert _refused(effects)


def _emitted_acknowledgement_reason_codes_are_defined() -> None:
    engine = _connected(V5)
    emitted: dict[PacketType, set[int]] = {}

    def record(effects: list[EngineEffect]) -> None:
        for packet in _wire(effects):
            body = bytes(packet.remaining)
            emitted.setdefault(packet.packet_type, set()).add(body[2] if len(body) > 2 else 0)

    record(_feed(engine, _inbound(V5, 1, 5)))  # PUBACK
    record(_feed(engine, _inbound(V5, 2, 6)))  # PUBREC
    record(_feed(engine, encode_frame(PacketType.PUBREL, 2, _u16(6))))  # PUBCOMP
    record(_feed(engine, encode_frame(PacketType.PUBREL, 2, _u16(77))))  # PUBCOMP, unknown
    handle = engine.queue_publish("out/topic", b"payload", qos=2)
    engine.take_effects()
    assert handle.mid is not None
    record(_feed(engine, PubRecPacket(mid=handle.mid).encode(V5)))  # PUBREL
    record(_feed(engine, encode_frame(PacketType.PUBREC, 0, _u16(78))))  # PUBREL, unknown
    defined = {
        PacketType.PUBACK: {0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99},
        PacketType.PUBREC: {0x00, 0x10, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99},
        PacketType.PUBREL: {0x00, 0x92},
        PacketType.PUBCOMP: {0x00, 0x92},
    }
    assert set(emitted) == set(defined)
    for packet_type, codes in emitted.items():
        assert codes <= defined[packet_type]
    assert 0x92 in emitted[PacketType.PUBREL]


def test_mqtt311_4_7_1_1_wildcards_only_in_filters() -> None:
    """[MQTT-4.7.1-1] The wildcard characters can be used in Topic Filters, but MUST
    NOT be used within a Topic Name"""
    _wildcards_only_in_filters(V311)


def test_mqtt5_4_7_0_1_wildcards_only_in_filters() -> None:
    """[MQTT-4.7.0-1] The wildcard characters can be used in Topic Filters, but MUST
    NOT be used within a Topic Name"""
    _wildcards_only_in_filters(V5)


def test_mqtt311_4_7_1_2_multi_level_wildcard_placement() -> None:
    """[MQTT-4.7.1-2] The multi-level wildcard character MUST be specified either on
    its own or following a topic level separator. In either case it MUST be the last
    character specified in the Topic Filter"""
    _multi_level_wildcard_placement(V311)


def test_mqtt5_4_7_1_1_multi_level_wildcard_placement() -> None:
    """[MQTT-4.7.1-1] The multi-level wildcard character MUST be specified either on
    its own or following a topic level separator. In either case it MUST be the last
    character specified in the Topic Filter"""
    _multi_level_wildcard_placement(V5)


def test_mqtt311_4_7_1_3_single_level_wildcard_placement() -> None:
    """[MQTT-4.7.1-3] The single-level wildcard can be used at any level in the Topic
    Filter, including first and last levels. Where it is used it MUST occupy an
    entire level of the filter"""
    _single_level_wildcard_placement(V311)


def test_mqtt5_4_7_1_2_single_level_wildcard_placement() -> None:
    """[MQTT-4.7.1-2] The single-level wildcard can be used at any level in the Topic
    Filter, including first and last levels. Where it is used, it MUST occupy an
    entire level of the filter"""
    _single_level_wildcard_placement(V5)


def test_mqtt311_4_7_3_1_topics_and_filters_are_not_empty() -> None:
    """[MQTT-4.7.3-1] All Topic Names and Topic Filters MUST be at least one character
    long"""
    _topics_and_filters_are_not_empty(V311)


def test_mqtt5_4_7_3_1_topics_and_filters_are_not_empty() -> None:
    """[MQTT-4.7.3-1] All Topic Names and Topic Filters MUST be at least one character
    long"""
    _topics_and_filters_are_not_empty(V5)


def test_mqtt311_4_7_3_3_topics_and_filters_fit_in_65535_bytes() -> None:
    """[MQTT-4.7.3-3] Topic Names and Topic Filters are UTF-8 encoded strings, they
    MUST NOT encode to more than 65535 bytes"""
    _topics_and_filters_fit_in_65535_bytes()


def test_mqtt5_4_8_2_1_shared_subscription_filter_shape() -> None:
    """[MQTT-4.8.2-1] A Shared Subscription's Topic Filter MUST start with $share/ and
    MUST contain a ShareName that is at least one character long"""
    _shared_subscription_filter_shape()


def test_mqtt311_3_8_3_1_subscribe_filters_are_utf8() -> None:
    """[MQTT-3.8.3-1] The Topic Filters in a SUBSCRIBE packet payload MUST be UTF-8
    encoded strings as defined in Section 1.5.3"""
    _subscribe_filters_are_utf8(V311)


def test_mqtt5_3_8_3_1_subscribe_filters_are_utf8() -> None:
    """[MQTT-3.8.3-1] The Topic Filters MUST be a UTF-8 Encoded String"""
    _subscribe_filters_are_utf8(V5)


def test_mqtt311_3_10_3_1_unsubscribe_filters_are_utf8() -> None:
    """[MQTT-3.10.3-1] The Topic Filters in an UNSUBSCRIBE packet MUST be UTF-8 encoded
    strings as defined in Section 1.5.3, packed contiguously"""
    _unsubscribe_filters_are_utf8(V311)


def test_mqtt5_3_10_3_1_unsubscribe_filters_are_utf8() -> None:
    """[MQTT-3.10.3-1] The Topic Filters in an UNSUBSCRIBE packet MUST be UTF-8 Encoded
    Strings"""
    _unsubscribe_filters_are_utf8(V5)


def test_mqtt311_2_3_1_7_suback_and_unsuback_carry_the_request_identifier() -> None:
    """[MQTT-2.3.1-7] Similarly SUBACK and UNSUBACK MUST contain the Packet Identifier
    that was used in the corresponding SUBSCRIBE and UNSUBSCRIBE Packet respectively"""
    _both_acknowledgements_carry_the_request_identifier(V311)


def test_mqtt5_2_2_1_6_suback_and_unsuback_carry_the_request_identifier() -> None:
    """[MQTT-2.2.1-6] A SUBACK and UNSUBACK MUST contain the Packet Identifier that was
    used in the corresponding SUBSCRIBE and UNSUBSCRIBE packet respectively"""
    _both_acknowledgements_carry_the_request_identifier(V5)


def test_mqtt311_3_8_4_2_suback_carries_the_subscribe_identifier() -> None:
    """[MQTT-3.8.4-2] The SUBACK Packet MUST have the same Packet Identifier as the
    SUBSCRIBE Packet that it is acknowledging"""
    _suback_carries_the_subscribe_identifier(V311)


def test_mqtt5_3_8_4_2_suback_carries_the_subscribe_identifier() -> None:
    """[MQTT-3.8.4-2] The SUBACK packet MUST have the same Packet Identifier as the
    SUBSCRIBE packet that it is acknowledging"""
    _suback_carries_the_subscribe_identifier(V5)


def test_mqtt311_3_9_3_1_suback_codes_follow_filter_order() -> None:
    """[MQTT-3.9.3-1] The order of return codes in the SUBACK Packet MUST match the
    order of Topic Filters in the SUBSCRIBE Packet"""
    _suback_codes_follow_filter_order(V311)


def test_mqtt5_3_9_3_1_suback_codes_follow_filter_order() -> None:
    """[MQTT-3.9.3-1] The order of Reason Codes in the SUBACK packet MUST match the
    order of Topic Filters in the SUBSCRIBE packet"""
    _suback_codes_follow_filter_order(V5)


def test_mqtt5_3_11_3_1_unsuback_codes_follow_filter_order() -> None:
    """[MQTT-3.11.3-1] The order of Reason Codes in the UNSUBACK packet MUST match the
    order of Topic Filters in the UNSUBSCRIBE packet"""
    _unsuback_codes_follow_filter_order()


def test_mqtt311_3_9_3_2_reserved_suback_codes_are_refused() -> None:
    """[MQTT-3.9.3-2] SUBACK return codes other than 0x00, 0x01, 0x02 and 0x80 are
    reserved and MUST NOT be used"""
    _reserved_suback_codes_are_refused()


def test_mqtt5_3_8_4_7_suback_code_grants_a_qos_or_reports_failure() -> None:
    """[MQTT-3.8.4-7] This Reason Code MUST either show the maximum QoS that was
    granted for that Subscription or indicate that the subscription failed"""
    _suback_code_grants_a_qos_or_reports_failure()


def test_mqtt311_4_6_0_2_puback_follows_arrival_order() -> None:
    """[MQTT-4.6.0-2] It MUST send PUBACK packets in the order in which the
    corresponding PUBLISH packets were received (QoS 1 messages)"""
    _acknowledgements_follow_arrival_order(V311)


def test_mqtt311_4_6_0_3_pubrec_follows_arrival_order() -> None:
    """[MQTT-4.6.0-3] It MUST send PUBREC packets in the order in which the
    corresponding PUBLISH packets were received (QoS 2 messages)"""
    _acknowledgements_follow_arrival_order(V311)


def test_mqtt5_4_6_0_3_pubrec_follows_arrival_order() -> None:
    """[MQTT-4.6.0-3] The Client MUST send PUBREC packets in the order in which the
    corresponding PUBLISH packets were received (QoS 2 messages)"""
    _acknowledgements_follow_arrival_order(V5)


def test_mqtt311_4_6_0_4_pubrel_follows_pubrec_order() -> None:
    """[MQTT-4.6.0-4] It MUST send PUBREL packets in the order in which the
    corresponding PUBREC packets were received (QoS 2 messages)"""
    _pubrel_follows_pubrec_order(V311)


def test_mqtt5_4_6_0_4_pubrel_follows_pubrec_order() -> None:
    """[MQTT-4.6.0-4] The Client MUST send PUBREL packets in the order in which the
    corresponding PUBREC packets were received (QoS 2 messages)"""
    _pubrel_follows_pubrec_order(V5)


def test_mqtt311_4_6_0_1_resend_keeps_the_original_order() -> None:
    """[MQTT-4.6.0-1] A Client MUST follow these rules when implementing the protocol
    flows defined elsewhere in this chapter: When it re-sends any PUBLISH packets,
    it MUST re-send them in the order in which the original PUBLISH packets were
    sent (this applies to QoS 1 and QoS 2 messages)"""
    _resend_keeps_the_original_order(V311)


def test_mqtt5_4_6_0_1_resend_keeps_the_original_order() -> None:
    """[MQTT-4.6.0-1] When the Client re-sends any PUBLISH packets, it MUST re-send
    them in the order in which the original PUBLISH packets were sent (this applies
    to QoS 1 and QoS 2 messages)"""
    _resend_keeps_the_original_order(V5)


def test_mqtt311_4_5_0_2_every_publish_is_acknowledged() -> None:
    """[MQTT-4.5.0-2] The Client MUST acknowledge any Publish Packet it receives
    according to the applicable QoS rules regardless of whether it elects to process
    the Application Message that it contains"""
    _every_publish_is_acknowledged(V311)


def test_mqtt5_4_5_0_2_every_publish_is_acknowledged() -> None:
    """[MQTT-4.5.0-2] The Client MUST acknowledge any Publish packet it receives
    according to the applicable QoS rules regardless of whether it elects to process
    the Application Message that it contains"""
    _every_publish_is_acknowledged(V5)


def test_mqtt311_2_2_2_1_reserved_flags_are_sent_as_specified() -> None:
    """[MQTT-2.2.2-1] Where a flag bit is marked as “Reserved” in Table 2.2 - Flag
    Bits, it is reserved for future use and MUST be set to the value listed in that
    table"""
    _reserved_flags_are_sent_as_specified()


async def test_mqtt311_2_2_2_2_invalid_flags_close_the_connection() -> None:
    """[MQTT-2.2.2-2] If invalid flags are received, the receiver MUST close the
    Network Connection"""
    await _invalid_flags_close_the_connection()


async def test_mqtt311_4_8_0_1_protocol_violation_closes_the_connection() -> None:
    """[MQTT-4.8.0-1] Unless stated otherwise, if either the Server or Client
    encounters a protocol violation, it MUST close the Network Connection on which
    it received that Control Packet which caused the protocol violation"""
    await _protocol_violation_closes_the_connection()


async def test_mqtt5_3_14_1_1_disconnect_reserved_bits_are_validated() -> None:
    """[MQTT-3.14.1-1] The Client or Server MUST validate that reserved bits are set to
    0. If they are not zero it sends a DISCONNECT packet with a Reason code of 0x81
    (Malformed Packet) as described in section 4.13"""
    await _disconnect_reserved_bits_are_validated()


def test_mqtt311_3_14_4_2_nothing_is_sent_after_disconnect() -> None:
    """[MQTT-3.14.4-2] MUST NOT send any more Control Packets on that Network
    Connection"""
    _nothing_is_sent_after_disconnect(V311)


async def test_mqtt311_3_14_4_1_client_closes_after_disconnect() -> None:
    """[MQTT-3.14.4-1] After sending a DISCONNECT Packet the Client: MUST close the
    Network Connection"""
    await _client_closes_after_disconnect(V311)


async def test_mqtt5_3_14_4_2_client_closes_after_disconnect() -> None:
    """[MQTT-3.14.4-2] MUST close the Network Connection"""
    await _client_closes_after_disconnect(V5)


def test_mqtt5_3_14_2_1_disconnect_reason_codes_are_defined() -> None:
    """[MQTT-3.14.2-1] The Client or Server sending the DISCONNECT packet MUST use one
    of the DISCONNECT Reason Code values"""
    _disconnect_reason_codes_are_defined()


def test_mqtt311_1_5_3_3_byte_order_mark_is_kept() -> None:
    """[MQTT-1.5.3-3] A UTF-8 encoded sequence 0xEF 0xBB 0xBF is always to be
    interpreted to mean U+FEFF ("ZERO WIDTH NO-BREAK SPACE") wherever it appears in
    a string and MUST NOT be skipped over or stripped off by a packet receiver"""
    _byte_order_mark_is_kept(V311)


def test_mqtt5_1_5_4_3_byte_order_mark_is_kept() -> None:
    """[MQTT-1.5.4-3] A UTF-8 encoded sequence 0xEF 0xBB 0xBF is always interpreted as
    U+FEFF ("ZERO WIDTH NO-BREAK SPACE") wherever it appears in a string and MUST
    NOT be skipped over or stripped off by a packet receiver"""
    _byte_order_mark_is_kept(V5)


def test_mqtt311_1_5_3_1_strings_are_well_formed_utf8() -> None:
    """[MQTT-1.5.3-1] The character data in a UTF-8 encoded string MUST be well-formed
    UTF-8 as defined by the Unicode specification [Unicode] and restated in RFC 3629
    [RFC3629]. In particular this data MUST NOT include encodings of code points
    between U+D800 and U+DFFF. If a Server or Client receives a Control Packet
    containing ill-formed UTF-8 it MUST close the Network Connection"""
    _strings_are_well_formed_utf8(V311)


def test_mqtt5_1_5_4_1_strings_are_well_formed_utf8() -> None:
    """[MQTT-1.5.4-1] The character data in a UTF-8 Encoded String MUST be well-formed
    UTF-8 as defined by the Unicode specification [Unicode] and restated in RFC 3629
    [RFC3629]. In particular, the character data MUST NOT include encodings of code
    points between U+D800 and U+DFFF"""
    _strings_are_well_formed_utf8(V5)


def test_mqtt5_1_5_4_2_strings_carry_no_null_character() -> None:
    """[MQTT-1.5.4-2] A UTF-8 Encoded String MUST NOT include an encoding of the null
    character U+0000."""
    _strings_are_well_formed_utf8(V5)


def test_mqtt5_1_5_5_1_variable_byte_integers_use_the_fewest_bytes() -> None:
    """[MQTT-1.5.5-1] The encoded value MUST use the minimum number of bytes necessary
    to represent the value"""
    _variable_byte_integers_use_the_fewest_bytes()


def test_mqtt5_2_2_2_1_no_properties_is_a_zero_property_length() -> None:
    """[MQTT-2.2.2-1] If there are no properties, this MUST be indicated by including a
    Property Length of zero"""
    _no_properties_is_a_zero_property_length()


def test_mqtt5_1_5_7_1_user_property_strings_are_utf8() -> None:
    """[MQTT-1.5.7-1] Both strings MUST comply with the requirements for UTF-8 Encoded
    Strings"""
    _user_property_strings_are_utf8()


def test_mqtt5_3_2_2_1_connack_reserved_flag_bits_are_validated() -> None:
    """[MQTT-3.2.2-1] Byte 1 is the "Connect Acknowledge Flags". Bits 7-1 are reserved
    and MUST be set to 0"""
    _connack_reserved_flag_bits_are_validated()


def test_mqtt5_4_9_0_1_send_quota_starts_at_receive_maximum() -> None:
    """[MQTT-4.9.0-1] The Client or Server MUST set its initial send quota to a non-
    zero value not exceeding the Receive Maximum"""
    _send_quota_starts_at_receive_maximum_and_stops_at_zero()


def test_mqtt5_4_9_0_2_no_qos_publish_at_zero_send_quota() -> None:
    """[MQTT-4.9.0-2] Each time the Client or Server sends a PUBLISH packet at QoS > 0,
    it decrements the send quota. If the send quota reaches zero, the Client or
    Server MUST NOT send any more PUBLISH packets with QoS > 0"""
    _send_quota_starts_at_receive_maximum_and_stops_at_zero()


def test_mqtt5_4_12_0_7_no_auth_without_an_authentication_method() -> None:
    """[MQTT-4.12.0-7] If the Client does not include an Authentication Method in the
    CONNECT, the Client MUST NOT send an AUTH packet to the Server"""
    _no_auth_without_an_authentication_method()


def test_mqtt5_4_12_1_1_reauthentication_starts_with_reason_0x19() -> None:
    """[MQTT-4.12.1-1] If the Client supplied an Authentication Method in the CONNECT
    packet it can initiate a re-authentication at any time after receiving a
    CONNACK. It does this by sending an AUTH packet with a Reason Code of 0x19 (Re-
    authentication). The Client MUST set the Authentication Method to the same value
    as the Authentication Method originally used to authenticate the Network
    Connection"""
    _reauthentication_starts_with_reason_0x19()


def test_mqtt5_4_12_0_5_auth_packets_repeat_the_connect_method() -> None:
    """[MQTT-4.12.0-5] If the initial CONNECT packet included an Authentication Method
    property then all AUTH packets, and any successful CONNACK packet MUST include
    an Authentication Method Property with the same value as in the CONNECT packet"""
    _auth_packets_repeat_the_connect_method()


def test_mqtt5_3_4_2_1_puback_reason_codes_are_defined() -> None:
    """[MQTT-3.4.2-1] The Client or Server sending the PUBACK packet MUST use one of
    the PUBACK Reason Codes"""
    _emitted_acknowledgement_reason_codes_are_defined()


def test_mqtt5_3_5_2_1_pubrec_reason_codes_are_defined() -> None:
    """[MQTT-3.5.2-1] The Client or Server sending the PUBREC packet MUST use one of
    the PUBREC Reason Code values."""
    _emitted_acknowledgement_reason_codes_are_defined()


def test_mqtt5_3_6_2_1_pubrel_reason_codes_are_defined() -> None:
    """[MQTT-3.6.2-1] The Client or Server sending the PUBREL packet MUST use one of
    the PUBREL Reason Code values"""
    _emitted_acknowledgement_reason_codes_are_defined()


def test_mqtt5_3_7_2_1_pubcomp_reason_codes_are_defined() -> None:
    """[MQTT-3.7.2-1] The Client or Server sending the PUBCOMP packet MUST use one of
    the PUBCOMP Reason Code values"""
    _emitted_acknowledgement_reason_codes_are_defined()
