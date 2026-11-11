"""Executable conformance checks for CONNECT, CONNACK limits and Session state.

Same convention as ``test_conformance_qos_flows.py``: one test per numbered
statement and specification version, with the statement quoted from the
vendored index as its docstring.

The CONNECT packet is read back by ``_parse_connect``, a reader written for
these tests from the specifications' field tables. It shares no code with the
encoder under test, so an encoder and decoder that agree on a wrong layout
cannot pass.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec import properties as property_codec
from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MQTTError, NotConnectedError, PacketTooLargeError, ProtocolError
from mqttium.packets import (
    PubAckPacket,
    PublishPacket,
    PubRecPacket,
    encode_frame,
)
from mqttium.packets.connect import ConnectPacket
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

_WILL = Message(topic="will/topic", payload=b"gone", qos=QoS.AT_LEAST_ONCE, retain=True)


@dataclass(frozen=True)
class _Connect:
    """A CONNECT packet as a broker reads it, field by field."""

    flags: int
    keepalive: int
    fields: tuple[str, ...]  # payload fields, in wire order
    client_id: bytes
    will_topic: bytes | None
    will_payload: bytes | None
    username: bytes | None
    password: bytes | None

    @property
    def clean(self) -> bool:
        return bool(self.flags & 0x02)

    @property
    def will_flag(self) -> bool:
        return bool(self.flags & 0x04)

    @property
    def will_qos(self) -> int:
        return (self.flags >> 3) & 0x03

    @property
    def will_retain(self) -> bool:
        return bool(self.flags & 0x20)

    @property
    def username_flag(self) -> bool:
        return bool(self.flags & 0x80)

    @property
    def password_flag(self) -> bool:
        return bool(self.flags & 0x40)


def _variable_byte_integer(body: bytes, at: int) -> tuple[int, int]:
    value = shift = 0
    while True:
        byte = body[at]
        at += 1
        value |= (byte & 0x7F) << shift
        shift += 7
        if not byte & 0x80:
            return value, at


def _parse_connect(wire: bytes) -> _Connect:
    assert wire[0] == 0x10  # CONNECT, reserved flags 0
    length, at = _variable_byte_integer(wire, 1)
    body = wire[at:]
    assert len(body) == length
    assert body[:6] == b"\x00\x04MQTT"
    level, flags = body[6], body[7]
    keepalive = int.from_bytes(body[8:10], "big")
    at = 10
    if level == 5:
        size, at = _variable_byte_integer(body, at)
        at += size

    def take() -> bytes:
        nonlocal at
        size = int.from_bytes(body[at : at + 2], "big")
        value = body[at + 2 : at + 2 + size]
        assert len(value) == size
        at += 2 + size
        return value

    fields = ["client_id"]
    client_id = take()
    will_topic = will_payload = username = password = None
    if flags & 0x04:
        if level == 5:
            size, at = _variable_byte_integer(body, at)
            at += size
            fields.append("will_properties")
        will_topic, will_payload = take(), take()
        fields += ["will_topic", "will_payload"]
    if flags & 0x80:
        username = take()
        fields.append("username")
    if flags & 0x40:
        password = take()
        fields.append("password")
    # Nothing may follow the fields the flags announce.
    assert at == len(body)
    return _Connect(
        flags, keepalive, tuple(fields), client_id, will_topic, will_payload, username, password
    )


def _engine(
    protocol: MQTTProtocolVersion, store: MemoryInflightStore | None = None, **options: object
) -> ProtocolEngine:
    options.setdefault("client_id", "conformance")
    return ProtocolEngine(
        EngineConfig(protocol=protocol, **options),  # type: ignore[arg-type]
        store if store is not None else MemoryInflightStore(),
    )


def _connect(protocol: MQTTProtocolVersion, **options: object) -> _Connect:
    return _parse_connect(_engine(protocol, **options).begin_connect())


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


# --- CONNECT ------------------------------------------------------------------


def _nothing_is_sent_before_connect(protocol: MQTTProtocolVersion) -> None:
    engine = _engine(protocol)
    for refused in (
        lambda: engine.queue_publish("out/topic", b"payload"),
        lambda: engine.queue_subscribe("in/#"),
        engine.queue_ping,
    ):
        with pytest.raises(NotConnectedError):
            refused()
    assert _wire(engine.take_effects()) == []
    assert engine.begin_connect()[0] == 0x10


async def _first_packet_on_the_connection_is_connect(protocol: MQTTProtocolVersion) -> None:
    _nothing_is_sent_before_connect(protocol)
    # And through the runtime: the first bytes written to a new transport.
    broker = ScriptedBrokerTransport(protocol=protocol)
    client = AsyncClient(client_id="first", protocol=protocol, keepalive=0)
    client._transport_factory = transport_factory(broker)
    await client.connect("broker", 1883, timeout=2.0)
    await client.publish("out/topic", b"payload", qos=1, timeout=2.0)
    await client.disconnect()
    decoder = IncrementalDecoder()
    decoder.feed(b"".join(broker.written))
    written = [packet.packet_type for packet in decoder.drain_packets()]
    assert written[0] is PacketType.CONNECT
    assert written.count(PacketType.CONNECT) == 1


def _no_will_means_no_will_fields(protocol: MQTTProtocolVersion) -> None:
    connect = _connect(protocol, username="user", password=b"secret")
    assert not connect.will_flag
    assert connect.will_qos == 0 and not connect.will_retain
    assert connect.will_topic is None and connect.will_payload is None
    assert connect.fields == ("client_id", "username", "password")
    # The packet layer refuses Will QoS or Will Retain without a Will.
    for stray in ({"will_qos": QoS.AT_LEAST_ONCE}, {"will_retain": True}):
        with pytest.raises(ProtocolError):
            ConnectPacket(client_id="conformance", protocol=protocol, **stray).encode()  # type: ignore[arg-type]


def _will_qos_is_never_three(protocol: MQTTProtocolVersion) -> None:
    for qos in (0, 1, 2):
        will = Message(topic="will/topic", payload=b"gone", qos=QoS(qos))
        connect = _connect(protocol, will=will)
        assert connect.will_flag and connect.will_qos == qos
    with pytest.raises(ProtocolError):
        ConnectPacket(
            client_id="conformance",
            protocol=protocol,
            will_topic="will/topic",
            will_qos=3,  # type: ignore[arg-type]
        ).encode()


def _will_fields_follow_the_will_flag(protocol: MQTTProtocolVersion) -> None:
    connect = _connect(protocol, will=_WILL)
    assert connect.will_flag and connect.will_qos == 1 and connect.will_retain
    assert connect.will_topic == b"will/topic" and connect.will_payload == b"gone"
    will_fields = ("will_topic", "will_payload")
    if protocol is V5:
        will_fields = ("will_properties", *will_fields)
    assert connect.fields == ("client_id", *will_fields)


def _user_name_follows_its_flag(protocol: MQTTProtocolVersion) -> None:
    absent = _connect(protocol)
    assert not absent.username_flag and absent.username is None
    assert absent.fields == ("client_id",)
    present = _connect(protocol, username="user")
    assert present.username_flag and present.username == b"user"
    assert present.fields == ("client_id", "username")


def _password_follows_its_flag(protocol: MQTTProtocolVersion) -> None:
    absent = _connect(protocol, username="user")
    assert not absent.password_flag and absent.password is None
    assert absent.fields == ("client_id", "username")
    present = _connect(protocol, username="user", password=b"\x00secret")
    assert present.password_flag and present.password == b"\x00secret"
    assert present.fields == ("client_id", "username", "password")


def _client_id_is_the_first_payload_field(protocol: MQTTProtocolVersion) -> None:
    connect = _connect(
        protocol, client_id="clïent-\U0001f600", will=_WILL, username="user", password=b"p"
    )
    assert connect.fields[0] == "client_id"
    assert connect.client_id == "clïent-\U0001f600".encode()
    # Present even when empty: a zero-length string, not an omitted field.
    empty = _connect(protocol, client_id="")
    assert empty.fields == ("client_id",) and empty.client_id == b""


def _connect_strings_are_utf8(protocol: MQTTProtocolVersion) -> None:
    will = Message(topic="wïll/\u00e9t\u00e9", payload=b"gone")
    connect = _connect(protocol, client_id="cli\u00e9nt", will=will, username="us\u00e9r")
    assert connect.client_id.decode("utf-8") == "cli\u00e9nt"
    assert connect.will_topic is not None and connect.username is not None
    assert connect.will_topic.decode("utf-8") == "wïll/\u00e9t\u00e9"
    assert connect.username.decode("utf-8") == "us\u00e9r"
    # What a UTF-8 Encoded String may not carry is refused, never encoded.
    for field in ("client_id", "username"):
        for bad in ("nul\x00", "surrogate\ud800"):
            with pytest.raises((MQTTError, ValueError)):
                _connect(protocol, **{field: bad})
    for bad in ("nul\x00", "surrogate\ud800"):
        with pytest.raises((MQTTError, ValueError)):
            _connect(protocol, will=Message(topic=bad, payload=b"gone"))


def _connect_payload_field_order() -> None:
    connect = _connect(V5, will=_WILL, username="user", password=b"secret")
    assert connect.fields == (
        "client_id",
        "will_properties",
        "will_topic",
        "will_payload",
        "username",
        "password",
    )


async def _idle_client_sends_pingreq(protocol: MQTTProtocolVersion) -> None:
    broker = ScriptedBrokerTransport(protocol=protocol)
    client = AsyncClient(client_id="idle", protocol=protocol, keepalive=1, ping_timeout=2.0)
    client._transport_factory = transport_factory(broker)
    await client.connect("broker", 1883, timeout=2.0)
    try:
        # Nothing else is sent. The bound is the one and a half times the Keep
        # Alive after which the Server closes the connection [MQTT-3.1.2-22].
        await wait_until(lambda: broker.pingreqs >= 1, timeout=1.5)
    finally:
        await client.disconnect()


class _ServerKeepAliveBroker(ScriptedBrokerTransport):
    def handle_packet(self, raw: RawPacket) -> None:
        if raw.packet_type is PacketType.CONNECT:
            self.push_rx(_connack(V5, server_keep_alive=1))
            return
        super().handle_packet(raw)


async def _server_keep_alive_replaces_the_requested_one() -> None:
    engine = _connected(V5, keepalive=600, connack=_connack(V5, server_keep_alive=1))
    assert engine.negotiated.server_keep_alive == 1
    # Through the runtime: asked for ten minutes, pings within the server's second.
    broker = _ServerKeepAliveBroker(protocol=V5)
    client = AsyncClient(client_id="ska", protocol=V5, keepalive=600, ping_timeout=2.0)
    client._transport_factory = transport_factory(broker)
    await client.connect("broker", 1883, timeout=2.0)
    try:
        await wait_until(lambda: broker.pingreqs >= 1, timeout=1.5)
    finally:
        await client.disconnect()


def _nothing_but_auth_is_sent_before_connack() -> None:
    engine = _engine(
        V5,
        connect_properties=Properties({"authentication_method": "SCRAM-SHA-1"}),
        accept_auth=True,
    )
    engine.begin_connect()
    # A publication may be accepted for later; it does not reach the wire.
    engine.queue_publish("out/topic", b"payload", qos=1)
    with pytest.raises(NotConnectedError):
        engine.queue_subscribe("in/#")
    with pytest.raises(NotConnectedError):
        engine.queue_unsubscribe("in/#")
    with pytest.raises(NotConnectedError):
        engine.queue_ping()
    assert _wire(engine.take_effects()) == []
    # The server's challenge is answered with AUTH, and still nothing else.
    challenge = encode_frame(
        PacketType.AUTH,
        0,
        b"\x18"
        + property_codec.encode_properties(
            Properties({"authentication_method": "SCRAM-SHA-1", "authentication_data": b"c"}),
            property_codec.AUTH,
        ),
    )
    (asked,) = (e for e in _feed(engine, challenge) if e.kind is EffectKind.AUTH)
    assert engine.respond_auth(asked.exchange_token, 0x18)
    assert _types(engine.take_effects()) == [PacketType.AUTH]
    accepted = _feed(engine, _connack(V5, authentication_method="SCRAM-SHA-1"))
    assert _types(accepted) == [PacketType.PUBLISH]


# --- Session state ------------------------------------------------------------


def _in_flight(engine: ProtocolEngine, qos: int) -> int:
    handle = engine.queue_publish("out/topic", b"payload", qos=qos)
    assert _types(engine.take_effects()) == [PacketType.PUBLISH]
    assert handle.mid is not None
    return handle.mid


def _session_with_state(
    protocol: MQTTProtocolVersion, store: MemoryInflightStore
) -> tuple[ProtocolEngine, int, int]:
    """A durable session holding every kind of QoS state a Client stores."""
    options: dict[str, object] = {"clean_start": False}
    if protocol is V5:
        options["connect_properties"] = Properties({"session_expiry_interval": 3600})
    engine = _connected(protocol, store, **options)
    qos1 = _in_flight(engine, 1)
    qos2 = _in_flight(engine, 2)
    # An inbound QoS 2 exchange stopped after PUBREC.
    received = _feed(engine, _inbound(protocol, 2, 7))
    assert len(_messages(received)) == 1 and _types(received) == [PacketType.PUBREC]
    return engine, qos1, qos2


def _assert_new_session(
    protocol: MQTTProtocolVersion,
    engine: ProtocolEngine,
    store: MemoryInflightStore,
    effects: list[EngineEffect],
    qos1: int,
    qos2: int,
) -> None:
    # Nothing is retransmitted. The QoS 2 exchange ends in failure. The QoS 1
    # Application Message is published again as a new one, with DUP clear: it
    # is the message that is kept, not the Session's record of having sent it.
    failed = [effect for effect in effects if effect.kind is EffectKind.PUBLISH_FAILED]
    assert len(failed) == 1
    sent = _wire(effects)
    assert [packet.packet_type for packet in sent] == [PacketType.PUBLISH]
    assert not sent[0].flags & _DUP
    assert store.get_out(qos2) is None
    # The half-finished inbound exchange is forgotten: its Packet Identifier
    # names a new Application Message, which is delivered.
    again = _feed(engine, _inbound(protocol, 2, 7))
    assert len(_messages(again)) == 1 and _types(again) == [PacketType.PUBREC]
    _feed(engine, PubAckPacket(mid=qos1).encode(protocol))
    assert engine.unacknowledged_messages == 0


def _clean_start_discards_the_previous_session(protocol: MQTTProtocolVersion) -> None:
    store = MemoryInflightStore()
    _previous, qos1, qos2 = _session_with_state(protocol, store)
    # A new process, same store and ClientId, asking for a clean session.
    engine = _engine(protocol, store, clean_start=True)
    assert _parse_connect(engine.begin_connect()).clean
    effects = _feed(engine, _connack(protocol))
    _assert_new_session(protocol, engine, store, effects, qos1, qos2)


def _session_state_is_stored_across_connections(protocol: MQTTProtocolVersion) -> None:
    store = MemoryInflightStore()
    engine, qos1, qos2 = _session_with_state(protocol, store)
    assert store.get_out(qos1) is not None and store.get_out(qos2) is not None
    replayed = _reconnect(engine, _connack(protocol, session_present=True))
    assert _types(replayed) == [PacketType.PUBLISH, PacketType.PUBLISH]
    assert all(packet.flags & _DUP for packet in _wire(replayed))
    # The inbound exchange survived too: the repeated PUBLISH is not delivered again.
    repeated = _feed(engine, _inbound(protocol, 2, 7))
    assert _messages(repeated) == [] and _types(repeated) == [PacketType.PUBREC]
    # So did a second process over the same store.
    restarted = _engine(protocol, store, clean_start=False)
    restarted.begin_connect()
    assert _types(_feed(restarted, _connack(protocol, session_present=True))) == [
        PacketType.PUBLISH,
        PacketType.PUBLISH,
    ]


def _session_present_zero_discards_client_state() -> None:
    store = MemoryInflightStore()
    engine, qos1, qos2 = _session_with_state(V5, store)
    effects = _reconnect(engine, _connack(V5, session_present=False))
    _assert_new_session(V5, engine, store, effects, qos1, qos2)


def _session_state_is_kept_while_connected(protocol: MQTTProtocolVersion) -> None:
    # Clean session: nothing outlives the connection, yet nothing may be
    # dropped during it.
    store = MemoryInflightStore()
    engine = _connected(protocol, store, clean_start=True)
    qos1 = _in_flight(engine, 1)
    qos2 = _in_flight(engine, 2)
    _feed(engine, _inbound(protocol, 2, 7))
    # Unrelated traffic in both directions.
    for index in range(50):
        engine.queue_publish("out/other", b"x")
        _feed(engine, _inbound(protocol, 0, None, topic=f"in/{index}"))
    engine.queue_subscribe("in/#")
    engine.queue_ping()
    engine.take_effects()
    assert engine.unacknowledged_messages == 2
    assert store.get_out(qos1) is not None and store.get_out(qos2) is not None
    repeated = _feed(engine, _inbound(protocol, 2, 7))
    assert _messages(repeated) == [] and _types(repeated) == [PacketType.PUBREC]
    assert _types(_feed(engine, PubRecPacket(mid=qos2).encode(protocol))) == [PacketType.PUBREL]
    _feed(engine, PubAckPacket(mid=qos1).encode(protocol))
    assert engine.unacknowledged_messages == 1


# --- Limits announced in CONNACK ---------------------------------------------


def _maximum_qos_is_not_exceeded() -> None:
    for maximum in (0, 1):
        engine = _connected(V5, connack=_connack(V5, maximum_qos=maximum))
        for qos in range(maximum + 1, 3):
            with pytest.raises(ProtocolError):
                engine.queue_publish("out/topic", b"payload", qos=qos)
        engine.queue_publish("out/topic", b"payload", qos=maximum)
        (sent,) = _wire(engine.take_effects())
        assert (sent.flags >> 1) & 0x03 == maximum


def _retain_is_not_sent_when_unavailable() -> None:
    engine = _connected(V5, connack=_connack(V5, retain_available=0))
    with pytest.raises(ProtocolError):
        engine.queue_publish("out/topic", b"payload", retain=True)
    with pytest.raises(ProtocolError):
        engine.queue_publish("out/topic", b"payload", qos=1, retain=True)
    assert _wire(engine.take_effects()) == []
    assert engine.unacknowledged_messages == 0


def _maximum_packet_size_is_not_exceeded() -> None:
    limit = 32
    engine = _connected(V5, connack=_connack(V5, maximum_packet_size=limit))
    with pytest.raises(PacketTooLargeError):
        engine.queue_publish("out/topic", b"x" * limit)
    with pytest.raises(PacketTooLargeError):
        engine.queue_publish("out/topic", b"x" * limit, qos=1)
    with pytest.raises(PacketTooLargeError):
        engine.queue_subscribe("in/" + "x" * limit)
    with pytest.raises(PacketTooLargeError):
        engine.queue_unsubscribe("in/" + "x" * limit)
    assert _wire(engine.take_effects()) == []
    assert engine.unacknowledged_messages == 0
    # The largest packet that fits is sent, and is exactly the limit.
    overhead = 2 + 2 + len("out/topic") + 1  # fixed header, topic, no properties
    engine.queue_publish("out/topic", b"x" * (limit - overhead))
    (effect,) = (e for e in engine.take_effects() if e.kind is EffectKind.SEND)
    data = effect.data
    assert len(b"".join(data) if isinstance(data, tuple) else data) == limit


def _topic_alias_stays_within_the_server_maximum() -> None:
    engine = _connected(V5, connack=_connack(V5, topic_alias_maximum=2))
    for alias in (1, 2):
        engine.queue_publish("out/topic", b"p", properties=Properties({"topic_alias": alias}))
    assert len(_wire(engine.take_effects())) == 2
    for alias in (3, 65535):
        with pytest.raises(ProtocolError):
            engine.queue_publish("out/topic", b"p", properties=Properties({"topic_alias": alias}))
    assert _wire(engine.take_effects()) == []


def _no_topic_alias_without_a_server_maximum() -> None:
    for connack in (_connack(V5), _connack(V5, topic_alias_maximum=0)):
        engine = _connected(V5, connack=connack)
        with pytest.raises(ProtocolError):
            engine.queue_publish("out/topic", b"p", properties=Properties({"topic_alias": 1}))
        engine.queue_publish("out/topic", b"p")
        (sent,) = _wire(engine.take_effects())
        publish = PublishPacket.decode(sent.flags, sent.remaining, V5)
        assert publish.properties is None or publish.properties.get("topic_alias") is None


# --- PUBLISH ------------------------------------------------------------------


def _dup_reflects_retransmission_only() -> None:
    store = MemoryInflightStore()
    engine = _connected(V5, store, clean_start=False)
    # The incoming PUBLISH has DUP set; that says nothing about what we send.
    incoming = PublishPacket(
        topic="in/topic", payload=b"payload", qos=QoS.AT_LEAST_ONCE, retain=False, dup=True, mid=9
    ).encode(V5)
    assert len(_messages(_feed(engine, incoming))) == 1
    engine.queue_publish("out/topic", b"payload", qos=1)
    (first,) = _wire(engine.take_effects())
    assert not first.flags & _DUP
    (again,) = _wire(_reconnect(engine, _connack(V5, session_present=True)))
    assert again.flags & _DUP


def _topic_name_leads_the_variable_header(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)
    topic = "out/t\u00f6pic/\U0001f600"
    encoded = topic.encode("utf-8")
    for qos in (0, 1, 2):
        engine.queue_publish(topic, b"payload", qos=qos)
        (sent,) = _wire(engine.take_effects())
        body = bytes(sent.remaining)
        assert body[:2] == len(encoded).to_bytes(2, "big")
        assert body[2 : 2 + len(encoded)] == encoded
    for bad in ("", "nul\x00", "surrogate\ud800"):
        with pytest.raises((MQTTError, ValueError)):
            engine.queue_publish(bad, b"payload")
    assert _wire(engine.take_effects()) == []


def _receiver_answers_by_qos(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)
    assert _types(_feed(engine, _inbound(protocol, 0, None))) == []
    (puback,) = _wire(_feed(engine, _inbound(protocol, 1, 11)))
    assert puback.packet_type is PacketType.PUBACK
    assert int.from_bytes(puback.remaining[:2], "big") == 11
    (pubrec,) = _wire(_feed(engine, _inbound(protocol, 2, 12)))
    assert pubrec.packet_type is PacketType.PUBREC
    assert int.from_bytes(pubrec.remaining[:2], "big") == 12


def _aliased(alias: int, topic: str = "") -> bytes:
    return _inbound(V5, 0, None, topic=topic, properties=Properties({"topic_alias": alias}))


def _inbound_aliases_do_not_outlive_the_connection() -> None:
    engine = _connected(V5, topic_alias_maximum=4, clean_start=False)
    _feed(engine, _aliased(1, "in/aliased"))
    (resolved,) = _messages(_feed(engine, _aliased(1)))
    assert resolved.topic == "in/aliased"
    _reconnect(engine, _connack(V5, session_present=True))
    # The mapping is gone even though the Session was resumed.
    effects = _feed(engine, _aliased(1))
    assert _messages(effects) == []
    assert effects[-1].kind is EffectKind.PROTOCOL_ERROR


def _every_alias_up_to_the_announced_maximum_is_accepted() -> None:
    maximum = 5
    engine = _connected(V5, topic_alias_maximum=maximum)
    for alias in range(1, maximum + 1):
        (set_up,) = _messages(_feed(engine, _aliased(alias, f"in/{alias}")))
        assert set_up.topic == f"in/{alias}"
    for alias in range(1, maximum + 1):
        (resolved,) = _messages(_feed(engine, _aliased(alias)))
        assert resolved.topic == f"in/{alias}"


def _publish_string_properties_are_utf8() -> None:
    engine = _connected(V5)
    response_topic = "r\u00e9ply/\U0001f600"
    content_type = "text/pl\u00e4in"
    engine.queue_publish(
        "out/topic",
        b"payload",
        properties=Properties({"response_topic": response_topic, "content_type": content_type}),
    )
    (sent,) = _wire(engine.take_effects())
    body = bytes(sent.remaining)
    for identifier, value in ((0x08, response_topic), (0x03, content_type)):
        encoded = value.encode("utf-8")
        assert bytes((identifier,)) + len(encoded).to_bytes(2, "big") + encoded in body
    for name in ("response_topic", "content_type"):
        for bad in ("nul\x00", "surrogate\ud800"):
            with pytest.raises((MQTTError, ValueError)):
                engine.queue_publish("out/topic", b"p", properties=Properties({name: bad}))
    assert _wire(engine.take_effects()) == []


def _receive_maximum_bounds_unacknowledged_publishes() -> None:
    engine = _connected(V5, connack=_connack(V5, receive_maximum=2))
    for qos in (1, 2, 1, 2):
        engine.queue_publish("out/topic", b"payload", qos=qos)
    sent = _wire(engine.take_effects())
    assert [PublishPacket.decode(p.flags, p.remaining, V5).mid for p in sent] == [1, 2]
    # PUBREC without an error does not return the slot: the exchange goes on.
    assert _types(_feed(engine, PubRecPacket(mid=2).encode(V5))) == [PacketType.PUBREL]
    # PUBACK does, and so does PUBREC with a Reason Code of 0x80 or more.
    (third,) = _wire(_feed(engine, PubAckPacket(mid=1).encode(V5)))
    assert PublishPacket.decode(third.flags, third.remaining, V5).mid == 3
    (fourth,) = _wire(_feed(engine, PubAckPacket(mid=3).encode(V5)))
    assert PublishPacket.decode(fourth.flags, fourth.remaining, V5).mid == 4
    refused = _feed(engine, PubRecPacket(mid=4, reason_code=0x80).encode(V5))
    assert _wire(refused) == []
    engine.queue_publish("out/topic", b"payload", qos=1)
    assert _types(engine.take_effects()) == [PacketType.PUBLISH]


def _a_full_send_window_delays_only_qos_publishes() -> None:
    engine = _connected(V5, connack=_connack(V5, receive_maximum=1))
    engine.queue_publish("out/topic", b"payload", qos=1)
    engine.queue_publish("out/topic", b"held", qos=1)
    assert _types(engine.take_effects()) == [PacketType.PUBLISH]
    engine.queue_subscribe("in/#")
    engine.queue_unsubscribe("in/#")
    engine.queue_ping()
    engine.queue_publish("out/topic", b"qos0")
    assert _types(engine.take_effects()) == [
        PacketType.SUBSCRIBE,
        PacketType.UNSUBSCRIBE,
        PacketType.PINGREQ,
        PacketType.PUBLISH,
    ]
    # Acknowledgements of inbound traffic are not held back either.
    assert _types(_feed(engine, _inbound(V5, 1, 5))) == [PacketType.PUBACK]
    assert _types(_feed(engine, _inbound(V5, 2, 6))) == [PacketType.PUBREC]


async def test_mqtt311_3_1_0_1_first_packet_is_connect() -> None:
    """[MQTT-3.1.0-1] After a Network Connection is established by a Client to a
    Server, the first Packet sent from the Client to the Server MUST be a CONNECT
    Packet"""
    await _first_packet_on_the_connection_is_connect(V311)


async def test_mqtt5_3_1_0_1_first_packet_is_connect() -> None:
    """[MQTT-3.1.0-1] After a Network Connection is established by a Client to a
    Server, the first packet sent from the Client to the Server MUST be a CONNECT
    packet"""
    await _first_packet_on_the_connection_is_connect(V5)


def test_mqtt311_3_1_2_11_no_will_flag_means_no_will_fields() -> None:
    """[MQTT-3.1.2-11] If the Will Flag is set to 0 the Will QoS and Will Retain fields
    in the Connect Flags MUST be set to zero and the Will Topic and Will Message
    fields MUST NOT be present in the payload"""
    _no_will_means_no_will_fields(V311)


def test_mqtt311_3_1_2_13_no_will_flag_means_will_qos_zero() -> None:
    """[MQTT-3.1.2-13] If the Will Flag is set to 0, then the Will QoS MUST be set to 0
    (0x00)"""
    _no_will_means_no_will_fields(V311)


def test_mqtt311_3_1_2_15_no_will_flag_means_will_retain_zero() -> None:
    """[MQTT-3.1.2-15] If the Will Flag is set to 0, then the Will Retain Flag MUST be
    set to 0"""
    _no_will_means_no_will_fields(V311)


def test_mqtt5_3_1_2_11_no_will_flag_means_will_qos_zero() -> None:
    """[MQTT-3.1.2-11] If the Will Flag is set to 0, then the Will QoS MUST be set to 0
    (0x00)"""
    _no_will_means_no_will_fields(V5)


def test_mqtt5_3_1_2_13_no_will_flag_means_will_retain_zero() -> None:
    """[MQTT-3.1.2-13] If the Will Flag is set to 0, then Will Retain MUST be set to 0"""
    _no_will_means_no_will_fields(V5)


def test_mqtt311_3_1_2_14_will_qos_is_never_three() -> None:
    """[MQTT-3.1.2-14] If the Will Flag is set to 1, the value of Will QoS can be 0
    (0x00), 1 (0x01), or 2 (0x02). It MUST NOT be 3 (0x03)"""
    _will_qos_is_never_three(V311)


def test_mqtt5_3_1_2_12_will_qos_is_zero_one_or_two() -> None:
    """[MQTT-3.1.2-12] If the Will Flag is set to 1, the value of Will QoS can be 0
    (0x00), 1 (0x01), or 2 (0x02)"""
    _will_qos_is_never_three(V5)


def test_mqtt5_3_1_2_9_will_flag_brings_the_will_fields() -> None:
    """[MQTT-3.1.2-9] If the Will Flag is set to 1, the Will Properties, Will Topic,
    and Will Payload fields MUST be present in the Payload"""
    _will_fields_follow_the_will_flag(V5)


def test_mqtt311_3_1_2_18_no_user_name_without_its_flag() -> None:
    """[MQTT-3.1.2-18] If the User Name Flag is set to 0, a user name MUST NOT be
    present in the payload"""
    _user_name_follows_its_flag(V311)


def test_mqtt311_3_1_2_19_user_name_present_with_its_flag() -> None:
    """[MQTT-3.1.2-19] If the User Name Flag is set to 1, a user name MUST be present
    in the payload"""
    _user_name_follows_its_flag(V311)


def test_mqtt5_3_1_2_16_no_user_name_without_its_flag() -> None:
    """[MQTT-3.1.2-16] If the User Name Flag is set to 0, a User Name MUST NOT be
    present in the Payload"""
    _user_name_follows_its_flag(V5)


def test_mqtt5_3_1_2_17_user_name_present_with_its_flag() -> None:
    """[MQTT-3.1.2-17] If the User Name Flag is set to 1, a User Name MUST be present
    in the Payload"""
    _user_name_follows_its_flag(V5)


def test_mqtt311_3_1_2_20_no_password_without_its_flag() -> None:
    """[MQTT-3.1.2-20] If the Password Flag is set to 0, a password MUST NOT be present
    in the payload"""
    _password_follows_its_flag(V311)


def test_mqtt311_3_1_2_21_password_present_with_its_flag() -> None:
    """[MQTT-3.1.2-21] If the Password Flag is set to 1, a password MUST be present in
    the payload"""
    _password_follows_its_flag(V311)


def test_mqtt5_3_1_2_18_no_password_without_its_flag() -> None:
    """[MQTT-3.1.2-18] If the Password Flag is set to 0, a Password MUST NOT be present
    in the Payload"""
    _password_follows_its_flag(V5)


def test_mqtt5_3_1_2_19_password_present_with_its_flag() -> None:
    """[MQTT-3.1.2-19] If the Password Flag is set to 1, a Password MUST be present in
    the Payload"""
    _password_follows_its_flag(V5)


def test_mqtt311_3_1_3_3_client_id_is_the_first_payload_field() -> None:
    """[MQTT-3.1.3-3] The Client Identifier (ClientId) MUST be present and MUST be the
    first field in the CONNECT packet payload"""
    _client_id_is_the_first_payload_field(V311)


def test_mqtt5_3_1_3_3_client_id_is_the_first_payload_field() -> None:
    """[MQTT-3.1.3-3] The ClientID MUST be present and is the first field in the
    CONNECT packet Payload"""
    _client_id_is_the_first_payload_field(V5)


def test_mqtt311_3_1_3_4_client_id_is_utf8() -> None:
    """[MQTT-3.1.3-4] The ClientId MUST be a UTF-8 encoded string as defined in Section
    1.5.3"""
    _connect_strings_are_utf8(V311)


def test_mqtt311_3_1_3_10_will_topic_is_utf8() -> None:
    """[MQTT-3.1.3-10] The Will Topic MUST be a UTF-8 encoded string as defined in
    Section 1.5.3"""
    _connect_strings_are_utf8(V311)


def test_mqtt311_3_1_3_11_user_name_is_utf8() -> None:
    """[MQTT-3.1.3-11] The User Name MUST be a UTF-8 encoded string as defined in
    Section 1.5.3"""
    _connect_strings_are_utf8(V311)


def test_mqtt5_3_1_3_4_client_id_is_utf8() -> None:
    """[MQTT-3.1.3-4] The ClientID MUST be a UTF-8 Encoded String"""
    _connect_strings_are_utf8(V5)


def test_mqtt5_3_1_3_11_will_topic_is_utf8() -> None:
    """[MQTT-3.1.3-11] The Will Topic MUST be a UTF-8 Encoded String"""
    _connect_strings_are_utf8(V5)


def test_mqtt5_3_1_3_12_user_name_is_utf8() -> None:
    """[MQTT-3.1.3-12] If the User Name Flag is set to 1, the User Name is the next
    field in the Payload. The User Name MUST be a UTF-8 Encoded String"""
    _connect_strings_are_utf8(V5)


def test_mqtt5_3_1_3_1_connect_payload_field_order() -> None:
    """[MQTT-3.1.3-1] The Payload of the CONNECT packet contains one or more length-
    prefixed fields, whose presence is determined by the flags in the Variable
    Header. These fields, if present, MUST appear in the order Client Identifier,
    Will Properties, Will Topic, Will Payload, User Name, Password"""
    _connect_payload_field_order()


async def test_mqtt5_3_1_2_20_idle_client_sends_pingreq() -> None:
    """[MQTT-3.1.2-20] If Keep Alive is non-zero and in the absence of sending any
    other MQTT Control Packets, the Client MUST send a PINGREQ packet"""
    await _idle_client_sends_pingreq(V5)


async def test_mqtt5_3_2_2_21_server_keep_alive_replaces_the_requested_one() -> None:
    """[MQTT-3.2.2-21] If the Server sends a Server Keep Alive on the CONNACK packet,
    the Client MUST use this value instead of the Keep Alive value the Client sent
    on CONNECT"""
    await _server_keep_alive_replaces_the_requested_one()


def test_mqtt5_3_1_2_30_nothing_but_auth_before_connack() -> None:
    """[MQTT-3.1.2-30] If a Client sets an Authentication Method in the CONNECT, the
    Client MUST NOT send any packets other than AUTH or DISCONNECT packets until it
    has received a CONNACK packet"""
    _nothing_but_auth_is_sent_before_connack()


def test_mqtt311_3_1_2_6_clean_session_discards_the_previous_session() -> None:
    """[MQTT-3.1.2-6] If CleanSession is set to 1, the Client and Server MUST discard
    any previous Session and start a new one. This Session lasts as long as the
    Network Connection. State data associated with this Session MUST NOT be reused
    in any subsequent Session"""
    _clean_start_discards_the_previous_session(V311)


def test_mqtt5_3_1_2_4_clean_start_discards_the_existing_session() -> None:
    """[MQTT-3.1.2-4] If a CONNECT packet is received with Clean Start is set to 1, the
    Client and Server MUST discard any existing Session and start a new Session"""
    _clean_start_discards_the_previous_session(V5)


def test_mqtt5_3_1_2_23_session_state_is_stored_after_the_connection_closes() -> None:
    """[MQTT-3.1.2-23] The Client and Server MUST store the Session State after the
    Network Connection is closed if the Session Expiry Interval is greater than 0"""
    _session_state_is_stored_across_connections(V5)


def test_mqtt311_4_1_0_1_session_state_is_stored_for_the_whole_session() -> None:
    """[MQTT-4.1.0-1] The Client and Server MUST store Session state for the entire
    duration of the Session"""
    _session_state_is_stored_across_connections(V311)


def test_mqtt5_3_2_2_5_session_present_zero_discards_client_state() -> None:
    """[MQTT-3.2.2-5] If the Client does have Session State and receives Session
    Present set to 0 it MUST discard its Session State if it continues with the
    Network Connection"""
    _session_present_zero_discards_client_state()


def test_mqtt311_4_1_0_2_session_lasts_as_long_as_the_connection() -> None:
    """[MQTT-4.1.0-2] A Session MUST last at least as long it has an active Network
    Connection"""
    _session_state_is_kept_while_connected(V311)


def test_mqtt5_4_1_0_1_session_state_is_kept_while_connected() -> None:
    """[MQTT-4.1.0-1] The Client and Server MUST NOT discard the Session State while
    the Network Connection is open"""
    _session_state_is_kept_while_connected(V5)


def test_mqtt5_3_2_2_11_maximum_qos_is_not_exceeded() -> None:
    """[MQTT-3.2.2-11] If a Client receives a Maximum QoS from a Server, it MUST NOT
    send PUBLISH packets at a QoS level exceeding the Maximum QoS level specified"""
    _maximum_qos_is_not_exceeded()


def test_mqtt5_3_2_2_14_retain_is_not_sent_when_unavailable() -> None:
    """[MQTT-3.2.2-14] A Client receiving Retain Available set to 0 from the Server
    MUST NOT send a PUBLISH packet with the RETAIN flag set to 1"""
    _retain_is_not_sent_when_unavailable()


def test_mqtt5_3_2_2_15_maximum_packet_size_is_not_exceeded() -> None:
    """[MQTT-3.2.2-15] The Client MUST NOT send packets exceeding Maximum Packet Size
    to the Server"""
    _maximum_packet_size_is_not_exceeded()


def test_mqtt5_3_2_2_17_topic_alias_stays_within_the_server_maximum() -> None:
    """[MQTT-3.2.2-17] The Client MUST NOT send a Topic Alias in a PUBLISH packet to
    the Server greater than this value"""
    _topic_alias_stays_within_the_server_maximum()


def test_mqtt5_3_3_2_9_topic_alias_stays_within_the_connack_maximum() -> None:
    """[MQTT-3.3.2-9] A Client MUST NOT send a PUBLISH packet with a Topic Alias
    greater than the Topic Alias Maximum value returned by the Server in the CONNACK
    packet"""
    _topic_alias_stays_within_the_server_maximum()


def test_mqtt5_3_2_2_18_no_topic_alias_without_a_server_maximum() -> None:
    """[MQTT-3.2.2-18] If Topic Alias Maximum is absent or 0, the Client MUST NOT send
    any Topic Aliases on to the Server"""
    _no_topic_alias_without_a_server_maximum()


def test_mqtt5_3_3_1_3_dup_reflects_retransmission_only() -> None:
    """[MQTT-3.3.1-3] The DUP flag in the outgoing PUBLISH packet is set independently
    to the incoming PUBLISH packet, its value MUST be determined solely by whether
    the outgoing PUBLISH packet is a retransmission"""
    _dup_reflects_retransmission_only()


def test_mqtt311_3_3_2_1_topic_name_leads_the_variable_header() -> None:
    """[MQTT-3.3.2-1] The Topic Name MUST be present as the first field in the PUBLISH
    Packet Variable header. It MUST be a UTF-8 encoded string"""
    _topic_name_leads_the_variable_header(V311)


def test_mqtt5_3_3_2_1_topic_name_leads_the_variable_header() -> None:
    """[MQTT-3.3.2-1] The Topic Name MUST be present as the first field in the PUBLISH
    packet Variable Header. It MUST be a UTF-8 Encoded String"""
    _topic_name_leads_the_variable_header(V5)


def test_mqtt311_3_3_4_1_receiver_answers_by_qos() -> None:
    """[MQTT-3.3.4-1] The receiver of a PUBLISH Packet MUST respond according to Table
    3.4 - Expected Publish Packet response as determined by the QoS in the PUBLISH
    Packet"""
    _receiver_answers_by_qos(V311)


def test_mqtt5_3_3_4_1_receiver_answers_by_qos() -> None:
    """[MQTT-3.3.4-1] The receiver of a PUBLISH Packet MUST respond with the packet as
    determined by the QoS in the PUBLISH Packet"""
    _receiver_answers_by_qos(V5)


def test_mqtt5_3_3_2_7_inbound_aliases_do_not_outlive_the_connection() -> None:
    """[MQTT-3.3.2-7] A receiver MUST NOT carry forward any Topic Alias mappings from
    one Network Connection to another"""
    _inbound_aliases_do_not_outlive_the_connection()


def test_mqtt5_3_3_2_10_every_alias_up_to_the_announced_maximum_is_accepted() -> None:
    """[MQTT-3.3.2-10] A Client MUST accept all Topic Alias values greater than 0 and
    less than or equal to the Topic Alias Maximum value that it sent in the CONNECT
    packet"""
    _every_alias_up_to_the_announced_maximum_is_accepted()


def test_mqtt5_3_3_2_13_response_topic_is_utf8() -> None:
    """[MQTT-3.3.2-13] The Response Topic MUST be a UTF-8 Encoded String"""
    _publish_string_properties_are_utf8()


def test_mqtt5_3_3_2_19_content_type_is_utf8() -> None:
    """[MQTT-3.3.2-19] The Content Type MUST be a UTF-8 Encoded String"""
    _publish_string_properties_are_utf8()


def test_mqtt5_3_3_4_7_receive_maximum_bounds_unacknowledged_publishes() -> None:
    """[MQTT-3.3.4-7] The Client MUST NOT send more than Receive Maximum QoS 1 and QoS
    2 PUBLISH packets for which it has not received PUBACK, PUBCOMP, or PUBREC with
    a Reason Code of 128 or greater from the Server"""
    _receive_maximum_bounds_unacknowledged_publishes()


def test_mqtt5_3_3_4_8_full_send_window_delays_only_qos_publishes() -> None:
    """[MQTT-3.3.4-8] The Client MUST NOT delay the sending of any packets other than
    PUBLISH packets due to having sent Receive Maximum PUBLISH packets without
    receiving acknowledgements for them"""
    _a_full_send_window_delays_only_qos_publishes()
