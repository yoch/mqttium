"""Executable conformance checks, each tied to a numbered MQTT statement.

Every test here names the normative statement it exercises and quotes it from
``docs/spec/mqtt-v*-statements.json``, which is extracted from reproducible
official OASIS archives. ``test_quoted_statements_match_the_vendored_specification``
then verifies those quotes are still accurate, so a docstring cannot drift away
from the text it claims to enforce.

This is a sample, not a proof of full conformance — see ``docs/conformance.md``
for what is and is not covered.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.properties import encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, OutboundQoSState, PacketType, QoS
from mqttium.errors import ProtocolError
from mqttium.packets import PublishPacket, encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import OutboundMessage, Properties

SPEC_DIR = Path(__file__).resolve().parents[2] / "docs" / "spec"


def statements(version: str) -> dict[str, str]:
    data = json.loads((SPEC_DIR / f"mqtt-v{version}-statements.json").read_text(encoding="utf-8"))
    return {item["id"]: item["text"] for item in data["statements"]}


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connected(protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv5) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(client_id="conformance", protocol=protocol), MemoryInflightStore()
    )
    engine.begin_connect()
    remaining = b"\x00\x00"
    if protocol is MQTTProtocolVersion.MQTTv5:
        remaining += b"\x00"
    _feed(engine, encode_frame(PacketType.CONNACK, 0, remaining))
    engine.take_effects()
    return engine


def _rejects(engine: ProtocolEngine, wire: bytes) -> bool:
    """True when the engine answers a malformed frame with a protocol error."""
    _feed(engine, wire)
    return any(e.kind is EffectKind.PROTOCOL_ERROR for e in engine.take_effects())


# --------------------------------------------------------------------- sending


def test_mqtt_3_3_4_6_outbound_publish_rejects_a_subscription_identifier() -> None:
    """[MQTT-3.3.4-6] A PUBLISH packet sent from a Client to a Server MUST NOT
    contain a Subscription Identifier.

    The property table cannot enforce this: the identifier is legal on the
    inbound PUBLISH the broker sends us, so the restriction is on the direction
    rather than on the packet type.
    """
    properties = Properties()
    properties.set("subscription_identifier", 7)

    for qos in (QoS.AT_MOST_ONCE, QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE):
        engine = _connected()
        with pytest.raises(ProtocolError, match="subscription_identifier"):
            engine.queue_publish("t/x", b"hi", qos=qos, properties=properties)
        assert engine.take_effects() == [], "nothing may reach the wire"
        assert engine.outbound.pending_messages == 0, "nothing may be queued"

    engine = _connected()
    with pytest.raises(ProtocolError, match="subscription_identifier"):
        engine.outbound.prepare_qos0("t/x", b"hi", properties=properties)


def test_inbound_publish_still_carries_a_subscription_identifier() -> None:
    """The mirror of [MQTT-3.3.4-6]: the broker→client direction is legal, and a
    subscription identifier must be delivered to the application untouched."""
    engine = _connected()
    properties = Properties()
    properties.set("subscription_identifier", 7)
    _feed(
        engine,
        PublishPacket(
            topic="in/t",
            payload=b"hello",
            qos=QoS.AT_MOST_ONCE,
            retain=False,
            dup=False,
            mid=None,
            properties=properties,
        ).encode(MQTTProtocolVersion.MQTTv5),
    )
    effects = engine.take_effects()
    messages = [
        effect.data
        for effect in effects
        if effect.kind in (EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE)
    ]
    assert len(messages) == 1
    assert messages[0].properties is not None
    assert messages[0].properties.get("subscription_identifier") == [7]


def test_mqtt_3_8_3_2_subscribe_requires_at_least_one_filter() -> None:
    """[MQTT-3.8.3-2] The Payload MUST contain at least one Topic Filter and
    Subscription Options pair."""
    engine = _connected()
    with pytest.raises(ProtocolError):
        engine.queue_subscribe([])
    assert engine.take_effects() == []


def test_mqtt_3_1_2_22_mqtt311_rejects_a_password_without_a_username() -> None:
    """[MQTT-3.1.2-22] If the User Name Flag is set to 0, the Password Flag MUST
    be set to 0.

    This is an MQTT 3.1.1 rule that MQTT 5 lifted, so the check is deliberately
    version-specific: the same configuration is legal over MQTT 5. Note that
    this label means something entirely different in the 5.0 document — 119
    labels appear in both and 112 of them disagree.
    """
    engine = ProtocolEngine(
        EngineConfig(
            client_id="c",
            protocol=MQTTProtocolVersion.MQTTv311,
            username=None,
            password=b"secret",
        ),
        MemoryInflightStore(),
    )
    with pytest.raises(ProtocolError, match="MQTT-3.1.2-22"):
        engine.begin_connect()


@pytest.mark.parametrize(
    ("username", "password", "expect_user_flag", "expect_password_flag"),
    [
        ("u", b"secret", True, True),
        ("u", None, True, False),
        (None, None, False, False),
    ],
)
def test_mqtt311_connect_flags_match_the_credentials_present(
    username: str | None,
    password: bytes | None,
    expect_user_flag: bool,
    expect_password_flag: bool,
) -> None:
    """The credential flags must describe the payload that follows them."""
    engine = ProtocolEngine(
        EngineConfig(
            client_id="c",
            protocol=MQTTProtocolVersion.MQTTv311,
            username=username,
            password=password,
        ),
        MemoryInflightStore(),
    )
    wire = engine.begin_connect()
    flags = wire[wire.index(b"MQTT") + 5]
    assert bool(flags & 0x80) is expect_user_flag
    assert bool(flags & 0x40) is expect_password_flag


def test_mqtt5_allows_a_password_without_a_username() -> None:
    """MQTT 5 removed the pairing rule; refusing it there would be wrong."""
    engine = ProtocolEngine(
        EngineConfig(
            client_id="c",
            protocol=MQTTProtocolVersion.MQTTv5,
            username=None,
            password=b"secret",
        ),
        MemoryInflightStore(),
    )
    wire = engine.begin_connect()
    flags = wire[wire.index(b"MQTT") + 5]
    assert flags & 0x40, "the password flag must be set"
    assert not flags & 0x80, "no username flag is expected"


def test_mqtt_3_3_2_2_publish_topic_must_not_contain_wildcards() -> None:
    """[MQTT-3.3.2-2] The Topic Name in the PUBLISH packet MUST NOT contain
    wildcard characters."""
    engine = _connected()
    for topic in ("a/+/b", "a/#", "+", "#"):
        with pytest.raises(ProtocolError):
            engine.queue_publish(topic, b"x", qos=QoS.AT_MOST_ONCE)
    assert engine.take_effects() == []


def test_mqtt_3_3_2_14_response_topic_must_not_contain_wildcards() -> None:
    """[MQTT-3.3.2-14] The Response Topic MUST NOT contain wildcard characters."""
    properties = Properties({"response_topic": "reply/#"})
    engine = _connected()
    with pytest.raises(ProtocolError, match="response_topic.*wildcards"):
        engine.queue_publish("request", b"outbound", properties=properties)
    assert engine.take_effects() == []

    inbound = PublishPacket(
        topic="request",
        payload=b"inbound",
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
        properties=Properties({"response_topic": "reply/x"}),
    ).encode(MQTTProtocolVersion.MQTTv5)
    inbound = inbound.replace(b"reply/x", b"reply/+")
    assert _rejects(engine, inbound)


def test_mqtt_3_1_2_21_server_keep_alive_replaces_the_requested_value() -> None:
    """[MQTT-3.1.2-21] If the Server returns a Server Keep Alive on the CONNACK
    packet, the Client MUST use that value instead of the value it sent as the
    Keep Alive."""
    engine = ProtocolEngine(
        EngineConfig(client_id="c", protocol=MQTTProtocolVersion.MQTTv5, keepalive=60),
        MemoryInflightStore(),
    )
    engine.begin_connect()
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x03\x13\x00\x1e"))
    engine.take_effects()
    assert engine.negotiated.server_keep_alive == 30
    assert engine.negotiated.effective_keepalive == 30


# ------------------------------------------------------------------- receiving


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
def test_mqtt_3_3_1_4_publish_must_not_have_both_qos_bits_set(
    protocol: MQTTProtocolVersion,
) -> None:
    """[MQTT-3.3.1-4] A PUBLISH Packet MUST NOT have both QoS bits set to 1."""
    engine = _connected(protocol)
    assert _rejects(engine, encode_frame(PacketType.PUBLISH, 0b0110, b"\x00\x01t\x00\x01\x00"))


def test_mqtt_3_3_1_2_qos0_publish_must_not_set_dup() -> None:
    """[MQTT-3.3.1-2] The DUP flag MUST be set to 0 for all QoS 0 messages."""
    engine = _connected()
    assert _rejects(engine, encode_frame(PacketType.PUBLISH, 0b1000, b"\x00\x01t\x00"))


def test_publish_above_qos0_must_carry_a_non_zero_packet_identifier() -> None:
    """The receiving mirror of [MQTT-2.2.1-3]/[MQTT-2.2.1-4]: an identifier of
    zero on a QoS > 0 PUBLISH is not a currently-unused identifier."""
    engine = _connected()
    assert _rejects(engine, encode_frame(PacketType.PUBLISH, 0b0010, b"\x00\x01t\x00\x00\x00"))


@pytest.mark.parametrize(
    ("name", "packet_type", "flags", "body"),
    [
        ("PUBACK", PacketType.PUBACK, 0b0010, b"\x00\x01\x00"),
        ("PUBREL", PacketType.PUBREL, 0b0000, b"\x00\x01\x00"),
        ("SUBACK", PacketType.SUBACK, 0b0100, b"\x00\x01\x00\x00"),
        ("PINGRESP", PacketType.PINGRESP, 0b0001, b""),
        ("DISCONNECT", PacketType.DISCONNECT, 0b0001, b"\x00\x00"),
    ],
)
def test_mqtt_2_1_3_1_reserved_fixed_header_flags_are_validated(
    name: str, packet_type: PacketType, flags: int, body: bytes
) -> None:
    """[MQTT-2.1.3-1] Where a flag bit is marked as "Reserved", it is reserved
    for future use and MUST be set to the value listed.

    DISCONNECT additionally has [MQTT-3.14.1-1], which names the reason code the
    receiver reports.
    """
    engine = _connected()
    assert _rejects(engine, encode_frame(packet_type, flags, body)), f"{name} accepted bad flags"


def _connack_wire(*, session_present: bool, protocol: MQTTProtocolVersion) -> bytes:
    remaining = bytes((1 if session_present else 0, 0))
    if protocol is MQTTProtocolVersion.MQTTv5:
        remaining += b"\x00"
    return encode_frame(PacketType.CONNACK, 0, remaining)


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
def test_session_present_after_clean_start_always_closes(
    protocol: MQTTProtocolVersion,
) -> None:
    """A successful clean connection can never report a previous session.

    MQTT 3.1.1 [MQTT-3.2.2-1] and MQTT 5 [MQTT-3.2.2-2] both require the
    Server to set Session Present to 0 in this case.
    """
    engine = ProtocolEngine(
        EngineConfig(client_id="c", protocol=protocol, clean_start=True), MemoryInflightStore()
    )
    engine.begin_connect()
    _feed(engine, _connack_wire(session_present=True, protocol=protocol))
    effects = engine.take_effects()

    assert engine.state is ConnectionState.DISCONNECTED
    disconnects = [e.data for e in effects if e.kind is EffectKind.DISCONNECTED]
    assert disconnects and disconnects[0].reason_code == 0x82
    if protocol is MQTTProtocolVersion.MQTTv5:
        assert any(e.kind is EffectKind.SEND for e in effects), "MQTT 5 announces the reason"


def test_clean_start_does_not_replay_stale_local_session_state() -> None:
    """Local records cannot override the clean session requested on the wire."""
    store = MemoryInflightStore()
    store.put_out(
        OutboundMessage(
            mid=1,
            topic="t",
            payload=b"x",
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBCOMP,
        )
    )
    engine = ProtocolEngine(
        EngineConfig(client_id="c", protocol=MQTTProtocolVersion.MQTTv5, clean_start=True), store
    )
    engine.begin_connect()
    _feed(engine, _connack_wire(session_present=True, protocol=MQTTProtocolVersion.MQTTv5))
    effects = engine.take_effects()
    assert engine.state is ConnectionState.DISCONNECTED
    assert store.get_out(1) is not None, "the rejected CONNACK must not mutate durable state"
    assert not any(e.kind is EffectKind.PUBLISH_COMPLETE for e in effects)


def test_session_present_zero_is_accepted_when_no_clean_start_was_requested() -> None:
    """A missing Server session remains valid when Clean Start was false."""
    engine = ProtocolEngine(
        EngineConfig(client_id="c", protocol=MQTTProtocolVersion.MQTTv5, clean_start=False),
        MemoryInflightStore(),
    )
    engine.begin_connect()
    _feed(engine, _connack_wire(session_present=False, protocol=MQTTProtocolVersion.MQTTv5))
    engine.take_effects()
    assert engine.state is ConnectionState.CONNECTED


def test_mqtt_3_3_2_8_topic_alias_zero_is_refused() -> None:
    """[MQTT-3.3.2-8] A sender MUST NOT send a PUBLISH packet containing a Topic
    Alias which has the value 0."""
    engine = _connected()
    properties = Properties()
    properties.set("topic_alias", 0)
    with pytest.raises(ProtocolError):
        engine.queue_publish("t/x", b"v", qos=QoS.AT_MOST_ONCE, properties=properties)


def _auth_connack(method: str = "M") -> bytes:
    properties = Properties({"authentication_method": method})
    body = bytearray((0x00, 0x00))
    body.extend(encode_properties(properties, "CONNACK"))
    return encode_frame(PacketType.CONNACK, 0, body)


def test_mqtt_3_15_1_1_auth_reserved_flags_are_validated() -> None:
    """[MQTT-3.15.1-1] Bits 3,2,1 and 0 of the Fixed Header of the AUTH packet
    are reserved and MUST all be set to 0."""
    properties = Properties()
    properties.set("authentication_method", "M")
    engine = ProtocolEngine(
        EngineConfig(
            client_id="c",
            protocol=MQTTProtocolVersion.MQTTv5,
            connect_properties=properties,
            accept_auth=True,
        ),
        MemoryInflightStore(),
    )
    engine.begin_connect()
    _feed(engine, _auth_connack())
    engine.take_effects()
    assert engine.state is ConnectionState.CONNECTED
    assert _rejects(engine, encode_frame(PacketType.AUTH, 0b0001, b"\x18\x00"))


def test_mqtt_4_6_0_2_pubacks_follow_the_order_the_publishes_arrived() -> None:
    """[MQTT-4.6.0-2] The Client MUST send PUBACK packets in the order in which
    the corresponding PUBLISH packets were received (QoS 1 messages)."""
    engine = _connected()
    arrival = (5, 3, 9, 1)
    for mid in arrival:
        _feed(
            engine,
            PublishPacket(
                topic="a/b",
                payload=b"v",
                qos=QoS.AT_LEAST_ONCE,
                retain=False,
                dup=False,
                mid=mid,
                properties=None,
            ).encode(MQTTProtocolVersion.MQTTv5),
        )
    sends = [e.data for e in engine.take_effects() if e.kind is EffectKind.SEND_PROTOCOL_RESPONSE]
    assert [int.from_bytes(frame[2:4], "big") for frame in sends] == list(arrival)


@pytest.mark.parametrize(
    ("connect_expiry", "disconnect_expiry", "allowed"),
    [
        (None, 300, False),
        (0, 300, False),
        (None, 0, True),
        (600, 300, True),
        (600, 0, True),
    ],
)
def test_mqtt5_disconnect_cannot_extend_a_session_that_was_never_durable(
    connect_expiry: int | None, disconnect_expiry: int, allowed: bool
) -> None:
    """MQTT 5 §3.14.2.2.2 — this one is prose rather than a numbered statement:

        If the Session Expiry Interval in the CONNECT packet was zero, then it
        is a Protocol Error to set a non-zero Session Expiry Interval in the
        DISCONNECT packet sent by the Client.

    A broker can treat the invalid DISCONNECT as ungraceful, which may publish a
    configured Will — the opposite of what a clean shutdown intends.
    An absent interval in CONNECT means zero (§3.1.2.11.2).
    """
    connect_properties = None
    if connect_expiry is not None:
        connect_properties = Properties()
        connect_properties.set("session_expiry_interval", connect_expiry)

    engine = ProtocolEngine(
        EngineConfig(
            client_id="c",
            protocol=MQTTProtocolVersion.MQTTv5,
            connect_properties=connect_properties,
        ),
        MemoryInflightStore(),
    )
    engine.begin_connect()
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    engine.take_effects()

    disconnect_properties = Properties()
    disconnect_properties.set("session_expiry_interval", disconnect_expiry)
    if allowed:
        assert engine.begin_disconnect(properties=disconnect_properties)
    else:
        with pytest.raises(ProtocolError, match="session_expiry_interval"):
            engine.begin_disconnect(properties=disconnect_properties)


@pytest.mark.parametrize(
    ("sent_expiry", "mutated_expiry", "allowed"),
    [(0, 600, False), (600, 0, True)],
)
def test_disconnect_uses_the_session_expiry_actually_sent_on_connect(
    sent_expiry: int, mutated_expiry: int, allowed: bool
) -> None:
    """Application mutation after CONNECT cannot rewrite wire history."""
    connect_properties = Properties({"session_expiry_interval": sent_expiry})
    engine = ProtocolEngine(
        EngineConfig(
            client_id="c",
            protocol=MQTTProtocolVersion.MQTTv5,
            connect_properties=connect_properties,
        ),
        MemoryInflightStore(),
    )
    engine.begin_connect()
    connect_properties.set("session_expiry_interval", mutated_expiry)
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    engine.take_effects()

    disconnect_properties = Properties({"session_expiry_interval": 300})
    if allowed:
        assert engine.begin_disconnect(properties=disconnect_properties)
    else:
        with pytest.raises(ProtocolError, match="session_expiry_interval"):
            engine.begin_disconnect(properties=disconnect_properties)


def test_mqtt_3_14_4_1_nothing_is_sent_after_disconnect() -> None:
    """[MQTT-3.14.4-1] MUST NOT send any more MQTT Control Packets on that
    Network Connection.

    A QoS 1 PUBLISH arriving after DISCONNECT would normally be acknowledged;
    it must not be.
    """
    engine = _connected()
    engine.begin_disconnect()
    engine.take_effects()

    _feed(
        engine,
        PublishPacket(
            topic="a/b",
            payload=b"v",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            dup=False,
            mid=3,
            properties=None,
        ).encode(MQTTProtocolVersion.MQTTv5),
    )
    assert not [e for e in engine.take_effects() if e.kind is EffectKind.SEND]


def test_mqtt_4_3_3_6_replay_sends_pubrel_not_publish() -> None:
    """[MQTT-4.3.3-6] MUST NOT re-send the PUBLISH once it has sent the
    corresponding PUBREL packet."""
    store = MemoryInflightStore()
    store.put_out(
        OutboundMessage(
            mid=4,
            topic="t",
            payload=b"x",
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBCOMP,
        )
    )
    engine = ProtocolEngine(
        EngineConfig(client_id="c", protocol=MQTTProtocolVersion.MQTTv5, clean_start=False), store
    )
    engine.begin_connect()
    _feed(engine, _connack_wire(session_present=True, protocol=MQTTProtocolVersion.MQTTv5))

    frames = [e.data for e in engine.take_effects() if e.kind is EffectKind.SEND]
    kinds = {(frame if isinstance(frame, bytes) else frame[0])[0] & 0xF0 for frame in frames}
    assert PacketType.PUBREL.value in kinds
    assert PacketType.PUBLISH.value not in kinds


def test_mqtt_4_9_0_3_a_full_send_quota_does_not_block_other_packets() -> None:
    """[MQTT-4.9.0-3] The Client and Server MUST continue to process and respond
    to all other MQTT Control Packets even if the quota is zero."""
    engine = ProtocolEngine(
        EngineConfig(client_id="c", protocol=MQTTProtocolVersion.MQTTv5, max_outbound_inflight=1),
        MemoryInflightStore(),
    )
    engine.begin_connect()
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    engine.take_effects()

    engine.queue_publish("t/a", b"x", qos=QoS.AT_LEAST_ONCE)
    engine.take_effects()
    engine.queue_publish("t/b", b"y", qos=QoS.AT_LEAST_ONCE)  # beyond the quota
    engine.take_effects()
    assert engine.flow.available == 0

    engine.queue_subscribe("s/x")
    assert [e for e in engine.take_effects() if e.kind is EffectKind.SEND], "SUBSCRIBE was delayed"
    engine.queue_ping()
    assert [e for e in engine.take_effects() if e.kind is EffectKind.SEND], "PINGREQ was delayed"


def test_mqtt_4_7_3_2_null_character_is_refused_in_topics_and_filters() -> None:
    """[MQTT-4.7.3-2] Topic Names and Topic Filters MUST NOT include the null
    character (Unicode U+0000)."""
    engine = _connected()
    with pytest.raises(ProtocolError):
        engine.queue_publish("a\x00b", b"x", qos=QoS.AT_MOST_ONCE)
    with pytest.raises(ProtocolError):
        engine.queue_subscribe("a\x00b")


def test_mqtt_4_7_3_3_topics_longer_than_65535_bytes_are_refused() -> None:
    """[MQTT-4.7.3-3] Topic Names and Topic Filters are UTF-8 Encoded Strings;
    they MUST NOT encode to more than 65,535 bytes."""
    engine = _connected()
    with pytest.raises(ProtocolError):
        engine.queue_publish("a" * 65_536, b"x", qos=QoS.AT_MOST_ONCE)


@pytest.mark.parametrize(
    ("topic_filter", "legal"),
    [
        ("$share/g/topic", True),
        ("$share/a/b/t", True),
        ("$share//topic", False),
        ("$share/a+b/t", False),
        ("$share/a#b/t", False),
    ],
)
def test_mqtt_4_8_2_2_share_name_rules(topic_filter: str, legal: bool) -> None:
    """[MQTT-4.8.2-2] The ShareName MUST NOT contain the characters "/", "+" or
    "#", but MUST be followed by a "/" character."""
    engine = _connected()
    if legal:
        engine.queue_subscribe(topic_filter)
    else:
        with pytest.raises(ProtocolError):
            engine.queue_subscribe(topic_filter)


def _auth_engine(*, connected: bool) -> ProtocolEngine:
    connect_properties = Properties()
    connect_properties.set("authentication_method", "M")
    engine = ProtocolEngine(
        EngineConfig(
            client_id="c",
            protocol=MQTTProtocolVersion.MQTTv5,
            connect_properties=connect_properties,
            accept_auth=True,
        ),
        MemoryInflightStore(),
    )
    engine.begin_connect()
    engine.take_effects()
    if connected:
        _feed(engine, _auth_connack())
        engine.take_effects()
        assert engine.state is ConnectionState.CONNECTED
    return engine


@pytest.mark.parametrize("reason_code", [0x00, 0x7F, 0x80])
def test_mqtt_3_15_2_1_client_cannot_send_a_server_only_auth_reason_code(
    reason_code: int,
) -> None:
    """[MQTT-3.15.2-1] The sender of the AUTH Packet MUST use one of the
    Authenticate Reason Codes.

    Table 3-11 assigns each code a sender: 0x00 (Success) is the Server's, and
    only 0x18 (Continue authentication) and 0x19 (Re-authenticate) may come from
    a Client. 0x00 is the interesting case — it is a valid AUTH reason code, just
    not one a Client may send.
    """
    engine = _auth_engine(connected=True)
    with pytest.raises(ProtocolError):
        engine.queue_auth(reason_code=reason_code)
    assert engine.take_effects() == []


def test_mqtt_4_12_0_3_client_answers_a_server_auth_with_continue() -> None:
    """[MQTT-4.12.0-3] The Client responds to an AUTH packet from the Server by
    sending a further AUTH packet. This packet MUST contain a Reason Code of
    0x18 (Continue authentication).

    Re-authentication (0x19) is the Client's other legal code but needs an
    established session, so it is refused mid-handshake.
    """
    engine = _auth_engine(connected=False)
    engine.queue_auth(reason_code=0x18)
    frames = [e.data for e in engine.take_effects() if e.kind is EffectKind.SEND]
    assert frames and frames[0][2] == 0x18

    engine = _auth_engine(connected=False)
    with pytest.raises(ProtocolError):
        engine.queue_auth(reason_code=0x19)


def test_reauthentication_uses_0x19_once_connected() -> None:
    """The mirror of [MQTT-4.12.0-3]: 0x19 is legal once the session exists."""
    engine = _auth_engine(connected=True)
    engine.queue_auth()  # default
    frames = [e.data for e in engine.take_effects() if e.kind is EffectKind.SEND]
    assert frames and frames[0][2] == 0x19


@pytest.mark.parametrize(
    ("protocol", "clean_start", "allowed"),
    [
        (MQTTProtocolVersion.MQTTv311, True, True),
        (MQTTProtocolVersion.MQTTv311, False, False),
        (MQTTProtocolVersion.MQTTv5, True, True),
        (MQTTProtocolVersion.MQTTv5, False, True),
    ],
)
def test_mqtt_3_1_3_7_empty_client_id_requires_a_clean_session(
    protocol: MQTTProtocolVersion, clean_start: bool, allowed: bool
) -> None:
    """[MQTT-3.1.3-7] If the Client supplies a zero-byte ClientId, the Client
    MUST also set CleanSession to 1.

    The broker is required to answer 0x02 (Identifier rejected) and close, so
    the connection cannot succeed — a durable session is keyed by the client
    identifier, which an empty one cannot provide. MQTT 5 allows the pairing and
    assigns an identifier instead, so the rule is version-specific.
    """
    engine = ProtocolEngine(
        EngineConfig(client_id="", protocol=protocol, clean_start=clean_start),
        MemoryInflightStore(),
    )
    if allowed:
        assert engine.begin_connect()
    else:
        with pytest.raises(ProtocolError, match="MQTT-3.1.3-7"):
            engine.begin_connect()
        assert engine.state is ConnectionState.NEW


def test_empty_client_id_is_caught_when_a_resumed_session_forces_clean_start() -> None:
    """The effective Clean Start is what counts: resuming a durable session
    rewrites it to 0, which is exactly when this would otherwise slip through."""
    engine = ProtocolEngine(
        EngineConfig(client_id="", protocol=MQTTProtocolVersion.MQTTv311, clean_start=True),
        MemoryInflightStore(),
    )
    engine._prefer_session_resume = True
    with pytest.raises(ProtocolError, match="MQTT-3.1.3-7"):
        engine.begin_connect()
    assert engine.state is ConnectionState.NEW


@pytest.mark.parametrize(
    ("field", "config"),
    [
        ("client_id", {"client_id": "a\x00b"}),
        ("username", {"client_id": "c", "username": "u\x00v"}),
        ("client_id surrogate", {"client_id": "a\ud800b"}),
    ],
)
def test_mqtt_1_5_3_2_connect_strings_reject_ill_formed_utf8(field: str, config: dict) -> None:
    """[MQTT-1.5.3-2] A UTF-8 encoded string MUST NOT include an encoding of the
    null character U+0000.

    [MQTT-1.5.3-1] additionally requires well-formed UTF-8, which is why the
    lone surrogate is here too.
    """
    engine = ProtocolEngine(
        EngineConfig(protocol=MQTTProtocolVersion.MQTTv311, **config), MemoryInflightStore()
    )
    with pytest.raises(ProtocolError):
        engine.begin_connect()


def test_mqtt_3_1_3_1_connect_payload_field_order() -> None:
    """[MQTT-3.1.3-1] These fields, if present, MUST appear in the order Client
    Identifier, Will Topic, Will Message, User Name, Password."""
    from mqttium.types import Message

    engine = ProtocolEngine(
        EngineConfig(
            client_id="CID",
            protocol=MQTTProtocolVersion.MQTTv311,
            username="USR",
            password=b"PWD",
            will=Message(topic="WT", payload=b"WM"),
        ),
        MemoryInflightStore(),
    )
    wire = engine.begin_connect()
    offsets = [wire.find(part) for part in (b"CID", b"WT", b"WM", b"USR", b"PWD")]
    assert -1 not in offsets
    assert offsets == sorted(offsets)


# ------------------------------------------------------- the quotes themselves


# A *quotation* opens a docstring: `"""[MQTT-x.y-n] <verbatim text>`. A label
# appearing anywhere else is a cross-reference and carries no text of its own.
_QUOTATION = re.compile(
    r'"""\[(MQTT-\d+(?:\.\d+)*-\d+)\]\s*(.+?)(?:\n\n|""")',
    re.DOTALL,
)
_ANY_LABEL = re.compile(r"\[(MQTT-\d+(?:\.\d+)*-\d+)\]")


def _normalise(text: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", text.lower()))


def test_quoted_statements_match_the_vendored_specification() -> None:
    """Every ``[MQTT-…]`` quote in this module must match the extracted text.

    Without this, a docstring could confidently cite a statement it does not
    actually reproduce — which is exactly how the wrong clause was cited for the
    inbound packet-id collision fix.
    """
    source = Path(__file__).read_text(encoding="utf-8")
    # 119 labels exist in both specifications and 112 of them say different
    # things, so a bare label is only meaningful per version. A quote is
    # accepted when it matches the statement in either vendored document.
    by_version = {version: statements(version) for version in ("5.0", "3.1.1")}

    for label in set(_ANY_LABEL.findall(source)):
        if not any(label in known for known in by_version.values()):
            pytest.fail(f"{label} is not a statement in either vendored specification")

    checked = 0
    for label, quoted in _QUOTATION.findall(source):
        quoted_words = _normalise(quoted)
        if not quoted_words:
            continue
        candidates = {
            version: known[label] for version, known in by_version.items() if label in known
        }
        matched = False
        for text in candidates.values():
            actual = _normalise(text)
            # The docstring may stop early, but what it does quote must be exact.
            prefix = " ".join(quoted_words.split()[: len(actual.split())])
            if actual.startswith(prefix) or prefix in actual:
                matched = True
                break
        if not matched:
            rendered = "\n".join(f"  MQTT {v}: {t[:170]}" for v, t in candidates.items())
            pytest.fail(f"{label} is misquoted\n  docstring: {quoted.strip()[:170]}\n{rendered}")
        checked += 1

    # A floor, not a coverage target: it exists so a broken _QUOTATION regex
    # cannot make this test pass by matching nothing. Adding tests need not
    # move it.
    assert checked >= 15, f"the quotation regex matched only {checked} docstrings"


@pytest.mark.parametrize("version", ["3.1.1", "5.0"])
def test_vendored_statement_index_is_well_formed(version: str) -> None:
    """The index is generated; a malformed or truncated one would silently
    weaken every check above that reads from it."""
    data = json.loads((SPEC_DIR / f"mqtt-v{version}-statements.json").read_text(encoding="utf-8"))

    assert data["mqtt_version"] == version
    expected = {
        "3.1.1": {
            "count": 139,
            "archive": "7c3932da425b6a20ca1732a18b358f17944c0bad6305e923de4735aa5508516e",
            "html": "df463403cdbe7e399e5247a467a7bf1a6795aed4338158eed2f638c0265651f5",
            "url_suffix": "errata01/os/mqtt-v3.1.1-errata01-os-complete.html",
        },
        "5.0": {
            "count": 251,
            "archive": "948793b3db21fb198345f11d49434de5300655b132d479561ffa9fe950a5931e",
            "html": "4326d27913c2ad12030aaff1ab50a86a190917c2f7e44af73300197bb7635c67",
            "url_suffix": "v5.0/os/mqtt-v5.0-os.html",
        },
    }[version]
    source = data["source"]
    assert source["url"].startswith("https://docs.oasis-open.org/mqtt/")
    assert source["url"].endswith(expected["url_suffix"])
    assert source["archive_url"].endswith("-os.zip")
    assert source["archive_member"].endswith(".html")
    assert data["archive_sha256"] == expected["archive"]
    assert data["source_sha256"] == expected["html"]
    assert re.fullmatch(r"[0-9a-f]{64}", data["source_sha256"])
    assert data["source_encoding"] == "cp1252"

    items = data["statements"]
    assert data["statement_count"] == expected["count"]
    assert data["statement_count"] == len(items)
    assert len({item["id"] for item in items}) == len(items), "duplicate statement ids"
    for item in items:
        assert re.fullmatch(r"MQTT-\d+(\.\d+)*-\d+", item["id"])
        assert len(item["text"].strip()) >= 20, f"{item['id']} looks truncated"
        assert item["section"] == item["id"].removeprefix("MQTT-").rsplit("-", 1)[0]
        assert item["origin"] in {"body", "appendix"}
        assert not any(ord(char) < 32 for char in item["section"])
        assert "\x01" not in item["text"] and "�" not in item["text"], (
            f"{item['id']} carries an extraction sentinel or a decoding failure"
        )

    by_id = {item["id"]: item for item in items}
    assert by_id["MQTT-3.1.2-11"]["text"] != "(0x00)"
    if version == "5.0":
        assert by_id["MQTT-4.2-1"]["appendix_id"] == "MQTT-4.2.0-1"
        assert "MQTT-4.2.0-1" not in by_id
