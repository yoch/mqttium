"""Deterministic tests demonstrating normative MQTT specification gaps.

Researched by Agent C (Parallel Adversarial Research Campaign on MQTTium).
Audited SHA: 78c8d4caddacf80d77382a67651174a6a9c8a6f5.
"""

from __future__ import annotations

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


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def test_gap_1_resumed_session_replays_pubrel_when_send_quota_exhausted() -> None:
    """[MQTT-4.9.0-2] / [MQTT-4.9.0-3] / [MQTT-4.4.0-1]: Send quota limits only
    QoS > 0 PUBLISH packets; PUBREL must not be withheld when quota is 0.

    When reconnecting with a present session, unacknowledged PUBREL packets
    must be re-sent regardless of the Receive Maximum quota.
    """
    store = MemoryInflightStore()
    store.put_out(
        OutboundMessage(
            mid=1,
            topic="t/1",
            payload=b"msg1",
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBREC,
        )
    )
    store.put_out(
        OutboundMessage(
            mid=2,
            topic="t/2",
            payload=b"msg2",
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBCOMP,
        )
    )

    engine = ProtocolEngine(
        EngineConfig(client_id="c", protocol=MQTTProtocolVersion.MQTTv5, clean_start=False),
        store,
    )
    engine.begin_connect()

    connack_props = Properties({"receive_maximum": 1})
    body = bytearray((0x01, 0x00))  # Session Present = 1, Success = 0
    body.extend(encode_properties(connack_props, "CONNACK"))

    _feed(engine, encode_frame(PacketType.CONNACK, 0, bytes(body)))
    sends = [e.data for e in engine.take_effects() if e.kind is EffectKind.SEND]
    packet_types = [((f if isinstance(f, bytes) else f[0])[0] >> 4) for f in sends]

    # In the unfixed codebase, PUBREL (MID 2) is blocked in _queued because
    # _replay_message checks self.flow.try_acquire() for WAIT_PUBCOMP.
    # A conformant client sends PUBREL even when quota is 0.
    assert PacketType.PUBREL.value in packet_types, "PUBREL was delayed by Receive Maximum quota"


def test_gap_2_inbound_topic_alias_accepted_when_configured_via_connect_properties() -> None:
    """[MQTT-3.3.2-10] A Client MUST accept all Topic Alias values greater than 0
    and less than or equal to the Topic Alias Maximum value that it sent in the
    CONNECT packet.
    """
    connect_props = Properties({"topic_alias_maximum": 5})
    engine = ProtocolEngine(
        EngineConfig(
            client_id="test",
            protocol=MQTTProtocolVersion.MQTTv5,
            connect_properties=connect_props,
        ),
        MemoryInflightStore(),
    )
    engine.begin_connect()
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    engine.take_effects()
    assert engine.state is ConnectionState.CONNECTED

    # Server sends PUBLISH with valid topic_alias=1 (<= 5)
    pub_props = Properties({"topic_alias": 1})
    pub = PublishPacket(
        topic="sensors/temp",
        payload=b"21.5",
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
        properties=pub_props,
    ).encode(MQTTProtocolVersion.MQTTv5)

    _feed(engine, pub)
    effects = engine.take_effects()

    assert not any(e.kind is EffectKind.PROTOCOL_ERROR for e in effects)
    assert not any(e.kind is EffectKind.DISCONNECTED for e in effects)
    messages = [
        e.data for e in effects if e.kind in (EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE)
    ]
    assert len(messages) == 1
    assert messages[0].topic == "sensors/temp"


def test_gap_3_disconnect_rejects_session_expiry_after_server_sets_zero_in_connack() -> None:
    """MQTT 5 §3.14.2.2.2: If the Session Expiry Interval was non-zero in the
    CONNECT packet and the Session Expiry Interval of the Session is zero at the
    time the DISCONNECT packet is sent, it is a Protocol Error to set a non-zero
    Session Expiry Interval in the DISCONNECT packet.
    """
    connect_props = Properties({"session_expiry_interval": 600})
    engine = ProtocolEngine(
        EngineConfig(
            client_id="test",
            protocol=MQTTProtocolVersion.MQTTv5,
            connect_properties=connect_props,
        ),
        MemoryInflightStore(),
    )
    engine.begin_connect()

    # Server sets Session Expiry Interval to 0 in CONNACK
    connack_props = Properties({"session_expiry_interval": 0})
    body = bytearray((0x00, 0x00))
    body.extend(encode_properties(connack_props, "CONNACK"))
    _feed(engine, encode_frame(PacketType.CONNACK, 0, bytes(body)))
    engine.take_effects()
    assert engine.negotiated.session_expiry_interval == 0

    # DISCONNECT must not be allowed to set non-zero session_expiry_interval
    disconnect_props = Properties({"session_expiry_interval": 300})
    with pytest.raises(ProtocolError, match="session_expiry_interval"):
        engine.begin_disconnect(properties=disconnect_props)


def test_gap_4_mqtt311_server_disconnect_is_protocol_error() -> None:
    """[MQTT-4.8.0-1] / MQTT 3.1.1 §2.1 Table 2.1 & §3.14: In MQTT 3.1.1,
    DISCONNECT is Client->Server only. A Server sending DISCONNECT is a
    protocol violation.
    """
    engine = ProtocolEngine(
        EngineConfig(client_id="c311", protocol=MQTTProtocolVersion.MQTTv311),
        MemoryInflightStore(),
    )
    engine.begin_connect()
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
    engine.take_effects()
    assert engine.state is ConnectionState.CONNECTED

    _feed(engine, encode_frame(PacketType.DISCONNECT, 0, b""))
    effects = engine.take_effects()

    assert any(e.kind is EffectKind.PROTOCOL_ERROR for e in effects)


def test_gap_5_mqtt5_publish_empty_topic_without_alias_emits_disconnect_0x82() -> None:
    """MQTT 5 §3.3.2.3.5: A PUBLISH packet with a Topic Alias that is not set
    and a zero length Topic Name is a Protocol Error. The Client or Server uses
    DISCONNECT with Reason Code 0x82 (Protocol Error).
    """
    engine = ProtocolEngine(
        EngineConfig(client_id="c5", protocol=MQTTProtocolVersion.MQTTv5),
        MemoryInflightStore(),
    )
    engine.begin_connect()
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    engine.take_effects()
    assert engine.state is ConnectionState.CONNECTED

    pub = PublishPacket(
        topic="",
        payload=b"test",
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
        properties=Properties(),
    ).encode(MQTTProtocolVersion.MQTTv5)

    _feed(engine, pub)
    effects = engine.take_effects()

    # Must emit DISCONNECT frame with reason code 0x82 (Protocol Error)
    sends = [e.data for e in effects if e.kind is EffectKind.SEND]
    assert len(sends) == 1, "Expected DISCONNECT frame to be sent to broker"
    frame = sends[0] if isinstance(sends[0], bytes) else sends[0][0]
    assert (frame[0] >> 4) == PacketType.DISCONNECT.value
    assert frame[2] == 0x82  # Protocol Error reason code
