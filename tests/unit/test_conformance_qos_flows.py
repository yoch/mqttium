"""Executable conformance checks for packet identifiers and the QoS flows.

Same convention as ``test_conformance_statements.py``: every test names the
numbered statement it exercises and quotes it from the vendored index, and
``tools/build_conformance_matrix.py`` counts a statement as verified only when
that quotation matches the text of the specification version under test.

Each test drives the synchronous protocol engine directly, as a broker would
see it: commands and packets in, wire frames and effects out. The docstrings
are the statements; the specifications' own wording is kept, including where a
statement is a fragment of a bulleted list.
"""

from __future__ import annotations

import pytest

from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import ProtocolError
from mqttium.packets import (
    PubAckPacket,
    PubCompPacket,
    PublishPacket,
    PubRecPacket,
    PubRelPacket,
    encode_frame,
)
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.protocol.engine import ProtocolEngine
from tests.support import mark_delivered_messages

V311 = MQTTProtocolVersion.MQTTv311
V5 = MQTTProtocolVersion.MQTTv5
_DUP = 0x08


def _feed(engine: ProtocolEngine, wire: bytes) -> list[EngineEffect]:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)
    return mark_delivered_messages(engine, engine.take_effects())


def _connack(protocol: MQTTProtocolVersion, *, session_present: bool = False) -> bytes:
    body = bytes((int(session_present), 0)) + (b"\x00" if protocol is V5 else b"")
    return encode_frame(PacketType.CONNACK, 0, body)


def _connected(
    protocol: MQTTProtocolVersion, store: MemoryInflightStore | None = None, **options: object
) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(client_id="flows", protocol=protocol, **options),  # type: ignore[arg-type]
        store if store is not None else MemoryInflightStore(),
    )
    engine.begin_connect()
    _feed(engine, _connack(protocol, session_present=bool(options.get("session_present"))))
    return engine


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


def _mid(packet: RawPacket) -> int:
    """The Packet Identifier of an acknowledgement or request packet."""
    return int.from_bytes(packet.remaining[:2], "big")


def _delivered(effects: list[EngineEffect]) -> int:
    return sum(effect.kind is EffectKind.MESSAGE for effect in effects)


def _sent_publish(engine: ProtocolEngine, qos: int) -> tuple[int | None, RawPacket, PublishPacket]:
    handle = engine.queue_publish("out/topic", b"payload", qos=qos)
    (raw,) = _wire(engine.take_effects())
    assert raw.packet_type is PacketType.PUBLISH
    return handle.mid, raw, PublishPacket.decode(raw.flags, raw.remaining, engine.config.protocol)


def _inbound(protocol: MQTTProtocolVersion, qos: int, mid: int, *, dup: bool = False) -> bytes:
    return PublishPacket(
        topic="in/topic", payload=b"payload", qos=QoS(qos), retain=False, dup=dup, mid=mid
    ).encode(protocol)


# The bodies below are shared by the two specification versions: the statements
# differ in wording and numbering, the required behaviour does not.


def _qos0_publish_has_no_identifier_and_no_dup(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)
    mid, raw, publish = _sent_publish(engine, 0)
    assert mid is None and publish.mid is None
    assert publish.qos is QoS.AT_MOST_ONCE
    assert not raw.flags & _DUP
    # Topic, then (MQTT 5) an empty property block, then the payload: no room
    # is left for a Packet Identifier.
    properties = 1 if protocol is V5 else 0
    assert len(raw.remaining) == 2 + len("out/topic") + properties + len(b"payload")


def _new_packets_get_unused_non_zero_identifiers(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)
    in_use = []
    for qos in (1, 2, 1, 2):
        mid, _raw, publish = _sent_publish(engine, qos)
        assert mid == publish.mid
        in_use.append(mid)
    subscribe = engine.queue_subscription_request(engine.prepare_subscribe("a/b", qos=1))
    unsubscribe = engine.queue_subscription_request(engine.prepare_unsubscribe("a/b"))
    requests = _wire(engine.take_effects())
    assert [packet.packet_type for packet in requests] == [
        PacketType.SUBSCRIBE,
        PacketType.UNSUBSCRIBE,
    ]
    assert [_mid(packet) for packet in requests] == [subscribe, unsubscribe]
    in_use += [subscribe, unsubscribe]
    assert all(isinstance(mid, int) and 0 < mid <= 0xFFFF for mid in in_use)
    assert len(set(in_use)) == len(in_use)


def _qos1_sender(protocol: MQTTProtocolVersion) -> None:
    store = MemoryInflightStore()
    engine = _connected(protocol, store)
    first, raw, publish = _sent_publish(engine, 1)
    second, _raw2, _publish2 = _sent_publish(engine, 1)
    assert first and second and first != second  # an unused identifier each time
    assert publish.qos is QoS.AT_LEAST_ONCE and publish.mid == first
    assert not raw.flags & _DUP
    # Unacknowledged until the PUBACK: the publication is still held.
    assert store.get_out(first) is not None
    effects = _feed(engine, PubAckPacket(mid=first).encode(protocol))
    assert any(effect.kind is EffectKind.PUBLISH_COMPLETE for effect in effects)
    assert store.get_out(first) is None
    assert store.get_out(second) is not None


def _qos1_receiver(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)
    effects = _feed(engine, _inbound(protocol, 1, 21))
    (ack,) = _wire(effects)
    assert ack.packet_type is PacketType.PUBACK and _mid(ack) == 21
    assert _delivered(effects) == 1
    # After the PUBACK the same identifier names a new Application Message,
    # whatever its DUP flag says.
    again = _feed(engine, _inbound(protocol, 1, 21, dup=True))
    (second_ack,) = _wire(again)
    assert second_ack.packet_type is PacketType.PUBACK and _mid(second_ack) == 21
    assert _delivered(again) == 1


def _qos2_sender(protocol: MQTTProtocolVersion) -> None:
    store = MemoryInflightStore()
    engine = _connected(protocol, store)
    first, raw, publish = _sent_publish(engine, 2)
    second, _raw2, _publish2 = _sent_publish(engine, 2)
    assert first and second and first != second
    assert publish.qos is QoS.EXACTLY_ONCE and publish.mid == first
    assert not raw.flags & _DUP
    assert store.get_out(first) is not None  # unacknowledged until PUBREC
    (pubrel,) = _wire(_feed(engine, PubRecPacket(mid=first).encode(protocol)))
    assert pubrel.packet_type is PacketType.PUBREL and _mid(pubrel) == first
    assert store.get_out(first) is not None  # the PUBREL is unacknowledged until PUBCOMP
    # Once the PUBREL is sent the PUBLISH is never sent again: a late or
    # repeated PUBREC is answered with PUBREL only.
    (again,) = _wire(_feed(engine, PubRecPacket(mid=first).encode(protocol)))
    assert again.packet_type is PacketType.PUBREL and _mid(again) == first
    effects = _feed(engine, PubCompPacket(mid=first).encode(protocol))
    assert any(effect.kind is EffectKind.PUBLISH_COMPLETE for effect in effects)
    assert store.get_out(first) is None


def _qos2_receiver(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)
    effects = _feed(engine, _inbound(protocol, 2, 31))
    (pubrec,) = _wire(effects)
    assert pubrec.packet_type is PacketType.PUBREC and _mid(pubrec) == 31
    assert _delivered(effects) == 1
    # Until the PUBREL arrives, a repeated PUBLISH is acknowledged again and
    # delivered to nobody a second time.
    repeated = _feed(engine, _inbound(protocol, 2, 31, dup=True))
    (pubrec_again,) = _wire(repeated)
    assert pubrec_again.packet_type is PacketType.PUBREC and _mid(pubrec_again) == 31
    assert _delivered(repeated) == 0
    (pubcomp,) = _wire(_feed(engine, PubRelPacket(mid=31).encode(protocol)))
    assert pubcomp.packet_type is PacketType.PUBCOMP and _mid(pubcomp) == 31
    # After the PUBCOMP the identifier names a new Application Message.
    fresh = _feed(engine, _inbound(protocol, 2, 31))
    assert _wire(fresh)[0].packet_type is PacketType.PUBREC
    assert _delivered(fresh) == 1


def _acknowledgements_carry_the_publish_identifier(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)
    (puback,) = _wire(_feed(engine, _inbound(protocol, 1, 41)))
    (pubrec,) = _wire(_feed(engine, _inbound(protocol, 2, 42)))
    (pubcomp,) = _wire(_feed(engine, PubRelPacket(mid=42).encode(protocol)))
    sent, _raw, _publish = _sent_publish(engine, 2)
    assert sent is not None
    (pubrel,) = _wire(_feed(engine, PubRecPacket(mid=sent).encode(protocol)))
    assert [(packet.packet_type, _mid(packet)) for packet in (puback, pubrec, pubcomp, pubrel)] == [
        (PacketType.PUBACK, 41),
        (PacketType.PUBREC, 42),
        (PacketType.PUBCOMP, 42),
        (PacketType.PUBREL, sent),
    ]


def _resumed_session_resends_what_was_unacknowledged(protocol: MQTTProtocolVersion) -> None:
    store = MemoryInflightStore()
    engine = _connected(protocol, store, clean_start=False)
    qos1, first_qos1, _ = _sent_publish(engine, 1)
    qos2, first_qos2, _ = _sent_publish(engine, 2)
    released, _raw, _ = _sent_publish(engine, 2)
    assert qos1 and qos2 and released
    assert not first_qos1.flags & _DUP and not first_qos2.flags & _DUP
    _feed(engine, PubRecPacket(mid=released).encode(protocol))  # now awaiting PUBCOMP

    engine.notify_transport_closed()
    engine.take_effects()
    engine.begin_connect()
    replayed = _wire(_feed(engine, _connack(protocol, session_present=True)))

    assert [packet.packet_type for packet in replayed] == [
        PacketType.PUBLISH,
        PacketType.PUBLISH,
        PacketType.PUBREL,
    ]
    publishes = [
        PublishPacket.decode(packet.flags, packet.remaining, protocol) for packet in replayed[:2]
    ]
    # The original Packet Identifiers, and DUP set on each re-delivered PUBLISH.
    assert [publish.mid for publish in publishes] == [qos1, qos2]
    assert all(packet.flags & _DUP for packet in replayed[:2])
    assert _mid(replayed[2]) == released
    # The identifier is released only once its acknowledgement is processed.
    assert store.get_out(qos1) is not None
    _feed(engine, PubAckPacket(mid=qos1).encode(protocol))
    assert store.get_out(qos1) is None


def _negatively_acknowledged_publish_is_not_retransmitted() -> None:
    store = MemoryInflightStore()
    engine = _connected(V5, store, clean_start=False)
    qos1, _raw1, _ = _sent_publish(engine, 1)
    qos2, _raw2, _ = _sent_publish(engine, 2)
    assert qos1 and qos2
    refused = _feed(engine, PubAckPacket(mid=qos1, reason_code=0x80).encode(V5))
    refused += _feed(engine, PubRecPacket(mid=qos2, reason_code=0x80).encode(V5))
    assert [effect.kind for effect in refused] == [EffectKind.PUBLISH_FAILED] * 2
    assert _wire(refused) == []  # no PUBREL for a refused PUBREC
    assert store.get_out(qos1) is None and store.get_out(qos2) is None

    engine.notify_transport_closed()
    engine.take_effects()
    engine.begin_connect()
    assert _wire(_feed(engine, _connack(V5, session_present=True))) == []


def _subscribe_needs_a_filter(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)
    with pytest.raises(ProtocolError):
        engine.queue_subscribe([])
    assert _wire(engine.take_effects()) == []


def _unsubscribe_needs_a_filter(protocol: MQTTProtocolVersion) -> None:
    engine = _connected(protocol)
    with pytest.raises(ProtocolError):
        engine.prepare_unsubscribe([])
    assert _wire(engine.take_effects()) == []


def test_mqtt5_2_2_1_2_qos0_publish_carries_no_packet_identifier() -> None:
    """[MQTT-2.2.1-2] A PUBLISH packet MUST NOT contain a Packet Identifier if its QoS
    value is set to 0"""
    _qos0_publish_has_no_identifier_and_no_dup(V5)


def test_mqtt311_2_3_1_5_qos0_publish_carries_no_packet_identifier() -> None:
    """[MQTT-2.3.1-5] A PUBLISH Packet MUST NOT contain a Packet Identifier if its QoS
    value is set to 0"""
    _qos0_publish_has_no_identifier_and_no_dup(V311)


def test_mqtt5_4_3_1_1_qos0_publish_is_sent_with_dup_clear() -> None:
    """[MQTT-4.3.1-1] MUST send a PUBLISH packet with QoS 0 and DUP flag set to 0"""
    _qos0_publish_has_no_identifier_and_no_dup(V5)


def test_mqtt311_4_3_1_1_qos0_publish_is_sent_with_dup_clear() -> None:
    """[MQTT-4.3.1-1] MUST send a PUBLISH packet with QoS=0, DUP=0"""
    _qos0_publish_has_no_identifier_and_no_dup(V311)


def test_mqtt5_2_2_1_3_new_packets_are_assigned_an_unused_identifier() -> None:
    """[MQTT-2.2.1-3] Each time a Client sends a new SUBSCRIBE, UNSUBSCRIBE,or PUBLISH
    (where QoS > 0) MQTT Control Packet it MUST assign it a non-zero Packet
    Identifier that is currently unused"""
    _new_packets_get_unused_non_zero_identifiers(V5)


def test_mqtt311_2_3_1_1_identified_packets_carry_a_non_zero_identifier() -> None:
    """[MQTT-2.3.1-1] SUBSCRIBE, UNSUBSCRIBE, and PUBLISH (in cases where QoS > 0)
    Control Packets MUST contain a non-zero 16-bit Packet Identifier"""
    _new_packets_get_unused_non_zero_identifiers(V311)


def test_mqtt311_2_3_1_2_new_packets_are_assigned_an_unused_identifier() -> None:
    """[MQTT-2.3.1-2] Each time a Client sends a new packet of one of these types it
    MUST assign it a currently unused Packet Identifier"""
    _new_packets_get_unused_non_zero_identifiers(V311)


def test_mqtt5_4_3_2_1_qos1_sender_assigns_an_unused_identifier() -> None:
    """[MQTT-4.3.2-1] MUST assign an unused Packet Identifier each time it has a new
    Application Message to publish"""
    _qos1_sender(V5)


def test_mqtt5_4_3_2_2_qos1_sender_sends_qos1_with_dup_clear() -> None:
    """[MQTT-4.3.2-2] MUST send a PUBLISH packet containing this Packet Identifier with
    QoS 1 and DUP flag set to 0"""
    _qos1_sender(V5)


def test_mqtt5_4_3_2_3_qos1_publish_is_unacknowledged_until_puback() -> None:
    """[MQTT-4.3.2-3] MUST treat the PUBLISH packet as “unacknowledged” until it has
    received the corresponding PUBACK packet from the receiver"""
    _qos1_sender(V5)


def test_mqtt311_4_3_2_1_qos1_sender_protocol() -> None:
    """[MQTT-4.3.2-1] In the QoS 1 delivery protocol, the Sender · MUST assign an
    unused Packet Identifier each time it has a new Application Message to publish.
    · MUST send a PUBLISH Packet containing this Packet Identifier with QoS=1,
    DUP=0. · MUST treat the PUBLISH Packet as “unacknowledged” until it has received
    the corresponding PUBACK packet from the receiver. See Section 4.4 for a
    discussion of unacknowledged messages."""
    _qos1_sender(V311)


def test_mqtt5_4_3_2_4_qos1_receiver_answers_with_puback() -> None:
    """[MQTT-4.3.2-4] In the QoS 1 delivery protocol, the receiver MUST respond with a
    PUBACK packet containing the Packet Identifier from the incoming PUBLISH packet,
    having accepted ownership of the Application Message"""
    _qos1_receiver(V5)


def test_mqtt5_4_3_2_5_qos1_identifier_is_new_after_puback() -> None:
    """[MQTT-4.3.2-5] After it has sent a PUBACK packet the receiver MUST treat any
    incoming PUBLISH packet that contains the same Packet Identifier as being a new
    Application Message, irrespective of the setting of its DUP flag"""
    _qos1_receiver(V5)


def test_mqtt311_4_3_2_2_qos1_receiver_protocol() -> None:
    """[MQTT-4.3.2-2] In the QoS 1 delivery protocol, the Receiver MUST respond with a
    PUBACK Packet containing the Packet Identifier from the incoming PUBLISH Packet,
    having accepted ownership of the Application Message After it has sent a PUBACK
    Packet the Receiver MUST treat any incoming PUBLISH packet that contains the
    same Packet Identifier as being a new publication, irrespective of the setting
    of its DUP flag."""
    _qos1_receiver(V311)


def test_mqtt5_4_3_3_1_qos2_sender_assigns_an_unused_identifier() -> None:
    """[MQTT-4.3.3-1] MUST assign an unused Packet Identifier when it has a new
    Application Message to publish"""
    _qos2_sender(V5)


def test_mqtt5_4_3_3_2_qos2_sender_sends_qos2_with_dup_clear() -> None:
    """[MQTT-4.3.3-2] MUST send a PUBLISH packet containing this Packet Identifier with
    QoS 2 and DUP flag set to 0"""
    _qos2_sender(V5)


def test_mqtt5_4_3_3_3_qos2_publish_is_unacknowledged_until_pubrec() -> None:
    """[MQTT-4.3.3-3] MUST treat the PUBLISH packet as “unacknowledged” until it has
    received the corresponding PUBREC packet from the receiver"""
    _qos2_sender(V5)


def test_mqtt5_4_3_3_4_qos2_sender_answers_pubrec_with_pubrel() -> None:
    """[MQTT-4.3.3-4] MUST send a PUBREL packet when it receives a PUBREC packet from
    the receiver with a Reason Code value less than 0x80. This PUBREL packet MUST
    contain the same Packet Identifier as the original PUBLISH packet"""
    _qos2_sender(V5)


def test_mqtt5_4_3_3_5_qos2_pubrel_is_unacknowledged_until_pubcomp() -> None:
    """[MQTT-4.3.3-5] MUST treat the PUBREL packet as “unacknowledged” until it has
    received the corresponding PUBCOMP packet from the receiver"""
    _qos2_sender(V5)


def test_mqtt311_4_3_3_1_qos2_sender_protocol() -> None:
    """[MQTT-4.3.3-1] In the QoS 2 delivery protocol, the Sender MUST assign an unused
    Packet Identifier when it has a new Application Message to publish. MUST send a
    PUBLISH packet containing this Packet Identifier with QoS=2, DUP=0. MUST treat
    the PUBLISH packet as “unacknowledged” until it has received the corresponding
    PUBREC packet from the receiver. See Section 4.4 for a discussion of
    unacknowledged messages. MUST send a PUBREL packet when it receives a PUBREC
    packet from the receiver. This PUBREL packet MUST contain the same Packet
    Identifier as the original PUBLISH packet. MUST treat the PUBREL packet as
    “unacknowledged” until it has received the corresponding PUBCOMP packet from the
    receiver. MUST NOT re-send the PUBLISH once it has sent the corresponding PUBREL
    packet."""
    _qos2_sender(V311)


def test_mqtt5_4_3_3_8_qos2_receiver_answers_with_pubrec() -> None:
    """[MQTT-4.3.3-8] MUST respond with a PUBREC containing the Packet Identifier from
    the incoming PUBLISH packet, having accepted ownership of the Application
    Message"""
    _qos2_receiver(V5)


def test_mqtt5_4_3_3_10_qos2_repeated_publish_is_not_delivered_twice() -> None:
    """[MQTT-4.3.3-10] Until it has received the corresponding PUBREL packet, the
    receiver MUST acknowledge any subsequent PUBLISH packet with the same Packet
    Identifier by sending a PUBREC. It MUST NOT cause duplicate messages to be
    delivered to any onward recipients in this case"""
    _qos2_receiver(V5)


def test_mqtt5_4_3_3_11_qos2_receiver_answers_pubrel_with_pubcomp() -> None:
    """[MQTT-4.3.3-11] MUST respond to a PUBREL packet by sending a PUBCOMP packet
    containing the same Packet Identifier as the PUBREL"""
    _qos2_receiver(V5)


def test_mqtt5_4_3_3_12_qos2_identifier_is_new_after_pubcomp() -> None:
    """[MQTT-4.3.3-12] After it has sent a PUBCOMP, the receiver MUST treat any
    subsequent PUBLISH packet that contains that Packet Identifier as being a new
    Application Message"""
    _qos2_receiver(V5)


def test_mqtt311_4_3_3_2_qos2_receiver_protocol() -> None:
    """[MQTT-4.3.3-2] In the QoS 2 delivery protocol, the Receiver MUST respond with a
    PUBREC containing the Packet Identifier from the incoming PUBLISH Packet, having
    accepted ownership of the Application Message. Until it has received the
    corresponding PUBREL packet, the Receiver MUST acknowledge any subsequent
    PUBLISH packet with the same Packet Identifier by sending a PUBREC. It MUST NOT
    cause duplicate messages to be delivered to any onward recipients in this case.
    MUST respond to a PUBREL packet by sending a PUBCOMP packet containing the same
    Packet Identifier as the PUBREL. After it has sent a PUBCOMP, the receiver MUST
    treat any subsequent PUBLISH packet that contains that Packet Identifier as
    being a new publication."""
    _qos2_receiver(V311)


def test_mqtt5_2_2_1_5_acknowledgements_carry_the_publish_identifier() -> None:
    """[MQTT-2.2.1-5] A PUBACK, PUBREC , PUBREL, or PUBCOMP packet MUST contain the
    same Packet Identifier as the PUBLISH packet that was originally sent"""
    _acknowledgements_carry_the_publish_identifier(V5)


def test_mqtt311_2_3_1_6_acknowledgements_carry_the_publish_identifier() -> None:
    """[MQTT-2.3.1-6] A PUBACK, PUBREC or PUBREL Packet MUST contain the same Packet
    Identifier as the PUBLISH Packet that was originally sent"""
    _acknowledgements_carry_the_publish_identifier(V311)


def test_mqtt5_4_4_0_1_resumed_session_resends_unacknowledged_packets() -> None:
    """[MQTT-4.4.0-1] When a Client reconnects with Clean Start set to 0 and a session
    is present, both the Client and Server MUST resend any unacknowledged PUBLISH
    packets (where QoS > 0) and PUBREL packets using their original Packet
    Identifiers. This is the only circumstance where a Client or Server is REQUIRED
    to resend messages. Clients and Servers MUST NOT resend messages at any other
    time"""
    _resumed_session_resends_what_was_unacknowledged(V5)


def test_mqtt311_4_4_0_1_resumed_session_resends_unacknowledged_packets() -> None:
    """[MQTT-4.4.0-1] When a Client reconnects with CleanSession set to 0, both the
    Client and Server MUST re-send any unacknowledged PUBLISH Packets (where QoS >
    0) and PUBREL Packets using their original Packet Identifiers"""
    _resumed_session_resends_what_was_unacknowledged(V311)


def test_mqtt5_3_3_1_1_redelivered_publish_sets_dup() -> None:
    """[MQTT-3.3.1-1] The DUP flag MUST be set to 1 by the Client or Server when it
    attempts to re-deliver a PUBLISH packet"""
    _resumed_session_resends_what_was_unacknowledged(V5)


def test_mqtt311_3_3_1_1_redelivered_publish_sets_dup() -> None:
    """[MQTT-3.3.1-1] The DUP flag MUST be set to 1 by the Client or Server when it
    attempts to re-deliver a PUBLISH Packet."""
    _resumed_session_resends_what_was_unacknowledged(V311)


def test_mqtt311_2_3_1_3_resent_packet_keeps_its_identifier() -> None:
    """[MQTT-2.3.1-3] If a Client re-sends a particular Control Packet, then it MUST
    use the same Packet Identifier in subsequent re-sends of that packet. The Packet
    Identifier becomes available for reuse after the Client has processed the
    corresponding acknowledgement packet. In the case of a QoS 1 PUBLISH this is the
    corresponding PUBACK; in the case of QoS 2 it is PUBCOMP. For SUBSCRIBE or
    UNSUBSCRIBE it is the corresponding SUBACK or UNSUBACK"""
    _resumed_session_resends_what_was_unacknowledged(V311)


def test_mqtt5_4_4_0_2_negatively_acknowledged_publish_is_not_retransmitted() -> None:
    """[MQTT-4.4.0-2] If PUBACK or PUBREC is received containing a Reason Code of 0x80
    or greater the corresponding PUBLISH packet is treated as acknowledged, and MUST
    NOT be retransmitted"""
    _negatively_acknowledged_publish_is_not_retransmitted()


def test_mqtt311_3_8_3_3_subscribe_requires_a_topic_filter() -> None:
    """[MQTT-3.8.3-3] The payload of a SUBSCRIBE packet MUST contain at least one Topic
    Filter / QoS pair. A SUBSCRIBE packet with no payload is a protocol violation"""
    _subscribe_needs_a_filter(V311)


def test_mqtt5_3_10_3_2_unsubscribe_requires_a_topic_filter() -> None:
    """[MQTT-3.10.3-2] The Payload of an UNSUBSCRIBE packet MUST contain at least one
    Topic Filter"""
    _unsubscribe_needs_a_filter(V5)


def test_mqtt311_3_10_3_2_unsubscribe_requires_a_topic_filter() -> None:
    """[MQTT-3.10.3-2] The Payload of an UNSUBSCRIBE packet MUST contain at least one
    Topic Filter. An UNSUBSCRIBE packet with no payload is a protocol violation"""
    _unsubscribe_needs_a_filter(V311)
