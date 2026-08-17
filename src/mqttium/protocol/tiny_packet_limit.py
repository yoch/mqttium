"""Rare inbound handling for MQTT 5 peer packet limits below the ACK floor.

A broker Maximum Packet Size of 1..3 is legal, but no minimal PUBACK, PUBREC or
PUBCOMP can fit.  The normal inbound handlers stay untouched; ProtocolEngine
selects this wrapper only for such a negotiated connection so ordinary ingress
keeps its existing hot path byte-for-byte.
"""

from __future__ import annotations

from typing import NoReturn

from mqttium.codec.buffer import RawPacket
from mqttium.enums import InboundQoSState, PacketType, QoS
from mqttium.errors import MalformedPacketError, MandatoryResponseTooLargeError, ProtocolError
from mqttium.packets._publish import decode_publish_fields_v5
from mqttium.protocol.inbound import InboundSession
from mqttium.types import InboundMessage


class TinyPacketLimitInbound:
    """Connection-scoped wrapper used only when peer Maximum Packet Size < 4."""

    __slots__ = ("_session",)

    def __init__(self, session: InboundSession) -> None:
        self._session = session

    def handle_publish(self, raw: RawPacket) -> None:
        session = self._session
        qos_raw = (raw.flags >> 1) & 0x03
        if qos_raw == int(QoS.AT_MOST_ONCE) or (
            qos_raw == int(QoS.AT_LEAST_ONCE) and session.config.manual_ack
        ):
            session._on_publish_v5(raw)
            return
        if qos_raw == 3:
            raise MalformedPacketError("Invalid PUBLISH QoS 3")

        qos = QoS(qos_raw)
        (
            topic,
            payload,
            decoded_mid,
            _retain,
            _dup,
            properties,
            property_wire_size,
        ) = decode_publish_fields_v5(raw, qos)
        if not topic or properties.get("topic_alias") is not None:
            topic = session._resolve_topic_fields(topic, properties)
        assert decoded_mid is not None

        if qos is QoS.AT_LEAST_ONCE:
            self._preflight_auto_qos1(decoded_mid)
            self._raise_too_large("PUBACK")

        logical_size = session.logical_size(
            topic,
            payload,
            properties,
            property_wire_size if properties.values else None,
        )
        self._preflight_qos2(decoded_mid, logical_size)
        self._raise_too_large("PUBREC")

    def _preflight_auto_qos1(self, mid: int) -> None:
        session = self._session
        existing = session._lookup_stored_inbound(mid)
        if existing is not None:
            if existing.state is not InboundQoSState.WAIT_PUBACK:
                session._reject_packet_id_collision(mid, "QoS 1", "QoS 2")
            return
        if mid in session._pending_auto_qos1_mids:
            return
        self._validate_slot_capacity()

    def _preflight_qos2(self, mid: int, logical_size: int) -> None:
        session = self._session
        existing = session._lookup_stored_inbound(mid)
        if existing is not None:
            if existing.state is InboundQoSState.WAIT_PUBACK:
                session._reject_packet_id_collision(mid, "QoS 2", "QoS 1")
            return
        if mid in session._pending_auto_qos1_mids:
            session._protocol_disconnect(0x82)
            raise ProtocolError(
                f"Inbound packet identifier {mid} reused by QoS 2 while QoS 1 PUBACK is pending"
            )
        self._validate_slot_capacity(logical_size)

    def _validate_slot_capacity(self, logical_size: int | None = None) -> None:
        """Preserve Receive Maximum/quota errors before the local ACK failure."""
        session = self._session
        if session._inflight >= session.config.local_receive_maximum:
            session._protocol_disconnect(0x93)
            raise ProtocolError("Receive Maximum exceeded")
        byte_limit = session.config.max_pending_inbound_bytes
        if (
            logical_size is not None
            and byte_limit is not None
            and session._pending_bytes + logical_size > byte_limit
        ):
            session._protocol_disconnect(0x97)
            raise ProtocolError("Pending inbound byte limit reached")

    def _raise_too_large(self, packet_name: str) -> NoReturn:
        limit = self._session._engine.negotiated.maximum_packet_size
        assert limit is not None and limit < 4
        raise MandatoryResponseTooLargeError(
            f"Mandatory {packet_name} size 4 exceeds broker maximum_packet_size {limit}"
        )

    def on_pubrel(self, raw: RawPacket) -> None:
        session = self._session
        engine = session._engine
        config = session.config
        mid, _reason_code, properties = session._decode_pubrel(raw.remaining)
        if properties is not None:
            engine._validate_inbound_problem_information(PacketType.PUBREL, properties)
        record = session._lookup_stored_inbound(mid)
        if record is None:
            self._raise_too_large("PUBCOMP")

        state = record.state
        if state is InboundQoSState.WAIT_USER_ACK:
            if config.manual_ack:
                return
        elif state is not InboundQoSState.WAIT_PUBREL:
            raise ProtocolError(f"PUBREL for inbound mid={mid} in invalid state {state!r}")
        elif config.manual_ack and not record.user_acked:
            transitions = session._transitions
            if transitions is not None:
                changed = transitions.transition_in(
                    mid,
                    InboundQoSState.WAIT_PUBREL,
                    InboundQoSState.WAIT_USER_ACK,
                )
                if changed is None:
                    raise ProtocolError(f"Inbound mid={mid} changed while processing PUBREL")
            else:
                assert isinstance(record, InboundMessage)
                record.state = InboundQoSState.WAIT_USER_ACK
                session.store.update_in(record)
            return
        self._raise_too_large("PUBCOMP")
