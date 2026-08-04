"""Broker→client publication: aliases, QoS state, receive window and replay.

`InboundSession` owns the state retained for inbound PUBLISH handling: topic
aliases, the local Receive Maximum count, persisted QoS 1/2 records and restart
redelivery. `ProtocolEngine` still owns connection state and the shared effect
stream; handlers emit through it so observable effect ordering does not change.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from mqttium.codec.buffer import RawPacket
from mqttium.enums import ConnectionState, InboundQoSState, MQTTProtocolVersion, QoS
from mqttium.errors import ProtocolError
from mqttium.packets import (
    PubAckPacket,
    PubCompPacket,
    PublishPacket,
    PubRecPacket,
    PubRelPacket,
    encode_disconnect,
)
from mqttium.protocol.effects import DisconnectInfo, EffectKind
from mqttium.topics import validate_received_publish_topic
from mqttium.types import InboundMessage, Message

if TYPE_CHECKING:
    from mqttium.protocol.engine import ProtocolEngine


class InboundSession:
    """Authoritative inbound QoS and connection-scoped alias state."""

    __slots__ = (
        "_aliases",
        "_engine",
        "_inflight",
        "_recovered_mids",
        "config",
        "store",
    )

    def __init__(self, engine: ProtocolEngine) -> None:
        self._engine = engine
        # EngineConfig is mutated in place by update(), so this identity remains
        # stable for the engine lifetime (the same contract as OutboundSession).
        self.config = engine.config
        self.store = engine.store
        self._aliases: dict[int, str] = {}
        self._inflight = 0
        self._recovered_mids = {message.mid for message in self.store.in_items()}

    # --- lifecycle ---------------------------------------------------------

    def start_connection(self) -> None:
        """Reset state scoped to one network connection before CONNECT."""
        self._aliases.clear()
        self._inflight = 0

    def transport_closed(self) -> None:
        self._aliases.clear()

    def discard_session(self) -> None:
        """Drop inbound state after CONNACK reports no previous session."""
        self.store.clear_in()
        self._recovered_mids.clear()
        self._inflight = 0

    # --- packet handlers ---------------------------------------------------

    def on_publish(self, raw: RawPacket) -> None:
        """Handle one inbound PUBLISH without adding an engine wrapper frame."""
        engine = self._engine
        config = self.config
        store = self.store
        packet = PublishPacket.decode(raw.flags, raw.remaining, config.protocol)
        topic = self._resolve_topic(packet)
        validate_received_publish_topic(topic, utf8_validated=True)

        if packet.qos == QoS.AT_MOST_ONCE:
            engine._emit(
                EffectKind.MESSAGE,
                Message(
                    topic=topic,
                    payload=packet.payload,
                    qos=packet.qos,
                    retain=packet.retain,
                    dup=packet.dup,
                    mid=None,
                    properties=packet.properties,
                ),
            )
            return

        assert packet.mid is not None
        if packet.qos == QoS.EXACTLY_ONCE:
            existing = store.get_in(packet.mid)
            if existing is not None:
                engine._send(PubRecPacket(mid=packet.mid).encode(config.protocol))
                return

        if packet.qos == QoS.AT_LEAST_ONCE and config.manual_ack:
            # A duplicate QoS1 publish reuses the existing Receive Maximum slot,
            # but is surfaced again so an application can complete manual ACK
            # after a reconnect or callback cancellation.
            existing = store.get_in(packet.mid)
            if existing is not None and existing.state is InboundQoSState.WAIT_PUBACK:
                self._emit_message(existing, dup=True)
                return

        self._acquire_slot()
        if packet.qos == QoS.AT_LEAST_ONCE:
            engine._emit(
                EffectKind.MESSAGE,
                Message(
                    topic=topic,
                    payload=packet.payload,
                    qos=packet.qos,
                    retain=packet.retain,
                    dup=packet.dup,
                    mid=packet.mid,
                    properties=packet.properties,
                ),
            )
            if config.manual_ack:
                store.put_in(
                    InboundMessage(
                        mid=packet.mid,
                        topic=topic,
                        payload=packet.payload,
                        qos=packet.qos,
                        retain=packet.retain,
                        state=InboundQoSState.WAIT_PUBACK,
                        delivered=False,
                        properties=packet.properties,
                    )
                )
            else:
                engine._send(PubAckPacket(mid=packet.mid).encode(config.protocol))
                self._release_slot()
            return

        inbound = InboundMessage(
            mid=packet.mid,
            topic=topic,
            payload=packet.payload,
            qos=packet.qos,
            retain=packet.retain,
            state=InboundQoSState.WAIT_PUBREL,
            delivered=False,
            properties=packet.properties,
        )
        store.put_in(inbound)
        engine._emit(
            EffectKind.MESSAGE,
            Message(
                topic=topic,
                payload=packet.payload,
                qos=packet.qos,
                retain=packet.retain,
                dup=packet.dup,
                mid=packet.mid,
                properties=packet.properties,
            ),
        )
        engine._send(PubRecPacket(mid=packet.mid).encode(config.protocol))

    def on_pubrel(self, raw: RawPacket) -> None:
        engine = self._engine
        config = self.config
        store = self.store
        rel = PubRelPacket.decode(raw.remaining, config.protocol)
        inbound = store.get_in(rel.mid)
        if inbound is None:
            # Orphan PUBREL: idempotent PUBCOMP (v5 reason 0x92 optional later).
            engine._send(PubCompPacket(mid=rel.mid).encode(config.protocol))
            return
        if config.manual_ack and not inbound.user_acked:
            inbound.state = InboundQoSState.WAIT_USER_ACK
            store.update_in(inbound)
            return
        store.pop_in(rel.mid)
        engine._send(PubCompPacket(mid=rel.mid).encode(config.protocol))
        self._release_slot()

    # --- application acknowledgement and replay ---------------------------

    def mark_delivered(self, mid: int) -> None:
        inbound = self.store.get_in(mid)
        if inbound is None or inbound.delivered:
            return
        inbound.delivered = True
        self.store.update_in(inbound)

    def ack(self, mid: int) -> None:
        """Complete a deferred PUBACK or PUBCOMP in manual-ack mode."""
        config = self.config
        if not config.manual_ack:
            raise ProtocolError("manual_ack is disabled")
        store = self.store
        inbound = store.get_in(mid)
        if inbound is None:
            raise ProtocolError(f"No pending inbound ack for mid={mid}")
        if inbound.state is InboundQoSState.WAIT_PUBACK:
            store.pop_in(mid)
            self._engine._send(PubAckPacket(mid=mid).encode(config.protocol))
            self._release_slot()
            return
        if inbound.state is InboundQoSState.WAIT_PUBREL:
            inbound.user_acked = True
            store.update_in(inbound)
            return
        if inbound.state is InboundQoSState.WAIT_USER_ACK:
            store.pop_in(mid)
            self._engine._send(PubCompPacket(mid=mid).encode(config.protocol))
            self._release_slot()
            return
        raise ProtocolError(f"Inbound mid={mid} is not awaiting ack (state={inbound.state!r})")

    def replay_session(self) -> None:
        """Restore Receive Maximum accounting and redeliver unresolved messages."""
        inbound_items = list(self.store.in_items())
        self._inflight = len(inbound_items)
        recovered = self._recovered_mids
        for inbound in inbound_items:
            should_redeliver = not inbound.delivered
            if inbound.mid in recovered and self.config.manual_ack:
                if inbound.state in (
                    InboundQoSState.WAIT_PUBACK,
                    InboundQoSState.WAIT_USER_ACK,
                ):
                    should_redeliver = True
                elif inbound.state is InboundQoSState.WAIT_PUBREL and not inbound.user_acked:
                    should_redeliver = True
            if should_redeliver:
                self._emit_message(inbound, dup=True)
        recovered.clear()

    def _emit_message(self, inbound: InboundMessage, *, dup: bool) -> None:
        self._engine._emit(
            EffectKind.MESSAGE,
            Message(
                topic=inbound.topic,
                payload=inbound.payload,
                qos=inbound.qos,
                retain=inbound.retain,
                dup=dup,
                mid=inbound.mid,
                properties=inbound.properties,
            ),
        )

    # --- aliases and Receive Maximum --------------------------------------

    def _resolve_topic(self, packet: PublishPacket) -> str:
        if self.config.protocol != MQTTProtocolVersion.MQTTv5:
            return packet.topic
        props = packet.properties
        alias = props.get("topic_alias") if props else None
        if alias is None:
            if not packet.topic:
                raise ProtocolError("PUBLISH with empty topic and no topic alias")
            return packet.topic
        alias = int(alias)
        max_alias = self.config.topic_alias_maximum
        # max_alias == 0 means inbound aliases are not accepted.
        if alias == 0 or alias > max_alias:
            # MQTT 5 §3.3.2.3.5: DISCONNECT 0x94 (Topic Alias invalid).
            self._protocol_disconnect(0x94)
            raise ProtocolError(f"Invalid topic alias {alias}")
        if packet.topic:
            self._aliases[alias] = packet.topic
            return packet.topic
        if alias not in self._aliases:
            self._protocol_disconnect(0x94)
            raise ProtocolError(f"Unknown topic alias {alias}")
        return self._aliases[alias]

    def _acquire_slot(self) -> None:
        if self._inflight >= self.config.local_receive_maximum:
            # MQTT 5 §3.3.4: DISCONNECT 0x93 (Receive Maximum exceeded).
            self._protocol_disconnect(0x93)
            raise ProtocolError("Receive Maximum exceeded")
        self._inflight += 1

    def _release_slot(self) -> None:
        if self._inflight > 0:
            self._inflight -= 1

    def _protocol_disconnect(self, reason_code: int) -> None:
        """Emit a normative DISCONNECT before the transport is torn down."""
        engine = self._engine
        if self.config.protocol == MQTTProtocolVersion.MQTTv5:
            engine._send(encode_disconnect(reason_code, self.config.protocol))
        engine.state = ConnectionState.DISCONNECTED
        engine._emit(
            EffectKind.DISCONNECTED,
            DisconnectInfo(reason_code=reason_code, from_broker=False),
        )
