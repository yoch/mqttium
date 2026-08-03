"""Synchronous MQTT protocol engine.

No asyncio, no sockets, no user callbacks. Feed packets / commands in, collect
effects out. This is the correctness core that AsyncClient adapts.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any

from mqttium.codec.buffer import RawPacket
from mqttium.codec.properties import PUBLISH, encode_properties
from mqttium.enums import (
    ConnectionState,
    InboundQoSState,
    MQTTProtocolVersion,
    OutboundQoSState,
    PacketType,
    QoS,
)
from mqttium.packets import (
    AuthPacket,
    ConnAckPacket,
    ConnectPacket,
    DisconnectPacket,
    PubAckPacket,
    PubCompPacket,
    PublishPacket,
    PubRecPacket,
    PubRelPacket,
    SubAckPacket,
    SubscribeOptions,
    SubscribePacket,
    Subscription,
    UnsubAckPacket,
    UnsubscribePacket,
    encode_disconnect,
    encode_pingreq,
)
from mqttium.persistence.memory import InflightStore, MemoryInflightStore
from mqttium.protocol.flow_control import FlowControl
from mqttium.protocol.negotiated import NegotiatedSettings
from mqttium.protocol.packet_ids import PacketIdPool
from mqttium.protocol.validate import validate_raw_packet
from mqttium.topics import (
    validate_publish_topic,
    validate_received_publish_topic,
    validate_subscribe_filter,
)
from mqttium.transport.writes import WriteItem, item_size
from mqttium.types import InboundMessage, Message, OutboundMessage, Properties
from mqttium.errors import (
    FlowControlError,
    MalformedPacketError,
    NotConnectedError,
    PacketTooLargeError,
    ProtocolError,
    SessionDiscardedError,
)


_ALLOWED_PACKETS_BY_STATE: dict[ConnectionState, frozenset[PacketType]] = {
    ConnectionState.CONNECTING: frozenset(
        {
            PacketType.CONNACK,
            PacketType.AUTH,
        }
    ),
    ConnectionState.CONNECTED: frozenset(
        {
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
        }
    ),
}


class EffectKind(Enum):
    SEND = auto()
    MESSAGE = auto()
    CONNACK = auto()
    PUBLISH_COMPLETE = auto()
    PUBLISH_FAILED = auto()
    SUBACK = auto()
    UNSUBACK = auto()
    DISCONNECTED = auto()
    PROTOCOL_ERROR = auto()
    PINGRESP = auto()
    AUTH = auto()


@dataclass(slots=True)
class EngineEffect:
    kind: EffectKind
    data: Any = None


@dataclass(slots=True)
class PublishHandle:
    mid: int | None
    qos: QoS


@dataclass(slots=True)
class PublishFailure:
    mid: int
    reason: BaseException


@dataclass(slots=True)
class DisconnectInfo:
    """Broker or local disconnect signal carried on EffectKind.DISCONNECTED."""

    reason_code: int = 0
    properties: Properties | None = None
    from_broker: bool = False


@dataclass
class EngineConfig:
    client_id: str = ""
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311
    clean_start: bool = True
    keepalive: int = 60
    username: str | None = None
    password: bytes | None = field(default=None, repr=False)
    local_receive_maximum: int = 65535
    # Optional local cap on outbound inflight QoS>0 (None = broker's Receive
    # Maximum only). Use to self-throttle a fast publisher.
    max_outbound_inflight: int | None = None
    max_queued: int = 0  # 0 = unlimited queued (not yet inflight)
    connect_properties: Properties | None = None
    will: Message | None = field(default=None, repr=False)
    will_properties: Properties | None = None
    # Local maximum packet size announced to broker (and enforced on ingress).
    maximum_packet_size: int | None = None
    topic_alias_maximum: int = 0  # announced to broker for inbound aliases
    manual_ack: bool = False  # defer PUBACK (QoS1) / PUBCOMP (QoS2) until ack()
    # When False, inbound AUTH is rejected with DISCONNECT 0x8C (legacy stub).
    # AsyncClient sets this True when an auth_handler is registered.
    accept_auth: bool = False

    def __post_init__(self) -> None:
        if not 0 <= self.keepalive <= 65535:
            raise ValueError("keepalive must be between 0 and 65535")
        if not 1 <= self.local_receive_maximum <= 65535:
            raise ValueError("local_receive_maximum must be between 1 and 65535")
        if self.max_outbound_inflight is not None and not (
            1 <= self.max_outbound_inflight <= 65535
        ):
            raise ValueError("max_outbound_inflight must be between 1 and 65535")
        if self.max_queued < 0:
            raise ValueError("max_queued must be non-negative")
        if self.maximum_packet_size is not None and not (
            2 <= self.maximum_packet_size <= 268_435_460
        ):
            raise ValueError("maximum_packet_size must be between 2 and 268435460")
        if not 0 <= self.topic_alias_maximum <= 65535:
            raise ValueError("topic_alias_maximum must be between 0 and 65535")


class ProtocolEngine:
    """Pure MQTT session/QoS state machine."""

    def __init__(
        self,
        config: EngineConfig | None = None,
        store: InflightStore | None = None,
    ) -> None:
        self.config = config or EngineConfig()
        self.store = store or MemoryInflightStore()
        self.packet_ids = PacketIdPool()
        self.flow = FlowControl(self.config.local_receive_maximum)
        self.state = ConnectionState.NEW
        self.session_present = False
        self.negotiated = NegotiatedSettings()
        self._pending_connect = False
        self._effects: list[EngineEffect] = []
        self._queued: deque[OutboundMessage] = deque()
        # Inbound topic aliases (connection-scoped).
        self._topic_aliases: dict[int, str] = {}
        # After a durable session is established, next CONNECT uses Clean Start 0.
        self._prefer_session_resume = False
        # Server→client QoS>0 not yet fully acknowledged (Receive Maximum).
        self._inbound_inflight = 0
        self._auth_method: str | None = None
        self._recovered_inbound_mids = {msg.mid for msg in self.store.in_items()}
        self._handlers = {
            PacketType.CONNACK: self._on_connack,
            PacketType.PUBLISH: self._on_publish,
            PacketType.PUBACK: self._on_puback,
            PacketType.PUBREC: self._on_pubrec,
            PacketType.PUBREL: self._on_pubrel,
            PacketType.PUBCOMP: self._on_pubcomp,
            PacketType.SUBACK: self._on_suback,
            PacketType.UNSUBACK: self._on_unsuback,
            PacketType.PINGRESP: self._on_pingresp,
            PacketType.PINGREQ: self._on_pingreq,
            PacketType.DISCONNECT: self._on_disconnect,
            PacketType.AUTH: self._on_auth,
        }
        # Hydrate packet ids + offline queue from a durable store (restart).
        for msg in list(self.store.out_items()):
            self.packet_ids.reserve(msg.mid)
            if msg.state is OutboundQoSState.QUEUED:
                self._queued.append(msg)
            elif msg.state in (
                OutboundQoSState.WAIT_PUBACK,
                OutboundQoSState.WAIT_PUBREC,
                OutboundQoSState.WAIT_PUBCOMP,
            ):
                self.flow.try_acquire()
        # MIDs of in-flight SUBSCRIBE/UNSUBSCRIBE (never collide with PUBLISH).
        self._pending_sub_mids: set[int] = set()

    def take_effects(self) -> list[EngineEffect]:
        effects = self._effects
        self._effects = []
        return effects

    def _emit(self, kind: EffectKind, data: Any = None) -> None:
        self._effects.append(EngineEffect(kind=kind, data=data))

    def _send(self, packet: WriteItem) -> None:
        self._emit(EffectKind.SEND, packet)

    def begin_connect(self) -> bytes:
        if self.state in (ConnectionState.CONNECTED, ConnectionState.CONNECTING):
            raise ProtocolError("Already connected or connecting")
        configured_auth_method = None
        if self.config.connect_properties is not None:
            configured_auth_method = self.config.connect_properties.get("authentication_method")
        if self.config.accept_auth:
            if self.config.protocol != MQTTProtocolVersion.MQTTv5:
                raise ProtocolError("Enhanced authentication requires MQTT 5")
            if not configured_auth_method:
                raise ProtocolError(
                    "auth_handler requires authentication_method in CONNECT properties"
                )
        self._auth_method = (
            str(configured_auth_method) if configured_auth_method is not None else None
        )
        self.state = ConnectionState.CONNECTING
        self._pending_connect = True
        self._topic_aliases.clear()
        # Negotiated capabilities are connection-scoped. Queued messages are
        # validated against the new values only after the next CONNACK.
        self.negotiated = NegotiatedSettings()
        self._inbound_inflight = 0

        clean_start = self.config.clean_start
        if self._prefer_session_resume:
            clean_start = False

        connect_props = self.config.connect_properties
        if self.config.protocol == MQTTProtocolVersion.MQTTv5:
            connect_props = Properties(values=dict(connect_props.values) if connect_props else {})
            if "receive_maximum" not in connect_props.values:
                connect_props.set("receive_maximum", self.config.local_receive_maximum)
            if (
                self.config.maximum_packet_size is not None
                and "maximum_packet_size" not in connect_props.values
            ):
                connect_props.set("maximum_packet_size", self.config.maximum_packet_size)
            if (
                self.config.topic_alias_maximum
                and "topic_alias_maximum" not in connect_props.values
            ):
                connect_props.set("topic_alias_maximum", self.config.topic_alias_maximum)

        will = self.config.will
        packet = ConnectPacket(
            client_id=self.config.client_id,
            clean_start=clean_start,
            keepalive=self.config.keepalive,
            username=self.config.username,
            password=self.config.password,
            will_topic=will.topic if will else None,
            will_payload=will.payload if will else b"",
            will_qos=will.qos if will else QoS.AT_MOST_ONCE,
            will_retain=will.retain if will else False,
            will_properties=self.config.will_properties,
            protocol=self.config.protocol,
            properties=connect_props,
        )
        return packet.encode()

    def queue_publish(
        self,
        topic: str,
        payload: bytes = b"",
        *,
        qos: QoS | int = QoS.AT_MOST_ONCE,
        retain: bool = False,
        properties: Properties | None = None,
    ) -> PublishHandle:
        qos = QoS(qos)
        allow_empty = bool(
            properties
            and properties.get("topic_alias")
            and self.config.protocol == MQTTProtocolVersion.MQTTv5
        )
        validate_publish_topic(topic, allow_empty=allow_empty)

        if self.state == ConnectionState.CONNECTED:
            if qos > self.negotiated.maximum_qos:
                raise ProtocolError(
                    f"QoS {int(qos)} exceeds broker maximum_qos {self.negotiated.maximum_qos}"
                )
            if retain and not self.negotiated.retain_available:
                raise ProtocolError("Broker does not support retain")
            if properties and properties.get("topic_alias") is not None:
                alias = int(properties.get("topic_alias"))
                if alias > self.negotiated.topic_alias_maximum:
                    raise ProtocolError(
                        f"topic_alias {alias} exceeds broker topic_alias_maximum "
                        f"{self.negotiated.topic_alias_maximum}"
                    )

        if self.state != ConnectionState.CONNECTED and qos == QoS.AT_MOST_ONCE:
            raise NotConnectedError("Cannot publish QoS 0 while disconnected")

        if qos == QoS.AT_MOST_ONCE:
            packet = PublishPacket(
                topic=topic,
                payload=payload,
                qos=qos,
                retain=retain,
                dup=False,
                mid=None,
                properties=properties,
            )
            wire = packet.encode_write_item(self.config.protocol)
            self._check_outbound_size(wire)
            self._send(wire)
            return PublishHandle(mid=None, qos=qos)

        mid = self.packet_ids.allocate()
        msg = OutboundMessage(
            mid=mid,
            topic=topic,
            payload=payload,
            qos=qos,
            retain=retain,
            state=OutboundQoSState.QUEUED,
            properties=properties,
        )
        # Defer wire encode until launch: avoids double work when messages sit
        # in the Receive Maximum queue. Still enforce maximum_packet_size via
        # a cheap upper-bound estimate when the broker advertised a limit.
        try:
            self._check_outbound_publish_budget(topic, payload, qos, properties)
        except Exception:
            self.packet_ids.release(mid)
            raise

        if self.state == ConnectionState.CONNECTED:
            try:
                launched = self._try_launch_outbound(msg)
            except Exception:
                self.packet_ids.release(mid)
                self._discard_outbound_store_record(mid)
                raise
            if launched:
                return PublishHandle(mid=mid, qos=qos)

        if self.config.max_queued and len(self._queued) >= self.config.max_queued:
            self.packet_ids.release(mid)
            raise FlowControlError("Outbound queue full")
        try:
            self.store.put_out(msg)
        except Exception:
            self.packet_ids.release(mid)
            raise
        self._queued.append(msg)
        return PublishHandle(mid=mid, qos=qos)

    def queue_publish_many(
        self,
        messages: Iterable[tuple[str, bytes, QoS | int, bool, Properties | None]],
    ) -> list[PublishHandle]:
        """Queue one bounded chunk atomically with respect to engine/store state."""
        effect_start = len(self._effects)
        queued_start = len(self._queued)
        inflight_start = self.flow.inflight
        handles: list[PublishHandle] = []
        try:
            with self.store.batch():
                for topic, payload, qos, retain, properties in messages:
                    handles.append(
                        self.queue_publish(
                            topic,
                            payload,
                            qos=qos,
                            retain=retain,
                            properties=properties,
                        )
                    )
        except BaseException:
            del self._effects[effect_start:]
            while len(self._queued) > queued_start:
                self._queued.pop()
            while self.flow.inflight > inflight_start:
                self.flow.release()
            for handle in handles:
                if handle.mid is None:
                    continue
                try:
                    self.store.delete_out(handle.mid)
                except Exception:
                    pass
                self.packet_ids.release(handle.mid)
            raise
        return handles

    def queue_subscribe(
        self,
        topics: str | Iterable[str | tuple[str, SubscribeOptions | int | QoS]],
        *,
        qos: QoS | int = QoS.AT_MOST_ONCE,
        properties: Properties | None = None,
    ) -> int:
        if self.state != ConnectionState.CONNECTED:
            raise NotConnectedError("subscribe requires an active connection")

        subscriptions: list[Subscription] = []
        if isinstance(topics, str):
            validate_subscribe_filter(topics)
            self._check_subscribe_capabilities(topics, properties)
            subscriptions.append(Subscription(topic=topics, options=SubscribeOptions(qos=QoS(qos))))
        else:
            for item in topics:
                if isinstance(item, str):
                    topic, options = item, SubscribeOptions(qos=QoS(qos))
                else:
                    topic, opt = item
                    if isinstance(opt, SubscribeOptions):
                        options = opt
                    else:
                        options = SubscribeOptions(qos=QoS(opt))
                validate_subscribe_filter(topic)
                self._check_subscribe_capabilities(topic, properties)
                subscriptions.append(Subscription(topic=topic, options=options))

        mid = self.packet_ids.allocate()
        try:
            packet = SubscribePacket(
                mid=mid,
                subscriptions=tuple(subscriptions),
                properties=properties,
            )
            wire = packet.encode(self.config.protocol)
            self._check_outbound_size(wire)
        except Exception:
            self.packet_ids.release(mid)
            raise
        self._pending_sub_mids.add(mid)
        self._send(wire)
        return mid

    def queue_unsubscribe(self, topics: str | Iterable[str]) -> int:
        if self.state != ConnectionState.CONNECTED:
            raise NotConnectedError("unsubscribe requires an active connection")
        topic_list: tuple[str, ...]
        if isinstance(topics, str):
            topic_list = (topics,)
        else:
            topic_list = tuple(topics)
        if not topic_list:
            raise ProtocolError("unsubscribe requires at least one topic")
        for topic in topic_list:
            validate_subscribe_filter(topic)
        mid = self.packet_ids.allocate()
        try:
            packet = UnsubscribePacket(mid=mid, topics=topic_list)
            wire = packet.encode(self.config.protocol)
            self._check_outbound_size(wire)
        except Exception:
            self.packet_ids.release(mid)
            raise
        self._pending_sub_mids.add(mid)
        self._send(wire)
        return mid

    def queue_ping(self) -> None:
        if self.state != ConnectionState.CONNECTED:
            raise NotConnectedError("PINGREQ requires an active connection")
        self._send(encode_pingreq())

    def begin_disconnect(
        self,
        reason_code: int = 0,
        properties: Properties | None = None,
    ) -> bytes:
        if self.state != ConnectionState.CONNECTED:
            raise NotConnectedError("disconnect requires an active connection")
        self.state = ConnectionState.DISCONNECTING
        return encode_disconnect(reason_code, self.config.protocol, properties)

    def notify_transport_closed(self) -> None:
        was = self.state
        self.state = ConnectionState.DISCONNECTED
        self._topic_aliases.clear()
        self._pending_connect = False
        # Release sub/unsub MIDs still in flight — no ACK will arrive now.
        for mid in self._pending_sub_mids:
            self.packet_ids.release(mid)
        self._pending_sub_mids.clear()
        if was != ConnectionState.DISCONNECTED:
            self._emit(EffectKind.DISCONNECTED, DisconnectInfo(from_broker=False))

    def handle_raw(self, raw: RawPacket) -> None:
        try:
            validate_raw_packet(raw)
            allowed = _ALLOWED_PACKETS_BY_STATE.get(self.state, frozenset())
            if raw.packet_type not in allowed:
                raise ProtocolError(
                    f"Unexpected {raw.packet_type.name} while state={self.state.name}"
                )
            handler = self._handlers.get(raw.packet_type)
            if handler is None:
                raise ProtocolError(f"Unhandled packet {raw.packet_type!r}")
            handler(raw)
        except (ProtocolError, MalformedPacketError) as exc:
            self._emit(EffectKind.PROTOCOL_ERROR, str(exc))
        except Exception as exc:
            # Isolate store/persistence errors: surface as protocol error rather
            # than killing the read loop with an untyped exception.
            self._emit(EffectKind.PROTOCOL_ERROR, f"Internal handler error: {exc!r}")

    def _on_connack(self, raw: RawPacket) -> None:
        if not self._pending_connect or self.state != ConnectionState.CONNECTING:
            raise ProtocolError("Unexpected CONNACK (already negotiated)")
        connack = ConnAckPacket.decode(raw.remaining, self.config.protocol)
        self._pending_connect = False
        if self.config.protocol == MQTTProtocolVersion.MQTTv5:
            connack_method = (
                connack.properties.get("authentication_method")
                if connack.properties is not None
                else None
            )
            if connack_method is not None and connack_method != self._auth_method:
                raise ProtocolError("CONNACK authentication_method does not match CONNECT")
            if (
                connack.properties is not None
                and connack.properties.get("authentication_data") is not None
                and self._auth_method is None
            ):
                raise ProtocolError("CONNACK authentication_data requires authentication_method")
        if connack.reason_code != 0:
            self.state = ConnectionState.DISCONNECTED
            self._emit(EffectKind.CONNACK, connack)
            self._emit(
                EffectKind.DISCONNECTED,
                DisconnectInfo(
                    reason_code=connack.reason_code,
                    properties=connack.properties,
                    from_broker=True,
                ),
            )
            return

        requested_expiry = None
        if self.config.connect_properties:
            requested_expiry = self.config.connect_properties.get("session_expiry_interval")
        self.negotiated = NegotiatedSettings.from_connack(
            connack.properties,
            requested_keepalive=self.config.keepalive,
            requested_session_expiry=requested_expiry,
            local_client_id=self.config.client_id,
        )
        self.flow.apply_broker_receive_maximum(
            self.negotiated.receive_maximum,
            self.config.local_receive_maximum,
            self.config.max_outbound_inflight,
        )

        self.state = ConnectionState.CONNECTED
        self.session_present = connack.session_present
        self._update_session_resume_preference()
        if not connack.session_present:
            for msg in list(self.store.out_items()):
                if msg.state is OutboundQoSState.QUEUED:
                    continue
                self.store.delete_out(msg.mid)
                self.packet_ids.release(msg.mid)
                self._emit(
                    EffectKind.PUBLISH_FAILED,
                    PublishFailure(
                        mid=msg.mid,
                        reason=SessionDiscardedError(
                            "Publish lost: clean session replaced the previous one"
                        ),
                    ),
                )
            self.store.clear_in()
            self._recovered_inbound_mids.clear()
            self._inbound_inflight = 0
            self.flow.reset()
            # Re-apply negotiated limit after reset.
            self.flow.apply_broker_receive_maximum(
                self.negotiated.receive_maximum,
                self.config.local_receive_maximum,
                self.config.max_outbound_inflight,
            )
        else:
            self._replay_session()

        self._fail_queued_violating_negotiation()
        self._emit(EffectKind.CONNACK, connack)
        if connack.session_present:
            self._replay_inbound_session()
        self._drain_queue()

    def _on_publish(self, raw: RawPacket) -> None:
        packet = PublishPacket.decode(raw.flags, raw.remaining, self.config.protocol)
        topic = self._resolve_inbound_topic(packet)
        validate_received_publish_topic(topic, utf8_validated=True)

        if packet.qos == QoS.AT_MOST_ONCE:
            self._emit(
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
            existing = self.store.get_in(packet.mid)
            if existing is not None:
                self._send(PubRecPacket(mid=packet.mid).encode(self.config.protocol))
                return

        if packet.qos == QoS.AT_LEAST_ONCE and self.config.manual_ack:
            # A duplicate QoS1 publish reuses the existing Receive Maximum slot,
            # but is surfaced again so an application can complete manual ACK
            # after a reconnect or callback cancellation.
            existing = self.store.get_in(packet.mid)
            if existing is not None and existing.state is InboundQoSState.WAIT_PUBACK:
                self._emit_inbound_message(existing, dup=True)
                return

        self._acquire_inbound_slot()
        if packet.qos == QoS.AT_LEAST_ONCE:
            self._emit(
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
            if self.config.manual_ack:
                self.store.put_in(
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
                self._send(PubAckPacket(mid=packet.mid).encode(self.config.protocol))
                self._release_inbound_slot()
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
        self.store.put_in(inbound)
        self._emit(
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
        self._send(PubRecPacket(mid=packet.mid).encode(self.config.protocol))

    def mark_inbound_delivered(self, mid: int) -> None:
        inbound = self.store.get_in(mid)
        if inbound is None or inbound.delivered:
            return
        inbound.delivered = True
        self.store.update_in(inbound)

    def _emit_inbound_message(self, inbound: InboundMessage, *, dup: bool) -> None:
        self._emit(
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

    def _replay_inbound_session(self) -> None:
        inbound_items = list(self.store.in_items())
        self._inbound_inflight = len(inbound_items)
        recovered = self._recovered_inbound_mids
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
                self._emit_inbound_message(inbound, dup=True)
        recovered.clear()

    def _on_puback(self, raw: RawPacket) -> None:
        ack = PubAckPacket.decode(raw.remaining, self.config.protocol)
        msg = self.store.get_out(ack.mid)
        if msg is None or msg.state is not OutboundQoSState.WAIT_PUBACK:
            return
        self.store.delete_out(ack.mid)
        self.packet_ids.release(ack.mid)
        self.flow.release()
        if ack.reason_code >= 128:
            self._emit(
                EffectKind.PUBLISH_FAILED,
                PublishFailure(
                    mid=ack.mid,
                    reason=ProtocolError(f"PUBACK reason_code={ack.reason_code}"),
                ),
            )
        else:
            self._emit(EffectKind.PUBLISH_COMPLETE, ack.mid)
        self._drain_queue()

    def _on_pubrec(self, raw: RawPacket) -> None:
        rec = PubRecPacket.decode(raw.remaining, self.config.protocol)
        msg = self.store.get_out(rec.mid)
        if msg is None:
            # Orphan PUBREC: reply PUBREL with 0x92 when MQTT 5.
            reason = 0x92 if self.config.protocol == MQTTProtocolVersion.MQTTv5 else 0
            self._send(PubRelPacket(mid=rec.mid, reason_code=reason).encode(self.config.protocol))
            return
        if msg.state is not OutboundQoSState.WAIT_PUBREC:
            return
        if rec.reason_code >= 128:
            self.store.delete_out(rec.mid)
            self.packet_ids.release(rec.mid)
            self.flow.release()
            self._emit(
                EffectKind.PUBLISH_FAILED,
                PublishFailure(
                    mid=rec.mid,
                    reason=ProtocolError(f"PUBREC reason_code={rec.reason_code}"),
                ),
            )
            self._drain_queue()
            return
        msg.state = OutboundQoSState.WAIT_PUBCOMP
        if msg.encoded_pubrel is None:
            msg.encoded_pubrel = PubRelPacket(mid=rec.mid).encode(self.config.protocol)
        self.store.update_out(msg)
        # Keep the local flow slot until PUBCOMP. MQTT 5 allows releasing at
        # PUBREC, but freeing early lets WAIT_PUBCOMP accumulate without bound
        # and has caused intermittent multi-second stalls under load.
        self._send(msg.encoded_pubrel)

    def _on_pubrel(self, raw: RawPacket) -> None:
        rel = PubRelPacket.decode(raw.remaining, self.config.protocol)
        inbound = self.store.get_in(rel.mid)
        if inbound is None:
            # Orphan PUBREL: idempotent PUBCOMP (v5 reason 0x92 optional later).
            self._send(PubCompPacket(mid=rel.mid).encode(self.config.protocol))
            return
        if self.config.manual_ack and not inbound.user_acked:
            inbound.state = InboundQoSState.WAIT_USER_ACK
            self.store.update_in(inbound)
            return
        self.store.pop_in(rel.mid)
        self._send(PubCompPacket(mid=rel.mid).encode(self.config.protocol))
        self._release_inbound_slot()

    def ack(self, mid: int) -> None:
        """Complete a deferred inbound ACK (manual_ack mode).

        QoS 1: send PUBACK. QoS 2: mark ready / send PUBCOMP if PUBREL already seen.
        """
        if not self.config.manual_ack:
            raise ProtocolError("manual_ack is disabled")
        inbound = self.store.get_in(mid)
        if inbound is None:
            raise ProtocolError(f"No pending inbound ack for mid={mid}")
        if inbound.state is InboundQoSState.WAIT_PUBACK:
            self.store.pop_in(mid)
            self._send(PubAckPacket(mid=mid).encode(self.config.protocol))
            self._release_inbound_slot()
            return
        if inbound.state is InboundQoSState.WAIT_PUBREL:
            inbound.user_acked = True
            self.store.update_in(inbound)
            return
        if inbound.state is InboundQoSState.WAIT_USER_ACK:
            self.store.pop_in(mid)
            self._send(PubCompPacket(mid=mid).encode(self.config.protocol))
            self._release_inbound_slot()
            return
        raise ProtocolError(f"Inbound mid={mid} is not awaiting ack (state={inbound.state!r})")

    def _on_pubcomp(self, raw: RawPacket) -> None:
        comp = PubCompPacket.decode(raw.remaining, self.config.protocol)
        msg = self.store.get_out(comp.mid)
        if msg is None or msg.state is not OutboundQoSState.WAIT_PUBCOMP:
            return
        self.store.delete_out(comp.mid)
        self.packet_ids.release(comp.mid)
        self.flow.release()
        if comp.reason_code >= 128:
            self._emit(
                EffectKind.PUBLISH_FAILED,
                PublishFailure(
                    mid=comp.mid,
                    reason=ProtocolError(f"PUBCOMP reason_code={comp.reason_code}"),
                ),
            )
        else:
            self._emit(EffectKind.PUBLISH_COMPLETE, comp.mid)
        self._drain_queue()

    def _on_suback(self, raw: RawPacket) -> None:
        ack = SubAckPacket.decode(raw.remaining, self.config.protocol)
        if ack.mid not in self._pending_sub_mids:
            # Orphan / cross-mid SUBACK: do not release a foreign packet id.
            self._emit(EffectKind.PROTOCOL_ERROR, f"SUBACK for unknown mid {ack.mid}")
            return
        self._pending_sub_mids.discard(ack.mid)
        self.packet_ids.release(ack.mid)
        self._emit(EffectKind.SUBACK, ack)

    def _on_unsuback(self, raw: RawPacket) -> None:
        ack = UnsubAckPacket.decode(raw.remaining, self.config.protocol)
        if ack.mid not in self._pending_sub_mids:
            self._emit(EffectKind.PROTOCOL_ERROR, f"UNSUBACK for unknown mid {ack.mid}")
            return
        self._pending_sub_mids.discard(ack.mid)
        self.packet_ids.release(ack.mid)
        self._emit(EffectKind.UNSUBACK, ack)

    def _on_pingresp(self, raw: RawPacket) -> None:
        self._emit(EffectKind.PINGRESP)

    def _on_pingreq(self, raw: RawPacket) -> None:
        # Brokers must not send PINGREQ to clients.
        raise ProtocolError("Unexpected PINGREQ from broker")

    def _on_disconnect(self, raw: RawPacket) -> None:
        packet = DisconnectPacket.decode(raw.remaining, self.config.protocol)
        self.state = ConnectionState.DISCONNECTED
        self._emit(
            EffectKind.DISCONNECTED,
            DisconnectInfo(
                reason_code=packet.reason_code,
                properties=packet.properties,
                from_broker=True,
            ),
        )

    def _on_auth(self, raw: RawPacket) -> None:
        if self.config.protocol != MQTTProtocolVersion.MQTTv5:
            self._send(encode_disconnect(0x82, self.config.protocol))
            self.state = ConnectionState.DISCONNECTED
            self._emit(
                EffectKind.DISCONNECTED,
                DisconnectInfo(reason_code=0x82, from_broker=False),
            )
            return
        packet = AuthPacket.decode(raw.remaining, self.config.protocol)
        if self._auth_method is None:
            self._reject_auth_method()
            return
        packet_method = (
            packet.properties.get("authentication_method")
            if packet.properties is not None
            else None
        )
        if packet_method is not None and packet_method != self._auth_method:
            self._reject_auth_method()
            return
        if packet.reason_code == 0x19 and self.state is ConnectionState.CONNECTING:
            raise ProtocolError("Re-authenticate AUTH is invalid before CONNACK")
        if not self.config.accept_auth:
            # No enhanced-auth handler configured — reject broker-initiated AUTH.
            self._send(
                encode_disconnect(
                    0x8C,
                    self.config.protocol,
                )
            )
            self.state = ConnectionState.DISCONNECTED
            self._emit(
                EffectKind.DISCONNECTED,
                DisconnectInfo(reason_code=0x8C, from_broker=False),
            )
            return
        self._emit(EffectKind.AUTH, packet)

    def _reject_auth_method(self) -> None:
        self._send(encode_disconnect(0x8C, self.config.protocol))
        self.state = ConnectionState.DISCONNECTED
        self._emit(
            EffectKind.DISCONNECTED,
            DisconnectInfo(reason_code=0x8C, from_broker=False),
        )

    def queue_auth(
        self,
        reason_code: int = 0x19,
        properties: Properties | None = None,
    ) -> None:
        """Queue a client AUTH (continue / re-authenticate). MQTT 5 only."""
        if self.config.protocol != MQTTProtocolVersion.MQTTv5:
            raise ProtocolError("AUTH requires MQTT 5")
        if self.state not in (ConnectionState.CONNECTING, ConnectionState.CONNECTED):
            raise NotConnectedError("AUTH requires an active or pending connection")
        if self._auth_method is None:
            raise ProtocolError("AUTH requires authentication_method in CONNECT")
        if reason_code == 0x19 and self.state is not ConnectionState.CONNECTED:
            raise ProtocolError("Re-authenticate AUTH requires a connected session")
        auth_properties = Properties(
            values=dict(properties.values) if properties is not None else {}
        )
        method = auth_properties.get("authentication_method")
        if method is not None and method != self._auth_method:
            raise ProtocolError("AUTH authentication_method does not match CONNECT")
        if method is None:
            auth_properties.set("authentication_method", self._auth_method)
        self._send(
            AuthPacket(
                reason_code=reason_code,
                properties=auth_properties,
            ).encode(self.config.protocol)
        )

    def _launch_outbound(self, msg: OutboundMessage) -> None:
        if msg.encoded_publish is None:
            msg.encoded_publish = PublishPacket(
                topic=msg.topic,
                payload=msg.payload,
                qos=msg.qos,
                retain=msg.retain,
                dup=msg.dup,
                mid=msg.mid,
                properties=msg.properties,
            ).encode_write_item(self.config.protocol)
        self._check_outbound_size(msg.encoded_publish)
        if msg.qos == QoS.AT_LEAST_ONCE:
            msg.state = OutboundQoSState.WAIT_PUBACK
        else:
            msg.state = OutboundQoSState.WAIT_PUBREC
        self.store.put_out(msg)
        self._send(msg.encoded_publish)

    def _try_launch_outbound(self, msg: OutboundMessage) -> bool:
        if not self.flow.try_acquire():
            return False
        try:
            self._launch_outbound(msg)
        except Exception:
            self.flow.release()
            raise
        return True

    def _retransmit_outbound(self, msg: OutboundMessage) -> None:
        if msg.state in (
            OutboundQoSState.WAIT_PUBACK,
            OutboundQoSState.WAIT_PUBREC,
        ):
            msg.dup = True
            msg.encoded_publish = PublishPacket(
                topic=msg.topic,
                payload=msg.payload,
                qos=msg.qos,
                retain=msg.retain,
                dup=True,
                mid=msg.mid,
                properties=msg.properties,
            ).encode_write_item(self.config.protocol)
            self._check_outbound_size(msg.encoded_publish)
            self.store.update_out(msg)
            self._send(msg.encoded_publish)
            return
        if msg.state is OutboundQoSState.WAIT_PUBCOMP:
            if msg.encoded_pubrel is None:
                msg.encoded_pubrel = PubRelPacket(mid=msg.mid).encode(self.config.protocol)
                self.store.update_out(msg)
            self._check_outbound_size(msg.encoded_pubrel)
            self._send(msg.encoded_pubrel)
            return
        raise ProtocolError(f"Cannot retransmit outbound state {msg.state!r}")

    def _drain_queue(self) -> None:
        while self._queued and self.flow.available > 0:
            if not self.flow.try_acquire():
                break
            msg = self._queued[0]
            try:
                if msg.state is OutboundQoSState.QUEUED:
                    self._launch_outbound(msg)
                else:
                    self._retransmit_outbound(msg)
            except Exception as exc:
                self.flow.release()
                self._queued.popleft()
                self._discard_outbound_store_record(msg.mid)
                self.packet_ids.release(msg.mid)
                self._emit(
                    EffectKind.PUBLISH_FAILED,
                    PublishFailure(mid=msg.mid, reason=exc),
                )
                continue
            self._queued.popleft()

    def _replay_session(self) -> None:
        self.flow.reset()
        self.flow.apply_broker_receive_maximum(
            self.negotiated.receive_maximum,
            self.config.local_receive_maximum,
            self.config.max_outbound_inflight,
        )
        self._queued.clear()
        for msg in list(self.store.out_items()):
            try:
                self._validate_outbound_against_negotiated(msg)
            except (ProtocolError, PacketTooLargeError) as exc:
                self._discard_outbound_store_record(msg.mid)
                self.packet_ids.release(msg.mid)
                self._emit(
                    EffectKind.PUBLISH_FAILED,
                    PublishFailure(mid=msg.mid, reason=exc),
                )
                continue
            if msg.state is OutboundQoSState.QUEUED:
                self._queued.append(msg)
                continue
            if not self.flow.try_acquire():
                self._queued.append(msg)
                continue
            try:
                self._retransmit_outbound(msg)
            except Exception as exc:
                self.flow.release()
                self._discard_outbound_store_record(msg.mid)
                self.packet_ids.release(msg.mid)
                self._emit(
                    EffectKind.PUBLISH_FAILED,
                    PublishFailure(mid=msg.mid, reason=exc),
                )
        self._drain_queue()

    def _discard_outbound_store_record(self, mid: int) -> None:
        try:
            self.store.delete_out(mid)
        except Exception:
            # Preserve the original launch/validation failure. A broken store is
            # surfaced separately by the read/client boundary and must not leak
            # flow slots or packet identifiers in memory.
            pass

    def _resolve_inbound_topic(self, packet: PublishPacket) -> str:
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
            self._protocol_disconnect(0x94, f"Invalid topic alias {alias}")
            raise ProtocolError(f"Invalid topic alias {alias}")
        if packet.topic:
            self._topic_aliases[alias] = packet.topic
            return packet.topic
        if alias not in self._topic_aliases:
            self._protocol_disconnect(0x94, f"Unknown topic alias {alias}")
            raise ProtocolError(f"Unknown topic alias {alias}")
        return self._topic_aliases[alias]

    def _check_outbound_size(self, wire: WriteItem) -> None:
        limit = self.negotiated.maximum_packet_size
        size = item_size(wire)
        if limit is not None and size > limit:
            raise PacketTooLargeError(
                f"Encoded packet size {size} exceeds broker maximum_packet_size {limit}"
            )

    def _check_outbound_publish_budget(
        self,
        topic: str,
        payload: bytes,
        qos: QoS,
        properties: Properties | None,
    ) -> None:
        """Cheap upper bound vs negotiated maximum_packet_size (before full encode)."""
        limit = self.negotiated.maximum_packet_size
        if limit is None:
            return
        topic_len = len(topic.encode("utf-8"))
        props_len = 0
        if self.config.protocol == MQTTProtocolVersion.MQTTv5:
            # Worst case: encode_properties; empty → 1 byte.
            props_len = len(encode_properties(properties, PUBLISH))
        mid_len = 2 if qos else 0
        # Fixed header worst case: 1 type + 4 VBI bytes.
        estimate = 1 + 4 + 2 + topic_len + mid_len + props_len + len(payload)
        if estimate > limit:
            # Exact check — estimate is pessimistic on VBI length.
            wire = PublishPacket(
                topic=topic,
                payload=payload,
                qos=qos,
                retain=False,
                dup=False,
                mid=1 if qos else None,
                properties=properties,
            ).encode_write_item(self.config.protocol)
            self._check_outbound_size(wire)

    def _check_subscribe_capabilities(
        self,
        topic: str,
        properties: Properties | None,
    ) -> None:
        if topic.startswith("$share/") and not self.negotiated.shared_subscription_available:
            raise ProtocolError("Broker does not support shared subscriptions")
        if ("+" in topic or "#" in topic) and not self.negotiated.wildcard_subscription_available:
            raise ProtocolError("Broker does not support wildcard subscriptions")
        if (
            properties
            and properties.get("subscription_identifier") is not None
            and not self.negotiated.subscription_identifier_available
        ):
            raise ProtocolError("Broker does not support subscription identifiers")

    def _acquire_inbound_slot(self) -> None:
        limit = self.config.local_receive_maximum
        if self._inbound_inflight >= limit:
            # MQTT 5 §3.3.4: DISCONNECT 0x93 (Receive Maximum exceeded).
            self._protocol_disconnect(0x93, "Receive Maximum exceeded")
            raise ProtocolError("Receive Maximum exceeded")
        self._inbound_inflight += 1

    def _protocol_disconnect(self, reason_code: int, message: str) -> None:
        """Emit a normative DISCONNECT before the transport is torn down."""
        if self.config.protocol == MQTTProtocolVersion.MQTTv5:
            self._send(encode_disconnect(reason_code, self.config.protocol))
        self.state = ConnectionState.DISCONNECTED
        self._emit(
            EffectKind.DISCONNECTED,
            DisconnectInfo(reason_code=reason_code, from_broker=False),
        )

    def _release_inbound_slot(self) -> None:
        if self._inbound_inflight > 0:
            self._inbound_inflight -= 1

    def _update_session_resume_preference(self) -> None:
        """Next CONNECT should use Clean Start 0 when the session is durable."""
        if self.config.protocol == MQTTProtocolVersion.MQTTv5:
            expiry = self.negotiated.session_expiry_interval
            self._prefer_session_resume = bool(expiry)
        else:
            self._prefer_session_resume = not self.config.clean_start

    def _validate_outbound_against_negotiated(self, msg: OutboundMessage) -> None:
        if msg.state is OutboundQoSState.WAIT_PUBCOMP:
            pubrel = msg.encoded_pubrel
            if pubrel is None:
                pubrel = PubRelPacket(mid=msg.mid).encode(self.config.protocol)
            self._check_outbound_size(pubrel)
            return
        if int(msg.qos) > self.negotiated.maximum_qos:
            raise ProtocolError(
                f"QoS {int(msg.qos)} exceeds broker maximum_qos {self.negotiated.maximum_qos}"
            )
        if msg.retain and not self.negotiated.retain_available:
            raise ProtocolError("Broker does not support retain")
        if msg.properties and msg.properties.get("topic_alias") is not None:
            alias = int(msg.properties.get("topic_alias"))
            if alias > self.negotiated.topic_alias_maximum:
                raise ProtocolError(
                    f"topic_alias {alias} exceeds broker topic_alias_maximum "
                    f"{self.negotiated.topic_alias_maximum}"
                )
        if msg.encoded_publish is not None:
            self._check_outbound_size(msg.encoded_publish)
        else:
            self._check_outbound_publish_budget(msg.topic, msg.payload, msg.qos, msg.properties)

    def _fail_queued_violating_negotiation(self) -> None:
        kept: deque[OutboundMessage] = deque()
        while self._queued:
            msg = self._queued.popleft()
            try:
                self._validate_outbound_against_negotiated(msg)
            except (ProtocolError, PacketTooLargeError) as exc:
                self._discard_outbound_store_record(msg.mid)
                self.packet_ids.release(msg.mid)
                self._emit(
                    EffectKind.PUBLISH_FAILED,
                    PublishFailure(mid=msg.mid, reason=exc),
                )
                continue
            kept.append(msg)
        self._queued = kept
