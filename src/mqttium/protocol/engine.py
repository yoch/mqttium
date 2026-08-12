"""Synchronous MQTT protocol engine.

No asyncio, no sockets, no user callbacks. Feed packets / commands in, collect
effects out. This is the correctness core that AsyncClient adapts.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from typing import Any

from mqttium.codec.buffer import RawPacket
from mqttium.codec.packet_validation import validate_raw_packet
from mqttium.enums import (
    ConnectionState,
    InboundQoSState,
    MQTTProtocolVersion,
    PacketType,
    QoS,
)
from mqttium.packets import (
    AuthPacket,
    ConnAckPacket,
    ConnectPacket,
    DisconnectPacket,
    PublishPacket,
    SubAckPacket,
    SubscribeOptions,
    SubscribePacket,
    Subscription,
    UnsubAckPacket,
    UnsubscribePacket,
    encode_disconnect,
    encode_pingreq,
)
from mqttium.persistence.memory import (
    InflightStore,
    MemoryInflightStore,
)
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.effects import (
    DisconnectInfo,
    EffectKind,
    EngineEffect,
    PublishFailure as PublishFailure,
    PublishHandle,
)
from mqttium.protocol.flow_control import FlowControl
from mqttium.protocol.inbound import InboundSession
from mqttium.protocol.negotiated import NegotiatedSettings
from mqttium.protocol.outbound import OutboundSession
from mqttium.protocol.packet_ids import PacketIdPool
from mqttium.topics import validate_publish_topic, validate_subscribe_filter
from mqttium.transport.writes import WriteItem, item_size
from mqttium.types import (
    OutboundMessage,
    OutboundMessageSummary,
    Properties,
)
from mqttium.errors import (
    MalformedPacketError,
    NotConnectedError,
    PacketTooLargeError,
    ProtocolError,
)


# MQTT 5 Table 3-11: 0x00 (Success) is sent by the Server only.
_CLIENT_AUTH_REASON_CODES = frozenset({0x18, 0x19})

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


class ProtocolEngine:
    """Pure MQTT session/QoS state machine."""

    def __init__(
        self,
        config: EngineConfig | None = None,
        store: InflightStore | None = None,
    ) -> None:
        self.config = config or EngineConfig()
        self.config._attached = True
        self.store = store or MemoryInflightStore()
        self.state = ConnectionState.NEW
        self.session_present = False
        self.negotiated = NegotiatedSettings()
        self._pending_connect = False
        self._effects: list[EngineEffect] = []
        # Each direction owns its protocol state; the engine keeps connection
        # state, packet dispatch and the one ordered effect stream.
        self.outbound = OutboundSession(self)
        self.inbound = InboundSession(self)
        # After a durable session is established, next CONNECT uses Clean Start 0.
        self._prefer_session_resume = False
        self._sent_clean_start = False
        self._sent_session_expiry_interval: int | None = None
        self._sent_request_problem_information = 1
        self._auth_method: str | None = None
        self._handlers = {
            PacketType.CONNACK: self._on_connack,
            PacketType.PUBLISH: self.inbound.on_publish,
            PacketType.PUBACK: self.outbound.on_puback,
            PacketType.PUBREC: self.outbound.on_pubrec,
            PacketType.PUBREL: self.inbound.on_pubrel,
            PacketType.PUBCOMP: self.outbound.on_pubcomp,
            PacketType.SUBACK: self._on_suback,
            PacketType.UNSUBACK: self._on_unsuback,
            PacketType.PINGRESP: self._on_pingresp,
            PacketType.PINGREQ: self._on_pingreq,
            PacketType.DISCONNECT: self._on_disconnect,
            PacketType.AUTH: self._on_auth,
        }
        # Hydrate packet ids + offline queue from a durable store (restart).
        self.outbound.hydrate()
        # MIDs of in-flight SUBSCRIBE/UNSUBSCRIBE (never collide with PUBLISH).
        self._pending_sub_mids: set[int] = set()
        # Keep the exact request shape so an acknowledgement cannot complete a
        # request merely because it happens to reuse the same packet identifier.
        self._pending_sub_requests: dict[int, tuple[PacketType, int]] = {}
        # Auto-ACK QoS1 exchanges whose PUBACK effect has been emitted but not
        # handed off from the synchronous engine yet. While these remain in the
        # current effect batch, a broker has necessarily pipelined any following
        # PUBLISH before that acknowledgement could leave the engine/runtime
        # boundary, so they still consume the advertised Receive Maximum.
        self._pending_auto_qos1_mids: set[int] = set()

    def take_effects(self) -> list[EngineEffect]:
        effects = self._effects
        self._effects = []
        if effects:
            # Handing the complete effect batch to the runtime is the first
            # boundary at which an auto-PUBACK can make progress toward the
            # transport. Do not keep it counted against a later engine batch.
            self._pending_auto_qos1_mids.clear()
        return effects

    @property
    def has_pending_effects(self) -> bool:
        return bool(self._effects)

    def reconfigure(self, **changes: Any) -> None:
        """Validate and apply fields that are safe to change after construction."""
        self.config.update(**changes)

    # --- outbound facade ---------------------------------------------------
    # The session owns this state. These views keep the engine's public surface
    # (and the tests, benchmarks and fuzzer that use it) exactly as it was when
    # these were plain attributes — including assignment, which tests use to
    # inject instrumented pools.

    @property
    def packet_ids(self) -> PacketIdPool:
        return self.outbound.packet_ids

    @packet_ids.setter
    def packet_ids(self, pool: PacketIdPool) -> None:
        self.outbound.packet_ids = pool

    @property
    def flow(self) -> FlowControl:
        return self.outbound.flow

    @flow.setter
    def flow(self, flow: FlowControl) -> None:
        self.outbound.flow = flow

    @property
    def _queued(self) -> deque[OutboundMessage | OutboundMessageSummary]:
        return self.outbound._queued

    @property
    def _paged_store(self) -> object | None:
        """Compatibility view of the outbound store pagination capability."""
        return self.outbound._paged_store

    @property
    def pending_outbound_messages(self) -> int:
        return self.outbound.pending_messages

    @property
    def pending_outbound_bytes(self) -> int:
        return self.outbound.pending_bytes

    # --- inbound facade ----------------------------------------------------
    # Preserve the diagnostic/test surface that pre-dates InboundSession while
    # keeping the state itself under one owner.

    @property
    def _topic_aliases(self) -> dict[int, str]:
        return self.inbound._aliases

    @property
    def _inbound_inflight(self) -> int:
        return self.inbound._inflight

    @_inbound_inflight.setter
    def _inbound_inflight(self, value: int) -> None:
        self.inbound._inflight = value

    @property
    def _recovered_inbound_mids(self) -> set[int]:
        return self.inbound._recovered_mids

    @_recovered_inbound_mids.setter
    def _recovered_inbound_mids(self, value: set[int]) -> None:
        self.inbound._recovered_mids = value

    def can_ever_admit_publish(
        self,
        topic: str,
        payload: bytes,
        qos: QoS | int,
        properties: Properties | None = None,
    ) -> bool:
        return self.outbound.can_ever_admit(topic, payload, qos, properties)

    def can_ever_admit_publish_many(
        self,
        messages: Iterable[tuple[str, bytes, QoS | int, bool, Properties | None]],
    ) -> bool:
        return self.outbound.can_ever_admit_many(messages)

    def _emit(self, kind: EffectKind, data: Any = None) -> None:
        self._effects.append(EngineEffect(kind=kind, data=data))

    def _send(self, packet: WriteItem) -> None:
        self._emit(EffectKind.SEND, packet)

    def begin_connect(self) -> bytes:
        if self.state in (ConnectionState.CONNECTED, ConnectionState.CONNECTING):
            raise ProtocolError("Already connected or connecting")

        clean_start = self.config.clean_start
        if self._prefer_session_resume:
            clean_start = False
        configured_auth_method = None
        if self.config.connect_properties is not None:
            configured_auth_method = self.config.connect_properties.get("authentication_method")
        if (
            self.config.protocol == MQTTProtocolVersion.MQTTv311
            and self.config.password is not None
            and self.config.username is None
        ):
            # [MQTT-3.1.2-22]: under MQTT 3.1.1, if the User Name Flag is 0 the
            # Password Flag MUST be 0. MQTT 5 lifted the restriction, so this is
            # deliberately version-specific rather than a general rule.
            raise ProtocolError(
                "MQTT 3.1.1 does not allow a password without a username "
                "[MQTT-3.1.2-22]; connect with MQTT 5 or supply a username"
            )
        if self.config.accept_auth:
            if self.config.protocol != MQTTProtocolVersion.MQTTv5:
                raise ProtocolError("Enhanced authentication requires MQTT 5")
            if not configured_auth_method:
                raise ProtocolError(
                    "auth_handler requires authentication_method in CONNECT properties"
                )
        if (
            self.config.protocol == MQTTProtocolVersion.MQTTv311
            and not self.config.client_id
            and not clean_start
        ):
            # [MQTT-3.1.3-7]: a zero-byte ClientId requires CleanSession 1. The
            # broker is required to answer 0x02 (Identifier rejected) and close
            # ([MQTT-3.1.3-8]), so this connection cannot succeed — failing here
            # says why instead of surfacing a bare rejection. MQTT 5 allows the
            # combination and assigns an identifier, hence the version check.
            # `clean_start` is the effective value: a resumed session forces it
            # to 0, which is exactly when this would otherwise slip through.
            raise ProtocolError(
                "MQTT 3.1.1 requires clean_start=True with an empty client_id "
                "[MQTT-3.1.3-7]; set a client_id or connect with MQTT 5"
            )

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
        if will is not None:
            validate_publish_topic(will.topic)
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
        wire = packet.encode()

        # Commit connection state only after every validation and the encoding
        # have succeeded. A synchronous configuration error must leave the
        # engine reusable rather than stranded in CONNECTING.
        self._auth_method = (
            str(configured_auth_method) if configured_auth_method is not None else None
        )
        self._sent_clean_start = clean_start
        self._sent_session_expiry_interval = (
            connect_props.get("session_expiry_interval") if connect_props is not None else None
        )
        request_problem_information = (
            connect_props.get("request_problem_information") if connect_props is not None else None
        )
        self._sent_request_problem_information = (
            1 if request_problem_information is None else int(request_problem_information)
        )
        self._pending_auto_qos1_mids.clear()
        self.state = ConnectionState.CONNECTING
        self._pending_connect = True
        self.inbound.start_connection()
        # Negotiated capabilities are connection-scoped. Queued messages are
        # validated against the new values only after the next CONNACK.
        self.negotiated = NegotiatedSettings()
        return wire

    def queue_publish(
        self,
        topic: str,
        payload: bytes = b"",
        *,
        qos: QoS | int = QoS.AT_MOST_ONCE,
        retain: bool = False,
        properties: Properties | None = None,
    ) -> PublishHandle:
        if self.state is ConnectionState.DISCONNECTING:
            raise NotConnectedError("publish is not allowed while disconnecting")
        return self.outbound.queue_publish(
            topic, payload, qos=qos, retain=retain, properties=properties
        )

    def queue_publish_many(
        self,
        messages: Iterable[tuple[str, bytes, QoS | int, bool, Properties | None]],
    ) -> list[PublishHandle]:
        if self.state is ConnectionState.DISCONNECTING:
            raise NotConnectedError("publish is not allowed while disconnecting")
        return self.outbound.queue_publish_many(messages)

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
            options = SubscribeOptions(qos=QoS(qos))
            validate_subscribe_filter(topics)
            self._check_subscribe_capabilities(topics, options, properties)
            subscriptions.append(Subscription(topic=topics, options=options))
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
                self._check_subscribe_capabilities(topic, options, properties)
                subscriptions.append(Subscription(topic=topic, options=options))

        mid = self.outbound.packet_ids.allocate()
        try:
            packet = SubscribePacket(
                mid=mid,
                subscriptions=tuple(subscriptions),
                properties=properties,
            )
            wire = packet.encode(self.config.protocol)
            self._check_outbound_size(wire)
        except Exception:
            self.outbound.packet_ids.release(mid)
            raise
        self._pending_sub_mids.add(mid)
        self._pending_sub_requests[mid] = (PacketType.SUBACK, len(subscriptions))
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
        mid = self.outbound.packet_ids.allocate()
        try:
            packet = UnsubscribePacket(mid=mid, topics=topic_list)
            wire = packet.encode(self.config.protocol)
            self._check_outbound_size(wire)
        except Exception:
            self.outbound.packet_ids.release(mid)
            raise
        self._pending_sub_mids.add(mid)
        self._pending_sub_requests[mid] = (PacketType.UNSUBACK, len(topic_list))
        self._send(wire)
        return mid

    def queue_ping(self) -> None:
        if self.state != ConnectionState.CONNECTED:
            raise NotConnectedError("PINGREQ requires an active connection")
        wire = encode_pingreq()
        self._check_outbound_size(wire)
        self._send(wire)

    def begin_disconnect(
        self,
        reason_code: int = 0,
        properties: Properties | None = None,
    ) -> bytes:
        if self.state != ConnectionState.CONNECTED:
            raise NotConnectedError("disconnect requires an active connection")
        self._check_disconnect_session_expiry(properties)
        wire = encode_disconnect(reason_code, self.config.protocol, properties)
        self._check_outbound_size(wire)
        self.state = ConnectionState.DISCONNECTING
        return wire

    def _check_disconnect_session_expiry(self, properties: Properties | None) -> None:
        """Refuse extending a session that was never allowed to outlive the connection.

        MQTT 5 §3.14.2.2.2: "If the Session Expiry Interval in the CONNECT packet
        was zero, then it is a Protocol Error to set a non-zero Session Expiry
        Interval in the DISCONNECT packet sent by the Client." A broker can treat
        that as an ungraceful disconnect, which may publish a configured Will.
        Failing locally preserves the semantics requested by clean shutdown.
        """
        if self.config.protocol != MQTTProtocolVersion.MQTTv5 or properties is None:
            return
        requested = properties.get("session_expiry_interval")
        if not requested:
            return
        # An absent Session Expiry Interval in CONNECT means zero (§3.1.2.11.2).
        # Use the value encoded on the wire, not the application-owned mutable
        # Properties object retained in EngineConfig.
        if not self._sent_session_expiry_interval:
            raise ProtocolError(
                "Cannot set a non-zero session_expiry_interval on DISCONNECT when "
                "CONNECT declared none (MQTT 5 §3.14.2.2.2); this can make the "
                "shutdown ungraceful and publish a configured Will"
            )

    def notify_transport_closed(self) -> None:
        was = self.state
        self.state = ConnectionState.DISCONNECTED
        self.inbound.transport_closed()
        self._pending_connect = False
        self._pending_auto_qos1_mids.clear()
        # Release sub/unsub MIDs still in flight — no ACK will arrive now.
        if self._pending_sub_mids:
            if self.outbound.pending_messages == 0:
                # No publish MID survives this transport: reset in constant time.
                self.outbound.packet_ids.clear()
            else:
                for mid in self._pending_sub_mids:
                    self.outbound.packet_ids.release(mid)
            self._pending_sub_mids.clear()
        self._pending_sub_requests.clear()
        if was != ConnectionState.DISCONNECTED:
            self._emit(EffectKind.DISCONNECTED, DisconnectInfo(from_broker=False))

    def handle_raw(self, raw: RawPacket) -> None:
        # DISCONNECT is the client's final MQTT Control Packet. The peer may still
        # have packets already in flight before the transport actually closes, but
        # dispatching them could emit ACKs or user-visible effects after DISCONNECT.
        if self.state is ConnectionState.DISCONNECTING:
            return
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
            self._check_pending_auto_qos1_receive_maximum(raw)
            effect_start = len(self._effects)
            handler(raw)
            self._validate_new_outbound_effects(effect_start)
            self._track_pending_auto_qos1(effect_start)
        except (ProtocolError, MalformedPacketError, PacketTooLargeError) as exc:
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
                self._protocol_disconnect(0x82)
                raise ProtocolError("CONNACK authentication_method does not match CONNECT")
            if (
                connack.properties is not None
                and connack.properties.get("authentication_data") is not None
                and self._auth_method is None
            ):
                self._protocol_disconnect(0x82)
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

        if self.config.protocol == MQTTProtocolVersion.MQTTv5 and not self.config.client_id:
            assigned_client_id = (
                connack.properties.get("assigned_client_identifier")
                if connack.properties is not None
                else None
            )
            if not assigned_client_id:
                self._protocol_disconnect(0x82)
                raise ProtocolError(
                    "Successful CONNACK for an empty ClientID requires a non-empty "
                    "Assigned Client Identifier [MQTT-3.2.2-16]"
                )

        if connack.session_present and self._sent_clean_start:
            # The Server MUST report Session Present 0 after accepting Clean
            # Start/CleanSession 1: MQTT 5 [MQTT-3.2.2-2], MQTT 3.1.1
            # [MQTT-3.2.2-1]. Local inflight records do not make a stale session
            # valid: replaying them would contradict the clean session the
            # Client explicitly requested.
            self._protocol_disconnect(0x82)
            raise ProtocolError(
                "CONNACK reports Session Present after Clean Start [MQTT-3.2.2-1/-2]"
            )

        self.negotiated = NegotiatedSettings.from_connack(
            connack.properties,
            requested_keepalive=self.config.keepalive,
            requested_session_expiry=self._sent_session_expiry_interval,
            local_client_id=self.config.client_id,
        )
        self.outbound.flow.apply_broker_receive_maximum(
            self.negotiated.receive_maximum,
            self.config.local_receive_maximum,
            self.config.max_outbound_inflight,
        )

        self.state = ConnectionState.CONNECTED
        self.session_present = connack.session_present
        self._update_session_resume_preference()
        outbound = self.outbound
        if not connack.session_present:
            outbound.purge_after_clean_session(sub_mids_pending=bool(self._pending_sub_mids))
            self.inbound.discard_session()
            outbound.flow.reset()
            # Re-apply negotiated limit after reset.
            outbound.flow.apply_broker_receive_maximum(
                self.negotiated.receive_maximum,
                self.config.local_receive_maximum,
                self.config.max_outbound_inflight,
            )
        else:
            outbound.replay_session()

        outbound.fail_queued_violating_negotiation()
        self._emit(EffectKind.CONNACK, connack)
        if connack.session_present:
            self.inbound.replay_session()
        outbound.drain()

    def continue_inbound_replay(self) -> None:
        """Emit the next bounded batch of restart redeliveries.

        Called by the runtime when it applies `CONTINUE_INBOUND_REPLAY`, i.e.
        once the previous batch has cleared delivery backpressure.
        """
        self.inbound.drain_replay()

    def mark_inbound_delivered(self, mid: int) -> None:
        self.inbound.mark_delivered(mid)

    def ack(self, mid: int) -> None:
        """Complete a deferred inbound ACK in manual-ack mode."""
        if self.state is not ConnectionState.CONNECTED:
            raise NotConnectedError("ack requires an active connection")
        self.inbound.ack(mid)

    def _on_suback(self, raw: RawPacket) -> None:
        ack = SubAckPacket.decode(raw.remaining, self.config.protocol)
        self._validate_inbound_problem_information(PacketType.SUBACK, ack.properties)
        self._complete_subscription_request(
            ack.mid,
            PacketType.SUBACK,
            len(ack.reason_codes),
        )
        self._emit(EffectKind.SUBACK, ack)

    def _on_unsuback(self, raw: RawPacket) -> None:
        ack = UnsubAckPacket.decode(raw.remaining, self.config.protocol)
        self._validate_inbound_problem_information(PacketType.UNSUBACK, ack.properties)
        reason_count = (
            len(ack.reason_codes) if self.config.protocol is MQTTProtocolVersion.MQTTv5 else None
        )
        self._complete_subscription_request(
            ack.mid,
            PacketType.UNSUBACK,
            reason_count,
        )
        self._emit(EffectKind.UNSUBACK, ack)

    def _complete_subscription_request(
        self,
        mid: int,
        ack_type: PacketType,
        reason_count: int | None,
    ) -> None:
        expected = self._pending_sub_requests.get(mid)
        if mid not in self._pending_sub_mids or expected is None:
            raise ProtocolError(f"{ack_type.name} for unknown mid {mid}")
        expected_type, expected_count = expected
        if ack_type is not expected_type:
            raise ProtocolError(
                f"Expected {expected_type.name} for mid {mid}, received {ack_type.name}"
            )
        if reason_count is not None and reason_count != expected_count:
            raise ProtocolError(
                f"{ack_type.name} for mid {mid} has {reason_count} reason codes; "
                f"expected {expected_count}"
            )
        self._pending_sub_mids.discard(mid)
        self._pending_sub_requests.pop(mid, None)
        self.outbound.packet_ids.release(mid)

    def _on_pingresp(self, raw: RawPacket) -> None:
        if raw.remaining:
            raise MalformedPacketError("PINGRESP remaining length must be 0")
        self._emit(EffectKind.PINGRESP)

    def _on_pingreq(self, raw: RawPacket) -> None:
        # Brokers must not send PINGREQ to clients.
        raise ProtocolError("Unexpected PINGREQ from broker")

    def _validate_inbound_problem_information(
        self,
        packet_type: PacketType,
        properties: Properties | None,
    ) -> None:
        """Enforce the Request Problem Information obligation negotiated in CONNECT."""
        if (
            self.config.protocol != MQTTProtocolVersion.MQTTv5
            or self._sent_request_problem_information != 0
            or packet_type in (PacketType.PUBLISH, PacketType.CONNACK, PacketType.DISCONNECT)
            or properties is None
        ):
            return
        if properties.get("reason_string") is None and properties.get("user_property") is None:
            return
        self._protocol_disconnect(0x82)
        raise ProtocolError(
            f"{packet_type.name} includes problem information after Request Problem Information=0"
        )

    def _on_disconnect(self, raw: RawPacket) -> None:
        packet = DisconnectPacket.decode(raw.remaining, self.config.protocol)
        if (
            self.config.protocol is MQTTProtocolVersion.MQTTv5
            and packet.properties is not None
            and packet.properties.get("session_expiry_interval") is not None
        ):
            self.state = ConnectionState.DISCONNECTED
            self._emit(
                EffectKind.DISCONNECTED,
                DisconnectInfo(
                    reason_code=packet.reason_code,
                    properties=packet.properties,
                    from_broker=True,
                ),
            )
            raise ProtocolError(
                "Server must not send Session Expiry Interval in DISCONNECT [MQTT-3.14.2-2]"
            )
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
            self._protocol_disconnect(0x82)
            raise ProtocolError("AUTH is not valid in MQTT 3.1.1")
        packet = AuthPacket.decode(raw.remaining, self.config.protocol)
        self._validate_inbound_problem_information(PacketType.AUTH, packet.properties)
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
        if packet.reason_code == 0x19:
            # MQTT 5 Table 3-11 assigns Re-authenticate to Client→Server only.
            self._protocol_disconnect(0x82)
            raise ProtocolError("Broker must not send Re-authenticate AUTH (0x19)")
        if not self.config.accept_auth:
            # The peer continued an exchange the runtime cannot service. This is
            # a protocol failure on the active connection, not a CONNACK refusal.
            self._protocol_disconnect(0x82)
            return
        self._emit(EffectKind.AUTH, packet)

    def _protocol_disconnect(self, reason_code: int) -> None:
        """Tear the connection down, announcing why when the version allows it."""
        if self.config.protocol == MQTTProtocolVersion.MQTTv5:
            # MQTT 3.1.1 DISCONNECT carries no reason code. MQTT 5 does, but a
            # negotiated Maximum Packet Size can make even the normative error
            # packet impossible to send. Connection state must still become
            # terminal in that case.
            packet = encode_disconnect(reason_code, self.config.protocol)
            try:
                self._check_outbound_size(packet)
            except PacketTooLargeError:
                pass
            else:
                self._send(packet)
        self.state = ConnectionState.DISCONNECTED
        self._emit(
            EffectKind.DISCONNECTED,
            DisconnectInfo(reason_code=reason_code, from_broker=False),
        )

    def _reject_auth_method(self) -> None:
        self._protocol_disconnect(0x82)

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
        if reason_code not in _CLIENT_AUTH_REASON_CODES:
            # MQTT 5 Table 3-11 assigns a sender to each Authenticate Reason
            # Code: 0x00 (Success) is the Server's, and only 0x18 (Continue
            # authentication) and 0x19 (Re-authenticate) may come from a Client.
            # [MQTT-3.15.2-1] requires the sender to use one of them.
            raise ProtocolError(
                f"AUTH reason_code 0x{reason_code:02X} is not one a Client may send; "
                "use 0x18 (Continue authentication) or 0x19 (Re-authenticate)"
            )
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
        wire = AuthPacket(
            reason_code=reason_code,
            properties=auth_properties,
        ).encode(self.config.protocol)
        self._check_outbound_size(wire)
        self._send(wire)

    def _check_outbound_size(self, wire: WriteItem) -> None:
        limit = self.negotiated.maximum_packet_size
        size = item_size(wire)
        if limit is not None and size > limit:
            raise PacketTooLargeError(
                f"Encoded packet size {size} exceeds broker maximum_packet_size {limit}"
            )

    def _validate_new_outbound_effects(self, start: int) -> None:
        """Refuse a handler batch if any SEND violates the broker's packet limit.

        Inbound automatic acknowledgements are emitted as effects rather than
        returned from a public queue_* method. Validate the whole newly-produced
        batch before exposing any of it so an oversized PUBACK/PUBREC/PUBCOMP
        cannot escape while its paired MESSAGE remains application-visible.
        """
        if self.negotiated.maximum_packet_size is None:
            return
        try:
            for effect in self._effects[start:]:
                if effect.kind is EffectKind.SEND:
                    self._check_outbound_size(effect.data)
        except PacketTooLargeError:
            del self._effects[start:]
            raise

    def _check_pending_auto_qos1_receive_maximum(self, raw: RawPacket) -> None:
        """Count auto-PUBACKs that have not left the current engine batch yet."""
        pending = self._pending_auto_qos1_mids
        qos_raw = (raw.flags >> 1) & 0x03 if raw.packet_type is PacketType.PUBLISH else 0
        if (
            not pending
            or self.config.manual_ack
            or raw.packet_type is not PacketType.PUBLISH
            or qos_raw not in (int(QoS.AT_LEAST_ONCE), int(QoS.EXACTLY_ONCE))
        ):
            return

        packet = PublishPacket.decode(raw.flags, raw.remaining, self.config.protocol)
        mid = packet.mid
        if mid is None:
            return

        if mid in pending:
            if packet.qos is QoS.AT_LEAST_ONCE:
                # Same QoS1 exchange: retransmission does not consume a second
                # Receive Maximum slot before the pending PUBACK is handed off.
                return
            # The same identifier cannot start a QoS2 exchange while the QoS1
            # exchange is still outstanding from the broker's point of view.
            self._protocol_disconnect(0x82)
            raise ProtocolError(
                f"Inbound packet identifier {mid} reused by QoS 2 while QoS 1 PUBACK is pending"
            )

        transitions = self.inbound._transitions
        existing = transitions.in_meta(mid) if transitions is not None else self.store.get_in(mid)
        if existing is not None:
            state = existing.state
            if packet.qos is QoS.AT_LEAST_ONCE and state is InboundQoSState.WAIT_PUBACK:
                return
            if packet.qos is QoS.EXACTLY_ONCE and state in (
                InboundQoSState.WAIT_PUBREL,
                InboundQoSState.WAIT_USER_ACK,
            ):
                return
            # Any other existing state is a cross-QoS packet-identifier
            # collision. Let InboundSession report that protocol error rather
            # than misclassifying it as Receive Maximum exhaustion.
            return

        if self.inbound._inflight + len(pending) >= self.config.local_receive_maximum:
            self._protocol_disconnect(0x93)
            raise ProtocolError(
                f"Receive Maximum exceeded by QoS {int(packet.qos)} PUBLISH received "
                "before pending PUBACK handoff"
            )

    def _track_pending_auto_qos1(self, start: int) -> None:
        """Remember auto-PUBACK packet ids until the effect batch is handed off."""
        if self.config.manual_ack:
            return
        for effect in self._effects[start:]:
            if effect.kind is not EffectKind.SEND:
                continue
            wire = effect.data
            if not isinstance(wire, bytes) or len(wire) < 4:
                continue
            if wire[0] & 0xF0 != int(PacketType.PUBACK):
                continue
            self._pending_auto_qos1_mids.add((wire[2] << 8) | wire[3])

    def _check_subscribe_capabilities(
        self,
        topic: str,
        options: SubscribeOptions,
        properties: Properties | None,
    ) -> None:
        if topic.startswith("$share/") and options.no_local:
            raise ProtocolError("No Local must be 0 for a shared subscription")
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

    def _update_session_resume_preference(self) -> None:
        """Next CONNECT should use Clean Start 0 when the session is durable."""
        if self.config.protocol == MQTTProtocolVersion.MQTTv5:
            expiry = self.negotiated.session_expiry_interval
            self._prefer_session_resume = bool(expiry)
        else:
            self._prefer_session_resume = not self.config.clean_start
