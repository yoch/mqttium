"""Synchronous MQTT protocol engine.

No asyncio, no sockets, no user callbacks. Feed packets / commands in, collect
effects out. This is the correctness core that AsyncClient adapts.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable, KeysView
from typing import Any

from mqttium.codec.buffer import RawPacket
from mqttium.codec.packet_validation import validate_raw_packet
from mqttium.enums import (
    ConnectionState,
    MQTTProtocolVersion,
    PacketType,
    QoS,
)
from mqttium.packets import (
    AuthPacket,
    ConnAckPacket,
    ConnectPacket,
    SubAckPacket,
    SubscribeOptions,
    Subscription,
    UnsubAckPacket,
)
from mqttium.packets._bindings import CodecBindings, bind_codec
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
        # Specialized codecs are bound once; handlers and sessions never branch
        # on protocol per packet.
        self.codec: CodecBindings = bind_codec(self.config.protocol)
        # Each direction owns its protocol state; the engine keeps connection
        # state, packet dispatch and the one ordered effect stream.
        self.outbound = OutboundSession(self)
        self.inbound = InboundSession(self)
        # After a durable session is established, next CONNECT uses Clean Start 0.
        self._prefer_session_resume = False
        self._sent_clean_start = False
        self._sent_session_expiry_interval: int | None = None
        self._sent_request_problem_information = 1
        self._sent_request_response_information = 0
        self._auth_method: str | None = None
        # True only between a Client Re-authenticate AUTH (0x19) and the
        # Server's terminal AUTH Success (0x00). Initial enhanced authentication
        # is represented by CONNECTING and completes with CONNACK instead.
        self._reauth_in_progress = False
        self._handlers = {
            PacketType.CONNACK: self._on_connack,
            PacketType.PUBLISH: self.inbound.handle_publish,
            PacketType.PUBACK: self.outbound.on_puback,
            PacketType.PUBREC: self.outbound.on_pubrec,
            PacketType.PUBREL: self.inbound.on_pubrel,
            PacketType.PUBCOMP: self.outbound.on_pubcomp,
            PacketType.SUBACK: self._on_suback,
            PacketType.UNSUBACK: self._on_unsuback,
            PacketType.PINGRESP: self._on_pingresp,
            PacketType.DISCONNECT: self._on_disconnect,
            PacketType.AUTH: self._on_auth,
        }
        # What a state accepts and what handles it are one table, so `handle_raw`
        # resolves both in a single lookup and adding a packet type is one edit.
        # Anything absent is refused — PINGREQ included.
        self._handlers_by_state = {
            state: {ptype: self._handlers[ptype] for ptype in allowed}
            for state, allowed in _ALLOWED_PACKETS_BY_STATE.items()
        }
        # Hydrate packet ids + offline queue from a durable store (restart).
        self.outbound.hydrate()
        # In-flight SUBSCRIBE/UNSUBSCRIBE by MID (never collides with PUBLISH).
        # The exact request shape is kept so an acknowledgement cannot complete
        # a request merely because it happens to reuse the same packet
        # identifier.
        self._pending_sub_requests: dict[int, tuple[PacketType, int]] = {}

    def take_effects(self) -> list[EngineEffect]:
        effects = self._effects
        self._effects = []
        # Handing the complete effect batch to the runtime is the first
        # boundary at which an auto-PUBACK can make progress toward the
        # transport. Release the Receive Maximum slots held for this batch.
        # `_autoack_handoff_required` is only ever set alongside a non-empty
        # mid set, so an empty set means there is nothing to release.
        inbound = self.inbound
        if inbound._pending_auto_qos1_mids:
            inbound.release_pending_auto_qos1()
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

    @property
    def _pending_sub_mids(self) -> KeysView[int]:
        """Read-only view of the in-flight SUBSCRIBE/UNSUBSCRIBE identifiers.

        `_pending_sub_requests` is the one source of truth; this keeps the
        diagnostic surface used by tests and the fuzzer.
        """
        return self._pending_sub_requests.keys()

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

    def _emit(
        self,
        kind: EffectKind,
        data: Any = None,
        *,
        requires_delivery_mark: bool = False,
        decoded_property_wire_size: int | None = None,
    ) -> None:
        self._effects.append(
            EngineEffect(
                kind=kind,
                data=data,
                requires_delivery_mark=requires_delivery_mark,
                decoded_property_wire_size=decoded_property_wire_size,
            )
        )

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
        self._reauth_in_progress = False
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
        request_response_information = (
            connect_props.get("request_response_information") if connect_props is not None else None
        )
        self._sent_request_response_information = (
            0 if request_response_information is None else int(request_response_information)
        )
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
        items = (topics,) if isinstance(topics, str) else topics
        for item in items:
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

        subs = tuple(subscriptions)
        return self._send_subscription_request(
            lambda mid: self.codec.encode_subscribe(mid, subs, properties),
            PacketType.SUBACK,
            len(subs),
        )

    def queue_unsubscribe(self, topics: str | Iterable[str]) -> int:
        if self.state != ConnectionState.CONNECTED:
            raise NotConnectedError("unsubscribe requires an active connection")
        topic_list = (topics,) if isinstance(topics, str) else tuple(topics)
        if not topic_list:
            raise ProtocolError("unsubscribe requires at least one topic")
        for topic in topic_list:
            validate_subscribe_filter(topic)
        return self._send_subscription_request(
            lambda mid: self.codec.encode_unsubscribe(mid, topic_list),
            PacketType.UNSUBACK,
            len(topic_list),
        )

    def _send_subscription_request(
        self,
        encode: Callable[[int], WriteItem],
        ack_type: PacketType,
        expected_count: int,
    ) -> int:
        """Allocate a MID, encode and send, registering the expected ACK shape."""
        mid = self.outbound.packet_ids.allocate()
        try:
            wire = encode(mid)
            self._check_outbound_size(wire)
        except Exception:
            self.outbound.packet_ids.release(mid)
            raise
        self._pending_sub_requests[mid] = (ack_type, expected_count)
        self._send(wire)
        return mid

    def queue_ping(self) -> None:
        if self.state != ConnectionState.CONNECTED:
            raise NotConnectedError("PINGREQ requires an active connection")
        wire = self.codec.encode_pingreq()
        self._check_outbound_size(wire)
        self._send(wire)

    def begin_disconnect(
        self,
        reason_code: int = 0,
        properties: Properties | None = None,
    ) -> bytes:
        if self.state not in (ConnectionState.CONNECTING, ConnectionState.CONNECTED):
            raise NotConnectedError("disconnect requires an active connection")
        self._check_disconnect_session_expiry(properties)
        wire = self.codec.encode_disconnect(reason_code, properties)
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
        if not self.codec.is_mqtt5 or properties is None:
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
        self._reauth_in_progress = False
        # Release sub/unsub MIDs still in flight — no ACK will arrive now.
        if self._pending_sub_requests:
            if self.outbound.pending_messages == 0:
                # No publish MID survives this transport: reset in constant time.
                self.outbound.packet_ids.clear()
            else:
                for mid in self._pending_sub_requests:
                    self.outbound.packet_ids.release(mid)
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
            handlers = self._handlers_by_state.get(self.state)
            handler = handlers.get(raw.packet_type) if handlers is not None else None
            if handler is None:
                raise ProtocolError(
                    f"Unexpected {raw.packet_type.name} while state={self.state.name}"
                )
            effect_start = len(self._effects)
            handler(raw)
            if self.negotiated.maximum_packet_size is not None:
                self._validate_new_outbound_effects(effect_start)
        except (ProtocolError, MalformedPacketError, PacketTooLargeError) as exc:
            self._emit(EffectKind.PROTOCOL_ERROR, str(exc))
        except Exception as exc:
            # Isolate store/persistence errors: surface as protocol error rather
            # than killing the read loop with an untyped exception.
            self._emit(EffectKind.PROTOCOL_ERROR, f"Internal handler error: {exc!r}")

    def _on_connack(self, raw: RawPacket) -> None:
        if not self._pending_connect or self.state != ConnectionState.CONNECTING:
            raise ProtocolError("Unexpected CONNACK (already negotiated)")
        session_present, reason_code, properties = self.codec.decode_connack(raw.remaining)
        connack = ConnAckPacket(
            session_present=session_present,
            reason_code=reason_code,
            properties=properties,
        )
        self._pending_connect = False
        if self.codec.is_mqtt5:
            connack_method = (
                connack.properties.get("authentication_method")
                if connack.properties is not None
                else None
            )
            if connack_method is not None and connack_method != self._auth_method:
                self._protocol_disconnect(0x82)
                raise ProtocolError("CONNACK authentication_method does not match CONNECT")
            if (
                connack.reason_code == 0
                and self._auth_method is not None
                and connack_method is None
            ):
                self._protocol_disconnect(0x82)
                raise ProtocolError(
                    "Successful CONNACK must repeat CONNECT authentication_method [MQTT-4.12.0-5]"
                )
            if (
                connack.properties is not None
                and connack.properties.get("authentication_data") is not None
                and self._auth_method is None
            ):
                self._protocol_disconnect(0x82)
                raise ProtocolError("CONNACK authentication_data requires authentication_method")
            if (
                self._sent_request_response_information == 0
                and connack.properties is not None
                and connack.properties.get("response_information") is not None
            ):
                self._protocol_disconnect(0x82)
                raise ProtocolError(
                    "CONNACK response_information was not requested [MQTT-3.1.2-28]"
                )
        if connack.reason_code != 0:
            self._reauth_in_progress = False
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

        if self.codec.is_mqtt5 and not self.config.client_id:
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
        if (
            self.codec.is_mqtt5
            and connack.session_present
            and not self.outbound.has_client_session_state()
            and not self.inbound.has_client_session_state()
        ):
            self._protocol_disconnect(0x82)
            raise ProtocolError(
                "CONNACK reports Session Present but the Client has no Session State [MQTT-3.2.2-4]"
            )

        self.negotiated = NegotiatedSettings.from_connack(
            connack.properties,
            requested_keepalive=self.config.keepalive,
            requested_session_expiry=self._sent_session_expiry_interval,
            local_client_id=self.config.client_id,
        )

        self._reauth_in_progress = False
        self.state = ConnectionState.CONNECTED
        self.session_present = connack.session_present
        self._update_session_resume_preference()
        outbound = self.outbound
        if not connack.session_present:
            outbound.purge_after_clean_session(sub_mids_pending=bool(self._pending_sub_requests))
            self.inbound.discard_session()
            outbound.reset_flow_for_connection()
            # Queued messages were admitted against the previous negotiation
            # (or none at all); check them against the new one now. The
            # resumed-session branch needs no second pass: replay_session()
            # validates every record before it is retransmitted or re-queued.
            outbound.fail_queued_violating_negotiation()
        else:
            # Begins by resetting the flow window to the negotiated limit.
            outbound.replay_session()

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
        mid, reason_codes, properties = self.codec.decode_suback(raw.remaining)
        self._validate_inbound_problem_information(PacketType.SUBACK, properties)
        self._complete_subscription_request(
            mid,
            PacketType.SUBACK,
            len(reason_codes),
        )
        self._emit(
            EffectKind.SUBACK,
            SubAckPacket(mid=mid, reason_codes=reason_codes, properties=properties),
        )

    def _on_unsuback(self, raw: RawPacket) -> None:
        mid, reason_codes, properties = self.codec.decode_unsuback(raw.remaining)
        self._validate_inbound_problem_information(PacketType.UNSUBACK, properties)
        reason_count = len(reason_codes) if self.codec.is_mqtt5 else None
        self._complete_subscription_request(
            mid,
            PacketType.UNSUBACK,
            reason_count,
        )
        self._emit(
            EffectKind.UNSUBACK,
            UnsubAckPacket(mid=mid, reason_codes=reason_codes, properties=properties),
        )

    def _complete_subscription_request(
        self,
        mid: int,
        ack_type: PacketType,
        reason_count: int | None,
    ) -> None:
        expected = self._pending_sub_requests.get(mid)
        if expected is None:
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
        del self._pending_sub_requests[mid]
        self.outbound.packet_ids.release(mid)

    def _on_pingresp(self, raw: RawPacket) -> None:
        if raw.remaining:
            raise MalformedPacketError("PINGRESP remaining length must be 0")
        self._emit(EffectKind.PINGRESP)

    def _validate_inbound_problem_information(
        self,
        packet_type: PacketType,
        properties: Properties | None,
    ) -> None:
        """Enforce the Request Problem Information obligation negotiated in CONNECT.

        PUBLISH, CONNACK and DISCONNECT are exempt by specification
        (§3.1.2.11.7) and are never routed through this check.
        """
        if (
            not self.codec.is_mqtt5
            or self._sent_request_problem_information != 0
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
        reason_code, properties = self.codec.decode_disconnect(raw.remaining)
        self._reauth_in_progress = False
        self.state = ConnectionState.DISCONNECTED
        self._emit(
            EffectKind.DISCONNECTED,
            DisconnectInfo(
                reason_code=reason_code,
                properties=properties,
                from_broker=True,
            ),
        )
        if (
            self.codec.is_mqtt5
            and properties is not None
            and properties.get("session_expiry_interval") is not None
        ):
            # The DISCONNECTED effect above still stands; this only adds the
            # PROTOCOL_ERROR effect via handle_raw, as before.
            raise ProtocolError(
                "Server must not send Session Expiry Interval in DISCONNECT [MQTT-3.14.2-2]"
            )

    def _on_auth(self, raw: RawPacket) -> None:
        if not self.codec.is_mqtt5:
            self._protocol_disconnect(0x82)
            raise ProtocolError("AUTH is not valid in MQTT 3.1.1")
        reason_code, properties = self.codec.decode_auth(raw.remaining)
        packet = AuthPacket(reason_code=reason_code, properties=properties)
        self._validate_inbound_problem_information(PacketType.AUTH, packet.properties)
        if self._auth_method is None:
            self._protocol_disconnect(0x82)
            raise ProtocolError("Broker AUTH is invalid without authentication_method in CONNECT")
        packet_method = (
            packet.properties.get("authentication_method")
            if packet.properties is not None
            else None
        )
        if packet_method != self._auth_method:
            self._protocol_disconnect(0x82)
            if packet_method is None:
                raise ProtocolError(
                    "AUTH must repeat CONNECT authentication_method [MQTT-4.12.0-5]"
                )
            raise ProtocolError("AUTH authentication_method does not match CONNECT")
        if packet.reason_code == 0x19:
            # MQTT 5 Table 3-11 assigns Re-authenticate to Client→Server only.
            self._protocol_disconnect(0x82)
            raise ProtocolError("Broker must not send Re-authenticate AUTH (0x19)")
        if self.state is ConnectionState.CONNECTING:
            if packet.reason_code != 0x18:
                self._protocol_disconnect(0x82)
                raise ProtocolError(
                    "Server AUTH during initial authentication must use Continue "
                    "authentication (0x18) [MQTT-4.12.0-2]"
                )
        elif self.state is ConnectionState.CONNECTED:
            if not self._reauth_in_progress:
                self._protocol_disconnect(0x82)
                raise ProtocolError(
                    "Server AUTH while connected requires Client-initiated re-authentication"
                )
            if packet.reason_code == 0x00:
                self._reauth_in_progress = False
            elif packet.reason_code != 0x18:
                self._protocol_disconnect(0x82)
                raise ProtocolError("Invalid Server AUTH reason during re-authentication")
        if not self.config.accept_auth:
            # The peer continued an exchange the runtime cannot service. This is
            # a protocol failure on the active connection, not a CONNACK refusal.
            self._protocol_disconnect(0x82)
            return
        self._emit(EffectKind.AUTH, packet)

    def _protocol_disconnect(self, reason_code: int) -> None:
        """Tear the connection down, announcing why when the version allows it."""
        if self.codec.is_mqtt5:
            # MQTT 3.1.1 DISCONNECT carries no reason code. MQTT 5 does, but a
            # negotiated Maximum Packet Size can make even the normative error
            # packet impossible to send. Connection state must still become
            # terminal in that case.
            packet = self.codec.encode_disconnect(reason_code)
            try:
                self._check_outbound_size(packet)
            except PacketTooLargeError:
                pass
            else:
                self._send(packet)
        self._reauth_in_progress = False
        self.state = ConnectionState.DISCONNECTED
        self._emit(
            EffectKind.DISCONNECTED,
            DisconnectInfo(reason_code=reason_code, from_broker=False),
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
        if reason_code not in _CLIENT_AUTH_REASON_CODES:
            # MQTT 5 Table 3-11 assigns a sender to each Authenticate Reason
            # Code: 0x00 (Success) is the Server's, and only 0x18 (Continue
            # authentication) and 0x19 (Re-authenticate) may come from a Client.
            # [MQTT-3.15.2-1] requires the sender to use one of them.
            raise ProtocolError(
                f"AUTH reason_code 0x{reason_code:02X} is not one a Client may send; "
                "use 0x18 (Continue authentication) or 0x19 (Re-authenticate)"
            )
        if reason_code == 0x19:
            if self.state is not ConnectionState.CONNECTED:
                raise ProtocolError("Re-authenticate AUTH requires a connected session")
            if self._reauth_in_progress:
                raise ProtocolError("Re-authentication is already in progress")
        elif self.state is ConnectionState.CONNECTED and not self._reauth_in_progress:
            raise ProtocolError("Continue authentication AUTH requires an active re-authentication")
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
        if reason_code == 0x19:
            self._reauth_in_progress = True

    def _check_outbound_size(self, wire: WriteItem) -> None:
        limit = self.negotiated.maximum_packet_size
        if limit is None:
            return
        size = item_size(wire)
        if size > limit:
            raise PacketTooLargeError(
                f"Encoded packet size {size} exceeds broker maximum_packet_size {limit}"
            )

    def _validate_new_outbound_effects(self, start: int) -> None:
        """Refuse a handler batch if any SEND violates the broker's packet limit.

        Inbound automatic acknowledgements are emitted as effects rather than
        returned from a public queue_* method. Validate the whole newly-produced
        batch before exposing any of it so an oversized PUBACK/PUBREC/PUBCOMP
        cannot escape while its paired MESSAGE remains application-visible.

        `handle_raw` only calls this when the broker negotiated a limit, so the
        common case costs no call at all.
        """
        try:
            for effect in self._effects[start:]:
                if effect.kind is EffectKind.SEND:
                    self._check_outbound_size(effect.data)
        except PacketTooLargeError:
            del self._effects[start:]
            raise

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
