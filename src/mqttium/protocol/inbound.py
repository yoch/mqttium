"""Broker→client publication: aliases, QoS state, receive window and replay.

`InboundSession` owns the state retained for inbound PUBLISH handling: topic
aliases, the local Receive Maximum count, persisted QoS 1/2 records and restart
redelivery. `ProtocolEngine` still owns connection state and the shared effect
stream; handlers emit through it so observable effect ordering does not change.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterator
from typing import TYPE_CHECKING, NoReturn

from mqttium.codec.buffer import RawPacket
from mqttium.enums import InboundQoSState, PacketType, QoS
from mqttium.errors import MalformedPacketError, ProtocolError
from mqttium.packets._ack import (
    encode_pubcomp_success as _encode_pubcomp_success,
    encode_puback_success as _encode_puback_success,
    encode_pubrec_success as _encode_pubrec_success,
)
from mqttium.packets._publish import (
    decode_qos0_message_v311,
    decode_qos0_message_v5,
    decode_qos12_fields_v311,
    decode_publish_fields_v5,
)
from mqttium.protocol.effects import EffectKind
from mqttium.protocol._sizing import publish_logical_size
from mqttium.protocol.stats import InboundStats
from mqttium.types import InboundMessage, InboundRecordMeta, Message, Properties

if TYPE_CHECKING:
    from mqttium.protocol.engine import ProtocolEngine


# One replay batch. The scan bound caps the store work a single continuation
# performs even when every record it reads turns out to be already delivered;
# the message and byte bounds cap what the delivery layer has to absorb before
# backpressure is consulted again.
REPLAY_PAGE_SIZE = 256
REPLAY_BATCH_MESSAGES = 64
REPLAY_BATCH_BYTES = 1 << 20


class InboundReplayCursor:
    """Bounded replay page iterator with one-message pushback."""

    __slots__ = ("_page_messages", "_pages", "_pending", "_remaining")

    def __init__(
        self,
        pages: Iterator[tuple[InboundMessage, ...]],
        *,
        remaining: int,
    ) -> None:
        self._pages = pages
        self._page_messages: deque[InboundMessage] | None = None
        self._pending: InboundMessage | None = None
        self._remaining = remaining

    @property
    def bounded_complete(self) -> bool:
        return self._remaining == 0 and self._pending is None and self._page_messages is None

    def begin_page(self) -> bool:
        if self._pending is not None or self._page_messages is not None:
            return True
        page = next(self._pages, None)
        if page is None:
            return False
        self._page_messages = deque(page)
        self._remaining -= len(page)
        return True

    def next_page_message(self) -> InboundMessage | None:
        if self._pending is not None:
            message = self._pending
            self._pending = None
            return message
        messages = self._page_messages
        if messages is None:
            return None
        if not messages:
            self._page_messages = None
            return None
        message = messages.popleft()
        if not messages:
            self._page_messages = None
        return message

    def push_back(self, message: InboundMessage) -> None:
        if self._pending is not None:
            raise AssertionError("replay cursor already has a pending message")
        self._pending = message


class InboundSession:
    """Authoritative inbound QoS and connection-scoped alias state."""

    __slots__ = (
        "_aliases",
        "_ack_tokens",
        "_decode_pubrel",
        "_engine",
        "_inflight",
        "_is_v5",
        "_on_qos1",
        "_autoack_handoff_required",
        "_pending_auto_qos1_mids",
        "_pending_manual_qos1_acks",
        "_manual_qos1_order",
        "_pending_bytes",
        "_pending_high_water_bytes",
        "_receive_maximum",
        "_recovered_mids",
        "_replay",
        "_stored_inbound",
        "_session_state_qos2",
        "_topic_alias_maximum",
        "config",
        "handle_publish",
        "store",
    )

    def __init__(self, engine: ProtocolEngine) -> None:
        self._engine = engine
        # EngineConfig is mutated in place by update(), so this identity remains
        # stable for the engine lifetime (the same contract as OutboundSession).
        self.config = engine.config
        self.store = engine.store
        self._decode_pubrel = engine.codec.decode_pubrel
        # Fixed for the engine's lifetime, like the codec bindings above.
        self._is_v5 = engine.codec.is_mqtt5
        self.handle_publish = self._on_publish_v5 if self._is_v5 else self._on_publish_v311
        # Same one-shot binding as the PUBLISH handler above: manual_ack is not
        # runtime-mutable, so the QoS 1 handler does not re-test it per message.
        self._on_qos1 = self._on_qos1_manual if self.config.manual_ack else self._on_qos1_auto
        self._aliases: dict[int, str] = {}
        self._ack_tokens: dict[int, object] | None = {} if self.config.manual_ack else None
        self._topic_alias_maximum = self.config.topic_alias_maximum
        self._receive_maximum = self.config.local_receive_maximum
        self._inflight = 0
        self._autoack_handoff_required = False
        # Auto-ACK QoS 1 identifiers whose PUBACK is still inside the current
        # effect batch. The Receive Maximum slot stays held until take_effects()
        # (or connection teardown) so a pipelined PUBLISH is admitted by the
        # ordinary acquire path instead of a second decode.
        self._pending_auto_qos1_mids: set[int] = set()
        self._pending_manual_qos1_acks: set[int] = set()
        self._replay: InboundReplayCursor | None = None
        (
            self._recovered_mids,
            self._pending_bytes,
            self._session_state_qos2,
            recovered_qos1,
        ) = self._load_recovered_state()
        self._manual_qos1_order: deque[int] = deque(
            recovered_qos1 if self.config.manual_ack else ()
        )
        self._pending_high_water_bytes = self._pending_bytes
        # Occupancy of the inbound store, not the Receive Maximum counter.
        # Automatic QoS 1 never writes a row, so a durable subscriber whose
        # inbound table is empty must not probe SQLite on every PUBLISH.
        self._stored_inbound = len(self._recovered_mids)

    def _load_recovered_state(self) -> tuple[set[int], int, int, tuple[int, ...]]:
        """Restore identifiers and accounting from the payload-free store index."""
        mids: set[int] = set()
        recovered_qos1: list[int] = []
        pending_bytes = 0
        session_state_qos2 = 0
        for page in self.store.in_index_pages(REPLAY_PAGE_SIZE):
            for meta in page:
                mids.add(meta.mid)
                if meta.state in (
                    InboundQoSState.WAIT_PUBREL,
                    InboundQoSState.WAIT_USER_ACK,
                ):
                    session_state_qos2 += 1
                if meta.state is InboundQoSState.WAIT_PUBACK:
                    recovered_qos1.append(meta.mid)
                if meta.logical_size <= 0:
                    raise ValueError("Persisted inbound logical_size must be positive")
                pending_bytes += meta.logical_size
        return mids, pending_bytes, session_state_qos2, tuple(recovered_qos1)

    # --- lifecycle ---------------------------------------------------------
    # --- lifecycle ---------------------------------------------------------

    def start_connection(self, *, receive_maximum: int, topic_alias_maximum: int) -> None:
        """Reset state scoped to one network connection before CONNECT."""
        self._aliases.clear()
        self._receive_maximum = receive_maximum
        self._topic_alias_maximum = topic_alias_maximum
        self._inflight = 0
        self._autoack_handoff_required = False
        self._pending_auto_qos1_mids.clear()
        self._pending_manual_qos1_acks.clear()
        # A replay belongs to the connection that started it: its continuation
        # effect is dropped with the epoch, so the cursor must go too.
        self._replay = None

    def transport_closed(self) -> None:
        self._aliases.clear()
        self._pending_manual_qos1_acks.clear()
        self.release_pending_auto_qos1()
        # A continuation is scoped to the connection that created its cursor.
        # Direct engine consumers have no runtime epoch filter, so invalidate it
        # here as well as during the next start_connection().
        self._replay = None

    def discard_session(self) -> None:
        """Drop inbound state after CONNACK reports no previous session."""
        self.store.clear_in()
        if self._ack_tokens is not None:
            self._ack_tokens.clear()
        self._recovered_mids.clear()
        self._replay = None
        self._inflight = 0
        self._pending_bytes = 0
        self._stored_inbound = 0
        self._session_state_qos2 = 0
        self._autoack_handoff_required = False
        self._pending_auto_qos1_mids.clear()
        self._pending_manual_qos1_acks.clear()
        self._manual_qos1_order.clear()

    def release_pending_auto_qos1(self) -> None:
        """Free Receive Maximum slots once auto-PUBACKs leave the engine batch."""
        self._autoack_handoff_required = False
        pending = self._pending_auto_qos1_mids
        n = len(pending)
        if not n:
            return
        pending.clear()
        if self._inflight >= n:
            self._inflight -= n
        else:
            self._inflight = 0

    @property
    def replay_pending(self) -> bool:
        """True while restart redelivery still has batches to emit."""
        return self._replay is not None

    def has_client_session_state(self) -> bool:
        """Whether an incomplete inbound QoS 2 exchange can be resumed."""
        return self._session_state_qos2 > 0

    def stats(self) -> InboundStats:
        """Snapshot this session's own accounting."""
        return InboundStats(
            inflight=self._inflight,
            receive_maximum=self._receive_maximum,
            topic_aliases=len(self._aliases),
            replay_pending=self._replay is not None,
            pending_bytes=self._pending_bytes,
            pending_high_water_bytes=max(
                self._pending_high_water_bytes,
                self._pending_bytes,
            ),
            pending_byte_limit=self.config.max_pending_inbound_bytes,
        )

    def _lookup_stored_inbound(self, mid: int) -> InboundRecordMeta | None:
        """Return persisted inbound metadata without probing an empty store."""
        if not self._stored_inbound:
            return None
        return self.store.in_meta(mid)

    def _remember_inbound(self) -> None:
        self._stored_inbound += 1

    def _forget_inbound(self) -> None:
        if self._stored_inbound <= 0:
            raise AssertionError("inbound stored-record count underflow")
        self._stored_inbound -= 1

    def _complete_stored_inbound(
        self, mid: int, expected_state: InboundQoSState, action: str
    ) -> int:
        """Conditionally delete one inbound record and return its logical size."""
        completed = self.store.complete_in(mid, expected_state)
        if completed is None:
            raise RuntimeError(f"Inbound mid={mid} changed while {action}")
        if self._ack_tokens is not None:
            self._ack_tokens.pop(mid, None)
        return completed.logical_size

    def _bind_ack_token(self, message: Message) -> Message:
        """Attach the active exchange identity before application exposure."""
        tokens = self._ack_tokens
        if tokens is not None:
            assert message.mid is not None
            token = tokens.get(message.mid)
            if token is None:
                token = tokens[message.mid] = object()
            object.__setattr__(message, "_ack_token", token)
        return message

    # --- packet handlers ---------------------------------------------------
    # --- packet handlers ---------------------------------------------------

    def _on_publish_v311(self, raw: RawPacket) -> None:
        engine = self._engine
        qos_raw = (raw.flags >> 1) & 0x03
        if qos_raw == int(QoS.AT_MOST_ONCE):
            engine._emit(EffectKind.MESSAGE, decode_qos0_message_v311(raw))
            return
        if qos_raw == 3:
            raise MalformedPacketError("Invalid PUBLISH QoS 3")
        topic, payload, mid, retain, dup = decode_qos12_fields_v311(raw)
        handler = self._on_qos1 if qos_raw == int(QoS.AT_LEAST_ONCE) else self._on_qos2
        handler(
            topic=topic,
            payload=payload,
            mid=mid,
            retain=retain,
            dup=dup,
            properties=None,
        )

    def _on_publish_v5(self, raw: RawPacket) -> None:
        qos_raw = (raw.flags >> 1) & 0x03
        if qos_raw == int(QoS.AT_MOST_ONCE):
            message, property_wire_size = decode_qos0_message_v5(raw)
            properties = message.properties
            assert properties is not None
            if not message.topic or properties.get("topic_alias") is not None:
                topic = self._resolve_topic_fields(message.topic, properties)
                if topic != message.topic:
                    message = Message(
                        topic=topic,
                        payload=message.payload,
                        qos=QoS.AT_MOST_ONCE,
                        retain=message.retain,
                        dup=False,
                        mid=None,
                        properties=properties,
                    )
            decoded_property_wire_size = property_wire_size if properties.values else None
            self._engine._emit(
                (
                    EffectKind.DECODED_MESSAGE
                    if decoded_property_wire_size is not None
                    else EffectKind.MESSAGE
                ),
                message,
                decoded_property_wire_size=decoded_property_wire_size,
            )
            return
        if qos_raw == 3:
            raise MalformedPacketError("Invalid PUBLISH QoS 3")
        qos = QoS(qos_raw)
        (
            topic,
            payload,
            decoded_mid,
            retain,
            dup,
            properties,
            property_wire_size,
        ) = decode_publish_fields_v5(raw, qos)
        if not topic or properties.get("topic_alias") is not None:
            topic = self._resolve_topic_fields(topic, properties)
        decoded_property_wire_size = property_wire_size if properties.values else None
        assert decoded_mid is not None
        handler = self._on_qos1 if qos is QoS.AT_LEAST_ONCE else self._on_qos2
        handler(
            topic=topic,
            payload=payload,
            mid=decoded_mid,
            retain=retain,
            dup=dup,
            properties=properties,
            decoded_property_wire_size=decoded_property_wire_size,
        )

    def _on_qos2(
        self,
        *,
        topic: str,
        payload: bytes,
        mid: int,
        retain: bool,
        dup: bool,
        properties: Properties | None,
        decoded_property_wire_size: int | None = None,
    ) -> None:
        engine = self._engine
        store = self.store
        existing: InboundRecordMeta | None = None
        if self._stored_inbound:
            # One lookup answers both questions. The store is keyed by identifier
            # alone, so a record under this mid is a QoS 2 duplicate only if it is
            # in a QoS 2 phase, and the state comes back with the existence test.
            # Both stores return None for an absent record without allocating,
            # and the metadata columns precede `payload` in the SQLite row, so a
            # miss costs the same as the bare existence query it replaces while a
            # duplicate no longer costs two. The `_stored_inbound` guard stays
            # inlined: it keeps the whole probe off the fresh-PUBLISH path.
            existing = store.in_meta(mid)
        if existing is not None:
            if existing.state is InboundQoSState.WAIT_PUBACK:
                self._reject_packet_id_collision(mid, "QoS 2", "QoS 1")
            engine._send_ack(_encode_pubrec_success(mid))
            return
        if mid in self._pending_auto_qos1_mids:
            # The same identifier cannot start a QoS 2 exchange while the QoS 1
            # auto-PUBACK is still outstanding from the broker's point of view.
            self._protocol_disconnect(0x82)
            raise ProtocolError(
                f"Inbound packet identifier {mid} reused by QoS 2 while QoS 1 PUBACK is pending"
            )

        logical_size = self.logical_size(topic, payload, properties, decoded_property_wire_size)
        self._acquire_slot(logical_size)
        inbound = InboundMessage(
            mid=mid,
            topic=topic,
            payload=payload,
            qos=QoS.EXACTLY_ONCE,
            retain=retain,
            state=InboundQoSState.WAIT_PUBREL,
            delivered=False,
            properties=properties,
            logical_size=logical_size,
        )
        try:
            store.put_in(inbound)
        except Exception:
            self._release_slot(logical_size)
            raise
        self._remember_inbound()
        self._session_state_qos2 += 1
        # Runtime effect application is SEND-first. Produce the protocol ACK in
        # that order here so every QoS2 delivery avoids EffectPump repartition.
        engine._send_ack(_encode_pubrec_success(mid))
        message = Message(
            topic=topic,
            payload=payload,
            qos=QoS.EXACTLY_ONCE,
            retain=retain,
            dup=dup,
            mid=mid,
            properties=properties,
        )
        if self._ack_tokens is not None:
            self._bind_ack_token(message)
        engine._emit(
            (
                EffectKind.DECODED_MESSAGE
                if decoded_property_wire_size is not None
                else EffectKind.MESSAGE
            ),
            message,
            requires_delivery_mark=True,
            decoded_property_wire_size=decoded_property_wire_size,
        )

    def _complete_recovered_qos1_auto(self, mid: int) -> None:
        """Settle a durable QoS 1 row redelivered into an auto-acknowledging session.

        A durable session may be reopened without manual_ack. Complete the old
        record rather than leaking it behind the automatic PUBACK that this
        retransmission triggers.
        """
        recovered_logical_size = self._complete_stored_inbound(
            mid, InboundQoSState.WAIT_PUBACK, "acknowledging"
        )
        self._forget_inbound()
        self._engine._send_ack(_encode_puback_success(mid))
        # The restored Receive Maximum slot remains owned until this PUBACK
        # leaves the engine effect batch, exactly like a fresh automatic QoS 1
        # acknowledgement. The persisted record is already complete, so its byte
        # reservation can be released immediately without freeing the slot early.
        self._release_pending_bytes(recovered_logical_size)
        self._pending_auto_qos1_mids.add(mid)
        if self._inflight >= self._receive_maximum:
            self._autoack_handoff_required = True

    def _on_qos1_auto(
        self,
        *,
        topic: str,
        payload: bytes,
        mid: int,
        retain: bool,
        dup: bool,
        properties: Properties | None,
        decoded_property_wire_size: int | None = None,
    ) -> None:
        """QoS 1 ingress with automatic acknowledgement.

        Bound once at construction from `config.manual_ack`, which is fixed for
        the engine's lifetime (it is not in
        `_RUNTIME_MUTABLE_ENGINE_CONFIG_FIELDS`), the same way the PUBLISH
        handler and the codec primitives are bound. This variant writes no store
        row and reserves no delivery bytes, so it carries none of the manual
        path's bookkeeping.
        """
        existing = self._lookup_stored_inbound(mid)
        if existing is not None:
            if existing.state is InboundQoSState.WAIT_PUBACK:
                self._complete_recovered_qos1_auto(mid)
                return
            # The record belongs to an unfinished QoS 2 exchange. Accepting the
            # QoS 1 PUBLISH would acknowledge an identifier the broker still
            # owns while leaving the QoS 2 record live.
            self._reject_packet_id_collision(mid, "QoS 1", "QoS 2")

        # A retransmission of an auto-ACK identifier still in this effect batch
        # already holds its Receive Maximum slot, and the handoff flag was
        # decided when the identifier first entered the set.
        retransmission = mid in self._pending_auto_qos1_mids
        if not retransmission:
            self._acquire_slot()
        # Match the runtime's mandatory SEND-before-application order at the
        # producer, avoiding an EffectPump repartition on every auto-ACK.
        self._engine._send_ack(_encode_puback_success(mid))
        self._engine._emit(
            (
                EffectKind.DECODED_MESSAGE
                if decoded_property_wire_size is not None
                else EffectKind.MESSAGE
            ),
            Message(
                topic=topic,
                payload=payload,
                qos=QoS.AT_LEAST_ONCE,
                retain=retain,
                dup=dup,
                mid=mid,
                properties=properties,
            ),
            decoded_property_wire_size=decoded_property_wire_size,
        )
        if not retransmission:
            # The slot stays owned until take_effects() hands this PUBACK to
            # the runtime; a pipelined PUBLISH is then admitted by the ordinary
            # acquire path rather than by a second decode.
            self._pending_auto_qos1_mids.add(mid)
            if self._inflight >= self._receive_maximum:
                self._autoack_handoff_required = True

    def _on_qos1_manual(
        self,
        *,
        topic: str,
        payload: bytes,
        mid: int,
        retain: bool,
        dup: bool,
        properties: Properties | None,
        decoded_property_wire_size: int | None = None,
    ) -> None:
        """QoS 1 ingress with application acknowledgement (see `_on_qos1_auto`)."""
        store = self.store
        existing = self._lookup_stored_inbound(mid)
        if existing is not None:
            # A duplicate QoS 1 publish reuses the existing Receive Maximum slot,
            # but is surfaced again so an application can complete manual ACK
            # after a reconnect or callback cancellation.
            if existing.state is InboundQoSState.WAIT_PUBACK:
                inbound = store.get_in(mid)
                if inbound is None:
                    raise RuntimeError(f"Inbound mid={mid} disappeared while redelivering")
                self._emit_message(inbound, dup=True)
                return
            self._reject_packet_id_collision(mid, "QoS 1", "QoS 2")

        logical_size = self.logical_size(topic, payload, properties, decoded_property_wire_size)
        self._acquire_slot(logical_size)
        try:
            store.put_in(
                InboundMessage(
                    mid=mid,
                    topic=topic,
                    payload=payload,
                    qos=QoS.AT_LEAST_ONCE,
                    retain=retain,
                    state=InboundQoSState.WAIT_PUBACK,
                    delivered=False,
                    properties=properties,
                    logical_size=logical_size,
                )
            )
        except Exception:
            self._release_slot(logical_size)
            raise
        self._remember_inbound()
        self._manual_qos1_order.append(mid)
        message = Message(
            topic=topic,
            payload=payload,
            qos=QoS.AT_LEAST_ONCE,
            retain=retain,
            dup=dup,
            mid=mid,
            properties=properties,
        )
        if self._ack_tokens is not None:
            self._bind_ack_token(message)
        self._engine._emit(
            (
                EffectKind.DECODED_MESSAGE
                if decoded_property_wire_size is not None
                else EffectKind.MESSAGE
            ),
            message,
            requires_delivery_mark=True,
            decoded_property_wire_size=decoded_property_wire_size,
        )

    def on_pubrel(self, raw: RawPacket) -> None:
        engine = self._engine
        config = self.config
        mid, _reason_code, properties = self._decode_pubrel(raw.remaining)
        if properties is not None:
            engine._validate_inbound_problem_information(PacketType.PUBREL, properties)
        record = self._lookup_stored_inbound(mid)
        if record is None:
            engine._send_ack(_encode_pubcomp_success(mid))
            return
        state = record.state
        if state is InboundQoSState.WAIT_USER_ACK:
            if config.manual_ack:
                return
        elif state is not InboundQoSState.WAIT_PUBREL:
            raise ProtocolError(f"PUBREL for inbound mid={mid} in invalid state {state!r}")
        elif config.manual_ack and not record.user_acked:
            changed = self.store.transition_in(
                mid,
                InboundQoSState.WAIT_PUBREL,
                InboundQoSState.WAIT_USER_ACK,
            )
            if changed is None:
                raise RuntimeError(f"Inbound mid={mid} changed while processing PUBREL")
            return
        logical_size = self._complete_stored_inbound(mid, state, "completing PUBREL")
        self._forget_inbound()
        self._session_state_qos2 -= 1
        engine._send_ack(_encode_pubcomp_success(mid))
        self._release_slot(logical_size)

    # --- application acknowledgement and replay ---------------------------
    # --- application acknowledgement and replay ---------------------------

    def mark_delivered(self, mid: int) -> None:
        if self._stored_inbound:
            self.store.mark_in_delivered(mid)

    def ack(self, mid: int, *, message: Message | None = None) -> None:
        """Complete a deferred PUBACK or PUBCOMP in manual-ack mode."""
        if not self.config.manual_ack:
            raise ProtocolError("manual_ack is disabled")
        if message is not None:
            tokens = self._ack_tokens
            if (
                message.mid != mid
                or message._ack_token is None
                or tokens is None
                or tokens.get(mid) is not message._ack_token
            ):
                raise ProtocolError(
                    f"Message is not an active inbound acknowledgement for mid={mid}"
                )
        record = self._lookup_stored_inbound(mid)
        if record is None:
            raise ProtocolError(f"No pending inbound ack for mid={mid}")
        state = record.state
        if state is InboundQoSState.WAIT_PUBREL:
            changed = self.store.transition_in(mid, state, state, user_acked=True)
            if changed is None:
                raise ProtocolError(f"Inbound mid={mid} changed while acknowledging")
            return
        if state is InboundQoSState.WAIT_PUBACK:
            self._engine._check_outbound_size(_encode_puback_success(mid))
            self._pending_manual_qos1_acks.add(mid)
            self._drain_manual_qos1_acks()
            return
        if state is not InboundQoSState.WAIT_USER_ACK:
            raise ProtocolError(f"Inbound mid={mid} is not awaiting ack (state={state!r})")

        wire = _encode_pubcomp_success(mid)
        self._engine._check_outbound_size(wire)
        logical_size = self._complete_stored_inbound(mid, state, "acknowledging")
        self._forget_inbound()
        self._session_state_qos2 -= 1
        self._engine._send_ack(wire)
        self._release_slot(logical_size)

    def _drain_manual_qos1_acks(self) -> None:
        """Emit the ready prefix of manual QoS 1 acknowledgements in arrival order."""
        order = self._manual_qos1_order
        ready = self._pending_manual_qos1_acks
        while order and order[0] in ready:
            mid = order[0]
            record = self._lookup_stored_inbound(mid)
            if record is None or record.state is not InboundQoSState.WAIT_PUBACK:
                raise ProtocolError(f"Inbound QoS 1 order lost pending mid={mid}")
            logical_size = self._complete_stored_inbound(
                mid, InboundQoSState.WAIT_PUBACK, "acknowledging"
            )
            order.popleft()
            ready.remove(mid)
            self._forget_inbound()
            self._engine._send_ack(_encode_puback_success(mid))
            self._release_slot(logical_size)

    def replay_session(self) -> None:
        """Restore Receive Maximum accounting and start bounded redelivery."""
        persisted = self.store.in_count()
        self._inflight = persisted
        if persisted == 0:
            self._recovered_mids.clear()
            return
        pages = self.store.in_replay_pages(REPLAY_BATCH_MESSAGES, REPLAY_BATCH_BYTES)
        self._replay = InboundReplayCursor(iter(pages), remaining=persisted)
        self.drain_replay()

    def drain_replay(self) -> None:
        """Emit one bounded replay batch."""
        cursor = self._replay
        if cursor is None:
            return
        if not cursor.begin_page():
            self._replay = None
            self._recovered_mids.clear()
            return
        emitted = 0
        emitted_bytes = 0
        while emitted < REPLAY_BATCH_MESSAGES and emitted_bytes < REPLAY_BATCH_BYTES:
            inbound = cursor.next_page_message()
            if inbound is None:
                break
            if not self._should_redeliver(inbound):
                continue
            message_bytes = len(inbound.payload) + len(inbound.topic.encode("utf-8"))
            if emitted and emitted_bytes + message_bytes > REPLAY_BATCH_BYTES:
                cursor.push_back(inbound)
                break
            self._emit_message(inbound, dup=True)
            emitted += 1
            emitted_bytes += message_bytes
        if cursor.bounded_complete:
            self._replay = None
            self._recovered_mids.clear()
            return
        self._engine._emit(EffectKind.CONTINUE_INBOUND_REPLAY, None)

    def _should_redeliver(self, inbound: InboundMessage) -> bool:
        if not inbound.delivered:
            return True
        if inbound.mid not in self._recovered_mids or not self.config.manual_ack:
            return False
        if inbound.state in (
            InboundQoSState.WAIT_PUBACK,
            InboundQoSState.WAIT_USER_ACK,
        ):
            return True
        return inbound.state is InboundQoSState.WAIT_PUBREL and not inbound.user_acked

    def _emit_message(self, inbound: InboundMessage, *, dup: bool) -> None:
        message = Message(
            topic=inbound.topic,
            payload=inbound.payload,
            qos=inbound.qos,
            retain=inbound.retain,
            dup=dup,
            mid=inbound.mid,
            properties=inbound.properties,
        )
        if self._ack_tokens is not None:
            self._bind_ack_token(message)
        self._engine._emit(
            EffectKind.MESSAGE,
            message,
            requires_delivery_mark=True,
        )

    # --- aliases and Receive Maximum --------------------------------------

    def _resolve_topic_fields(self, topic: str, props: Properties | None) -> str:
        """Resolve an MQTT 5 Topic Alias. Only the MQTT 5 handlers call this."""
        alias = props.get("topic_alias") if props else None
        if alias is None:
            if not topic:
                self._protocol_disconnect(0x82)
                raise ProtocolError("PUBLISH with empty topic and no topic alias")
            return topic
        alias = int(alias)
        max_alias = self._topic_alias_maximum
        # max_alias == 0 means inbound aliases are not accepted.
        if alias == 0 or alias > max_alias:
            # MQTT 5 §3.3.2.3.4: DISCONNECT 0x94 (Topic Alias invalid).
            self._protocol_disconnect(0x94)
            raise ProtocolError(f"Invalid topic alias {alias}")
        if topic:
            self._aliases[alias] = topic
            return topic
        if alias not in self._aliases:
            self._protocol_disconnect(0x94)
            raise ProtocolError(f"Unknown topic alias {alias}")
        return self._aliases[alias]

    def logical_size(
        self,
        topic: str,
        payload: bytes,
        properties: Properties | None,
        decoded_property_wire_size: int | None = None,
    ) -> int:
        return publish_logical_size(
            self._is_v5, topic, len(payload), properties, decoded_property_wire_size
        )

    def stored_logical_size(self, message: InboundMessage) -> int:
        if message.logical_size <= 0:
            raise ValueError("Persisted inbound logical_size must be positive")
        return message.logical_size

    def _validate_slot_capacity(self, logical_size: int | None = None) -> None:
        """Validate Receive Maximum/quota without reserving anything."""
        if self._inflight >= self._receive_maximum:
            self._protocol_disconnect(0x93)
            raise ProtocolError("Receive Maximum exceeded")
        byte_limit = self.config.max_pending_inbound_bytes
        if (
            logical_size is not None
            and byte_limit is not None
            and self._pending_bytes + logical_size > byte_limit
        ):
            self._protocol_disconnect(0x97)
            raise ProtocolError("Pending inbound byte limit reached")

    def _acquire_slot(self, logical_size: int | None = None) -> None:
        receive_maximum = self._receive_maximum
        inflight = self._inflight
        if inflight >= receive_maximum:
            # MQTT 5 §3.3.4: DISCONNECT 0x93 (Receive Maximum exceeded).
            self._protocol_disconnect(0x93)
            raise ProtocolError("Receive Maximum exceeded")
        byte_limit = self.config.max_pending_inbound_bytes
        if (
            logical_size is not None
            and byte_limit is not None
            and self._pending_bytes + logical_size > byte_limit
        ):
            # MQTT 5 §4.13: DISCONNECT 0x97 (Quota exceeded).
            self._protocol_disconnect(0x97)
            raise ProtocolError("Pending inbound byte limit reached")
        inflight += 1
        self._inflight = inflight
        # A preceding automatic QoS 1 PUBLISH in this engine batch may own
        # slots that take_effects() can release. QoS 2 (or another acquiring
        # path) can fill the remainder after that QoS 1 handler returned, so
        # detect the handoff boundary at the shared counter owner as well.
        if inflight >= receive_maximum and self._pending_auto_qos1_mids:
            self._autoack_handoff_required = True
        if logical_size is not None:
            pending = self._pending_bytes + logical_size
            self._pending_bytes = pending
            # Plain compare, matching OutboundSession._reserve; max() is a
            # builtin call on a per-message path.
            if pending > self._pending_high_water_bytes:
                self._pending_high_water_bytes = pending

    def _release_slot(self, logical_size: int | None = None) -> None:
        if self._inflight > 0:
            self._inflight -= 1
        self._release_pending_bytes(logical_size)

    def _release_pending_bytes(self, logical_size: int | None) -> None:
        if logical_size is None:
            return
        if logical_size <= 0 or logical_size > self._pending_bytes:
            raise AssertionError(
                "inbound byte reservation underflow: "
                f"release={logical_size}, pending={self._pending_bytes}"
            )
        self._pending_bytes -= logical_size

    def _reject_packet_id_collision(self, mid: int, arriving: str, held: str) -> NoReturn:
        """Refuse a PUBLISH whose identifier is still owned by another exchange.

        MQTT 5 [MQTT-2.2.1-4] and MQTT 3.1.1 [MQTT-2.3.1-4] require the Server
        to assign every new QoS > 0 PUBLISH a Packet Identifier that is
        *currently unused*, and an identifier stays in use until its sender has
        processed the corresponding acknowledgement — for QoS 2, our PUBCOMP.
        A record in WAIT_PUBREL or WAIT_USER_ACK proves the broker has not
        received one, so the identifier is provably still its own and the reuse
        is a protocol violation, not a race.

        There is no consistent way to continue: honouring the PUBLISH replaces
        the live record and leaks the Receive Maximum slot and byte reservation
        it still owns, while dropping it silently discards a message the broker
        believes is in flight and will retransmit. Refusing the connection is
        the only answer that neither corrupts local accounting nor loses a
        message without telling anyone.
        """
        # 0x82 Protocol Error, the same tear-down the receive-window and topic
        # alias violations above use.
        self._protocol_disconnect(0x82)
        raise ProtocolError(
            f"Inbound {arriving} PUBLISH reuses mid={mid}, still held by an "
            f"unfinished {held} exchange"
        )

    def _protocol_disconnect(self, reason_code: int) -> None:
        """Delegate local fatal teardown to the engine's single implementation."""
        self._engine._protocol_disconnect(reason_code)
