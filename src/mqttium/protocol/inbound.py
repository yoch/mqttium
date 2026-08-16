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
from mqttium.codec.properties import PUBLISH, encode_properties
from mqttium.enums import InboundQoSState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MalformedPacketError, ProtocolError
from mqttium.persistence.memory import (
    BoundedInboundReplayStore,
    PagedInflightStore,
    TransitionInflightStore,
)
from mqttium.packets import (
    PublishPacket,
)
from mqttium.packets._publish import (
    decode_qos0_message_v311,
    decode_qos0_message_v5,
    decode_qos12_fields_v311,
    decode_publish_fields_v5,
)
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.stats import InboundStats
from mqttium.topics import validate_received_publish_topic
from mqttium.types import InboundMessage, InboundRecordMeta, Message, Properties

if TYPE_CHECKING:
    from mqttium.protocol.engine import ProtocolEngine


# One replay batch. The scan bound caps the store work a single continuation
# performs even when every record it reads turns out to be already delivered;
# the message and byte bounds cap what the delivery layer has to absorb before
# backpressure is consulted again.
REPLAY_PAGE_SIZE = 256
REPLAY_SCAN_LIMIT = 256
REPLAY_BATCH_MESSAGES = 64
REPLAY_BATCH_BYTES = 1 << 20


def _encode_puback_success(mid: int) -> bytes:
    """Encode the fixed success/no-properties PUBACK for an already-validated MID."""
    return bytes((0x40, 2, mid >> 8, mid & 0xFF))


def _encode_pubrec_success(mid: int) -> bytes:
    return bytes((0x50, 2, mid >> 8, mid & 0xFF))


def _encode_pubcomp_success(mid: int) -> bytes:
    return bytes((0x70, 2, mid >> 8, mid & 0xFF))


class InboundReplayCursor:
    """Position inside a paged inbound replay.

    Holding a page iterator instead of a materialised list is the whole point:
    only the current page of messages is alive at any time.
    """

    __slots__ = ("_pages", "_page", "_offset", "_pending")

    def __init__(self, pages: Iterator[tuple[InboundMessage, ...]]) -> None:
        self._pages = pages
        self._page: tuple[InboundMessage, ...] = ()
        self._offset = 0
        self._pending: InboundMessage | None = None

    def next_message(self) -> InboundMessage | None:
        if self._pending is not None:
            message = self._pending
            self._pending = None
            return message
        while self._offset >= len(self._page):
            page = next(self._pages, None)
            if page is None:
                return None
            self._page = page
            self._offset = 0
        message = self._page[self._offset]
        self._offset += 1
        return message

    def push_back(self, message: InboundMessage) -> None:
        if self._pending is not None:
            raise AssertionError("replay cursor already has a pending message")
        self._pending = message


class InboundSession:
    """Authoritative inbound QoS and connection-scoped alias state."""

    __slots__ = (
        "_aliases",
        "_bounded_replay_store",
        "_decode_pubrel",
        "_engine",
        "_inflight",
        "_autoack_handoff_required",
        "_paged_store",
        "_pending_auto_qos1_mids",
        "_pending_manual_qos1_acks",
        "_manual_qos1_order",
        "_pending_bytes",
        "_pending_high_water_bytes",
        "_recovered_mids",
        "_replay",
        "_stored_inbound",
        "_session_state_qos2",
        "_transitions",
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
        # Resolved once, like the paged extension: with a transition-capable
        # store, existence checks, delivery marks and acknowledgements never
        # materialise an inbound payload.
        self._transitions = self.store if isinstance(self.store, TransitionInflightStore) else None
        self._paged_store = self.store if isinstance(self.store, PagedInflightStore) else None
        self._bounded_replay_store = (
            self.store if isinstance(self.store, BoundedInboundReplayStore) else None
        )
        self._decode_pubrel = engine.codec.decode_pubrel
        protocol = self.config.protocol
        if protocol is MQTTProtocolVersion.MQTTv5:
            self.handle_publish = self._on_publish_v5
        elif protocol is MQTTProtocolVersion.MQTTv311:
            self.handle_publish = self._on_publish_v311
        else:
            self.handle_publish = self._on_publish_v31
        self._aliases: dict[int, str] = {}
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
        """Restore identifiers, bytes, QoS 2 count, and durable QoS 1 arrival order."""
        transitions = self._transitions
        if transitions is None:
            eager_mids: set[int] = set()
            eager_qos1: list[int] = []
            pending_bytes = 0
            session_state_qos2 = 0
            for message in self.store.in_items():
                eager_mids.add(message.mid)
                if message.state in (
                    InboundQoSState.WAIT_PUBREL,
                    InboundQoSState.WAIT_USER_ACK,
                ):
                    session_state_qos2 += 1
                if message.state is InboundQoSState.WAIT_PUBACK:
                    eager_qos1.append(message.mid)
                size = message.logical_size or self.logical_size(
                    message.topic,
                    message.payload,
                    message.properties,
                )
                message.logical_size = size
                pending_bytes += size
            return eager_mids, pending_bytes, session_state_qos2, tuple(eager_qos1)
        mids: set[int] = set()
        recovered_qos1: list[int] = []
        pending_bytes = 0
        session_state_qos2 = 0
        unknown_sizes: set[int] = set()
        for page in transitions.in_index_pages(REPLAY_PAGE_SIZE):
            for meta in page:
                mids.add(meta.mid)
                if meta.state in (
                    InboundQoSState.WAIT_PUBREL,
                    InboundQoSState.WAIT_USER_ACK,
                ):
                    session_state_qos2 += 1
                if meta.state is InboundQoSState.WAIT_PUBACK:
                    recovered_qos1.append(meta.mid)
                if meta.logical_size > 0:
                    pending_bytes += meta.logical_size
                else:
                    unknown_sizes.add(meta.mid)
        if unknown_sizes:
            for message in self.store.in_items():
                if message.mid not in unknown_sizes:
                    continue
                size = self.logical_size(message.topic, message.payload, message.properties)
                message.logical_size = size
                if not transitions.set_in_logical_size(message.mid, size):
                    raise RuntimeError(
                        f"Inbound mid={message.mid} disappeared while restoring byte accounting"
                    )
                pending_bytes += size
        return mids, pending_bytes, session_state_qos2, tuple(recovered_qos1)

    # --- lifecycle ---------------------------------------------------------

    def start_connection(self) -> None:
        """Reset state scoped to one network connection before CONNECT."""
        self._aliases.clear()
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

    def discard_session(self) -> None:
        """Drop inbound state after CONNACK reports no previous session."""
        self.store.clear_in()
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
            receive_maximum=self.config.local_receive_maximum,
            topic_aliases=len(self._aliases),
            replay_pending=self._replay is not None,
            pending_bytes=self._pending_bytes,
            pending_high_water_bytes=max(
                self._pending_high_water_bytes,
                self._pending_bytes,
            ),
            pending_byte_limit=self.config.max_pending_inbound_bytes,
        )

    def _lookup_stored_inbound(self, mid: int) -> InboundMessage | InboundRecordMeta | None:
        """Return a persisted inbound record without probing an empty store."""
        if not self._stored_inbound:
            return None
        transitions = self._transitions
        if transitions is not None:
            return transitions.in_meta(mid)
        return self.store.get_in(mid)

    def _remember_inbound(self) -> None:
        self._stored_inbound += 1

    def _forget_inbound(self) -> None:
        if self._stored_inbound <= 0:
            raise AssertionError("inbound stored-record count underflow")
        self._stored_inbound -= 1

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
        if qos_raw == int(QoS.AT_LEAST_ONCE):
            self._on_qos1(
                topic=topic,
                payload=payload,
                mid=mid,
                retain=retain,
                dup=dup,
                properties=None,
            )
            return
        self._on_qos2(
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
        if qos is QoS.AT_LEAST_ONCE:
            self._on_qos1(
                topic=topic,
                payload=payload,
                mid=decoded_mid,
                retain=retain,
                dup=dup,
                properties=properties,
                decoded_property_wire_size=decoded_property_wire_size,
            )
            return
        self._on_qos2(
            topic=topic,
            payload=payload,
            mid=decoded_mid,
            retain=retain,
            dup=dup,
            properties=properties,
            decoded_property_wire_size=decoded_property_wire_size,
        )

    def _on_publish_v31(self, raw: RawPacket) -> None:
        """Retain the generic decoder only for the legacy MQTT 3.1 protocol."""
        packet = PublishPacket.decode(raw.flags, raw.remaining, MQTTProtocolVersion.MQTTv31)
        topic = self._resolve_topic(packet)
        validate_received_publish_topic(topic, utf8_validated=True)
        if packet.qos is QoS.AT_MOST_ONCE:
            self._engine._emit(
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
        if packet.qos is QoS.AT_LEAST_ONCE:
            self._on_qos1(
                topic=topic,
                payload=packet.payload,
                mid=packet.mid,
                retain=packet.retain,
                dup=packet.dup,
                properties=packet.properties,
            )
            return
        self._on_qos2(
            topic=topic,
            payload=packet.payload,
            mid=packet.mid,
            retain=packet.retain,
            dup=packet.dup,
            properties=packet.properties,
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
        transitions = self._transitions
        existing: InboundMessage | InboundRecordMeta | None = None
        if self._stored_inbound:
            # Existence is the common question and the cheapest to answer, so it
            # stays first: a fresh PUBLISH finds nothing and pays exactly one
            # lookup. Only once a record turns up does the state matter, and it
            # has to, because the inbound store is keyed by identifier alone —
            # a record under this mid is a QoS 2 duplicate only if it is in a
            # QoS 2 phase. Reading it back costs a second metadata query on the
            # rare duplicate path rather than on every inbound QoS 2 PUBLISH.
            if transitions is not None:
                if transitions.contains_in(mid):
                    existing = transitions.in_meta(mid)
            else:
                existing = store.get_in(mid)
        if existing is not None:
            if existing.state is InboundQoSState.WAIT_PUBACK:
                self._reject_packet_id_collision(mid, "QoS 2", "QoS 1")
            engine._send(_encode_pubrec_success(mid))
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
        engine._send(_encode_pubrec_success(mid))
        engine._emit(
            (
                EffectKind.DECODED_MESSAGE
                if decoded_property_wire_size is not None
                else EffectKind.MESSAGE
            ),
            Message(
                topic=topic,
                payload=payload,
                qos=QoS.EXACTLY_ONCE,
                retain=retain,
                dup=dup,
                mid=mid,
                properties=properties,
            ),
            requires_delivery_mark=True,
            decoded_property_wire_size=decoded_property_wire_size,
        )

    def _on_qos1(
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
        config = self.config
        store = self.store
        transitions = self._transitions
        existing: InboundMessage | InboundRecordMeta | None = self._lookup_stored_inbound(mid)
        if existing is not None:
            # A duplicate QoS1 publish reuses the existing Receive Maximum slot,
            # but is surfaced again so an application can complete manual ACK
            # after a reconnect or callback cancellation.
            if existing.state is InboundQoSState.WAIT_PUBACK:
                if config.manual_ack:
                    message = (
                        existing if isinstance(existing, InboundMessage) else store.get_in(mid)
                    )
                    if message is None:
                        raise ProtocolError(f"Inbound mid={mid} disappeared while redelivering")
                    self._emit_message(message, dup=True)
                    return
                # A durable session may have been reopened without manual ACK.
                # Complete the old QoS 1 record rather than leaking it after the
                # automatic PUBACK sent for this retransmission.
                if isinstance(existing, InboundMessage):
                    completed = store.pop_in(mid)
                    if completed is None:
                        raise ProtocolError(f"Inbound mid={mid} disappeared while acknowledging")
                    recovered_logical_size = self.stored_logical_size(completed)
                else:
                    assert transitions is not None
                    completed_meta = transitions.complete_in(mid, InboundQoSState.WAIT_PUBACK)
                    if completed_meta is None:
                        raise ProtocolError(f"Inbound mid={mid} changed while acknowledging")
                    recovered_logical_size = completed_meta.logical_size
                self._forget_inbound()
                self._engine._send(_encode_puback_success(mid))
                # The restored Receive Maximum slot remains owned until this
                # PUBACK leaves the engine effect batch, exactly like a fresh
                # automatic QoS 1 acknowledgement. The persisted record is
                # already complete, so its byte reservation can be released
                # immediately without freeing the protocol slot early.
                self._release_pending_bytes(recovered_logical_size)
                self._pending_auto_qos1_mids.add(mid)
                if self._inflight >= config.local_receive_maximum:
                    self._autoack_handoff_required = True
                return
            # The record belongs to an unfinished QoS 2 exchange. Accepting the
            # QoS 1 PUBLISH would acknowledge an identifier the broker still
            # owns while leaving the QoS 2 record live.
            self._reject_packet_id_collision(mid, "QoS 1", "QoS 2")

        if not config.manual_ack and mid in self._pending_auto_qos1_mids:
            # Retransmission of an auto-ACK identifier still in this effect
            # batch: the Receive Maximum slot is already held.
            self._engine._send(_encode_puback_success(mid))
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
            return

        logical_size = (
            self.logical_size(topic, payload, properties, decoded_property_wire_size)
            if config.manual_ack
            else None
        )
        self._acquire_slot(logical_size)
        if config.manual_ack:
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
                        logical_size=logical_size or 0,
                    )
                )
            except Exception:
                self._release_slot(logical_size)
                raise
            self._remember_inbound()
            self._manual_qos1_order.append(mid)
        if not config.manual_ack:
            # Match the runtime's mandatory SEND-before-application order at the
            # producer, avoiding an EffectPump repartition on every auto-ACK.
            self._engine._send(_encode_puback_success(mid))
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
            requires_delivery_mark=config.manual_ack,
            decoded_property_wire_size=decoded_property_wire_size,
        )
        if not config.manual_ack:
            self._pending_auto_qos1_mids.add(mid)
            if self._inflight >= config.local_receive_maximum:
                self._autoack_handoff_required = True

    def on_pubrel(self, raw: RawPacket) -> None:
        engine = self._engine
        config = self.config
        store = self.store
        mid, _reason_code, properties = self._decode_pubrel(raw.remaining)
        if properties is not None:
            engine._validate_inbound_problem_information(PacketType.PUBREL, properties)
        # One state machine for both store shapes: a transition-capable store
        # answers through metadata only, the base interface reads the record.
        transitions = self._transitions
        record: InboundMessage | InboundRecordMeta | None = self._lookup_stored_inbound(mid)
        if record is None:
            engine._send(_encode_pubcomp_success(mid))
            return
        state = record.state
        if state is InboundQoSState.WAIT_USER_ACK:
            if config.manual_ack:
                return
        elif state is not InboundQoSState.WAIT_PUBREL:
            raise ProtocolError(f"PUBREL for inbound mid={mid} in invalid state {state!r}")
        elif config.manual_ack and not record.user_acked:
            # The exchange completes when the application calls ack().
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
                store.update_in(record)
            return
        if transitions is not None:
            completed = transitions.complete_in(mid, state)
            if completed is None:
                raise ProtocolError(f"Inbound mid={mid} changed while completing PUBREL")
            logical_size = completed.logical_size
        else:
            popped = store.pop_in(mid)
            if popped is None:
                raise ProtocolError(f"Inbound mid={mid} disappeared while completing PUBREL")
            logical_size = self.stored_logical_size(popped)
        self._forget_inbound()
        self._session_state_qos2 -= 1
        engine._send(_encode_pubcomp_success(mid))
        self._release_slot(logical_size)

    # --- application acknowledgement and replay ---------------------------

    def mark_delivered(self, mid: int) -> None:
        if not self._stored_inbound:
            return
        transitions = self._transitions
        if transitions is not None:
            # One conditional UPDATE. This runs for every delivered QoS 1/2
            # message, so the whole-object read it replaces was the most
            # frequent payload reconstruction in the library.
            transitions.mark_in_delivered(mid)
            return
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
        transitions = self._transitions
        record: InboundMessage | InboundRecordMeta | None = self._lookup_stored_inbound(mid)
        if record is None:
            raise ProtocolError(f"No pending inbound ack for mid={mid}")
        state = record.state
        if state is InboundQoSState.WAIT_PUBREL:
            if isinstance(record, InboundMessage):
                record.user_acked = True
                store.update_in(record)
            else:
                assert transitions is not None
                changed = transitions.transition_in(mid, state, state, user_acked=True)
                if changed is None:
                    raise ProtocolError(f"Inbound mid={mid} changed while acknowledging")
            return
        if state is InboundQoSState.WAIT_PUBACK:
            # PUBACK ordering is defined by PUBLISH arrival, not by application
            # ack() call order [MQTT-4.6.0-2]. Keep the Receive Maximum slot and
            # durable record until every earlier QoS 1 delivery is ready too.
            self._engine._check_outbound_size(_encode_puback_success(mid))
            self._pending_manual_qos1_acks.add(mid)
            self._drain_manual_qos1_acks()
            return
        if state is not InboundQoSState.WAIT_USER_ACK:
            raise ProtocolError(f"Inbound mid={mid} is not awaiting ack (state={state!r})")

        wire = _encode_pubcomp_success(mid)
        self._engine._check_outbound_size(wire)

        if isinstance(record, InboundMessage):
            popped = store.pop_in(mid)
            if popped is None:
                raise ProtocolError(f"Inbound mid={mid} disappeared while acknowledging")
            logical_size = self.stored_logical_size(popped)
        else:
            assert transitions is not None
            completed = transitions.complete_in(mid, state)
            if completed is None:
                raise ProtocolError(f"Inbound mid={mid} changed while acknowledging")
            logical_size = completed.logical_size
        self._forget_inbound()
        if state is InboundQoSState.WAIT_USER_ACK:
            self._session_state_qos2 -= 1
        self._engine._send(wire)
        self._release_slot(logical_size)

    def _drain_manual_qos1_acks(self) -> None:
        """Emit the ready prefix of manual QoS 1 acknowledgements in arrival order."""
        order = self._manual_qos1_order
        ready = self._pending_manual_qos1_acks
        store = self.store
        transitions = self._transitions
        while order and order[0] in ready:
            mid = order[0]
            record = self._lookup_stored_inbound(mid)
            if record is None or record.state is not InboundQoSState.WAIT_PUBACK:
                raise ProtocolError(f"Inbound QoS 1 order lost pending mid={mid}")
            if isinstance(record, InboundMessage):
                popped = store.pop_in(mid)
                if popped is None:
                    raise ProtocolError(f"Inbound mid={mid} disappeared while acknowledging")
                logical_size = self.stored_logical_size(popped)
            else:
                assert transitions is not None
                completed = transitions.complete_in(mid, InboundQoSState.WAIT_PUBACK)
                if completed is None:
                    raise ProtocolError(f"Inbound mid={mid} changed while acknowledging")
                logical_size = completed.logical_size
            order.popleft()
            ready.remove(mid)
            self._forget_inbound()
            self._engine._send(_encode_puback_success(mid))
            self._release_slot(logical_size)

    def replay_session(self) -> None:
        """Restore Receive Maximum accounting and start redelivering.

        Only the first bounded batch is emitted here. The rest is pumped through
        `CONTINUE_INBOUND_REPLAY`, so a 10,000-message durable session no longer
        materialises 10,000 payloads — nor 10,000 effects — before the runtime
        gets a chance to apply delivery backpressure.
        """
        paged = self._paged_store
        bounded = self._bounded_replay_store
        transitions = self._transitions
        if transitions is None or (bounded is None and paged is None):
            # A store without the paging or metadata extensions keeps the
            # original eager behaviour: correct, just not bounded.
            inbound_items = list(self.store.in_items())
            self._inflight = len(inbound_items)
            for inbound in inbound_items:
                if self._should_redeliver(inbound):
                    self._emit_message(inbound, dup=True)
            self._recovered_mids.clear()
            return

        # Built-in stores count directly without re-scanning the index that
        # construction already traversed. Third-party paged stores keep the
        # metadata-only fallback.
        if bounded is not None:
            persisted = bounded.in_count()
            pages = bounded.in_replay_pages(
                REPLAY_SCAN_LIMIT,
                REPLAY_BATCH_BYTES,
            )
        else:
            persisted = 0
            for page in transitions.in_index_pages(REPLAY_PAGE_SIZE):
                persisted += len(page)
            assert paged is not None
            pages = paged.in_pages(REPLAY_PAGE_SIZE)
        self._inflight = persisted
        if persisted == 0:
            self._recovered_mids.clear()
            return
        self._replay = InboundReplayCursor(iter(pages))
        self.drain_replay()

    def drain_replay(self) -> None:
        """Emit one bounded batch of redeliveries.

        Whether more remain is read back from `replay_pending`, so there is one
        way to ask rather than two.
        """
        cursor = self._replay
        if cursor is None:
            return
        scanned = 0
        emitted = 0
        emitted_bytes = 0
        while (
            scanned < REPLAY_SCAN_LIMIT
            and emitted < REPLAY_BATCH_MESSAGES
            and emitted_bytes < REPLAY_BATCH_BYTES
        ):
            inbound = cursor.next_message()
            if inbound is None:
                self._replay = None
                self._recovered_mids.clear()
                return
            if not self._should_redeliver(inbound):
                scanned += 1
                continue
            message_bytes = len(inbound.payload) + len(inbound.topic.encode("utf-8"))
            if emitted and emitted_bytes + message_bytes > REPLAY_BATCH_BYTES:
                cursor.push_back(inbound)
                break
            scanned += 1
            self._emit_message(inbound, dup=True)
            emitted += 1
            emitted_bytes += message_bytes
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
            requires_delivery_mark=True,
        )

    # --- aliases and Receive Maximum --------------------------------------

    def _resolve_topic(self, packet: PublishPacket) -> str:
        return self._resolve_topic_fields(packet.topic, packet.properties)

    def _resolve_topic_fields(self, topic: str, props: Properties | None) -> str:
        if self.config.protocol != MQTTProtocolVersion.MQTTv5:
            return topic
        alias = props.get("topic_alias") if props else None
        if alias is None:
            if not topic:
                raise ProtocolError("PUBLISH with empty topic and no topic alias")
            return topic
        alias = int(alias)
        max_alias = self.config.topic_alias_maximum
        # max_alias == 0 means inbound aliases are not accepted.
        if alias == 0 or alias > max_alias:
            # MQTT 5 §3.3.2.3.5: DISCONNECT 0x94 (Topic Alias invalid).
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
        property_bytes = 0
        if (
            self.config.protocol == MQTTProtocolVersion.MQTTv5
            and properties is not None
            and properties.values
        ):
            if decoded_property_wire_size is None:
                property_bytes = len(encode_properties(properties, PUBLISH))
            else:
                property_bytes = decoded_property_wire_size
        topic_bytes = len(topic) if topic.isascii() else len(topic.encode("utf-8"))
        return len(payload) + topic_bytes + property_bytes

    def stored_logical_size(self, message: InboundMessage) -> int:
        if message.logical_size > 0:
            return message.logical_size
        message.logical_size = self.logical_size(
            message.topic,
            message.payload,
            message.properties,
        )
        return message.logical_size

    def _acquire_slot(self, logical_size: int | None = None) -> None:
        if self._inflight >= self.config.local_receive_maximum:
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
        self._inflight += 1
        # A preceding automatic QoS 1 PUBLISH in this engine batch may own
        # slots that take_effects() can release. QoS 2 (or another acquiring
        # path) can fill the remainder after that QoS 1 handler returned, so
        # detect the handoff boundary at the shared counter owner as well.
        if self._pending_auto_qos1_mids and self._inflight >= self.config.local_receive_maximum:
            self._autoack_handoff_required = True
        if logical_size is not None:
            self._pending_bytes += logical_size
            self._pending_high_water_bytes = max(
                self._pending_high_water_bytes,
                self._pending_bytes,
            )

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
