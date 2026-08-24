"""Client→broker publication: QoS state, packet ids, flow, budget and replay.

`OutboundSession` owns every resource a QoS 1/2 publication acquires — the
admission budget, the packet identifier, the store record and the flow-control
slot — so that acquiring and releasing them stays in one place. `ProtocolEngine`
drives it; it never touches the counters itself.

The session deliberately does *not* own connection state. It reads `state` and
`negotiated` back from the engine because both are rebound on every connection,
and it emits through the engine rather than buffering effects of its own: the
relative order of outbound and connection effects is observable by AsyncClient.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from typing import TYPE_CHECKING

from mqttium.codec.buffer import RawPacket
from mqttium.codec.properties import PUBLISH, encode_properties
from mqttium.codec.vbi import MAX_VBI, vbi_len
from mqttium.enums import (
    ConnectionState,
    OutboundQoSState,
    PacketType,
    QoS,
)
from mqttium.errors import (
    MandatoryResponseTooLargeError,
    FlowControlError,
    NotConnectedError,
    PacketTooLargeError,
    ProtocolError,
    SessionDiscardedError,
)
from mqttium.persistence.memory import PagedInflightStore, TransitionInflightStore
from mqttium.protocol.effects import EffectKind, PublishFailure, PublishHandle
from mqttium.protocol.flow_control import FlowControl
from mqttium.protocol.packet_ids import PacketIdPool
from mqttium.packets._ack import encode_pubrel_success as _encode_pubrel_success
from mqttium.protocol._sizing import publish_logical_size
from mqttium.protocol.stats import OutboundStats
from mqttium.topics import encode_validated_publish_topic, validate_publish_topic
from mqttium.transport.writes import WriteItem
from mqttium.types import OutboundMessage, OutboundMessageSummary, Properties

if TYPE_CHECKING:
    from mqttium.protocol.engine import ProtocolEngine


def _retain_publish_item(item: WriteItem) -> WriteItem | None:
    """Keep only a segmented frame, whose payload is already shared."""
    return item if not isinstance(item, bytes) else None


def _mark_publish_dup(item: WriteItem) -> WriteItem:
    """Set PUBLISH DUP by replacing only the first header byte."""
    if isinstance(item, bytes):
        if item[0] & 0x08:
            return item
        return bytes((item[0] | 0x08,)) + item[1:]
    header, payload = item
    if header[0] & 0x08:
        return item
    return (bytes((header[0] | 0x08,)) + header[1:], payload)


class OutboundSession:
    __slots__ = (
        "_decode_puback",
        "_decode_pubcomp",
        "_decode_pubrec",
        "_encode_publish",
        "_encode_pubrel",
        "_engine",
        "_is_v5",
        "_queued",
        "_paged_store",
        "_transitions",
        "_pending_bytes",
        "_pending_high_water_bytes",
        "_pending_high_water_messages",
        "_pending_messages",
        "_topic_aliases",
        "config",
        "flow",
        "packet_ids",
        "store",
    )

    def __init__(self, engine: ProtocolEngine) -> None:
        self._engine = engine
        # Bound once: the config object is mutated in place by update(), never
        # rebound, so its identity is stable for the engine's lifetime.
        self.config = engine.config
        self.store = engine.store
        # Resolved once: a store either pages or it does not, for its lifetime.
        self._paged_store = self.store if isinstance(self.store, PagedInflightStore) else None
        # Same contract for conditional transitions: acknowledgement handling
        # settles records without ever materialising a payload when the store
        # supports it, and falls back to the whole-object path when it does not.
        self._transitions = self.store if isinstance(self.store, TransitionInflightStore) else None
        # Version-specialized ack codecs come from the engine's one-shot bind.
        codec = engine.codec
        # The protocol is fixed for the engine's lifetime (it is not in
        # _RUNTIME_MUTABLE_ENGINE_CONFIG_FIELDS), so resolve it once here
        # rather than comparing enums on the publish path.
        self._is_v5 = codec.is_mqtt5
        self._decode_puback = codec.decode_puback
        self._decode_pubrec = codec.decode_pubrec
        self._decode_pubcomp = codec.decode_pubcomp
        self._encode_publish = codec.encode_publish_item
        self._encode_pubrel = codec.encode_pubrel
        self.packet_ids = PacketIdPool()
        self.flow = FlowControl(self.config.local_receive_maximum)
        self._queued: deque[OutboundMessage | OutboundMessageSummary] = deque()
        self._pending_messages = 0
        self._pending_bytes = 0
        self._pending_high_water_messages = 0
        self._pending_high_water_bytes = 0
        # Topic Alias mappings are Network Connection state, never MQTT
        # Session state. Durable records retain canonical Topic Names instead.
        self._topic_aliases: dict[int, str] = {}

    # --- connection-scoped Topic Alias state -------------------------------

    def start_connection(self) -> None:
        """Forget every mapping before a new CONNECT is sent."""
        self._topic_aliases.clear()

    def transport_closed(self) -> None:
        """A resumed MQTT Session never resumes Topic Alias mappings."""
        self._topic_aliases.clear()

    def commit_topic_alias(self, topic: str, properties: Properties | None) -> None:
        """Record an explicit mapping after its PUBLISH has writer admission."""
        if topic and properties is not None:
            alias = properties.get("topic_alias")
            if alias is not None:
                self._topic_aliases[int(alias)] = topic

    # --- effect emission ---------------------------------------------------
    # Always routed through the engine, never into a cached list: take_effects()
    # rebinds the list, so the engine's `_effects` is the one sink. Emission
    # calls `self._engine._emit` / `._send` directly, as InboundSession does;
    # a forwarding wrapper here cost a Python frame per publish and per ACK.

    def _fail(self, mid: int, reason: BaseException) -> None:
        self._engine._emit(EffectKind.PUBLISH_FAILED, PublishFailure(mid=mid, reason=reason))

    # --- admission ---------------------------------------------------------

    @property
    def pending_messages(self) -> int:
        return self._pending_messages

    @property
    def pending_bytes(self) -> int:
        return self._pending_bytes

    @property
    def pending_high_water_messages(self) -> int:
        return self._pending_high_water_messages

    @property
    def pending_high_water_bytes(self) -> int:
        return self._pending_high_water_bytes

    def can_ever_admit(
        self,
        topic: str,
        payload: bytes,
        qos: QoS | int,
        properties: Properties | None = None,
    ) -> bool:
        if QoS(qos) == QoS.AT_MOST_ONCE:
            return True
        message_limit = self.config.max_pending_outbound_messages
        if message_limit is not None and message_limit < 1:
            return False
        byte_limit = self.config.max_pending_outbound_bytes
        if not topic and properties is not None:
            alias = properties.get("topic_alias")
            if alias is not None:
                topic = self._topic_aliases.get(int(alias), topic)
        logical_size = self.logical_size(topic, payload, properties)
        return byte_limit is None or logical_size <= byte_limit

    def can_ever_admit_many(
        self,
        messages: Iterable[tuple[str, bytes, QoS | int, bool, Properties | None]],
    ) -> bool:
        pending_count = 0
        pending_bytes = 0
        for topic, payload, qos, _retain, properties in messages:
            if QoS(qos) == QoS.AT_MOST_ONCE:
                continue
            pending_count += 1
            if not topic and properties is not None:
                alias = properties.get("topic_alias")
                if alias is not None:
                    topic = self._topic_aliases.get(int(alias), topic)
            pending_bytes += self.logical_size(topic, payload, properties)
        message_limit = self.config.max_pending_outbound_messages
        if message_limit is not None and pending_count > message_limit:
            return False
        byte_limit = self.config.max_pending_outbound_bytes
        return byte_limit is None or pending_bytes <= byte_limit

    def _reserve(self, logical_size: int) -> None:
        message_limit = self.config.max_pending_outbound_messages
        if message_limit is not None and self._pending_messages >= message_limit:
            raise FlowControlError("Pending outbound message limit reached")
        byte_limit = self.config.max_pending_outbound_bytes
        if byte_limit is not None and self._pending_bytes + logical_size > byte_limit:
            raise FlowControlError("Pending outbound byte limit reached")
        self._pending_messages += 1
        self._pending_bytes += logical_size

    def _release_reservation(self, logical_size: int) -> None:
        if self._pending_messages <= 0:
            raise AssertionError("outbound message reservation underflow")
        if logical_size < 0 or logical_size > self._pending_bytes:
            raise AssertionError(
                "outbound byte reservation underflow: "
                f"release={logical_size}, pending={self._pending_bytes}"
            )
        if self._pending_messages > self._pending_high_water_messages:
            self._pending_high_water_messages = self._pending_messages
        if self._pending_bytes > self._pending_high_water_bytes:
            self._pending_high_water_bytes = self._pending_bytes
        self._pending_messages -= 1
        self._pending_bytes -= logical_size

    def stats(self) -> OutboundStats:
        """Snapshot this session's own accounting.

        The high-water fields are sampled lazily on release, so a snapshot taken
        while records are still in flight compares them against the live value.
        """
        flow = self.flow
        return OutboundStats(
            pending_messages=self._pending_messages,
            pending_bytes=self._pending_bytes,
            pending_high_water_messages=max(
                self._pending_high_water_messages, self._pending_messages
            ),
            pending_high_water_bytes=max(self._pending_high_water_bytes, self._pending_bytes),
            queued_messages=len(self._queued),
            flow_inflight=flow.inflight,
            flow_limit=flow.limit,
            packet_ids_in_use=len(self.packet_ids),
        )

    # --- admission rollback --------------------------------------------------

    def _rollback(
        self,
        inflight_start: int,
        messages_start: int,
        bytes_start: int,
        mids: Iterable[int],
        *,
        effect_start: int | None = None,
        queued_start: int | None = None,
        packet_ids_empty_start: bool = False,
    ) -> None:
        """Undo every resource a failed admission acquired, in one fixed order.

        Effects, queue index, flow slots, store rows, packet ids, budget. Both
        `queue_publish` and `queue_publish_many` unwind through here: the callers
        only snapshot, which is why this costs nothing on the success path and
        cannot drift between the two.

        `packet_ids_empty_start` says the pool held nothing before the chunk, so
        every live id was allocated by it and the whole pool is reset in constant
        time instead of released one id at a time — the pool reclaims its
        accumulated hashing capacity only on a full clear.

        `effect_start` / `queued_start` are chunk-only. A single publish emits
        its SEND as the last statement of a successful launch and appends to
        `_queued` as its last statement overall, so neither can be left behind by
        one failed message — only by an earlier message of a chunk. Omitting them
        keeps two attribute reads off the per-message path.

        The budget is restored wholesale from the snapshot rather than released
        record by record, and store rows go through `delete_record` rather than
        `discard_record` for the same reason: a transactional store has already
        rolled its batch back by the time this runs, so the per-record sizes are
        unrecoverable and a second per-record release would double-count.
        """
        if effect_start is not None:
            del self._engine._effects[effect_start:]
        if queued_start is not None:
            queued = self._queued
            while len(queued) > queued_start:
                queued.pop()
        flow = self.flow
        while flow.inflight > inflight_start:
            flow.release()
        packet_ids = self.packet_ids
        if packet_ids_empty_start:
            for mid in mids:
                self.delete_record(mid)
            # Every live MID was allocated by this failed atomic chunk.
            packet_ids.clear()
        else:
            for mid in mids:
                self.delete_record(mid)
                packet_ids.release(mid)
        self._pending_messages = messages_start
        self._pending_bytes = bytes_start

    # --- queueing ----------------------------------------------------------

    def _resolve_outbound_alias(
        self,
        topic: str,
        alias: int,
    ) -> str:
        """Validate and resolve the uncommon MQTT 5 Topic Alias path."""
        if alias == 0:
            raise ProtocolError("topic_alias 0 is invalid")
        if topic:
            return topic

        engine = self._engine
        if engine.state is not ConnectionState.CONNECTED:
            raise ProtocolError(f"Unknown outbound topic alias {alias} on this connection")
        maximum = engine.negotiated.topic_alias_maximum
        if alias > maximum:
            raise ProtocolError(f"topic_alias {alias} exceeds broker topic_alias_maximum {maximum}")
        canonical_topic = self._topic_aliases.get(alias, "")
        if not canonical_topic:
            raise ProtocolError(f"Unknown outbound topic alias {alias} on this connection")
        return canonical_topic

    def _validate_publish_request(
        self,
        topic: str,
        qos: QoS | int,
        retain: bool,
        properties: Properties | None,
    ) -> tuple[QoS, bytes | None, int, str]:
        """Validate one outbound PUBLISH and retain Topic Name bytes when launching.

        QoS 0 always encodes immediately. QoS 1/2 encode when the session is
        connected so the launch path can reuse the bytes; an offline queue
        only needs the byte length, and must not retain a discarded copy.
        """

        level = QoS(qos)
        engine = self._engine
        alias: int | None = None
        if properties is not None:
            if not self._is_v5 and properties.values:
                raise ProtocolError("PUBLISH properties require MQTT 5")
            if self._is_v5 and properties.get("subscription_identifier") is not None:
                # [MQTT-3.3.4-6]: a PUBLISH sent from a Client to a Server MUST NOT
                # contain a Subscription Identifier. The property table cannot catch
                # this, because it is legal on the inbound PUBLISH the broker sends
                # us — the restriction is on the direction, not on the packet type.
                raise ProtocolError(
                    "subscription_identifier is not allowed on an outbound PUBLISH [MQTT-3.3.4-6]"
                )
            alias_value = properties.get("topic_alias") if self._is_v5 else None
            if alias_value is not None:
                alias = int(alias_value)
                canonical_topic = self._resolve_outbound_alias(topic, alias)
            else:
                canonical_topic = topic
        else:
            canonical_topic = topic

        allow_empty = alias is not None and not topic
        topic_bytes: bytes | None
        if level is QoS.AT_MOST_ONCE or (
            engine.state == ConnectionState.CONNECTED and self.flow.available > 0
        ):
            topic_bytes = (
                encode_validated_publish_topic(topic, allow_empty=True)
                if allow_empty
                else encode_validated_publish_topic(topic)
            )
            topic_size = len(topic_bytes)
        else:
            topic_size = (
                validate_publish_topic(topic, allow_empty=True)
                if allow_empty
                else validate_publish_topic(topic)
            )
            topic_bytes = None

        if engine.state == ConnectionState.CONNECTED:
            negotiated = engine.negotiated
            if level > negotiated.maximum_qos:
                raise ProtocolError(
                    f"QoS {int(level)} exceeds broker maximum_qos {negotiated.maximum_qos}"
                )
            if retain and not negotiated.retain_available:
                raise ProtocolError("Broker does not support retain")
            if alias is not None:
                if alias > negotiated.topic_alias_maximum:
                    raise ProtocolError(
                        f"topic_alias {alias} exceeds broker topic_alias_maximum "
                        f"{negotiated.topic_alias_maximum}"
                    )

        if engine.state != ConnectionState.CONNECTED and level == QoS.AT_MOST_ONCE:
            raise NotConnectedError("Cannot publish QoS 0 while disconnected")
        return level, topic_bytes, topic_size, canonical_topic

    def _prepare_qos0_validated(
        self,
        topic: str,
        payload: bytes,
        *,
        retain: bool,
        properties: Properties | None,
        topic_bytes: bytes,
    ) -> WriteItem:
        item = self._encode_publish(
            topic,
            payload,
            qos=QoS.AT_MOST_ONCE,
            retain=retain,
            dup=False,
            mid=None,
            properties=properties,
            _topic_bytes=topic_bytes,
        )
        self._engine._check_outbound_size(item)
        return item

    def prepare_qos0(
        self,
        topic: str,
        payload: bytes,
        *,
        retain: bool = False,
        properties: Properties | None = None,
    ) -> WriteItem:
        """Prepare a mutation-free QoS 0 publication for the native runtime."""

        _qos, topic_bytes, _topic_size, _canonical_topic = self._validate_publish_request(
            topic, QoS.AT_MOST_ONCE, retain, properties
        )
        assert topic_bytes is not None
        return self._prepare_qos0_validated(
            topic,
            payload,
            retain=retain,
            properties=properties,
            topic_bytes=topic_bytes,
        )

    def queue_publish(
        self,
        topic: str,
        payload: bytes = b"",
        *,
        qos: QoS | int = QoS.AT_MOST_ONCE,
        retain: bool = False,
        properties: Properties | None = None,
    ) -> PublishHandle:
        if self._engine.state is ConnectionState.DISCONNECTING:
            raise NotConnectedError("publish is not allowed while disconnecting")
        qos, topic_bytes, topic_size, canonical_topic = self._validate_publish_request(
            topic, qos, retain, properties
        )

        if qos == QoS.AT_MOST_ONCE:
            assert topic_bytes is not None
            item = self._prepare_qos0_validated(
                topic,
                payload,
                retain=retain,
                properties=properties,
                topic_bytes=topic_bytes,
            )
            self._engine._send(item)
            if properties is not None and properties.get("topic_alias") is not None:
                self.commit_topic_alias(topic, properties)
            # Completion follows SEND so compatibility on_publish cannot run
            # before the outbound queue has accepted the frame.
            self._engine._emit(EffectKind.PUBLISH_COMPLETE, None)
            return PublishHandle(mid=None, qos=qos)

        # One property encode and the Topic Name bytes from validation feed both
        # the wire-size check and the logical budget. The encoder reuses both so
        # a property table is not walked a second time and a non-ASCII topic is
        # not encoded again on the launch path.
        topic_size, wire_property_bytes, logical_property_bytes, property_bytes = self.size_parts(
            topic, properties, topic_size=topic_size
        )
        # Validate packet size before reserving local memory or a packet id.
        self._check_publish_wire_size(topic_size, wire_property_bytes, len(payload), qos)
        logical_topic_size = topic_size
        if canonical_topic != topic:
            logical_topic_size = (
                len(canonical_topic)
                if canonical_topic.isascii()
                else len(canonical_topic.encode("utf-8"))
            )
        logical_size = len(payload) + logical_topic_size + logical_property_bytes
        # Snapshot before the first acquisition. Three local reads is all the
        # success path pays for a shared rollback; _rollback itself is a call
        # only taken on failure. This path is the hottest in the library and
        # each extra Python frame on it measured ~1.5%.
        messages_start = self._pending_messages
        bytes_start = self._pending_bytes
        inflight_start = self.flow.inflight
        mid: int | None = None
        self._reserve(logical_size)
        try:
            mid = self.packet_ids.allocate()
            msg = OutboundMessage(
                mid=mid,
                topic=canonical_topic,
                payload=payload,
                qos=qos,
                retain=retain,
                state=OutboundQoSState.QUEUED,
                properties=properties,
                logical_size=logical_size,
            )

            # Defer wire encode until launch: avoids double work when messages sit
            # in the Receive Maximum queue.
            #
            # `topic_bytes is not None` records that validation observed an
            # active connection with window available. Keep the explicit state
            # check and `_try_launch` acquisition here as the launch commit point:
            # `_try_launch` owns the slot compensation if encoding/persistence
            # raises, while the outer rollback restores the remaining admission
            # bookkeeping.
            if self._engine.state == ConnectionState.CONNECTED and self._try_launch(
                msg,
                _property_bytes=property_bytes,
                _topic_bytes=topic_bytes,
                _wire_topic=topic,
            ):
                return PublishHandle(mid=mid, qos=qos)

            self.store.put_out(msg)
            self._queued.append(msg)
            return PublishHandle(mid=mid, qos=qos)
        except BaseException:
            self._rollback(
                inflight_start,
                messages_start,
                bytes_start,
                () if mid is None else (mid,),
            )
            raise

    def queue_publish_many(
        self,
        messages: Iterable[tuple[str, bytes, QoS | int, bool, Properties | None]],
    ) -> list[PublishHandle]:
        """Queue one bounded chunk atomically with respect to engine/store state.

        Admission itself is `queue_publish`, once per message — there is exactly
        one place that acquires a budget slot, a packet id and a store row. The
        chunk only widens the rollback scope: the inner call unwinds the message
        that failed, this one unwinds the messages that had already succeeded.

        A chunk that cannot fit the pending-message limit is rejected up front.
        That matters because AsyncClient retries the *same* chunk after waiting
        for space: without this, every retry would re-admit and re-unwind the
        whole prefix. Only the count is checked, never the byte budget — counting
        is free, whereas sizing the chunk here would encode every MQTT 5 property
        table a second time.
        """
        batch = messages if isinstance(messages, list) else list(messages)
        message_limit = self.config.max_pending_outbound_messages
        if message_limit is not None:
            reserving = 0
            for _topic, _payload, qos, _retain, _properties in batch:
                if qos:  # QoS 0 reserves nothing
                    reserving += 1
            if self._pending_messages + reserving > message_limit:
                raise FlowControlError("Pending outbound message limit reached")

        messages_start = self._pending_messages
        bytes_start = self._pending_bytes
        effect_start = len(self._engine._effects)
        queued_start = len(self._queued)
        inflight_start = self.flow.inflight
        packet_ids_empty_start = len(self.packet_ids) == 0
        alias_snapshot: dict[int, str] | None = None
        if self._is_v5:
            for _topic, _payload, _qos, _retain, properties in batch:
                if properties is not None and properties.get("topic_alias") is not None:
                    alias_snapshot = self._topic_aliases.copy()
                    break
        handles: list[PublishHandle] = []
        try:
            with self.store.batch():
                for topic, payload, qos, retain, properties in batch:
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
            self._rollback(
                inflight_start,
                messages_start,
                bytes_start,
                [h.mid for h in handles if h.mid is not None],
                effect_start=effect_start,
                queued_start=queued_start,
                packet_ids_empty_start=packet_ids_empty_start,
            )
            if alias_snapshot is not None:
                self._topic_aliases.clear()
                self._topic_aliases.update(alias_snapshot)
            raise
        return handles

    # --- broker acknowledgements -------------------------------------------

    def _settle(self, mid: int, expected_state: OutboundQoSState) -> bool:
        """Delete a record and release its budget iff it is in `expected_state`.

        With a transition-capable store this never reads the payload back: a
        PUBACK for an 8 MiB publication touches metadata columns only. Without
        one it degrades to the original read-then-delete.
        """
        transitions = self._transitions
        if transitions is not None:
            meta = transitions.complete_out(mid, expected_state)
            if meta is None:
                return False
            self._release_reservation(meta.logical_size)
            return True
        msg = self.store.get_out(mid)
        if msg is None or msg.state is not expected_state:
            return False
        self.complete_record(mid, msg)
        return True

    def on_puback(self, raw: RawPacket) -> None:
        mid, reason_code, properties = self._decode_puback(raw.remaining)
        if properties is not None:
            self._engine._validate_inbound_problem_information(PacketType.PUBACK, properties)
        if not self._settle(mid, OutboundQoSState.WAIT_PUBACK):
            return
        self.flow.release()
        # Emit before freeing the packet id so FIFO receipt settlement remains
        # ordered even if a concurrent publish reuses the mid immediately.
        if reason_code >= 128:
            self._fail(mid, ProtocolError(f"PUBACK reason_code={reason_code}"))
        else:
            self._engine._emit(EffectKind.PUBLISH_COMPLETE, mid)
        self.packet_ids.release(mid)
        self.drain()

    def _require_pubrel_capacity(self, size: int) -> None:
        """Fail locally when a mandatory PUBREL cannot fit the peer limit."""
        limit = self._engine.negotiated.maximum_packet_size
        if limit is not None and size > limit:
            raise MandatoryResponseTooLargeError(
                f"Mandatory PUBREL size {size} exceeds broker maximum_packet_size {limit}"
            )

    def on_pubrec(self, raw: RawPacket) -> None:
        mid, reason_code, properties = self._decode_pubrec(raw.remaining)
        if properties is not None:
            self._engine._validate_inbound_problem_information(PacketType.PUBREC, properties)
        # MQTT 5 §4.3.3: a PUBREC carrying a Reason Code of 0x80 or greater ends
        # the QoS 2 exchange -- the publication failed and no PUBREL follows.
        # This is shared by both store paths on purpose: it used to sit inside
        # the transition branch only, so a store without conditional transitions
        # reached the `msg is None` test first and answered an unknown MID with
        # an orphan PUBREL 0x92 that the specification does not allow.
        if reason_code >= 128:
            self._fail_after_pubrec(mid, reason_code)
            return
        transitions = self._transitions
        limit = self._engine.negotiated.maximum_packet_size
        if limit is not None and limit < 4:
            # The success PUBREL cannot fit. Inspect state only on this rare
            # connection so an unrelated/duplicate PUBREC that would emit
            # nothing keeps its historical behavior, while a real WAIT_PUBREC
            # exchange fails before any durable transition/compaction.
            record = (
                transitions.out_meta(mid) if transitions is not None else self.store.get_out(mid)
            )
            if record is None:
                self._send_orphan_pubrel(mid)
                return
            if record.state is not OutboundQoSState.WAIT_PUBREC:
                return
            self._require_pubrel_capacity(4)
            return
        if transitions is not None:
            changed = transitions.transition_out(
                mid,
                OutboundQoSState.WAIT_PUBREC,
                OutboundQoSState.WAIT_PUBCOMP,
                compact=True,
            )
            if changed is not None:
                self._engine._send(_encode_pubrel_success(mid))
                return
            if transitions.out_meta(mid) is None:
                self._send_orphan_pubrel(mid)
            return

        msg = self.store.get_out(mid)
        if msg is None:
            self._send_orphan_pubrel(mid)
            return
        if msg.state is not OutboundQoSState.WAIT_PUBREC:
            return
        msg.state = OutboundQoSState.WAIT_PUBCOMP
        msg.topic = ""
        msg.payload = b""
        msg.properties = None
        msg.encoded_publish = None
        if msg.encoded_pubrel is None:
            msg.encoded_pubrel = _encode_pubrel_success(mid)
        self.store.update_out(msg)
        self._engine._send(msg.encoded_pubrel)

    def _send_orphan_pubrel(self, mid: int) -> None:
        """Answer a PUBREC with no matching record: PUBREL, 0x92 when MQTT 5."""
        reason = 0x92 if self._engine.codec.is_mqtt5 else 0
        wire = self._encode_pubrel(mid, reason)
        self._require_pubrel_capacity(len(wire))
        self._engine._send(wire)

    def _fail_after_pubrec(self, mid: int, reason_code: int) -> None:
        if not self._settle(mid, OutboundQoSState.WAIT_PUBREC):
            return
        self.flow.release()
        self._fail(mid, ProtocolError(f"PUBREC reason_code={reason_code}"))
        self.packet_ids.release(mid)
        self.drain()

    def on_pubcomp(self, raw: RawPacket) -> None:
        mid, reason_code, properties = self._decode_pubcomp(raw.remaining)
        if properties is not None:
            self._engine._validate_inbound_problem_information(PacketType.PUBCOMP, properties)
        if not self._settle(mid, OutboundQoSState.WAIT_PUBCOMP):
            return
        self.flow.release()
        if reason_code >= 128:
            self._fail(mid, ProtocolError(f"PUBCOMP reason_code={reason_code}"))
        else:
            self._engine._emit(EffectKind.PUBLISH_COMPLETE, mid)
        self.packet_ids.release(mid)
        self.drain()

    # --- launching and retransmission ---------------------------------------

    def _launch(
        self,
        msg: OutboundMessage,
        *,
        persisted: bool = False,
        _property_bytes: bytes | None = None,
        _topic_bytes: bytes | None = None,
        _wire_topic: str | None = None,
    ) -> None:
        wire_topic = msg.topic if _wire_topic is None else _wire_topic
        wire = msg.encoded_publish
        if wire is None:
            wire = self._encode_publish(
                wire_topic,
                msg.payload,
                qos=msg.qos,
                retain=msg.retain,
                dup=msg.dup,
                mid=msg.mid,
                properties=msg.properties,
                _property_bytes=_property_bytes,
                _topic_bytes=_topic_bytes,
            )
        self._engine._check_outbound_size(wire)
        target_state = (
            OutboundQoSState.WAIT_PUBACK
            if msg.qos == QoS.AT_LEAST_ONCE
            else OutboundQoSState.WAIT_PUBREC
        )
        # A contiguous frame owns another payload-sized bytes object and replay
        # re-encodes it anyway. A segmented item owns only its small header; its
        # payload is the application bytes already retained by the record.
        # An alias-only wire form is valid for this connection only. Persisting
        # it would make reconnect replay omit the canonical Topic Name before
        # the replacement connection has learned the mapping.
        retained = _retain_publish_item(wire) if wire_topic == msg.topic else None
        if persisted:
            transitions = self._transitions
            if transitions is not None:
                changed = transitions.transition_out(
                    msg.mid,
                    OutboundQoSState.QUEUED,
                    target_state,
                )
                if changed is None:
                    raise ProtocolError(
                        f"Outbound mid={msg.mid} changed while launching queued publish"
                    )
                # MemoryInflightStore transitions the same object; SQLite only
                # updates durable metadata. Keep the materialised object aligned
                # in either case without rewriting its payload to the store.
                msg.state = target_state
                msg.encoded_publish = retained
            else:
                # Third-party stores keep working through the base interface.
                # update_out guarantees state/dup persistence and may implement
                # the same payload-free optimization as the built-in SQLite store.
                msg.state = target_state
                msg.encoded_publish = retained
                self.store.update_out(msg)
        else:
            # First launch: no durable row exists yet, so persist the full record.
            msg.state = target_state
            msg.encoded_publish = retained
            self.store.put_out(msg)
        self._engine._send(wire)
        properties = msg.properties
        if properties is not None and properties.get("topic_alias") is not None:
            self.commit_topic_alias(wire_topic, properties)

    def _try_launch(
        self,
        msg: OutboundMessage,
        *,
        _property_bytes: bytes | None = None,
        _topic_bytes: bytes | None = None,
        _wire_topic: str | None = None,
    ) -> bool:
        if not self.flow.try_acquire():
            return False
        try:
            self._launch(
                msg,
                _property_bytes=_property_bytes,
                _topic_bytes=_topic_bytes,
                _wire_topic=_wire_topic,
            )
        except Exception:
            self.flow.release()
            raise
        return True

    def _retransmit(self, msg: OutboundMessage) -> None:
        if msg.state in (
            OutboundQoSState.WAIT_PUBACK,
            OutboundQoSState.WAIT_PUBREC,
        ):
            msg.dup = True
            retained = msg.encoded_publish
            if retained is None:
                wire = self._encode_publish(
                    msg.topic,
                    msg.payload,
                    qos=msg.qos,
                    retain=msg.retain,
                    dup=True,
                    mid=msg.mid,
                    properties=msg.properties,
                )
            else:
                # Segmented frames share the original payload. The first replay
                # replaces only their small header; later replays reuse the
                # already-DUP tuple. The bytes branch accepts legacy/custom-store
                # records and drops their contiguous frame after this send.
                wire = _mark_publish_dup(retained)
            self._engine._check_outbound_size(wire)
            msg.encoded_publish = _retain_publish_item(wire)
            self.store.update_out(msg)
            self._engine._send(wire)
            properties = msg.properties
            if properties is not None and properties.get("topic_alias") is not None:
                self.commit_topic_alias(msg.topic, properties)
            return
        if msg.state is OutboundQoSState.WAIT_PUBCOMP:
            if msg.encoded_pubrel is None:
                msg.encoded_pubrel = _encode_pubrel_success(msg.mid)
                self.store.update_out(msg)
            self._engine._check_outbound_size(msg.encoded_pubrel)
            self._engine._send(msg.encoded_pubrel)
            return
        raise ProtocolError(f"Cannot retransmit outbound state {msg.state!r}")

    def drain(self) -> None:
        while self._queued and self.flow.try_acquire():
            stored = self._queued[0]
            try:
                msg = self.materialize(stored)
                if msg.state is OutboundQoSState.QUEUED:
                    self._launch(msg, persisted=True)
                else:
                    self._retransmit(msg)
            except Exception as exc:
                self.flow.release()
                self._queued.popleft()
                self.discard_record(stored.mid, stored)
                self.packet_ids.release(stored.mid)
                self._fail(stored.mid, exc)
                continue
            self._queued.popleft()

    # --- store records and budget -------------------------------------------

    def discard_record(
        self,
        mid: int,
        stored: OutboundMessage | OutboundMessageSummary,
    ) -> None:
        # `stored` is required: recovering it from the store here is what leaked
        # the byte budget when a transactional store had already rolled back.
        self.delete_record(mid)
        self._release_reservation(self.stored_logical_size(stored))

    def delete_record(self, mid: int) -> None:
        try:
            self.store.delete_out(mid)
        except Exception:
            # Preserve the original launch/validation failure. A broken store is
            # surfaced separately by the read/client boundary and must not leak
            # flow slots or packet identifiers in memory.
            pass

    def complete_record(
        self,
        mid: int,
        stored: OutboundMessage | OutboundMessageSummary,
    ) -> None:
        self.store.delete_out(mid)
        self._release_reservation(self.stored_logical_size(stored))

    def materialize(self, stored: OutboundMessage | OutboundMessageSummary) -> OutboundMessage:
        if isinstance(stored, OutboundMessage):
            return stored
        message = self.store.get_out(stored.mid)
        if message is None:
            raise ProtocolError(f"Missing durable outbound record mid={stored.mid}")
        if message.logical_size <= 0:
            message.logical_size = self.stored_logical_size(stored)
        return message

    # --- sizing --------------------------------------------------------------

    def logical_size(
        self,
        topic: str,
        payload: bytes,
        properties: Properties | None,
    ) -> int:
        return self.logical_size_from_size(topic, len(payload), properties)

    def logical_size_from_size(
        self,
        topic: str,
        payload_size: int,
        properties: Properties | None,
    ) -> int:
        return publish_logical_size(self._is_v5, topic, payload_size, properties)

    def stored_logical_size(self, stored: OutboundMessage | OutboundMessageSummary) -> int:
        if stored.logical_size > 0:
            return stored.logical_size
        payload_size = (
            len(stored.payload) if isinstance(stored, OutboundMessage) else stored.payload_size
        )
        logical_size = self.logical_size_from_size(stored.topic, payload_size, stored.properties)
        if isinstance(stored, OutboundMessage):
            stored.logical_size = logical_size
        else:
            object.__setattr__(stored, "logical_size", logical_size)
        return logical_size

    def size_parts(
        self,
        topic: str,
        properties: Properties | None,
        *,
        topic_size: int | None = None,
    ) -> tuple[int, int, int, bytes | None]:
        """Topic byte count, wire property bytes, logical property bytes, encoded table.

        The wire size must count MQTT 5's mandatory property-length byte even
        when there are no properties; the logical budget accounts for
        application data only, so it does not. MQTT 5 returns the encoded table
        so the PUBLISH encoder can reuse it; MQTT 3.1.1 returns ``None``.
        Admission passes *topic_size* from validation so a non-ASCII Topic Name
        is not encoded a second time just to learn its length.
        """
        if topic_size is None:
            topic_size = len(topic) if topic.isascii() else len(topic.encode("utf-8"))
        if not self._is_v5:
            return topic_size, 0, 0, None
        if properties is None or not properties.values:
            # An empty property table encodes to the single length byte 0x00.
            return topic_size, 1, 0, b"\x00"
        encoded = encode_properties(properties, PUBLISH)
        wire_property_bytes = len(encoded)
        return topic_size, wire_property_bytes, wire_property_bytes, encoded

    @staticmethod
    def _publish_wire_size_from_parts(
        topic_bytes: int,
        wire_property_bytes: int,
        payload_size: int,
        qos: QoS,
    ) -> int:
        remaining = 2 + topic_bytes + (2 if qos else 0) + wire_property_bytes + payload_size
        if remaining > MAX_VBI:
            raise PacketTooLargeError(
                f"PUBLISH remaining length {remaining} exceeds MQTT wire maximum {MAX_VBI}"
            )
        return 1 + vbi_len(remaining) + remaining

    def publish_wire_size(
        self,
        topic: str,
        payload_size: int,
        qos: QoS | int,
        properties: Properties | None,
    ) -> int:
        level = QoS(qos)
        topic_bytes, wire_property_bytes, _, _ = self.size_parts(topic, properties)
        return self._publish_wire_size_from_parts(
            topic_bytes, wire_property_bytes, payload_size, level
        )

    def _check_publish_wire_size(
        self,
        topic_bytes: int,
        wire_property_bytes: int,
        payload_size: int,
        qos: QoS,
    ) -> None:
        """Cheap upper bound vs negotiated maximum_packet_size (before full encode)."""
        limit = self._engine.negotiated.maximum_packet_size
        if limit is None:
            return
        encoded_size = self._publish_wire_size_from_parts(
            topic_bytes, wire_property_bytes, payload_size, qos
        )
        if encoded_size > limit:
            raise PacketTooLargeError(
                f"Encoded packet size {encoded_size} exceeds broker maximum_packet_size {limit}"
            )

    def _check_publish_size(
        self,
        topic: str,
        payload_size: int,
        qos: QoS,
        properties: Properties | None,
    ) -> None:
        topic_bytes, wire_property_bytes, _, _ = self.size_parts(topic, properties)
        self._check_publish_wire_size(topic_bytes, wire_property_bytes, payload_size, qos)

    # --- hydration and replay -------------------------------------------------

    def store_summary_pages(
        self,
    ) -> Iterable[tuple[OutboundMessage | OutboundMessageSummary, ...]]:
        if self._paged_store is not None:
            yield from self._paged_store.out_summary_pages()
        else:
            yield tuple(self.store.out_items())

    def has_client_session_state(self) -> bool:
        """Whether the client has an outbound exchange the Server can resume."""
        # Hydration reserves a flow slot for every persisted WAIT_* exchange it
        # can admit. The window is never reset before CONNACK, so a non-zero
        # count is an O(1) proof that at least one sent QoS 1/2 exchange exists.
        return self.flow.inflight > 0

    def hydrate(self) -> None:
        """Recover packet ids and the offline queue from a durable store."""
        with self.store.batch():
            for page in self.store_summary_pages():
                for msg in page:
                    self._hydrate_message(msg)

    def _hydrate_message(self, msg: OutboundMessage | OutboundMessageSummary) -> None:
        self.packet_ids.reserve(msg.mid)
        unknown_size = msg.logical_size <= 0
        logical_size = self.stored_logical_size(msg)
        if unknown_size and self._transitions is not None:
            # Records written before the store persisted logical sizes would
            # otherwise release nothing from the byte budget when a
            # metadata-only acknowledgement settles them. One write per legacy
            # record, inside the hydration batch, and never again.
            self._transitions.set_out_logical_size(msg.mid, logical_size)
        self._pending_messages += 1
        self._pending_bytes += logical_size
        if msg.state is OutboundQoSState.QUEUED:
            self._queued.append(msg)
        elif msg.state in (
            OutboundQoSState.WAIT_PUBACK,
            OutboundQoSState.WAIT_PUBREC,
            OutboundQoSState.WAIT_PUBCOMP,
        ):
            self.flow.try_acquire()

    def _replay_message(self, msg: OutboundMessage | OutboundMessageSummary) -> None:
        try:
            self.validate_against_negotiated(msg)
        except (ProtocolError, PacketTooLargeError) as exc:
            self.discard_record(msg.mid, msg)
            self.packet_ids.release(msg.mid)
            self._fail(msg.mid, exc)
            return
        if msg.state is OutboundQoSState.QUEUED:
            self._queued.append(msg)
            return
        if not self.flow.try_acquire():
            self._queued.append(msg)
            return
        try:
            self._retransmit(self.materialize(msg))
        except Exception as exc:
            self.flow.release()
            self.discard_record(msg.mid, msg)
            self.packet_ids.release(msg.mid)
            self._fail(msg.mid, exc)

    def reset_flow_for_connection(self) -> None:
        """Restart the outbound window from the CONNACK-negotiated limit."""
        self.flow.reset()
        self.flow.apply_broker_receive_maximum(
            self._engine.negotiated.receive_maximum,
            self.config.max_outbound_inflight,
        )

    def replay_session(self) -> None:
        self.reset_flow_for_connection()
        self._queued.clear()
        for page in self.store_summary_pages():
            for msg in page:
                self._replay_message(msg)
        self.drain()

    def purge_after_clean_session(self, *, sub_mids_pending: bool) -> None:
        """Fail every unacknowledged publication: the broker dropped the session.

        `_queued` mirrors every outbound record that must survive a missing
        broker session — including WAIT_* entries `replay_session()` left there
        when the Receive Maximum window could not admit their retransmission.
        Those records are failed below, so their queue entries must go with
        them: a stale entry made `drain()` re-materialise a deleted record,
        double-release its byte reservation and retransmit a packet id the
        pool no longer owns.

        With no queued work and no SUB/UNSUB in flight, every packet id belongs
        to a record discarded here, so the pool is reset in constant time
        rather than id by id — it reclaims its accumulated hashing capacity
        only on a full clear.
        """
        if any(m.state is not OutboundQoSState.QUEUED for m in self._queued):
            self._queued = deque(m for m in self._queued if m.state is OutboundQoSState.QUEUED)
        clear_abandoned_packet_ids = not self._queued and not sub_mids_pending
        for page in self.store_summary_pages():
            for msg in page:
                if msg.state is OutboundQoSState.QUEUED:
                    continue
                self.complete_record(msg.mid, msg)
                if not clear_abandoned_packet_ids:
                    self.packet_ids.release(msg.mid)
                self._fail(
                    msg.mid,
                    SessionDiscardedError("Publish lost: clean session replaced the previous one"),
                )
        if clear_abandoned_packet_ids:
            self.packet_ids.clear()

    # --- negotiation ----------------------------------------------------------

    def validate_against_negotiated(self, msg: OutboundMessage | OutboundMessageSummary) -> None:
        engine = self._engine
        negotiated = engine.negotiated
        if msg.state is OutboundQoSState.WAIT_PUBCOMP:
            pubrel = msg.encoded_pubrel if isinstance(msg, OutboundMessage) else None
            if pubrel is None:
                pubrel = _encode_pubrel_success(msg.mid)
            engine._check_outbound_size(pubrel)
            return
        if int(msg.qos) > negotiated.maximum_qos:
            raise ProtocolError(
                f"QoS {int(msg.qos)} exceeds broker maximum_qos {negotiated.maximum_qos}"
            )
        if msg.retain and not negotiated.retain_available:
            raise ProtocolError("Broker does not support retain")
        if msg.properties and msg.properties.get("topic_alias") is not None:
            alias = int(msg.properties.get("topic_alias"))
            if alias == 0 or alias > negotiated.topic_alias_maximum:
                raise ProtocolError(
                    f"topic_alias {alias} exceeds broker topic_alias_maximum "
                    f"{negotiated.topic_alias_maximum}"
                )
        encoded_publish = msg.encoded_publish if isinstance(msg, OutboundMessage) else None
        if encoded_publish is not None:
            engine._check_outbound_size(encoded_publish)
        else:
            payload_size = (
                len(msg.payload) if isinstance(msg, OutboundMessage) else msg.payload_size
            )
            self._check_publish_size(msg.topic, payload_size, msg.qos, msg.properties)

    def fail_queued_violating_negotiation(self) -> None:
        kept: deque[OutboundMessage | OutboundMessageSummary] = deque()
        while self._queued:
            msg = self._queued.popleft()
            try:
                self.validate_against_negotiated(msg)
            except (ProtocolError, PacketTooLargeError) as exc:
                self.discard_record(msg.mid, msg)
                self.packet_ids.release(msg.mid)
                self._fail(msg.mid, exc)
                continue
            kept.append(msg)
        self._queued = kept
