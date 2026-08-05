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
from mqttium.codec.vbi import encode_vbi
from mqttium.enums import (
    ConnectionState,
    MQTTProtocolVersion,
    OutboundQoSState,
    QoS,
)
from mqttium.errors import (
    FlowControlError,
    NotConnectedError,
    PacketTooLargeError,
    ProtocolError,
    SessionDiscardedError,
)
from mqttium.packets import (
    PubAckPacket,
    PubCompPacket,
    PublishPacket,
    PubRecPacket,
    PubRelPacket,
)
from mqttium.persistence.memory import PagedInflightStore, TransitionInflightStore
from mqttium.protocol.effects import EffectKind, PublishFailure, PublishHandle
from mqttium.protocol.flow_control import FlowControl
from mqttium.protocol.packet_ids import PacketIdPool
from mqttium.protocol.stats import OutboundStats
from mqttium.topics import validate_publish_topic
from mqttium.transport.writes import WriteItem
from mqttium.types import OutboundMessage, OutboundMessageSummary, Properties

if TYPE_CHECKING:
    from mqttium.protocol.engine import ProtocolEngine


class OutboundSession:
    __slots__ = (
        "_engine",
        "_queued",
        "_paged_store",
        "_transitions",
        "_pending_bytes",
        "_pending_high_water_bytes",
        "_pending_high_water_messages",
        "_pending_messages",
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
        self.packet_ids = PacketIdPool()
        self.flow = FlowControl(self.config.local_receive_maximum)
        self._queued: deque[OutboundMessage | OutboundMessageSummary] = deque()
        self._pending_messages = 0
        self._pending_bytes = 0
        self._pending_high_water_messages = 0
        self._pending_high_water_bytes = 0

    # --- effect emission ---------------------------------------------------
    # Always routed through the engine, never into a cached list: take_effects()
    # rebinds the list, and _emit is an interception point that must see every
    # effect in the order it was produced.

    def _emit(self, kind: EffectKind, data: object = None) -> None:
        self._engine._emit(kind, data)

    def _send(self, packet: WriteItem) -> None:
        self._engine._send(packet)

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

    def queue_publish(
        self,
        topic: str,
        payload: bytes = b"",
        *,
        qos: QoS | int = QoS.AT_MOST_ONCE,
        retain: bool = False,
        properties: Properties | None = None,
    ) -> PublishHandle:
        engine = self._engine
        qos = QoS(qos)
        allow_empty = bool(
            properties
            and properties.get("topic_alias")
            and self.config.protocol == MQTTProtocolVersion.MQTTv5
        )
        validate_publish_topic(topic, allow_empty=allow_empty)

        if engine.state == ConnectionState.CONNECTED:
            negotiated = engine.negotiated
            if qos > negotiated.maximum_qos:
                raise ProtocolError(
                    f"QoS {int(qos)} exceeds broker maximum_qos {negotiated.maximum_qos}"
                )
            if retain and not negotiated.retain_available:
                raise ProtocolError("Broker does not support retain")
            if properties and properties.get("topic_alias") is not None:
                alias = int(properties.get("topic_alias"))
                if alias > negotiated.topic_alias_maximum:
                    raise ProtocolError(
                        f"topic_alias {alias} exceeds broker topic_alias_maximum "
                        f"{negotiated.topic_alias_maximum}"
                    )

        if engine.state != ConnectionState.CONNECTED and qos == QoS.AT_MOST_ONCE:
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
            engine._check_outbound_size(wire)
            self._send(wire)
            # Completion follows SEND so compatibility on_publish cannot run
            # before the outbound queue has accepted the frame.
            self._emit(EffectKind.PUBLISH_COMPLETE, None)
            return PublishHandle(mid=None, qos=qos)

        # One property encode and one topic measurement feed both the wire-size
        # check and the logical budget.
        topic_bytes, wire_property_bytes, logical_property_bytes = self.size_parts(
            topic, properties
        )
        # Validate packet size before reserving local memory or a packet id.
        self._check_publish_wire_size(topic_bytes, wire_property_bytes, len(payload), qos)
        logical_size = len(payload) + topic_bytes + logical_property_bytes
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
                topic=topic,
                payload=payload,
                qos=qos,
                retain=retain,
                state=OutboundQoSState.QUEUED,
                properties=properties,
                logical_size=logical_size,
            )

            # Defer wire encode until launch: avoids double work when messages sit
            # in the Receive Maximum queue.
            if engine.state == ConnectionState.CONNECTED and self._try_launch(msg):
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
        ack = PubAckPacket.decode(raw.remaining, self.config.protocol)
        if not self._settle(ack.mid, OutboundQoSState.WAIT_PUBACK):
            return
        self.flow.release()
        # Emit before freeing the packet id so FIFO receipt settlement remains
        # ordered even if a concurrent publish reuses the mid immediately.
        if ack.reason_code >= 128:
            self._fail(ack.mid, ProtocolError(f"PUBACK reason_code={ack.reason_code}"))
        else:
            self._emit(EffectKind.PUBLISH_COMPLETE, ack.mid)
        self.packet_ids.release(ack.mid)
        self.drain()

    def on_pubrec(self, raw: RawPacket) -> None:
        rec = PubRecPacket.decode(raw.remaining, self.config.protocol)
        transitions = self._transitions
        if transitions is not None:
            # Two metadata statements replace a full record read plus update:
            # the PUBLISH phase is over, so nothing but the state is needed.
            meta = transitions.out_meta(rec.mid)
            if meta is None:
                self._send_orphan_pubrel(rec.mid)
                return
            if meta.state is not OutboundQoSState.WAIT_PUBREC:
                return
            if rec.reason_code >= 128:
                self._fail_after_pubrec(rec.mid, rec.reason_code)
                return
            transitions.transition_out(
                rec.mid,
                OutboundQoSState.WAIT_PUBREC,
                OutboundQoSState.WAIT_PUBCOMP,
                compact=True,
            )
            # Keep the local flow slot until PUBCOMP. MQTT 5 allows releasing at
            # PUBREC, but freeing early lets WAIT_PUBCOMP accumulate without
            # bound and has caused intermittent multi-second stalls under load.
            self._send(PubRelPacket(mid=rec.mid).encode(self.config.protocol))
            return

        msg = self.store.get_out(rec.mid)
        if msg is None:
            self._send_orphan_pubrel(rec.mid)
            return
        if msg.state is not OutboundQoSState.WAIT_PUBREC:
            return
        if rec.reason_code >= 128:
            self._fail_after_pubrec(rec.mid, rec.reason_code)
            return
        msg.state = OutboundQoSState.WAIT_PUBCOMP
        msg.encoded_publish = None
        if msg.encoded_pubrel is None:
            msg.encoded_pubrel = PubRelPacket(mid=rec.mid).encode(self.config.protocol)
        self.store.update_out(msg)
        self._send(msg.encoded_pubrel)

    def _send_orphan_pubrel(self, mid: int) -> None:
        """Answer a PUBREC with no matching record: PUBREL, 0x92 when MQTT 5."""
        reason = 0x92 if self.config.protocol == MQTTProtocolVersion.MQTTv5 else 0
        self._send(PubRelPacket(mid=mid, reason_code=reason).encode(self.config.protocol))

    def _fail_after_pubrec(self, mid: int, reason_code: int) -> None:
        if not self._settle(mid, OutboundQoSState.WAIT_PUBREC):
            return
        self.flow.release()
        self._fail(mid, ProtocolError(f"PUBREC reason_code={reason_code}"))
        self.packet_ids.release(mid)
        self.drain()

    def on_pubcomp(self, raw: RawPacket) -> None:
        comp = PubCompPacket.decode(raw.remaining, self.config.protocol)
        if not self._settle(comp.mid, OutboundQoSState.WAIT_PUBCOMP):
            return
        self.flow.release()
        if comp.reason_code >= 128:
            self._fail(comp.mid, ProtocolError(f"PUBCOMP reason_code={comp.reason_code}"))
        else:
            self._emit(EffectKind.PUBLISH_COMPLETE, comp.mid)
        self.packet_ids.release(comp.mid)
        self.drain()

    # --- launching and retransmission ---------------------------------------

    def _launch(self, msg: OutboundMessage) -> None:
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
        self._engine._check_outbound_size(msg.encoded_publish)
        if msg.qos == QoS.AT_LEAST_ONCE:
            msg.state = OutboundQoSState.WAIT_PUBACK
        else:
            msg.state = OutboundQoSState.WAIT_PUBREC
        self.store.put_out(msg)
        self._send(msg.encoded_publish)

    def _try_launch(self, msg: OutboundMessage) -> bool:
        if not self.flow.try_acquire():
            return False
        try:
            self._launch(msg)
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
            msg.encoded_publish = PublishPacket(
                topic=msg.topic,
                payload=msg.payload,
                qos=msg.qos,
                retain=msg.retain,
                dup=True,
                mid=msg.mid,
                properties=msg.properties,
            ).encode_write_item(self.config.protocol)
            self._engine._check_outbound_size(msg.encoded_publish)
            self.store.update_out(msg)
            self._send(msg.encoded_publish)
            return
        if msg.state is OutboundQoSState.WAIT_PUBCOMP:
            if msg.encoded_pubrel is None:
                msg.encoded_pubrel = PubRelPacket(mid=msg.mid).encode(self.config.protocol)
                self.store.update_out(msg)
            self._engine._check_outbound_size(msg.encoded_pubrel)
            self._send(msg.encoded_pubrel)
            return
        raise ProtocolError(f"Cannot retransmit outbound state {msg.state!r}")

    def drain(self) -> None:
        while self._queued and self.flow.available > 0:
            if not self.flow.try_acquire():
                break
            stored = self._queued[0]
            try:
                msg = self.materialize(stored)
                if msg.state is OutboundQoSState.QUEUED:
                    self._launch(msg)
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
        property_bytes = 0
        if (
            self.config.protocol == MQTTProtocolVersion.MQTTv5
            and properties is not None
            and properties.values
        ):
            property_bytes = len(encode_properties(properties, PUBLISH))
        topic_bytes = len(topic) if topic.isascii() else len(topic.encode("utf-8"))
        return payload_size + topic_bytes + property_bytes

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
    ) -> tuple[int, int, int]:
        """Topic bytes, wire property bytes, logical property bytes — one encode.

        The wire size must count MQTT 5's mandatory property-length byte even
        when there are no properties; the logical budget accounts for
        application data only, so it does not.
        """
        topic_bytes = len(topic) if topic.isascii() else len(topic.encode("utf-8"))
        if self.config.protocol != MQTTProtocolVersion.MQTTv5:
            return topic_bytes, 0, 0
        if properties is None or not properties.values:
            # An empty property table encodes to the single length byte 0x00.
            return topic_bytes, 1, 0
        wire_property_bytes = len(encode_properties(properties, PUBLISH))
        logical_property_bytes = (
            wire_property_bytes if properties is not None and properties.values else 0
        )
        return topic_bytes, wire_property_bytes, logical_property_bytes

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
        remaining = 2 + topic_bytes + (2 if qos else 0) + wire_property_bytes + payload_size
        encoded_size = 1 + len(encode_vbi(remaining)) + remaining
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
        topic_bytes, wire_property_bytes, _ = self.size_parts(topic, properties)
        self._check_publish_wire_size(topic_bytes, wire_property_bytes, payload_size, qos)

    # --- hydration and replay -------------------------------------------------

    def store_summary_pages(
        self,
    ) -> Iterable[tuple[OutboundMessage | OutboundMessageSummary, ...]]:
        if self._paged_store is not None:
            yield from self._paged_store.out_summary_pages()
        else:
            yield tuple(self.store.out_items())

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

    def replay_session(self) -> None:
        engine = self._engine
        self.flow.reset()
        self.flow.apply_broker_receive_maximum(
            engine.negotiated.receive_maximum,
            self.config.local_receive_maximum,
            self.config.max_outbound_inflight,
        )
        self._queued.clear()
        for page in self.store_summary_pages():
            for msg in page:
                self._replay_message(msg)
        self.drain()

    def purge_after_clean_session(self, *, sub_mids_pending: bool) -> None:
        """Fail every unacknowledged publication: the broker dropped the session.

        `_queued` mirrors every outbound record that must survive a missing
        broker session. With no queued work and no SUB/UNSUB in flight, every
        packet id belongs to a record discarded here, so the pool is reset in
        constant time rather than id by id — it reclaims its accumulated hashing
        capacity only on a full clear.
        """
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
                pubrel = PubRelPacket(mid=msg.mid).encode(self.config.protocol)
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
            if alias > negotiated.topic_alias_maximum:
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
