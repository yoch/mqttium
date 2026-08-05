"""Broker→client publication: aliases, QoS state, receive window and replay.

`InboundSession` owns the state retained for inbound PUBLISH handling: topic
aliases, the local Receive Maximum count, persisted QoS 1/2 records and restart
redelivery. `ProtocolEngine` still owns connection state and the shared effect
stream; handlers emit through it so observable effect ordering does not change.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

from mqttium.codec.buffer import RawPacket
from mqttium.enums import ConnectionState, InboundQoSState, MQTTProtocolVersion, QoS
from mqttium.errors import ProtocolError
from mqttium.persistence.memory import PagedInflightStore, TransitionInflightStore
from mqttium.packets import (
    PubAckPacket,
    PubCompPacket,
    PublishPacket,
    PubRecPacket,
    PubRelPacket,
    encode_disconnect,
)
from mqttium.protocol.effects import DisconnectInfo, EffectKind
from mqttium.protocol.stats import InboundStats
from mqttium.topics import validate_received_publish_topic
from mqttium.types import InboundMessage, Message

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


class InboundReplayCursor:
    """Position inside a paged inbound replay.

    Holding a page iterator instead of a materialised list is the whole point:
    only the current page of messages is alive at any time.
    """

    __slots__ = ("_pages", "_page", "_offset")

    def __init__(self, pages: Iterator[tuple[InboundMessage, ...]]) -> None:
        self._pages = pages
        self._page: tuple[InboundMessage, ...] = ()
        self._offset = 0

    def next_message(self) -> InboundMessage | None:
        while self._offset >= len(self._page):
            page = next(self._pages, None)
            if page is None:
                return None
            self._page = page
            self._offset = 0
        message = self._page[self._offset]
        self._offset += 1
        return message


class InboundSession:
    """Authoritative inbound QoS and connection-scoped alias state."""

    __slots__ = (
        "_aliases",
        "_engine",
        "_inflight",
        "_paged_store",
        "_recovered_mids",
        "_replay",
        "_transitions",
        "config",
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
        self._aliases: dict[int, str] = {}
        self._inflight = 0
        self._replay: InboundReplayCursor | None = None
        self._recovered_mids = self._load_recovered_mids()

    def _load_recovered_mids(self) -> set[int]:
        """Identifiers persisted by a previous run, without their payloads."""
        transitions = self._transitions
        if transitions is None:
            return {message.mid for message in self.store.in_items()}
        mids: set[int] = set()
        for page in transitions.in_index_pages(REPLAY_PAGE_SIZE):
            mids.update(meta.mid for meta in page)
        return mids

    # --- lifecycle ---------------------------------------------------------

    def start_connection(self) -> None:
        """Reset state scoped to one network connection before CONNECT."""
        self._aliases.clear()
        self._inflight = 0
        # A replay belongs to the connection that started it: its continuation
        # effect is dropped with the epoch, so the cursor must go too.
        self._replay = None

    def transport_closed(self) -> None:
        self._aliases.clear()

    def discard_session(self) -> None:
        """Drop inbound state after CONNACK reports no previous session."""
        self.store.clear_in()
        self._recovered_mids.clear()
        self._replay = None
        self._inflight = 0

    @property
    def replay_pending(self) -> bool:
        """True while restart redelivery still has batches to emit."""
        return self._replay is not None

    def stats(self) -> InboundStats:
        """Snapshot this session's own accounting."""
        return InboundStats(
            inflight=self._inflight,
            receive_maximum=self.config.local_receive_maximum,
            topic_aliases=len(self._aliases),
            replay_pending=self._replay is not None,
        )

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
            transitions = self._transitions
            # Only existence matters here; re-reading the stored payload to
            # answer a duplicate PUBLISH was pure waste.
            duplicate = (
                transitions.contains_in(packet.mid)
                if transitions is not None
                else store.get_in(packet.mid) is not None
            )
            if duplicate:
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
        transitions = self._transitions
        if transitions is not None:
            meta = transitions.in_meta(rel.mid)
            if meta is None:
                # Orphan PUBREL: idempotent PUBCOMP (v5 reason 0x92 optional).
                engine._send(PubCompPacket(mid=rel.mid).encode(config.protocol))
                return
            if config.manual_ack and not meta.user_acked:
                transitions.transition_in(rel.mid, meta.state, InboundQoSState.WAIT_USER_ACK)
                return
            transitions.complete_in(rel.mid, meta.state)
            engine._send(PubCompPacket(mid=rel.mid).encode(config.protocol))
            self._release_slot()
            return

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
        if transitions is not None:
            meta = transitions.in_meta(mid)
            if meta is None:
                raise ProtocolError(f"No pending inbound ack for mid={mid}")
            state = meta.state
            if state is InboundQoSState.WAIT_PUBACK:
                transitions.complete_in(mid, state)
                self._engine._send(PubAckPacket(mid=mid).encode(config.protocol))
                self._release_slot()
                return
            if state is InboundQoSState.WAIT_PUBREL:
                transitions.transition_in(mid, state, state, user_acked=True)
                return
            if state is InboundQoSState.WAIT_USER_ACK:
                transitions.complete_in(mid, state)
                self._engine._send(PubCompPacket(mid=mid).encode(config.protocol))
                self._release_slot()
                return
            raise ProtocolError(f"Inbound mid={mid} is not awaiting ack (state={state!r})")

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
        """Restore Receive Maximum accounting and start redelivering.

        Only the first bounded batch is emitted here. The rest is pumped through
        `CONTINUE_INBOUND_REPLAY`, so a 10,000-message durable session no longer
        materialises 10,000 payloads — nor 10,000 effects — before the runtime
        gets a chance to apply delivery backpressure.
        """
        paged = self._paged_store
        transitions = self._transitions
        if paged is None or transitions is None:
            # A store without the paging or metadata extensions keeps the
            # original eager behaviour: correct, just not bounded.
            inbound_items = list(self.store.in_items())
            self._inflight = len(inbound_items)
            for inbound in inbound_items:
                if self._should_redeliver(inbound):
                    self._emit_message(inbound, dup=True)
            self._recovered_mids.clear()
            return

        # The Receive Maximum window must be restored in full before the first
        # redelivery, so the count is taken from the index — no payload read.
        persisted = 0
        for page in transitions.in_index_pages(REPLAY_PAGE_SIZE):
            persisted += len(page)
        self._inflight = persisted
        if persisted == 0:
            self._recovered_mids.clear()
            return
        self._replay = InboundReplayCursor(iter(paged.in_pages(REPLAY_PAGE_SIZE)))
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
            scanned += 1
            if not self._should_redeliver(inbound):
                continue
            self._emit_message(inbound, dup=True)
            emitted += 1
            emitted_bytes += len(inbound.payload) + len(inbound.topic)
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
