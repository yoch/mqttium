"""Immutable runtime statistics for :class:`mqttium.api.AsyncClient`."""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.enums import ConnectionState
from mqttium.protocol.stats import InboundStats, OutboundStats
from mqttium.transport.stats import TransportStats


@dataclass(slots=True, frozen=True)
class TaskStats:
    reader: bool
    writer: bool
    keepalive: bool
    reconnect: bool
    effect_flush: bool
    callback_worker: bool


@dataclass(slots=True, frozen=True)
class EffectStats:
    pending: int
    pending_high_water: int
    enqueued: int
    applied: int
    waiters: int
    # Decision counters: how often several effects arrive together, how often
    # that actually required reordering, and how much rode the inline fast path.
    # Effects that did not is `enqueued`, so there is no separate counter.
    batches: int
    multi_effect_batches: int
    reordered_batches: int
    inline_effects: int
    apply_suspensions: int


@dataclass(slots=True, frozen=True)
class WriterStats:
    queued_messages: int
    queued_bytes: int
    high_water_messages: int
    high_water_bytes: int
    max_messages: int
    max_bytes: int
    waiters: int
    last_outbound: float
    # Decision counters: batch count and shape, plus how often a producer had to
    # wait for queue space.
    batches: int
    batched_items: int
    batched_bytes: int
    segmented_writes: int
    enqueue_suspensions: int


@dataclass(slots=True, frozen=True)
class DecoderStats:
    buffered_bytes: int
    high_water_bytes: int
    max_packet_size: int
    ingress_batch_limit_bytes: int


@dataclass(slots=True, frozen=True)
class DeliveryStats:
    iterator_queued: int
    iterator_limit: int
    callback_queued: int
    callback_limit: int
    pending_bytes: int
    pending_high_water_bytes: int
    accounted_limit: int | None
    small_budget_bytes: int
    small_message_limit: int | None
    waiters: int


@dataclass(slots=True, frozen=True)
class ReceiptStats:
    publish: int
    publish_batches: int
    subscribe: int
    unsubscribe: int
    publish_waiters: int


@dataclass(slots=True, frozen=True)
class ClientStats:
    """One point-in-time, side-effect-free runtime snapshot.

    Every section is produced by the component that owns the state — the two
    protocol sessions, the effect pump, the write pump and the transport — so
    this class only assembles them. High-water fields are measured over the
    lifetime of the client or protocol engine. Calling :meth:`AsyncClient.stats`
    does not enable background sampling and does not reset any counter.
    """

    state: ConnectionState
    connection_epoch: int
    reconnect_attempt: int
    tasks: TaskStats
    outbound: OutboundStats
    inbound: InboundStats
    effects: EffectStats
    writer: WriterStats
    decoder: DecoderStats
    delivery: DeliveryStats
    receipts: ReceiptStats
    transport: TransportStats


__all__ = [
    "ClientStats",
    "DecoderStats",
    "DeliveryStats",
    "EffectStats",
    "InboundStats",
    "OutboundStats",
    "ReceiptStats",
    "TaskStats",
    "TransportStats",
    "WriterStats",
]
