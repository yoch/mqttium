"""Immutable runtime statistics for :class:`mqttium.api.AsyncClient`."""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.enums import ConnectionState


@dataclass(slots=True, frozen=True)
class TaskStats:
    reader: bool
    writer: bool
    keepalive: bool
    reconnect: bool
    effect_flush: bool
    callback_worker: bool


@dataclass(slots=True, frozen=True)
class ProtocolStats:
    pending_outbound_messages: int
    pending_outbound_bytes: int
    pending_outbound_high_water_messages: int
    pending_outbound_high_water_bytes: int
    queued_outbound_messages: int
    flow_inflight: int
    flow_limit: int
    packet_ids_in_use: int
    inbound_inflight: int


@dataclass(slots=True, frozen=True)
class EffectStats:
    pending: int
    pending_high_water: int
    enqueued: int
    applied: int
    waiters: int


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


@dataclass(slots=True, frozen=True)
class DecoderStats:
    buffered_bytes: int
    high_water_bytes: int
    max_packet_size: int


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
class TransportStats:
    kind: str | None
    closing: bool
    pending_write_bytes: int
    buffered_read_bytes: int
    fragmented_read_bytes: int
    pending_control_frames: int
    pending_control_bytes: int


@dataclass(slots=True, frozen=True)
class ClientStats:
    """One point-in-time, side-effect-free runtime snapshot.

    High-water fields are measured over the lifetime of the client or protocol
    engine. Calling :meth:`AsyncClient.stats` does not enable background
    sampling and does not reset any counter.
    """

    state: ConnectionState
    connection_epoch: int
    reconnect_attempt: int
    tasks: TaskStats
    protocol: ProtocolStats
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
    "ProtocolStats",
    "ReceiptStats",
    "TaskStats",
    "TransportStats",
    "WriterStats",
]
