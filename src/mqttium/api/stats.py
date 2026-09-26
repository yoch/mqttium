"""Immutable runtime statistics for :class:`mqttium.api.AsyncClient`.

The snapshot gives the application a view of what the client is doing without
any logger: connection state, queue occupancy against its configured bound,
lifetime high-water marks, and whether anyone is currently waiting. Fields
describe the client's contract with the application and the broker, not how
the runtime is scheduled internally.
"""

from __future__ import annotations

from dataclasses import dataclass

from mqttium.enums import ConnectionState
from mqttium.protocol.stats import InboundStats, OutboundStats
from mqttium.transport.stats import TransportStats


@dataclass(slots=True, frozen=True)
class WriterStats:
    """Encoded frames waiting for the transport, against the write-queue bounds."""

    queued_messages: int
    queued_bytes: int
    high_water_messages: int
    high_water_bytes: int
    message_limit: int
    byte_limit: int
    waiters: int
    last_outbound: float


@dataclass(slots=True, frozen=True)
class DecoderStats:
    """Received bytes not yet decoded."""

    buffered_bytes: int
    high_water_bytes: int


@dataclass(slots=True, frozen=True)
class DeliveryStats:
    """Messages held for the application, against the iterator bounds.

    Callback delivery retains nothing, so every field stays at its idle value
    in that mode. Iterator byte occupancy is tracked only when
    ``max_iterator_bytes`` is finite; when that bound is ``None``,
    ``iterator_bytes`` and ``iterator_high_water_bytes`` stay at zero and the
    message-count fields remain authoritative.
    """

    iterator_queued: int
    iterator_limit: int
    iterator_bytes: int
    iterator_high_water_bytes: int
    iterator_byte_limit: int | None
    waiters: int


@dataclass(slots=True, frozen=True)
class ReceiptStats:
    """Outstanding public operation receipts and parked publishers."""

    publish: int
    publish_batches: int
    subscribe: int
    unsubscribe: int
    publish_waiters: int


@dataclass(slots=True, frozen=True)
class ClientStats:
    """One point-in-time, side-effect-free runtime snapshot.

    Every section is produced by the component that owns the state — the two
    protocol sessions, the write pump, the decoder, application delivery and
    the transport — so this class only assembles them. High-water fields are
    measured over the lifetime of the client or protocol engine. Calling
    :meth:`AsyncClient.stats` does not enable background sampling and does not
    reset any counter.
    """

    state: ConnectionState
    connections: int
    reconnect_attempt: int
    outbound: OutboundStats
    inbound: InboundStats
    writer: WriterStats
    decoder: DecoderStats
    delivery: DeliveryStats
    receipts: ReceiptStats
    transport: TransportStats
