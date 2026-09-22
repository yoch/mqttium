"""Point-in-time snapshots owned by the protocol sessions.

These live under `protocol/` rather than `api/` on purpose: each directional
session computes its own snapshot, so its private fields can change without
touching `AsyncClient`, and the dependency still points from the runtime adapter
to the protocol core rather than back.

Field names follow the constructor bounds they are measured against:
``unacknowledged_*`` for retained outbound QoS 1/2 publications
(``max_unacknowledged_*``), ``inflight`` for the Receive Maximum windows
(``max_outbound_inflight``, ``max_inbound_inflight``).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class OutboundStats:
    """Client→broker publication state, as `OutboundSession` accounts for it."""

    # Admitted QoS 1/2 publications not yet completed: on the wire or waiting
    # for an inflight slot. Bounded by max_unacknowledged_messages/bytes.
    unacknowledged_messages: int
    unacknowledged_bytes: int
    unacknowledged_high_water_messages: int
    unacknowledged_high_water_bytes: int
    # Admitted publications waiting for an inflight slot.
    awaiting_slot: int
    # Publications on the wire against the negotiated window.
    inflight: int
    inflight_limit: int
    packet_ids_in_use: int


@dataclass(slots=True, frozen=True)
class InboundStats:
    """Broker→client publication state, as `InboundSession` accounts for it."""

    # Unfinished inbound QoS 1/2 exchanges against the advertised window.
    inflight: int
    inflight_limit: int
    inflight_bytes: int
    inflight_high_water_bytes: int
    inflight_byte_limit: int | None
    topic_aliases: int
    replay_pending: bool


__all__ = ["InboundStats", "OutboundStats"]
