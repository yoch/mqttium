"""Point-in-time snapshots owned by the protocol sessions.

These live under `protocol/` rather than `api/` on purpose: each directional
session computes its own snapshot, so its private fields can change without
touching `AsyncClient`, and the dependency still points from the runtime adapter
to the protocol core rather than back.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class OutboundStats:
    """Client→broker publication state, as `OutboundSession` accounts for it."""

    pending_messages: int
    pending_bytes: int
    pending_high_water_messages: int
    pending_high_water_bytes: int
    queued_messages: int
    flow_inflight: int
    flow_limit: int
    packet_ids_in_use: int


@dataclass(slots=True, frozen=True)
class InboundStats:
    """Broker→client publication state, as `InboundSession` accounts for it."""

    inflight: int
    receive_maximum: int
    topic_aliases: int
    replay_pending: bool
    pending_bytes: int
    pending_high_water_bytes: int
    pending_byte_limit: int | None


__all__ = ["InboundStats", "OutboundStats"]
