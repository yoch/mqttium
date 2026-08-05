"""Diagnostic snapshot a transport reports about itself.

Kept under `transport/` so a third-party transport has an explicit contract it
can satisfy without importing the runtime adapter. `AsyncClient` falls back to
`TransportStats.unavailable()` for a transport that does not implement
`stats()`, which keeps the protocol optional.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class TransportStats:
    kind: str | None
    closing: bool
    pending_write_bytes: int
    buffered_read_bytes: int
    fragmented_read_bytes: int
    pending_control_frames: int
    pending_control_bytes: int

    @classmethod
    def unavailable(cls, transport: object | None) -> TransportStats:
        """Snapshot for a disconnected client or a transport without `stats()`."""
        return cls(
            kind=None if transport is None else type(transport).__name__,
            closing=bool(getattr(transport, "is_closing", bool)()) if transport else False,
            pending_write_bytes=0,
            buffered_read_bytes=0,
            fragmented_read_bytes=0,
            pending_control_frames=0,
            pending_control_bytes=0,
        )


__all__ = ["TransportStats"]
