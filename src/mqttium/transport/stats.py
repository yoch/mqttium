"""Diagnostic snapshot a transport reports about itself.

Kept under `transport/` so a third-party transport has an explicit contract it
can satisfy without importing the runtime adapter. `AsyncClient` falls back to
`TransportStats._unavailable()` for a transport that does not implement
`stats()`, which keeps the protocol optional.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class TransportStats:
    """Transport occupancy; receive bytes are None when not measurable.

    Zero denotes a measured empty buffer or no transport, not an unknown
    StreamReader backlog. OS socket buffers are outside this snapshot.
    """

    closing: bool
    pending_write_bytes: int
    buffered_read_bytes: int | None

    @classmethod
    def _unavailable(cls, transport: object | None) -> TransportStats:
        """Snapshot for a disconnected client or a transport without `stats()`."""
        return cls(
            closing=bool(getattr(transport, "is_closing", bool)()) if transport else False,
            pending_write_bytes=0,
            buffered_read_bytes=0 if transport is None else None,
        )


__all__ = ["TransportStats"]
