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
    """Bytes the transport holds in either direction, and its closing state."""

    kind: str | None
    closing: bool
    pending_write_bytes: int
    buffered_read_bytes: int

    @classmethod
    def unavailable(cls, transport: object | None) -> TransportStats:
        """Snapshot for a disconnected client or a transport without `stats()`."""
        return cls(
            kind=None if transport is None else type(transport).__name__,
            closing=bool(getattr(transport, "is_closing", bool)()) if transport else False,
            pending_write_bytes=0,
            buffered_read_bytes=0,
        )


__all__ = ["TransportStats"]
