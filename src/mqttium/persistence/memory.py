"""In-memory inflight persistence (ordered, injectable interface)."""

from __future__ import annotations

from collections.abc import Iterator
from itertools import islice
from contextlib import AbstractContextManager, nullcontext
from typing import Protocol

from mqttium.types import InboundMessage, OutboundMessage, OutboundMessageSummary


class InflightStore(Protocol):
    def batch(self) -> AbstractContextManager[None]: ...
    def put_out(self, msg: OutboundMessage) -> None: ...
    def get_out(self, mid: int) -> OutboundMessage | None: ...
    def pop_out(self, mid: int) -> OutboundMessage | None: ...
    def delete_out(self, mid: int) -> bool: ...
    def update_out(self, msg: OutboundMessage) -> None: ...
    def out_items(self) -> Iterator[OutboundMessage]: ...
    def out_pages(self, page_size: int = 256) -> Iterator[tuple[OutboundMessage, ...]]: ...
    def out_summary_pages(
        self, page_size: int = 256
    ) -> Iterator[tuple[OutboundMessageSummary, ...]]: ...
    def clear_out(self) -> None: ...

    def put_in(self, msg: InboundMessage) -> None: ...
    def get_in(self, mid: int) -> InboundMessage | None: ...
    def pop_in(self, mid: int) -> InboundMessage | None: ...
    def update_in(self, msg: InboundMessage) -> None: ...
    def in_items(self) -> Iterator[InboundMessage]: ...
    def in_pages(self, page_size: int = 256) -> Iterator[tuple[InboundMessage, ...]]: ...
    def clear_in(self) -> None: ...


class MemoryInflightStore:
    """Ordered dict-backed store. Insertion order is retransmission order."""

    __slots__ = ("_out", "_in")

    def __init__(self) -> None:
        self._out: dict[int, OutboundMessage] = {}
        self._in: dict[int, InboundMessage] = {}

    def batch(self) -> AbstractContextManager[None]:
        return nullcontext()

    def put_out(self, msg: OutboundMessage) -> None:
        self._out[msg.mid] = msg

    def get_out(self, mid: int) -> OutboundMessage | None:
        return self._out.get(mid)

    def pop_out(self, mid: int) -> OutboundMessage | None:
        msg = self._out.pop(mid, None)
        if msg is not None and not self._out:
            self._out = {}
        return msg

    def delete_out(self, mid: int) -> bool:
        deleted = self._out.pop(mid, None) is not None
        if deleted and not self._out:
            # Drop the peak-sized hash table after the last inflight record is
            # acknowledged instead of retaining its capacity indefinitely.
            self._out = {}
        return deleted

    def update_out(self, msg: OutboundMessage) -> None:
        if msg.mid not in self._out:
            raise KeyError(msg.mid)
        self._out[msg.mid] = msg

    def out_items(self) -> Iterator[OutboundMessage]:
        return iter(self._out.values())

    def out_pages(self, page_size: int = 256) -> Iterator[tuple[OutboundMessage, ...]]:
        if page_size <= 0:
            raise ValueError("page_size must be positive")
        mids = iter(tuple(self._out))
        while page := tuple(islice(mids, page_size)):
            messages = tuple(self._out[mid] for mid in page if mid in self._out)
            if messages:
                yield messages

    def out_summary_pages(
        self, page_size: int = 256
    ) -> Iterator[tuple[OutboundMessageSummary, ...]]:
        for page in self.out_pages(page_size):
            yield tuple(OutboundMessageSummary.from_message(message) for message in page)

    def clear_out(self) -> None:
        old = self._out
        old.clear()
        self._out = {}

    def put_in(self, msg: InboundMessage) -> None:
        self._in[msg.mid] = msg

    def get_in(self, mid: int) -> InboundMessage | None:
        return self._in.get(mid)

    def pop_in(self, mid: int) -> InboundMessage | None:
        msg = self._in.pop(mid, None)
        if msg is not None and not self._in:
            self._in = {}
        return msg

    def update_in(self, msg: InboundMessage) -> None:
        if msg.mid not in self._in:
            raise KeyError(msg.mid)
        self._in[msg.mid] = msg

    def in_items(self) -> Iterator[InboundMessage]:
        return iter(self._in.values())

    def in_pages(self, page_size: int = 256) -> Iterator[tuple[InboundMessage, ...]]:
        if page_size <= 0:
            raise ValueError("page_size must be positive")
        mids = iter(tuple(self._in))
        while page := tuple(islice(mids, page_size)):
            messages = tuple(self._in[mid] for mid in page if mid in self._in)
            if messages:
                yield messages

    def clear_in(self) -> None:
        old = self._in
        old.clear()
        self._in = {}
