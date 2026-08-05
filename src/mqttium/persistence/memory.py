"""In-memory inflight persistence (ordered, injectable interface)."""

from __future__ import annotations

from collections.abc import Iterator
from itertools import islice
from contextlib import AbstractContextManager, nullcontext
from typing import Protocol, runtime_checkable

from mqttium.enums import InboundQoSState, OutboundQoSState
from mqttium.types import (
    InboundMessage,
    InboundRecordMeta,
    OutboundMessage,
    OutboundMessageSummary,
    OutboundRecordMeta,
)


class InflightStore(Protocol):
    """The minimum a store must provide. Session replay materialises the whole
    store at once, which is proportional to total session size."""

    def batch(self) -> AbstractContextManager[None]: ...
    def put_out(self, msg: OutboundMessage) -> None: ...
    def get_out(self, mid: int) -> OutboundMessage | None: ...
    def delete_out(self, mid: int) -> bool: ...
    def update_out(self, msg: OutboundMessage) -> None: ...
    def out_items(self) -> Iterator[OutboundMessage]: ...
    def clear_out(self) -> None: ...

    def put_in(self, msg: InboundMessage) -> None: ...
    def get_in(self, mid: int) -> InboundMessage | None: ...
    def pop_in(self, mid: int) -> InboundMessage | None: ...
    def update_in(self, msg: InboundMessage) -> None: ...
    def in_items(self) -> Iterator[InboundMessage]: ...
    def clear_in(self) -> None: ...


@runtime_checkable
class PagedInflightStore(InflightStore, Protocol):
    """Opt-in extension that keeps replay memory proportional to page size.

    ``out_summary_pages`` must never read the payload column: the engine uses it
    to rebuild the queue index and only materialises a payload when a message is
    actually replayed. A store that does not implement this protocol still
    works; the engine falls back to the eager ``*_items()`` path, which on a
    6,000 x 4 KiB session is the difference between roughly 4.5 MiB and 50 MiB
    of peak allocation.
    """

    def out_pages(self, page_size: int = 256) -> Iterator[tuple[OutboundMessage, ...]]: ...
    def out_summary_pages(
        self, page_size: int = 256
    ) -> Iterator[tuple[OutboundMessageSummary, ...]]: ...
    def in_pages(self, page_size: int = 256) -> Iterator[tuple[InboundMessage, ...]]: ...


@runtime_checkable
class BoundedInboundReplayStore(InflightStore, Protocol):
    """Opt-in extension for payload hydration bounded by messages and bytes.

    Every yielded page must contain at most ``max_messages`` records and
    at most ``max_bytes`` of payload plus UTF-8 topic data. A single record
    larger than ``max_bytes`` is yielded alone, because replay cannot make
    progress without materialising it.
    """

    def in_count(self) -> int:
        """Return the durable inbound record count without reading payloads."""
        ...

    def in_replay_pages(
        self,
        max_messages: int = 64,
        max_bytes: int = 1 << 20,
    ) -> Iterator[tuple[InboundMessage, ...]]: ...


@runtime_checkable
class TransitionInflightStore(InflightStore, Protocol):
    """Opt-in extension: conditional, payload-free record transitions.

    The base interface only exposes whole objects, so settling a PUBACK means
    asking a durable store to rebuild a multi-megabyte message just to delete
    its row. These methods carry the *decision* — the state the session expects
    and the state it wants — and return metadata only.

    The store does not own the state machine. ``expected_state`` and
    ``new_state`` always come from the session; the store only guarantees that
    the durable mutation is atomic and conditional, returning ``None`` when the
    record is absent or no longer in the expected state. A store that does not
    implement this protocol keeps working through the whole-object path.
    """

    def set_out_logical_size(self, mid: int, logical_size: int) -> bool:
        """Persist a logical size recomputed after reading a legacy record."""
        ...

    def out_meta(self, mid: int) -> OutboundRecordMeta | None:
        """Read one outbound record's state and logical size, nothing else."""
        ...

    def complete_out(
        self,
        mid: int,
        expected_state: OutboundQoSState,
    ) -> OutboundRecordMeta | None: ...

    def transition_out(
        self,
        mid: int,
        expected_state: OutboundQoSState,
        new_state: OutboundQoSState,
        *,
        compact: bool = False,
    ) -> OutboundRecordMeta | None:
        """Move one outbound record between states.

        ``compact`` says the PUBLISH phase is over and only PUBREL can still be
        retransmitted, so retransmission material kept for the publish frame may
        be released.
        """
        ...

    def contains_in(self, mid: int) -> bool: ...

    def in_meta(self, mid: int) -> InboundRecordMeta | None: ...

    def in_index_pages(self, page_size: int = 256) -> Iterator[tuple[InboundRecordMeta, ...]]:
        """Page the inbound index without reading a single payload.

        Restart recovery needs the identifier set and the record count, not the
        messages; reading them back was the largest allocation a reconnect made.
        """
        ...

    def mark_in_delivered(self, mid: int) -> bool:
        """Flag a record as delivered; False when absent or already flagged."""
        ...

    def transition_in(
        self,
        mid: int,
        expected_state: InboundQoSState,
        new_state: InboundQoSState,
        *,
        user_acked: bool | None = None,
    ) -> InboundRecordMeta | None: ...

    def complete_in(
        self,
        mid: int,
        expected_state: InboundQoSState,
    ) -> InboundRecordMeta | None: ...


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

    # --- conditional transitions (TransitionInflightStore) ------------------

    def set_out_logical_size(self, mid: int, logical_size: int) -> bool:
        msg = self._out.get(mid)
        if msg is None:
            return False
        msg.logical_size = logical_size
        return True

    def out_meta(self, mid: int) -> OutboundRecordMeta | None:
        msg = self._out.get(mid)
        if msg is None:
            return None
        return OutboundRecordMeta(mid=mid, state=msg.state, logical_size=msg.logical_size)

    def complete_out(
        self,
        mid: int,
        expected_state: OutboundQoSState,
    ) -> OutboundRecordMeta | None:
        msg = self._out.get(mid)
        if msg is None or msg.state is not expected_state:
            return None
        self.delete_out(mid)
        return OutboundRecordMeta(mid=mid, state=expected_state, logical_size=msg.logical_size)

    def transition_out(
        self,
        mid: int,
        expected_state: OutboundQoSState,
        new_state: OutboundQoSState,
        *,
        compact: bool = False,
    ) -> OutboundRecordMeta | None:
        msg = self._out.get(mid)
        if msg is None or msg.state is not expected_state:
            return None
        msg.state = new_state
        if compact:
            # Only PUBREL is retransmissible from here on.
            msg.encoded_publish = None
        return OutboundRecordMeta(mid=mid, state=new_state, logical_size=msg.logical_size)

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

    def in_count(self) -> int:
        return len(self._in)

    def in_replay_pages(
        self,
        max_messages: int = 64,
        max_bytes: int = 1 << 20,
    ) -> Iterator[tuple[InboundMessage, ...]]:
        if max_messages <= 0:
            raise ValueError("max_messages must be positive")
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        messages: list[InboundMessage] = []
        hydrated_bytes = 0
        for mid in tuple(self._in):
            message = self._in.get(mid)
            if message is None:
                continue
            message_bytes = len(message.payload) + len(message.topic.encode("utf-8"))
            if messages and (
                len(messages) >= max_messages or hydrated_bytes + message_bytes > max_bytes
            ):
                yield tuple(messages)
                messages = []
                hydrated_bytes = 0
            messages.append(message)
            hydrated_bytes += message_bytes
            if len(messages) >= max_messages or hydrated_bytes >= max_bytes:
                yield tuple(messages)
                messages = []
                hydrated_bytes = 0
        if messages:
            yield tuple(messages)

    def clear_in(self) -> None:
        old = self._in
        old.clear()
        self._in = {}

    # --- conditional transitions (TransitionInflightStore) ------------------

    def contains_in(self, mid: int) -> bool:
        return mid in self._in

    def in_meta(self, mid: int) -> InboundRecordMeta | None:
        msg = self._in.get(mid)
        if msg is None:
            return None
        return InboundRecordMeta(mid=mid, state=msg.state, user_acked=msg.user_acked)

    def in_index_pages(self, page_size: int = 256) -> Iterator[tuple[InboundRecordMeta, ...]]:
        if page_size <= 0:
            raise ValueError("page_size must be positive")
        mids = iter(tuple(self._in))
        while page := tuple(islice(mids, page_size)):
            metas = tuple(
                InboundRecordMeta(mid=mid, state=message.state, user_acked=message.user_acked)
                for mid in page
                if (message := self._in.get(mid)) is not None
            )
            if metas:
                yield metas

    def mark_in_delivered(self, mid: int) -> bool:
        msg = self._in.get(mid)
        if msg is None or msg.delivered:
            return False
        msg.delivered = True
        return True

    def transition_in(
        self,
        mid: int,
        expected_state: InboundQoSState,
        new_state: InboundQoSState,
        *,
        user_acked: bool | None = None,
    ) -> InboundRecordMeta | None:
        msg = self._in.get(mid)
        if msg is None or msg.state is not expected_state:
            return None
        msg.state = new_state
        if user_acked is not None:
            msg.user_acked = user_acked
        return InboundRecordMeta(mid=mid, state=new_state, user_acked=msg.user_acked)

    def complete_in(
        self,
        mid: int,
        expected_state: InboundQoSState,
    ) -> InboundRecordMeta | None:
        msg = self._in.get(mid)
        if msg is None or msg.state is not expected_state:
            return None
        self.pop_in(mid)
        return InboundRecordMeta(mid=mid, state=msg.state, user_acked=msg.user_acked)
