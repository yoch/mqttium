"""In-memory inflight persistence (ordered, injectable interface)."""

from __future__ import annotations

from collections.abc import Iterator
from itertools import islice
from contextlib import AbstractContextManager, nullcontext
from typing import Protocol, TypeVar

from mqttium.enums import InboundQoSState, OutboundQoSState
from mqttium.types import (
    InboundMessage,
    InboundRecordMeta,
    OutboundMessage,
    OutboundMessageSummary,
    OutboundRecordMeta,
)


class InflightStore(Protocol):
    """Persistence contract required by the MQTT state machine.

    Stores provide atomic batches, bounded replay, payload-free metadata reads,
    and conditional transitions.  These are correctness/resource guarantees,
    not optional optimisations: the runtime has one persistence state-machine
    path and never falls back to eager whole-store hydration or read/modify/write
    settlement.

    Storage/backend failures must use backend-native or ordinary Python
    exceptions rather than ``MQTTError`` subclasses.  ``batch()`` must roll back
    prior mutations when its body fails and must not suppress that failure.
    """

    def batch(self) -> AbstractContextManager[None]: ...

    # Outbound records: full payloads are written/materialised only when needed;
    # acknowledgement and state changes use metadata-only operations.
    def put_out(self, msg: OutboundMessage) -> None: ...
    def get_out(self, mid: int) -> OutboundMessage | None: ...
    def delete_out(self, mid: int) -> bool: ...
    def out_summary_pages(
        self, page_size: int = 256
    ) -> Iterator[tuple[OutboundMessageSummary, ...]]: ...
    def set_out_logical_size(self, mid: int, logical_size: int) -> bool: ...
    def out_meta(self, mid: int) -> OutboundRecordMeta | None: ...
    def complete_out(
        self, mid: int, expected_state: OutboundQoSState
    ) -> OutboundRecordMeta | None: ...
    def transition_out(
        self,
        mid: int,
        expected_state: OutboundQoSState,
        new_state: OutboundQoSState,
        *,
        compact: bool = False,
    ) -> OutboundRecordMeta | None: ...
    def clear_out(self) -> None: ...

    # Inbound records: index/replay operations are bounded and transitions do
    # not reconstruct payloads. get_in() exists only when the application must
    # actually redeliver one stored message.
    def put_in(self, msg: InboundMessage) -> None: ...
    def get_in(self, mid: int) -> InboundMessage | None: ...
    def in_count(self) -> int: ...
    def in_replay_pages(
        self,
        max_messages: int = 64,
        max_bytes: int = 1 << 20,
    ) -> Iterator[tuple[InboundMessage, ...]]: ...
    def set_in_logical_size(self, mid: int, logical_size: int) -> bool: ...
    def in_meta(self, mid: int) -> InboundRecordMeta | None: ...
    def in_index_pages(self, page_size: int = 256) -> Iterator[tuple[InboundRecordMeta, ...]]: ...
    def mark_in_delivered(self, mid: int) -> bool: ...
    def transition_in(
        self,
        mid: int,
        expected_state: InboundQoSState,
        new_state: InboundQoSState,
        *,
        user_acked: bool | None = None,
    ) -> InboundRecordMeta | None: ...
    def complete_in(
        self, mid: int, expected_state: InboundQoSState
    ) -> InboundRecordMeta | None: ...
    def clear_in(self) -> None: ...


_RecordT = TypeVar("_RecordT")


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
            # Gating this on sys.getsizeof to skip the reallocation for a
            # shallow window was tried and reverted: the probe measured ~151 ns
            # against ~19 ns for the allocation it avoids, and qos1_cycle_memory
            # regressed accordingly. See docs/reports/PERFORMANCE-AUDIT-0.2.0b4.md.
            self._out = {}
        return deleted

    @staticmethod
    def _pages(
        records: dict[int, _RecordT],
        page_size: int,
    ) -> Iterator[tuple[_RecordT, ...]]:
        """Page a record table, snapshotting identifiers before the first yield.

        A page whose records were acknowledged meanwhile comes back shorter --
        the same contract SqliteInflightStore offers.
        """
        if page_size <= 0:
            raise ValueError("page_size must be positive")
        mids = iter(tuple(records))
        while page := tuple(islice(mids, page_size)):
            messages = tuple(record for mid in page if (record := records.get(mid)) is not None)
            if messages:
                yield messages

    def out_summary_pages(
        self, page_size: int = 256
    ) -> Iterator[tuple[OutboundMessageSummary, ...]]:
        for page in self._pages(self._out, page_size):
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
            # The PUBLISH phase is over. PUBREL needs only the packet id and
            # protocol version, so no application data should survive PUBREC.
            msg.topic = ""
            msg.payload = b""
            msg.properties = None
            msg.encoded_publish = None
        return OutboundRecordMeta(mid=mid, state=new_state, logical_size=msg.logical_size)

    def put_in(self, msg: InboundMessage) -> None:
        self._in[msg.mid] = msg

    def get_in(self, mid: int) -> InboundMessage | None:
        return self._in.get(mid)

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

        def hydrate_page() -> tuple[InboundMessage, ...]:
            page = tuple(current for saved_mid in mids if (current := self._in.get(saved_mid)))
            mids.clear()
            return page

        index = tuple(self._in.values())
        mids: list[int] = []
        hydrated_bytes = 0
        for indexed_message in index:
            message_bytes = indexed_message.logical_size or (
                len(indexed_message.payload)
                + (
                    len(indexed_message.topic)
                    if indexed_message.topic.isascii()
                    else len(indexed_message.topic.encode("utf-8"))
                )
            )
            if mids and (len(mids) >= max_messages or hydrated_bytes + message_bytes > max_bytes):
                if page := hydrate_page():
                    yield page
                hydrated_bytes = 0
            mids.append(indexed_message.mid)
            hydrated_bytes += message_bytes
            if len(mids) >= max_messages or hydrated_bytes >= max_bytes:
                if page := hydrate_page():
                    yield page
                hydrated_bytes = 0
        if mids:
            if page := hydrate_page():
                yield page

    def clear_in(self) -> None:
        old = self._in
        old.clear()
        self._in = {}

    # --- conditional transitions (TransitionInflightStore) ------------------

    def set_in_logical_size(self, mid: int, logical_size: int) -> bool:
        msg = self._in.get(mid)
        if msg is None:
            return False
        msg.logical_size = logical_size
        return True

    def in_meta(self, mid: int) -> InboundRecordMeta | None:
        msg = self._in.get(mid)
        if msg is None:
            return None
        return InboundRecordMeta(
            mid=mid,
            state=msg.state,
            user_acked=msg.user_acked,
            delivered=msg.delivered,
            logical_size=msg.logical_size,
        )

    def in_index_pages(self, page_size: int = 256) -> Iterator[tuple[InboundRecordMeta, ...]]:
        if page_size <= 0:
            raise ValueError("page_size must be positive")
        mids = iter(tuple(self._in))
        while page := tuple(islice(mids, page_size)):
            metas = tuple(
                InboundRecordMeta(
                    mid=mid,
                    state=message.state,
                    user_acked=message.user_acked,
                    delivered=message.delivered,
                    logical_size=message.logical_size,
                )
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
        return InboundRecordMeta(
            mid=mid,
            state=new_state,
            user_acked=msg.user_acked,
            delivered=msg.delivered,
            logical_size=msg.logical_size,
        )

    def complete_in(
        self,
        mid: int,
        expected_state: InboundQoSState,
    ) -> InboundRecordMeta | None:
        msg = self._in.get(mid)
        if msg is None or msg.state is not expected_state:
            return None
        self._in.pop(mid)
        if not self._in:
            self._in = {}
        return InboundRecordMeta(
            mid=mid,
            state=msg.state,
            user_acked=msg.user_acked,
            delivered=msg.delivered,
            logical_size=msg.logical_size,
        )
