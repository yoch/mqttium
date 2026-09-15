"""Public API models."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from types import MappingProxyType
from collections.abc import Mapping

from mqttium.enums import QoS
from mqttium.errors import PublishBatchError
from mqttium.packets import ConnAckPacket, SubAckPacket, UnsubAckPacket
from mqttium.types import Message, Properties, _owned_payload


@dataclass(slots=True, frozen=True)
class PublishMessage:
    """One immutable entry accepted by :meth:`AsyncClient.publish_many`."""

    topic: str
    payload: bytes | str = b""
    qos: QoS | int = QoS.AT_MOST_ONCE
    retain: bool = False
    properties: Properties | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.payload, (bytes, str)):
            object.__setattr__(self, "payload", _owned_payload(self.payload))


class PublishBatchReceipt:
    """Aggregate completion handle returned by ``publish_many``.

    The receipt retains bounded failure details while counting every failure.
    Completion covers the entire submitted iterable; no task or event is
    allocated per publication.
    """

    __slots__ = (
        "_pending",
        "_failures",
        "_failure_counts",
        "_max_failure_details",
        "_done",
        "_progress",
        "_sealed",
        "_submitted",
        "_fatal",
    )

    def __init__(
        self,
        *,
        max_failure_details: int = 128,
    ) -> None:
        if type(max_failure_details) is not int or max_failure_details < 0:
            raise ValueError("max_failure_details must be a non-negative integer")
        # At most the client's bounded pending window is retained. MQTT
        # packet identifiers may be reused during a long batch, so failures are
        # keyed by the stable zero-based input index stored as the value.
        self._pending: dict[int, int] = {}
        self._failures: dict[int, BaseException] = {}
        self._failure_counts: dict[str, int] = {}
        self._max_failure_details = max_failure_details
        self._done = asyncio.Event()
        self._progress = asyncio.Event()
        self._sealed = False
        self._submitted = 0
        self._fatal: BaseException | None = None

    @property
    def submitted(self) -> int:
        """Number of messages successfully admitted to the batch."""
        return self._submitted

    @property
    def completed(self) -> int:
        """Number of admitted messages that reached terminal completion."""
        return self._submitted - len(self._pending)

    @property
    def pending_count(self) -> int:
        """Number of admitted QoS 1/2 messages still awaiting completion."""
        return len(self._pending)

    @property
    def failures(self) -> Mapping[int, BaseException]:
        """Bounded failure details keyed by stable input index."""
        return MappingProxyType(self._failures)

    @property
    def failure_count(self) -> int:
        """Total failures, including details omitted by the retention limit."""
        return sum(self._failure_counts.values())

    @property
    def failure_counts(self) -> Mapping[str, int]:
        """Total failures grouped by exception class name."""
        return MappingProxyType(self._failure_counts)

    def is_done(self) -> bool:
        """Whether submission is sealed and all admitted messages settled."""
        return self._done.is_set()

    async def wait(self) -> None:
        """Wait for aggregate completion and raise on any failure.

        Raises:
            PublishBatchError: If admission failed fatally or one or more
                admitted publications completed with an error. The exception
                references this receipt and its bounded failure details.
        """
        await self._done.wait()
        if self._fatal is not None:
            raise PublishBatchError(
                self._failures,
                failure_count=self.failure_count,
                failure_counts=self._failure_counts,
                cause=self._fatal,
                receipt=self,
            ) from self._fatal
        if self._failure_counts:
            raise PublishBatchError(
                self._failures,
                failure_count=self.failure_count,
                failure_counts=self._failure_counts,
                receipt=self,
            )

    def _register(self, mid: int | None) -> None:
        index = self._submitted
        self._submitted += 1
        if mid is not None:
            self._pending[mid] = index

    def _rollback_qos0_registration(self) -> None:
        """Undo the last QoS 0 registration after a synchronous clean refusal."""
        assert not self._sealed and self._submitted > 0
        self._submitted -= 1

    def _complete(self, mid: int, error: BaseException | None = None) -> None:
        index = self._pending.pop(mid, None)
        if index is None:
            return
        if error is not None:
            self._record_failure(index, error)
        self._progress.set()
        self._finish_if_ready()

    def _seal(self) -> None:
        self._sealed = True
        self._finish_if_ready()

    def _fail_remaining(self, error: BaseException) -> None:
        self._fatal = error
        for index in self._pending.values():
            self._record_failure(index, error)
        self._pending.clear()
        self._sealed = True
        self._progress.set()
        self._done.set()

    def _record_failure(self, index: int, error: BaseException) -> None:
        name = type(error).__name__
        self._failure_counts[name] = self._failure_counts.get(name, 0) + 1
        limit = self._max_failure_details
        if len(self._failures) < limit:
            self._failures[index] = error

    async def _wait_pending_at_most(self, limit: int) -> None:
        while len(self._pending) > limit:
            self._progress.clear()
            if len(self._pending) <= limit:
                break
            await self._progress.wait()

    def _finish_if_ready(self) -> None:
        if self._sealed and not self._pending:
            self._done.set()


@dataclass(slots=True)
class PublishReceipt:
    """Handle returned by ``AsyncClient.publish``.

    Completion is a flag, and the waiter list behind :meth:`wait` is created
    only if somebody actually waits. A publication that is never awaited -- the
    whole point of ``publish_nowait`` -- therefore allocates no completion
    primitive at all. Each active waiter parks on its own future, so cancelling
    one wait cancels only that waiter: receipt completion and the other waiters
    are untouched by construction, without an ``asyncio.shield()`` wrapper on
    the awaited path.

    Waiter futures only ever carry ``None``. Failures live in ``_error`` and
    are raised by :meth:`wait` after it resolves, so a receipt that fails and is
    never awaited cannot leave an unretrieved exception behind -- which matters
    here, because the library installs no logging to absorb one.

    """

    mid: int | None
    qos: QoS
    _waiters: list[asyncio.Future[None]] | None = None
    _error: BaseException | None = None
    _settled: bool = False

    def _settle(self) -> None:
        """Mark completion and wake every parked waiter."""
        self._settled = True
        waiters = self._waiters
        if waiters is not None:
            # Release the collection before resolving so a defensive duplicate
            # settlement cannot retain waiters or resolve them twice.
            self._waiters = None
            for waiter in waiters:
                # A waiter cancelled between its cancellation and its removal
                # is already done; resolving it would raise InvalidStateError.
                if not waiter.done():
                    waiter.set_result(None)

    async def wait(self) -> None:
        """Wait for protocol completion and re-raise its terminal error.

        QoS 0 is already complete when the receipt is returned. Each waiter
        parks on its own future, so cancelling one waiter does not cancel the
        receipt or any other waiter.
        """
        if self.qos != QoS.AT_MOST_ONCE and not self._settled:
            waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
            waiters = self._waiters
            if waiters is None:
                self._waiters = [waiter]
            else:
                waiters.append(waiter)
            try:
                await waiter
            except BaseException:
                # Settlement clears the collection, so only a waiter leaving
                # before completion -- cancellation -- has anything to retire.
                waiters = self._waiters
                if waiters is not None:
                    try:
                        waiters.remove(waiter)
                    except ValueError:  # pragma: no cover - defensive
                        pass
                    if not waiters:
                        # The last waiter left: return to the lazy shape a
                        # never-awaited receipt has.
                        self._waiters = None
                raise
        if self._error is not None:
            raise self._error

    def is_done(self) -> bool:
        """Whether the publication reached its completion boundary."""
        return self.qos == QoS.AT_MOST_ONCE or self._settled


@dataclass(slots=True)
class SubscribeResult:
    """Broker acknowledgement for one subscribe request.

    Attributes:
        mid: MQTT packet identifier used by the request.
        reason_codes: One broker reason code for every requested filter.
    """

    mid: int
    reason_codes: tuple[int, ...]

    @classmethod
    def from_packet(cls, packet: SubAckPacket) -> SubscribeResult:
        """Build a public result from a decoded SUBACK packet."""
        return cls(mid=packet.mid, reason_codes=packet.reason_codes)


@dataclass(slots=True)
class UnsubscribeResult:
    """Broker acknowledgement for one unsubscribe request.

    MQTT 3.1.1 acknowledgements have no per-filter reason codes and therefore
    expose an empty ``reason_codes`` tuple.
    """

    mid: int
    reason_codes: tuple[int, ...]

    @classmethod
    def from_packet(cls, packet: UnsubAckPacket) -> UnsubscribeResult:
        """Build a public result from a decoded UNSUBACK packet."""
        return cls(mid=packet.mid, reason_codes=packet.reason_codes)


__all__ = [
    "ConnAckPacket",
    "Message",
    "PublishBatchReceipt",
    "PublishMessage",
    "PublishReceipt",
    "SubscribeResult",
    "UnsubscribeResult",
]
