"""Public API models."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from types import MappingProxyType
from collections.abc import Callable, Mapping

from mqttium.enums import QoS
from mqttium.errors import PublishBatchError
from mqttium.packets import ConnAckPacket, SubAckPacket, UnsubAckPacket
from mqttium.types import Message, Properties


@dataclass(slots=True, frozen=True)
class PublishMessage:
    """One immutable entry accepted by :meth:`AsyncClient.publish_many`."""

    topic: str
    payload: bytes | str = b""
    qos: QoS | int = QoS.AT_MOST_ONCE
    retain: bool = False
    properties: Properties | None = None


class PublishBatchReceipt:
    """Aggregate completion handle without one task/event per publication."""

    __slots__ = (
        "_pending",
        "_failures",
        "_failure_count",
        "_failure_counts",
        "_max_failure_details",
        "_failure_sink",
        "_done",
        "_progress",
        "_sealed",
        "_submitted",
        "_completed",
        "_fatal",
    )

    def __init__(
        self,
        *,
        max_failure_details: int | None = 128,
        failure_sink: Callable[[int, BaseException], None] | None = None,
    ) -> None:
        if max_failure_details is not None and max_failure_details < 0:
            raise ValueError("max_failure_details must be non-negative or None")
        # At most the client's bounded pending window is retained. MQTT
        # packet identifiers may be reused during a long batch, so failures are
        # keyed by the stable zero-based input index stored as the value.
        self._pending: dict[int, int] = {}
        self._failures: dict[int, BaseException] = {}
        self._failure_count = 0
        self._failure_counts: dict[str, int] = {}
        self._max_failure_details = max_failure_details
        self._failure_sink = failure_sink
        self._done = asyncio.Event()
        self._progress = asyncio.Event()
        self._sealed = False
        self._submitted = 0
        self._completed = 0
        self._fatal: BaseException | None = None

    @property
    def submitted(self) -> int:
        return self._submitted

    @property
    def completed(self) -> int:
        return self._completed

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    @property
    def failures(self) -> Mapping[int, BaseException]:
        """Bounded failure details keyed by stable input index."""
        return MappingProxyType(self._failures)

    @property
    def failure_count(self) -> int:
        """Total failures, including details omitted by the retention limit."""
        return self._failure_count

    @property
    def failure_counts(self) -> Mapping[str, int]:
        """Total failures grouped by exception class name."""
        return MappingProxyType(self._failure_counts)

    def is_done(self) -> bool:
        return self._done.is_set()

    async def wait(self) -> None:
        await self._done.wait()
        if self._fatal is not None:
            raise PublishBatchError(
                self._failures,
                failure_count=self._failure_count,
                failure_counts=self._failure_counts,
                cause=self._fatal,
                receipt=self,
            ) from self._fatal
        if self._failure_count:
            raise PublishBatchError(
                self._failures,
                failure_count=self._failure_count,
                failure_counts=self._failure_counts,
                receipt=self,
            )

    def _register(self, mid: int | None) -> None:
        index = self._submitted
        self._submitted += 1
        if mid is None:
            self._completed += 1
        else:
            self._pending[mid] = index

    def _complete(self, mid: int, error: BaseException | None = None) -> None:
        index = self._pending.pop(mid, None)
        if index is None:
            return
        self._completed += 1
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
        self._completed += len(self._pending)
        self._pending.clear()
        self._sealed = True
        self._progress.set()
        self._done.set()

    def _record_failure(self, index: int, error: BaseException) -> None:
        self._failure_count += 1
        name = type(error).__name__
        self._failure_counts[name] = self._failure_counts.get(name, 0) + 1
        limit = self._max_failure_details
        if limit is None or len(self._failures) < limit:
            self._failures[index] = error
        if self._failure_sink is not None:
            self._failure_sink(index, error)

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
    """Handle returned by ``AsyncClient.publish``."""

    mid: int | None
    qos: QoS
    _event: asyncio.Event | None = None
    _error: BaseException | None = None

    async def wait(self) -> None:
        if self.qos == QoS.AT_MOST_ONCE or self._event is None:
            if self._error is not None:
                raise self._error
            return
        await self._event.wait()
        if self._error is not None:
            raise self._error

    def is_done(self) -> bool:
        return self.qos == QoS.AT_MOST_ONCE or self._event is None or self._event.is_set()


@dataclass(slots=True)
class SubscribeResult:
    mid: int
    reason_codes: tuple[int, ...]

    @classmethod
    def from_packet(cls, packet: SubAckPacket) -> SubscribeResult:
        return cls(mid=packet.mid, reason_codes=packet.reason_codes)


@dataclass(slots=True)
class UnsubscribeResult:
    mid: int
    reason_codes: tuple[int, ...]

    @classmethod
    def from_packet(cls, packet: UnsubAckPacket) -> UnsubscribeResult:
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
