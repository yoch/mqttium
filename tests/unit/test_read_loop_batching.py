"""Ingress batching contract: one bounded decode per read unless a bound is hit."""

from __future__ import annotations

import asyncio

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState


def puback(mid: int) -> bytes:
    """A minimal MQTT 3.1.1 PUBACK for a mid that is not in flight."""
    return b"\x40\x02" + mid.to_bytes(2, "big")


class _ScriptedTransport:
    """Serve a fixed list of reads, then report closure."""

    def __init__(self, reads: list[bytes]) -> None:
        self._reads = list(reads)
        self._closing = False

    async def read(self, n: int = 65536) -> bytes:
        del n
        if not self._reads:
            self._closing = True
            return b""
        return self._reads.pop(0)

    async def close(self) -> None:
        self._closing = True

    def is_closing(self) -> bool:
        return self._closing


class _CountingLock:
    """Delegate to a real lock while counting critical sections entered."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.acquisitions = 0

    async def __aenter__(self) -> None:
        self.acquisitions += 1
        await self._lock.acquire()

    async def __aexit__(self, *exc_info: object) -> None:
        self._lock.release()

    def locked(self) -> bool:
        return self._lock.locked()


class _CountingDecoder:
    """Delegate to the real decoder while counting bounded decode calls."""

    def __init__(self, decoder) -> None:
        self._decoder = decoder
        self.decodes = 0
        self.handled = 0
        self.limits: list[int] = []

    def feed(self, data: bytes) -> None:
        self._decoder.feed(data)

    def process_packets_bounded(self, callback, *, limit: int, max_bytes: int):
        self.decodes += 1
        self.limits.append(limit)
        count, decoded_bytes = self._decoder.process_packets_bounded(
            callback, limit=limit, max_bytes=max_bytes
        )
        self.handled += count
        return count, decoded_bytes


# _read_loop's finally block takes the engine lock once to notify closure.
TEARDOWN_ACQUISITIONS = 1


async def _run_reads(
    reads: list[bytes],
    *,
    max_ingress_batch_bytes: int = 1024 * 1024,
    local_receive_maximum: int = 100,
):
    client = AsyncClient(
        max_ingress_batch_bytes=max_ingress_batch_bytes,
        local_receive_maximum=local_receive_maximum,
    )
    client._engine.state = ConnectionState.CONNECTED
    client._transport = _ScriptedTransport(reads)
    lock = _CountingLock()
    client._engine_lock = lock
    decoder = _CountingDecoder(client._decoder)
    client._decoder = decoder

    await client._read_loop()
    return decoder.decodes, lock.acquisitions, decoder.handled, decoder.limits


async def test_short_batch_decodes_once_per_read() -> None:
    """A batch under both bounds emptied the buffer: no confirming re-entry."""
    decodes, acquisitions, handled, limits = await _run_reads([puback(1)])

    assert handled == 1
    assert decodes == 1
    assert acquisitions == 1 + TEARDOWN_ACQUISITIONS
    assert limits == [100]


async def test_each_read_costs_exactly_one_decode() -> None:
    decodes, acquisitions, handled, limits = await _run_reads([puback(1), puback(2), puback(3)])

    assert handled == 3
    assert decodes == 3
    assert acquisitions == 3 + TEARDOWN_ACQUISITIONS
    assert limits == [100, 100, 100]


async def test_full_count_batch_re_enters_to_find_the_buffer_empty() -> None:
    """Hitting the count bound is not evidence the buffer drained."""
    wire = b"".join(puback(mid) for mid in range(1, 257))
    decodes, acquisitions, handled, limits = await _run_reads([wire])

    assert handled == 256
    assert decodes == 3
    assert acquisitions == 3 + TEARDOWN_ACQUISITIONS
    assert limits == [100, 100, 100]


async def test_byte_bounded_batch_re_enters_until_the_buffer_drains() -> None:
    """Each PUBACK charges len(remaining) + 5, so 20 bytes admits three."""
    wire = b"".join(puback(mid) for mid in range(1, 7))
    decodes, acquisitions, handled, limits = await _run_reads([wire], max_ingress_batch_bytes=20)

    assert handled == 6
    assert decodes == 3
    assert acquisitions == 3 + TEARDOWN_ACQUISITIONS
    assert limits == [100, 100, 100]


async def test_packet_split_across_reads_is_completed() -> None:
    """A partial trailing packet decodes nothing until its remainder arrives."""
    frame = puback(9)
    decodes, acquisitions, handled, limits = await _run_reads([frame[:3], frame[3:]])

    assert handled == 1
    assert decodes == 2
    assert acquisitions == 2 + TEARDOWN_ACQUISITIONS
    assert limits == [100, 100]
