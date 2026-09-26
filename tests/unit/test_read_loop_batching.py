"""Ingress batching contract: one bounded decode per read unless a bound is hit."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.api import async_client as async_client_module
from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
from mqttium.errors import ProtocolError
from mqttium.packets import PublishPacket


def puback(mid: int) -> bytes:
    """A minimal MQTT 3.1.1 PUBACK for a mid that is not in flight."""
    return b"\x40\x02" + mid.to_bytes(2, "big")


def pingresp() -> bytes:
    return b"\xd0\x00"


def publish(mid: int, qos: QoS = QoS.AT_LEAST_ONCE) -> bytes:
    return PublishPacket(
        topic="batch/receive-maximum",
        payload=b"x",
        qos=qos,
        retain=False,
        dup=False,
        mid=mid,
    ).encode(MQTTProtocolVersion.MQTTv311)


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
    """Delegate to the real decoder while counting packet extraction."""

    def __init__(self, decoder) -> None:
        self._decoder = decoder
        self.next_calls = 0
        self.handled = 0

    def feed(self, data: bytes) -> None:
        self._decoder.feed(data)

    def next_packet(self):
        self.next_calls += 1
        packet = self._decoder.next_packet()
        if packet is not None:
            self.handled += 1
        return packet


# _read_loop's finally block retires the connection synchronously, without
# taking the engine lock: nothing awaits while holding it (#544).
TEARDOWN_ACQUISITIONS = 0


async def _run_reads(
    reads: list[bytes],
    *,
    max_inbound_inflight: int = 100,
    initial_inflight: int = 0,
):
    client = AsyncClient(max_inbound_inflight=max_inbound_inflight)
    client._engine.state = ConnectionState.CONNECTED
    # Slots owned by persisted exchanges observed earlier on this connection.
    client._engine.inbound._current_persisted_mids.update(range(60001, 60001 + initial_inflight))
    client._transport = _ScriptedTransport(reads)
    lock = _CountingLock()
    client._engine_lock = lock
    decoder = _CountingDecoder(client._decoder)
    client._decoder = decoder

    await client._read_loop()
    return decoder.next_calls, lock.acquisitions, decoder.handled, client


async def test_short_batch_decodes_once_per_read() -> None:
    """A batch under both bounds emptied the buffer: no confirming re-entry."""
    next_calls, acquisitions, handled, _client = await _run_reads([puback(1)])

    assert handled == 1
    assert next_calls == 2
    assert acquisitions == 1 + TEARDOWN_ACQUISITIONS


async def test_each_read_costs_exactly_one_decode() -> None:
    next_calls, acquisitions, handled, _client = await _run_reads([puback(1), puback(2), puback(3)])

    assert handled == 3
    assert next_calls == 6
    assert acquisitions == 3 + TEARDOWN_ACQUISITIONS


async def test_full_count_batch_re_enters_to_find_the_buffer_empty() -> None:
    """Hitting the count bound is not evidence the buffer drained."""
    wire = b"".join(puback(mid) for mid in range(1, 257))
    next_calls, acquisitions, handled, _client = await _run_reads([wire])

    assert handled == 256
    assert next_calls == 257
    assert acquisitions == 2 + TEARDOWN_ACQUISITIONS


async def test_byte_bounded_batch_re_enters_until_the_buffer_drains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each PUBACK charges len(remaining) + 5, so 20 bytes admits three.

    The byte quantum is a fixed module constant; it is lowered here so the
    bound is reachable with a handful of packets.
    """
    monkeypatch.setattr(async_client_module, "_MAX_INGRESS_BATCH_BYTES", 20)
    wire = b"".join(puback(mid) for mid in range(1, 7))
    next_calls, acquisitions, handled, _client = await _run_reads([wire])

    assert handled == 6
    assert next_calls == 7
    assert acquisitions == 3 + TEARDOWN_ACQUISITIONS


async def test_packet_split_across_reads_is_completed() -> None:
    """A partial trailing packet decodes nothing until its remainder arrives."""
    frame = puback(9)
    next_calls, acquisitions, handled, _client = await _run_reads([frame[:3], frame[3:]])

    assert handled == 1
    assert next_calls == 3
    assert acquisitions == 2 + TEARDOWN_ACQUISITIONS


async def test_receive_maximum_only_bounds_autoack_publish_batches() -> None:
    next_calls, acquisitions, handled, client = await _run_reads(
        [pingresp() * 256],
        max_inbound_inflight=1,
    )

    assert handled == 256
    assert next_calls == 257
    assert acquisitions == 2 + TEARDOWN_ACQUISITIONS
    assert not isinstance(client._disconnect_exc, ProtocolError)


async def test_autoack_batch_hands_off_before_receive_maximum_is_exceeded() -> None:
    next_calls, acquisitions, handled, client = await _run_reads(
        [publish(1) + publish(2) + publish(3)],
        max_inbound_inflight=2,
    )

    assert handled == 3
    assert next_calls == 4
    assert acquisitions == 2 + TEARDOWN_ACQUISITIONS
    assert not isinstance(client._disconnect_exc, ProtocolError)


async def test_autoack_batch_uses_only_the_remaining_receive_maximum_window() -> None:
    next_calls, acquisitions, handled, client = await _run_reads(
        [publish(1) + publish(2)],
        max_inbound_inflight=2,
        initial_inflight=1,
    )

    assert handled == 2
    assert next_calls == 3
    assert acquisitions == 3 + TEARDOWN_ACQUISITIONS
    assert not isinstance(client._disconnect_exc, ProtocolError)


async def test_qos2_filling_window_after_autoack_forces_handoff() -> None:
    next_calls, acquisitions, handled, client = await _run_reads(
        [publish(1) + publish(2, QoS.EXACTLY_ONCE) + publish(3)],
        max_inbound_inflight=2,
    )

    assert handled == 3
    assert next_calls == 4
    # Two handoff boundaries and the confirming empty decode each acquire the
    # engine lock before teardown. QoS 2's persisted delivery mark runs on the
    # reader while the lock is free, so it costs no acquisition.
    assert acquisitions == 3 + TEARDOWN_ACQUISITIONS
    assert not isinstance(client._disconnect_exc, ProtocolError)
