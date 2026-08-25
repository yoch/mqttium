"""Adversarial audit probe A: is the eager / latency-batch write path reachable
in the runtime fuzzer, and does a stale eager re-arm cross a reconnect?

The V1/V2 harness transport (``_ScheduleTransport``) provides ``write``,
``read``, ``close`` but NOT ``write_nowait`` or ``write_many``. ``WritePump``
only takes the eager path when ``write_nowait`` exists, and the latency-batch
flush only when at least four frames are queued. So those code paths are dead
in the runtime target despite being load-bearing for the single-writer
invariant.

Part 1 proves the gap numerically under the unmodified fuzzer. Part 2 runs the
real ``AsyncClient`` against a ``write_nowait``-capable transport through eager
write + EOF teardown + reconnect, asserting generation isolation (no stale eager
re-arm writes into the replacement transport).

Run: ``PYTHONPATH=src:. python tests/fuzz/audit_eager_write_surface.py``
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from mqttium.api import AsyncClient
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import encode_frame
from mqttium.protocol.reconnect import ReconnectPolicy

from tests.fuzz import runtime_fuzzer as v1


async def part1_fuzzer_eager_path_is_dead() -> None:
    schedule = v1.generate_schedule(seed=0, steps=24)
    harness = v1._RuntimeHarness(schedule, None)
    loop = asyncio.get_running_loop()
    prev = loop.get_exception_handler()
    loop.set_exception_handler(lambda _l, c: harness.loop_contexts.append(c))
    try:
        for operation in schedule.operations:
            await harness.execute(operation)
    finally:
        await harness.cleanup()
        loop.set_exception_handler(prev)
    pump = harness.client._write_pump
    print("part1: eager_writes =", pump.eager_writes)
    print("part1: eager_bytes  =", pump.eager_bytes)
    print("part1: _write_nowait =", pump._write_nowait)
    print("part1: batched_items =", pump.batched_items)
    print("part1: batches (writer task) =", pump.batches)
    print("part1: segmented_writes =", pump.segmented_writes)
    print("part1: enqueue_suspensions =", pump.enqueue_suspensions)


class _EagerTransport:
    """A minimal write_nowait-capable transport with epoch-tagged write logs."""

    def __init__(
        self,
        generation: int,
        epoch_now: Callable[[], int],
        log: list[tuple[str, int, int, bytes]],
    ) -> None:
        self.generation = generation
        self._epoch_now = epoch_now
        self.log = log
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._closing = False

    def _maybe_connack(self, data: bytes) -> None:
        if data.startswith(b"\x10"):  # CONNECT fixed header
            self.push(encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))

    def write_nowait(self, data: bytes) -> bool:
        self.log.append(("eager", self._epoch_now(), self.generation, data))
        self._maybe_connack(data)
        return True

    async def write(self, data: bytes) -> None:
        self.log.append(("write", self._epoch_now(), self.generation, data))
        self._maybe_connack(data)

    async def write_many(self, parts: list[bytes]) -> None:
        for part in parts:
            self.log.append(("write_many", self._epoch_now(), self.generation, part))
            self._maybe_connack(part)

    async def read(self, n: int = 65536) -> bytes:
        del n
        return await self._rx.get()

    async def close(self) -> None:
        if not self._closing:
            self._closing = True
            self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing

    def push(self, data: bytes) -> None:
        if not self._closing:
            self._rx.put_nowait(data)


async def part2_eager_reconnect_generation_isolation() -> None:
    transports: list[_EagerTransport] = []

    async def factory(host: str, port: int, *, ssl: object = None) -> _EagerTransport:
        del host, port, ssl
        transport = _EagerTransport(len(transports) + 1, lambda: client._connection_epoch, [])
        transports.append(transport)
        return transport

    client = AsyncClient(
        client_id="eager-probe",
        protocol=MQTTProtocolVersion.MQTTv5,
        reconnect=ReconnectPolicy(enabled=True, initial_delay=0, max_delay=0, stable_after=0.05),
        message_delivery="callback",
        keepalive=0,
    )
    client._transport_factory = factory

    async def wait_connected() -> None:
        for _ in range(300):
            if client.is_connected:
                return
            await asyncio.sleep(0)
        raise AssertionError("client never reached CONNECTED")

    try:
        connect_task = asyncio.create_task(client.connect("x"))
        for _ in range(300):
            if transports and any(e[0] in ("eager", "write") for e in transports[0].log):
                break
            await asyncio.sleep(0)
        transports[0].push(encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
        await connect_task
        await wait_connected()

        await client.publish("a/b", b"1", qos=QoS.AT_MOST_ONCE)
        await asyncio.sleep(0.05)
        gen1_eager = [e for e in transports[0].log if e[0] == "eager"]

        # EOF tears down generation 1 and triggers automatic reconnect.
        transports[0].push(b"")
        for _ in range(300):
            if len(transports) >= 2 and client.is_connected:
                break
            await asyncio.sleep(0)
        assert len(transports) >= 2, f"expected reconnect, have {len(transports)}"
        await wait_connected()

        await client.publish("a/b", b"2", qos=QoS.AT_MOST_ONCE)
        await asyncio.sleep(0.05)
    finally:
        client._intentional_disconnect = True
        try:
            await client.disconnect()
        except Exception:
            pass

    # A write crossing a generation boundary would land on a retired transport
    # with the replacement connection's epoch (or on two different epochs on one
    # transport). The honest invariant: every write a transport receives carries
    # exactly one epoch, and retired transports receive no writes after their
    # successor is installed.
    gen1_epochs = {e[1] for e in transports[0].log}
    gen2_epochs = {e[1] for e in transports[1].log}
    print("part2: generation-1 eager writes =", len(gen1_eager))
    print("part2: generation-1 epochs =", sorted(gen1_epochs))
    print("part2: generation-2 epochs =", sorted(gen2_epochs))
    print("part2: generation-2 writes =", [(e[0], e[1]) for e in transports[1].log])
    cross = gen1_epochs & gen2_epochs
    single_owner = len(gen1_epochs) == 1 and len(gen2_epochs) == 1
    verdict = "CLEAN (no cross-generation write)" if single_owner and not cross else "VIOLATION"
    print("part2: VERDICT:", verdict)


async def main() -> None:
    await part1_fuzzer_eager_path_is_dead()
    print("----")
    await part2_eager_reconnect_generation_isolation()


if __name__ == "__main__":
    asyncio.run(main())
