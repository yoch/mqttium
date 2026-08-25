"""Adversarial audit probe C: is the producer-parked path (publish waiters) ever
exercised by the runtime fuzzer, and is its teardown accounting oracled?

The harness configures default engine bounds and never saturates the QoS flow
window or the pending-message limit, so ``AsyncClient._publish_waiters`` /
``_wait_publish_space`` / ``_publish_wait_failure`` are never entered. There is
also no terminal oracle for ``stats.receipts.publish_waiters`` (only
writer/effect/delivery waiters are checked at teardown).

Part 1 shows the counters stay at zero across a fuzzer campaign. Part 2 drives
the real client to actually park a producer on the pending-message limit and
then tears down, to check the (unoracled) accounting in isolation.

Run: ``PYTHONPATH=src:. python tests/fuzz/audit_publish_waiter.py``
"""

from __future__ import annotations

import asyncio
from contextlib import suppress

from mqttium.api import AsyncClient
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import MQTTError
from mqttium.packets import encode_frame
from mqttium.protocol.reconnect import ReconnectPolicy

from tests.fuzz import runtime_fuzzer as v1


async def _max_publish_waiters_for_seed(seed: int) -> int:
    schedule = v1.generate_schedule(seed, 24)
    harness = v1._RuntimeHarness(schedule, None)
    loop = asyncio.get_running_loop()
    prev = loop.get_exception_handler()
    loop.set_exception_handler(lambda _l, c: harness.loop_contexts.append(c))
    try:
        for operation in schedule.operations:
            await harness.execute(operation)
        return harness.client._publish_waiters
    finally:
        await harness.cleanup()
        loop.set_exception_handler(prev)


async def part1_publish_waiters_dead_in_fuzzer() -> None:
    seen_waiter = 0
    for seed in range(24):
        seen_waiter = max(seen_waiter, await _max_publish_waiters_for_seed(seed))
    print("part1: max _publish_waiters observed across 24 fuzzer seeds =", seen_waiter)


class _Broker:
    """A write_nowait broker that answers CONNECT and never ACKs PUBLISH."""

    def __init__(self) -> None:
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._closing = False
        self.written: list[bytes] = []

    def write_nowait(self, data: bytes) -> bool:
        self.written.append(data)
        if data.startswith(b"\x10"):
            self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
        return True

    async def write(self, data: bytes) -> None:
        self.write_nowait(data)

    async def read(self, n: int = 65536) -> bytes:
        del n
        return await self._rx.get()

    async def close(self) -> None:
        if not self._closing:
            self._closing = True
            self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closing


async def part2_parked_producer_teardown() -> None:
    broker = _Broker()

    async def factory(host: str, port: int, *, ssl: object = None) -> _Broker:
        del host, port, ssl
        return broker

    client = AsyncClient(
        client_id="flow-probe",
        protocol=MQTTProtocolVersion.MQTTv5,
        max_outbound_inflight=1,  # outbound flow window of exactly one QoS message
        max_pending_outbound_messages=1,  # pending-message limit forces parking
        reconnect=ReconnectPolicy(enabled=False),
        message_delivery="callback",
        keepalive=0,
        ack_timeout=10.0,
    )
    client._transport_factory = factory
    try:
        await client.connect("x")
        await client.publish("t/1", b"a", qos=QoS.AT_LEAST_ONCE)
        # The second QoS 1 publish exceeds the pending-message limit and parks.
        parked = asyncio.create_task(client.publish("t/2", b"b", qos=QoS.AT_LEAST_ONCE))
        for _ in range(300):
            if client._publish_waiters >= 1:
                break
            await asyncio.sleep(0)
        print("part2: _publish_waiters while parked =", client._publish_waiters)
        assert client._publish_waiters >= 1, "producer never parked on the pending limit"

        # Teardown without acknowledging the in-flight publish.
        client._intentional_disconnect = True
        await client.disconnect()

        try:
            await parked
            outcome_name = "completed"
        except MQTTError as exc:
            outcome_name = f"raised {type(exc).__name__}: {exc}"
        print("part2: parked publish settled =", outcome_name)
        print("part2: _publish_waiters after teardown =", client._publish_waiters)
    finally:
        client._intentional_disconnect = True
        with suppress(Exception):
            await client.disconnect()


async def main() -> None:
    await part1_publish_waiters_dead_in_fuzzer()
    print("----")
    await part2_parked_producer_teardown()


if __name__ == "__main__":
    asyncio.run(main())
