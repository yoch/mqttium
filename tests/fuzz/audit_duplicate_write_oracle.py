"""Adversarial audit probe B: does the runtime fuzzer detect a duplicate wire write?

Hypothesis (blind spot): the runtime fuzzer's only wire-count oracle is the
checkpoint ``sum(transport.count(t)) >= target``. There is no exact-multiplicity
or terminal oracle for outbound packet counts. A transport that transmits a
PUBLISH twice therefore passes every oracle, even though duplicate delivery is
an "invalid outcome accepted by existing oracles" class.

This probe installs a mechanical, test-only "duplicate PUBLISH write" mutation
into the harness transport and runs the unmodified V1 oracles against it. If the
schedule returns ``failure is None`` while the transport observed two PUBLISH
writes for a single outbound publish, the gap is demonstrated.

Run: ``PYTHONPATH=src:. python tests/fuzz/audit_duplicate_write_oracle.py``
"""

from __future__ import annotations

import asyncio

from mqttium.enums import PacketType

from tests.fuzz import runtime_fuzzer as v1
from tests.fuzz.runtime_fuzzer import RuntimeOperation, RuntimeSchedule


class _DuplicatePublishTransport(v1._ScheduleTransport):
    """Test-only transport that transmits each PUBLISH frame twice on the wire."""

    async def write(self, data):
        payload = b"".join(data) if isinstance(data, tuple) else data
        self.active_payload = payload
        self._decoder.feed(payload)
        decoded = list(self._decoder.drain_packets())
        attempted: list = []
        for original in decoded:
            attempted.append(original)
            if original.packet_type is PacketType.PUBLISH:
                attempted.append(original)
        self.attempted.extend(attempted)
        self.write_entered.set()
        try:
            await self.write_gate.wait()
            if self.fail_active_write:
                self.fail_active_write = False
                raise ConnectionResetError("runtime fuzzer injected write failure")
            completion_epoch = self._current_epoch()
            self.completed.extend((packet, completion_epoch) for packet in attempted)
        finally:
            self.active_payload = None


class _Harness(v1._RuntimeHarness):
    def __init__(self, schedule: RuntimeSchedule) -> None:
        super().__init__(schedule, None)

    async def _factory(self, host, port, *, ssl=None):
        del host, port, ssl
        self.factory_attempts += 1
        transport = _DuplicatePublishTransport(
            len(self.transports) + 1,
            self.client._connection_epoch,
            lambda: self.client._connection_epoch,
        )
        self.transports.append(transport)
        return transport


async def _run(schedule: RuntimeSchedule) -> tuple[object | None, dict]:
    harness = _Harness(schedule)
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: harness.loop_contexts.append(context))
    failure: object | None = None
    owners: dict = {}

    async def execute_schedule() -> None:
        for operation in schedule.operations:
            await harness.execute(operation)
        harness._check_application_tasks(final=True)
        harness._check_loop_contexts(final=True)

    try:
        await asyncio.wait_for(execute_schedule(), timeout=2.0)
        owners = harness.owner_snapshot()
    except Exception as exc:  # noqa: BLE001 - probe reports any failure verbatim
        failure = exc
        owners = harness.owner_snapshot()
    finally:
        await harness.cleanup()
        loop.set_exception_handler(previous_handler)
    return failure, owners


async def main() -> None:
    schedule = RuntimeSchedule(
        seed=999,
        operations=(
            RuntimeOperation("app", "connect"),
            RuntimeOperation("checkpoint", "wire", "CONNECT"),
            RuntimeOperation("broker", "connack"),
            RuntimeOperation("checkpoint", "connected"),
            RuntimeOperation("app", "publish", 0),
            RuntimeOperation("checkpoint", "wire", "PUBLISH"),
            RuntimeOperation("checkpoint", "callbacks_drained"),
            RuntimeOperation("app", "disconnect"),
            RuntimeOperation("checkpoint", "terminal"),
        ),
    )
    failure, owners = await _run(schedule)
    completed = [p for t in owners.get("transports", []) for p in t["completed_packets"]]
    publish_count = completed.count("PUBLISH")
    print("failure:", failure)
    print("completed_packets:", completed)
    print("PUBLISH writes observed on the wire:", publish_count)
    verdict = (
        "ACCEPTED (oracle gap confirmed)" if failure is None and publish_count == 2 else "detected"
    )
    print("VERDICT:", verdict)


if __name__ == "__main__":
    asyncio.run(main())
