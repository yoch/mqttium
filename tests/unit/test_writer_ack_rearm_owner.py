"""The ACK eager permit restored by its sole producer instead of a callback.

Without manual acknowledgement on a push transport in callback delivery, the
reader produces every success ACK. Its eager ACK then schedules no next-turn
re-arm callback (which cost one extra event-loop iteration per read); the
reader restores the permit itself just before it suspends, under the same
conditions, so the rule stays one ACK eager write per loop turn.
"""

from __future__ import annotations

import asyncio

from mqttium.api._delivery import _CALLBACK_QUANTUM, ApplicationDelivery
from mqttium.api._writer import WritePump
from mqttium.types import Message
from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.transport._push import PushStreamTransport

ACK = b"\x40\x02\x00\x01"


async def _ignore_failure(_exc: BaseException) -> None:
    pass


def _pump(writes: list[bytes]) -> WritePump:
    pump = WritePump(max_bytes=1024, max_messages=16, on_failure=_ignore_failure)

    def write_nowait(data: bytes) -> bool:
        writes.append(data)
        return True

    pump._write_nowait = write_nowait
    pump._eager_armed = True
    pump._ack_eager_armed = True
    return pump


async def test_owner_eager_ack_defers_rearm_without_a_callback() -> None:
    writes: list[bytes] = []
    pump = _pump(writes)
    pump.own_ack_rearm(asyncio.current_task())

    assert pump.try_enqueue_ack(ACK)
    assert writes == [ACK]
    assert pump._ack_eager_armed is False
    assert pump._eager_rearm_scheduled is False
    # A second ACK in the same turn still queues for the coalescing writer.
    assert pump.try_enqueue_ack(ACK)
    assert writes == [ACK]
    assert pump.queue.qsize() == 1


async def test_owner_restores_the_permit_only_under_the_callback_conditions() -> None:
    writes: list[bytes] = []
    pump = _pump(writes)
    pump.own_ack_rearm(asyncio.current_task())
    assert pump.try_enqueue_ack(ACK)
    assert pump.try_enqueue_ack(ACK)  # queued behind the eager write

    pump.rearm_deferred_ack()
    assert pump._ack_eager_armed is False  # the writer still owns queued frames

    pump.queue.get_nowait()
    pump.queue.task_done()
    pump.queued_bytes = 0
    pump._release_resident()
    pump._ack_rearm_deferred = True
    pump._writing = True
    pump.rearm_deferred_ack()
    assert pump._ack_eager_armed is False  # never during a writer batch

    pump._writing = False
    pump._ack_rearm_deferred = True
    pump.rearm_deferred_ack()
    assert pump._ack_eager_armed is True
    assert pump._ack_rearm_deferred is False


async def test_rearm_without_a_deferred_eager_write_is_a_no_op() -> None:
    pump = _pump([])
    pump.own_ack_rearm(asyncio.current_task())
    pump._ack_eager_armed = False

    pump.rearm_deferred_ack()

    assert pump._ack_eager_armed is False


async def test_other_tasks_keep_the_next_turn_callback() -> None:
    writes: list[bytes] = []
    pump = _pump(writes)
    owner = asyncio.create_task(asyncio.sleep(0))
    pump.own_ack_rearm(owner)

    assert pump.try_enqueue_ack(ACK)
    assert pump._eager_rearm_scheduled is True
    assert pump._ack_rearm_deferred is False
    await owner
    await asyncio.sleep(0)
    assert pump._ack_eager_armed is True


async def test_unowned_pump_keeps_the_next_turn_callback() -> None:
    writes: list[bytes] = []
    pump = _pump(writes)

    assert pump.try_enqueue_ack(ACK)
    assert pump._eager_rearm_scheduled is True
    await asyncio.sleep(0)
    assert pump._ack_eager_armed is True


async def test_dropping_the_transport_binding_retires_ownership() -> None:
    writes: list[bytes] = []
    pump = _pump(writes)
    pump.own_ack_rearm(asyncio.current_task())
    assert pump.try_enqueue_ack(ACK)

    pump.reset()

    assert pump._ack_rearm_owner is None
    assert pump._ack_rearm_deferred is False
    pump.rearm_deferred_ack()
    assert pump._ack_eager_armed is False


async def test_before_wait_runs_only_when_receive_really_suspends() -> None:
    from tests.unit.test_decoder_push_transport import _deliver, _publish, _wire

    from mqttium.codec.buffer import IncrementalDecoder

    protocol, _fake = _wire(IncrementalDecoder())
    transport = PushStreamTransport(asyncio.StreamReader(), None, protocol)  # type: ignore[arg-type]
    calls: list[str] = []
    transport.before_wait = lambda: calls.append("wait")

    _deliver(protocol, _publish(8))
    assert await transport.receive() is True
    assert calls == []  # bytes were already there: no suspension

    receiving = asyncio.create_task(transport.receive())
    await asyncio.sleep(0)
    assert calls == ["wait"]
    _deliver(protocol, _publish(8))
    assert await receiving is True
    assert calls == ["wait"]


def _message() -> Message:
    return Message(topic="t", payload=b"x", qos=QoS.AT_MOST_ONCE)


async def test_before_yield_runs_at_the_callback_fairness_yield_only() -> None:
    delivery = ApplicationDelivery(
        mode="callback",
        protocol=MQTTProtocolVersion.MQTTv311,
        max_iterator_messages=1,
        max_iterator_bytes=None,
        iterator_admission_timeout=None,
    )
    calls: list[str] = []
    delivery.before_yield = lambda: calls.append("yield")

    yielded = []
    for _ in range(_CALLBACK_QUANTUM):
        pending = delivery.accept(_message(), lambda _message: None)
        if pending is not None:
            yielded.append(pending)
    assert calls == ["yield"]
    assert len(yielded) == 1
    await yielded[0]
