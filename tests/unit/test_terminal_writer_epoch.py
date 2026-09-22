"""Terminal admission may only fence its own connection epoch."""

import asyncio

import pytest

from mqttium.api._effects import StaleConnectionEffect
from mqttium.api._writer import WritePump


async def unexpected_failure(exc):
    raise AssertionError("No writer task is started by these admission tests") from exc


@pytest.mark.parametrize("submission", ["try_enqueue", "try_enqueue_ack", "enqueue"])
async def test_stale_terminal_does_not_seal_replacement_writer(submission):
    pump = WritePump(max_bytes=1024, max_messages=4, on_failure=unexpected_failure)
    await pump.advance_epoch(2)
    with pytest.raises(StaleConnectionEffect):
        pump.try_enqueue_terminal(b"\xe0\x00", epoch=1)
    assert pump.queued_messages == 0
    assert pump.resident_messages == 0
    submit = getattr(pump, submission)
    if submission == "enqueue":
        await asyncio.wait_for(submit(b"\xc0\x00", epoch=2), 1)
    else:
        assert submit(b"\xc0\x00", epoch=2)
    assert pump.queued_messages == 1
    assert pump.resident_messages == 1
    assert pump.queued_bytes == 2
    assert pump.queue.get_nowait() == b"\xc0\x00"


@pytest.mark.parametrize("has_capacity", [True, False])
async def test_current_terminal_seals_even_when_full_and_reset_reopens(has_capacity):
    pump = WritePump(max_bytes=1024, max_messages=1, on_failure=unexpected_failure)
    await pump.advance_epoch(2)
    if not has_capacity:
        assert pump.try_enqueue(b"\xc0\x00", epoch=2)
    assert pump.try_enqueue_terminal(b"\xe0\x00", epoch=2) is has_capacity
    with pytest.raises(StaleConnectionEffect):
        pump.try_enqueue(b"\xc0\x00", epoch=2)
    with pytest.raises(StaleConnectionEffect):
        pump.try_enqueue_ack(b"\x40\x02\x00\x01", epoch=2)
    with pytest.raises(StaleConnectionEffect):
        await asyncio.wait_for(pump.enqueue(b"\xc0\x00", epoch=2), 1)
    assert pump.queued_messages == 1
    assert pump.resident_messages == 1
    pump.reset()
    await pump.advance_epoch(3)
    assert pump.try_enqueue(b"\xc0\x00", epoch=3)
    assert pump.resident_messages == 1
