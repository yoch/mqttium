"""A late cancellation must not cancel the shared stream-close future."""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from mqttium.transport._stream import StreamTransport, StreamTransportBase
from mqttium.transport.websocket import WebSocketTransport


@pytest.mark.parametrize(
    "transport_type", [StreamTransportBase, StreamTransport, WebSocketTransport]
)
@pytest.mark.parametrize("first_abort", ["timeout", "cancellation"])
@pytest.mark.parametrize("late_cancellations", [1, 2])
async def test_late_cancellation_preserves_shared_close_future(
    monkeypatch, transport_type, first_abort, late_cancellations
):
    # Keep connection_lost under explicit control. abort() schedules it; it
    # does not synchronously complete the shared asyncio close future.
    monkeypatch.setattr(
        "mqttium.transport._stream._STREAM_CLOSE_TIMEOUT",
        0.0 if first_abort == "timeout" else 60.0,
    )
    shared_close = asyncio.get_running_loop().create_future()
    waiting = asyncio.Event()
    aborted = asyncio.Event()
    finished = asyncio.Event()

    async def wait_closed():
        waiting.set()
        try:
            await shared_close
        finally:
            finished.set()

    writer = Mock()
    writer.wait_closed = AsyncMock(side_effect=wait_closed)
    writer.transport.abort.side_effect = aborted.set
    transport = transport_type(asyncio.StreamReader(), writer)
    task = asyncio.create_task(transport.close())
    try:
        await asyncio.wait_for(waiting.wait(), 1)
        if first_abort == "cancellation":
            task.cancel()
        await asyncio.wait_for(aborted.wait(), 1)
        for _ in range(late_cancellations):
            task.cancel()
            await asyncio.sleep(0)
            assert not shared_close.cancelled()
            assert not task.done()
        shared_close.set_result(None)
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        assert finished.is_set()
        assert not shared_close.cancelled()
        # A different cleanup owner still sees normal stream closure.
        await asyncio.wait_for(transport.close(), 1)
    finally:
        if not shared_close.done():
            shared_close.set_result(None)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
