"""``PublishReceipt.add_done_callback`` and ``exception``.

A done callback is a per-publication observer that needs no coroutine or
task: it shares the receipt's lazy waiter list, is resolved by the same
settlement, and always runs later on the loop, never inside the client.
"""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api.async_client import AsyncClient, _fifo_register
from mqttium.api.models import PublishReceipt
from mqttium.enums import QoS
from mqttium.errors import NotConnectedError


def _receipt() -> PublishReceipt:
    return PublishReceipt(mid=1, qos=QoS.AT_LEAST_ONCE)


async def test_callback_runs_on_the_loop_after_settlement() -> None:
    receipt = _receipt()
    seen: list[PublishReceipt] = []
    receipt.add_done_callback(seen.append)

    receipt._settle()
    assert seen == []  # never synchronously inside settlement
    await asyncio.sleep(0)
    assert seen == [receipt]
    assert receipt.exception() is None
    assert receipt._waiters is None  # released by settlement


async def test_failure_is_readable_from_the_callback() -> None:
    receipt = _receipt()
    failure = NotConnectedError("gone")
    outcomes: list[BaseException | None] = []
    receipt.add_done_callback(lambda done: outcomes.append(done.exception()))

    receipt._error = failure
    receipt._settle()
    await asyncio.sleep(0)

    assert outcomes == [failure]


async def test_a_done_receipt_schedules_the_callback_at_once() -> None:
    for receipt in (PublishReceipt(mid=None, qos=QoS.AT_MOST_ONCE), _receipt()):
        if receipt.qos is not QoS.AT_MOST_ONCE:
            receipt._settle()
        seen: list[PublishReceipt] = []
        receipt.add_done_callback(seen.append)
        assert seen == []
        await asyncio.sleep(0)
        assert seen == [receipt]
        assert receipt._waiters is None


async def test_callbacks_and_waiters_share_one_settlement_in_order() -> None:
    receipt = _receipt()
    order: list[str] = []
    receipt.add_done_callback(lambda _r: order.append("first"))
    waiter = asyncio.create_task(receipt.wait())
    await asyncio.sleep(0)
    receipt.add_done_callback(lambda _r: order.append("second"))

    receipt._settle()
    await waiter
    await asyncio.sleep(0)

    assert order == ["first", "second"]


async def test_a_cancelled_waiter_leaves_callbacks_registered() -> None:
    receipt = _receipt()
    seen: list[PublishReceipt] = []
    waiter = asyncio.create_task(receipt.wait())
    await asyncio.sleep(0)
    receipt.add_done_callback(seen.append)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter

    receipt._settle()
    await asyncio.sleep(0)

    assert seen == [receipt]


async def test_exception_before_completion_is_an_invalid_state() -> None:
    with pytest.raises(asyncio.InvalidStateError):
        _receipt().exception()


async def test_a_failing_callback_reaches_the_loop_handler_only() -> None:
    loop = asyncio.get_running_loop()
    reported: list[dict[str, object]] = []
    loop.set_exception_handler(lambda _loop, context: reported.append(context))
    try:
        receipt = _receipt()
        seen: list[PublishReceipt] = []

        def broken(_receipt: PublishReceipt) -> None:
            raise RuntimeError("application bug")

        receipt.add_done_callback(broken)
        receipt.add_done_callback(seen.append)
        receipt._settle()
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(None)

    assert seen == [receipt]
    assert [type(context["exception"]) for context in reported] == [RuntimeError]


async def test_client_teardown_failure_reaches_done_callbacks() -> None:
    client = AsyncClient(client_id="done-callback-teardown")
    receipt = PublishReceipt(mid=7, qos=QoS.AT_LEAST_ONCE)
    _fifo_register(client._receipts, 7, receipt)
    outcomes: list[BaseException | None] = []
    receipt.add_done_callback(lambda done: outcomes.append(done.exception()))
    failure = NotConnectedError("client stopped")

    client._fail_pending(failure)
    await asyncio.sleep(0)

    assert outcomes == [failure]
