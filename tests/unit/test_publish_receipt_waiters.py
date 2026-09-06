"""Waiter invariants for :class:`PublishReceipt`.

Each waiter parks on its own future rather than on a shielded view of one
shared future. That representation must keep every guarantee established by
https://github.com/yoch/mqttium/pull/76: cancelling one waiter never cancels
receipt completion nor contaminates another waiter, a waiter that arrives after
a cancellation still completes, and a receipt nobody waits on allocates no
completion primitive at all.
"""

from __future__ import annotations

import asyncio
import gc

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.api.models import PublishReceipt
from mqttium.enums import ConnectionState, QoS
from mqttium.errors import MQTTError


def _connected_client(client_id: str) -> AsyncClient:
    client = AsyncClient(client_id=client_id, max_outbound_messages=256)
    client._engine.state = ConnectionState.CONNECTED
    return client


async def test_receipt_never_awaited_builds_no_waiter_structure() -> None:
    """publish_nowait's whole point: nobody waits, so nothing is allocated."""
    client = _connected_client("nowait-receipt")

    receipt = client.publish_nowait("lazy/qos1", b"x", qos=1)

    assert receipt._waiters is None
    client._settle_publish(receipt.mid, None)
    assert receipt.is_done()
    assert receipt._waiters is None


async def test_wait_after_settlement_builds_no_waiter_structure() -> None:
    """A settled receipt answers from the flag without touching the loop."""
    receipt = PublishReceipt(mid=7, qos=QoS.AT_LEAST_ONCE)
    receipt._settle()

    await receipt.wait()

    assert receipt._waiters is None
    # Repeated waits stay on the same fast path.
    await receipt.wait()
    await receipt.wait()
    assert receipt._waiters is None


async def test_single_pending_waiter_completes_on_settlement() -> None:
    receipt = PublishReceipt(mid=8, qos=QoS.AT_LEAST_ONCE)

    waiter = asyncio.create_task(receipt.wait())
    await asyncio.sleep(0)
    assert receipt._waiters is not None
    assert len(receipt._waiters) == 1
    assert not waiter.done()

    receipt._settle()
    await waiter
    assert receipt.is_done()
    assert receipt._waiters is None


@pytest.mark.parametrize("count", [2, 8, 32])
async def test_concurrent_waiters_all_complete(count: int) -> None:
    receipt = PublishReceipt(mid=9, qos=QoS.AT_LEAST_ONCE)

    waiters = [asyncio.create_task(receipt.wait()) for _ in range(count)]
    await asyncio.sleep(0)
    assert receipt._waiters is not None
    assert len(receipt._waiters) == count

    receipt._settle()
    await asyncio.gather(*waiters)

    assert all(waiter.done() and waiter.exception() is None for waiter in waiters)
    assert receipt._waiters is None


async def test_cancelling_one_waiter_leaves_the_others_and_the_receipt_intact() -> None:
    """The load-bearing guarantee from PR #76, restated for per-waiter futures."""
    receipt = PublishReceipt(mid=10, qos=QoS.AT_LEAST_ONCE)

    first = asyncio.create_task(receipt.wait())
    second = asyncio.create_task(receipt.wait())
    await asyncio.sleep(0)

    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first

    assert not receipt._settled
    assert not second.done()
    assert receipt._waiters is not None
    assert len(receipt._waiters) == 1

    # A waiter created after the cancellation still attaches to the receipt.
    third = asyncio.create_task(receipt.wait())
    await asyncio.sleep(0)
    assert len(receipt._waiters) == 2

    receipt._settle()
    await asyncio.gather(second, third)
    assert receipt.is_done()
    assert receipt._waiters is None


async def test_cancelling_every_waiter_returns_the_receipt_to_its_lazy_shape() -> None:
    receipt = PublishReceipt(mid=11, qos=QoS.AT_LEAST_ONCE)

    waiters = [asyncio.create_task(receipt.wait()) for _ in range(16)]
    await asyncio.sleep(0)
    assert receipt._waiters is not None and len(receipt._waiters) == 16

    for waiter in waiters:
        waiter.cancel()
    for waiter in waiters:
        with pytest.raises(asyncio.CancelledError):
            await waiter

    # No waiter future is retained, and the receipt is still completable.
    assert receipt._waiters is None
    assert not receipt._settled
    receipt._settle()
    assert receipt.is_done()


async def test_settlement_tolerates_waiter_futures_that_are_already_done() -> None:
    """A future cancelled outside ``wait`` must not break settlement."""
    receipt = PublishReceipt(mid=12, qos=QoS.AT_LEAST_ONCE)

    live = asyncio.create_task(receipt.wait())
    await asyncio.sleep(0)
    assert receipt._waiters is not None

    # Cancel one parked future directly, so it stays listed while done.
    stale = receipt._waiters[0]
    doomed = asyncio.get_running_loop().create_future()
    doomed.cancel()
    receipt._waiters.append(doomed)
    stale.cancel()

    receipt._settle()
    assert receipt._waiters is None
    with pytest.raises(asyncio.CancelledError):
        await live


async def test_repeated_settlement_is_idempotent() -> None:
    receipt = PublishReceipt(mid=13, qos=QoS.AT_LEAST_ONCE)
    waiter = asyncio.create_task(receipt.wait())
    await asyncio.sleep(0)

    receipt._settle()
    receipt._settle()
    receipt._settle()

    await waiter
    assert receipt._waiters is None


async def test_every_waiter_receives_the_exact_error_instance() -> None:
    receipt = PublishReceipt(mid=14, qos=QoS.AT_LEAST_ONCE)
    failure = MQTTError("terminal")

    waiters = [asyncio.create_task(receipt.wait()) for _ in range(4)]
    await asyncio.sleep(0)

    receipt._error = failure
    receipt._settle()

    for waiter in waiters:
        with pytest.raises(MQTTError) as excinfo:
            await waiter
        assert excinfo.value is failure

    # A late waiter raises the same instance from the settled fast path.
    with pytest.raises(MQTTError) as excinfo:
        await receipt.wait()
    assert excinfo.value is failure


async def test_qos0_receipt_never_parks_a_waiter() -> None:
    receipt = PublishReceipt(mid=None, qos=QoS.AT_MOST_ONCE)

    assert receipt.is_done()
    await receipt.wait()
    assert receipt._waiters is None

    failing = PublishReceipt(mid=None, qos=QoS.AT_MOST_ONCE)
    failure = MQTTError("qos0 terminal")
    failing._error = failure
    with pytest.raises(MQTTError) as excinfo:
        await failing.wait()
    assert excinfo.value is failure
    assert failing._waiters is None


@pytest.mark.parametrize("qos", [QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
async def test_waiter_futures_belong_to_the_running_loop(qos: QoS) -> None:
    receipt = PublishReceipt(mid=15, qos=qos)
    loop = asyncio.get_running_loop()

    waiter = asyncio.create_task(receipt.wait())
    await asyncio.sleep(0)
    assert receipt._waiters is not None
    assert [future.get_loop() for future in receipt._waiters] == [loop]

    receipt._settle()
    await waiter


async def test_timed_out_waiter_is_retired_without_touching_completion() -> None:
    receipt = PublishReceipt(mid=16, qos=QoS.EXACTLY_ONCE)

    survivor = asyncio.create_task(receipt.wait())
    await asyncio.sleep(0)

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(receipt.wait(), timeout=0.01)

    assert receipt._waiters is not None
    assert len(receipt._waiters) == 1
    assert not survivor.done()

    receipt._settle()
    await survivor


async def test_no_waiter_future_is_retained_after_settlement() -> None:
    receipt = PublishReceipt(mid=17, qos=QoS.AT_LEAST_ONCE)

    waiters = [asyncio.create_task(receipt.wait()) for _ in range(8)]
    await asyncio.sleep(0)
    parked = list(receipt._waiters or ())
    assert len(parked) == 8

    receipt._settle()
    await asyncio.gather(*waiters)

    assert receipt._waiters is None
    del parked, waiters
    gc.collect()
    # The receipt itself keeps no reference into the loop's callback graph.
    assert receipt._waiters is None


async def test_settled_and_never_retrieved_receipt_stays_silent() -> None:
    """A failed receipt nobody waits on must not surface an unretrieved error."""
    loop = asyncio.get_running_loop()
    handled: list[dict[str, object]] = []
    loop.set_exception_handler(lambda _loop, context: handled.append(context))

    receipt = PublishReceipt(mid=18, qos=QoS.AT_LEAST_ONCE)
    receipt._error = MQTTError("never awaited")
    receipt._settle()

    del receipt
    gc.collect()
    await asyncio.sleep(0)

    assert handled == []


async def test_cancelled_waiter_does_not_consume_the_receipt_error() -> None:
    receipt = PublishReceipt(mid=19, qos=QoS.AT_LEAST_ONCE)
    failure = MQTTError("terminal after cancellation")

    cancelled = asyncio.create_task(receipt.wait())
    await asyncio.sleep(0)
    cancelled.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled

    late = asyncio.create_task(receipt.wait())
    await asyncio.sleep(0)

    receipt._error = failure
    receipt._settle()

    with pytest.raises(MQTTError) as excinfo:
        await late
    assert excinfo.value is failure
