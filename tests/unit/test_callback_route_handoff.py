"""Guard the cold handoff without surrendering the steady-state inline paths."""

from __future__ import annotations

import asyncio
from collections import deque

import pytest

from mqttium.api import AsyncClient
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message
from tests.unit.test_callback_route_reconfiguration import _callback_errors, _messages


@pytest.mark.parametrize("route", ["exact", "wildcard", "fallback", "overlap"])
@pytest.mark.parametrize("burst", [1, 2])
@pytest.mark.parametrize("kind", [EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE])
async def test_stable_sync_topic_routes_remain_inline(route, burst, kind):
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=2)
    seen = []

    def callback(message):
        assert not client._engine_lock.locked()
        seen.append(message.payload)

    if route == "fallback":
        client.on_message = callback
        client.message_callback_add("unmatched/#", callback)
    else:
        client.message_callback_add("audit/x" if route == "exact" else "audit/#", callback)
        if route == "overlap":
            client.message_callback_add("audit/+", callback)
    with _callback_errors() as errors:
        try:
            assert client._apply_message_effect_batch_inline(_messages(burst, kind), 0) == burst
            assert seen == [
                str(i).encode() for i in range(burst) for _ in range(2 if route == "overlap" else 1)
            ]
            assert client._callback_worker_task is None
            assert client.stats().delivery.callback_queued == 0
            assert not client._delivery._callback_active
            assert errors == []
        finally:
            await client._shutdown_callback_worker(drain=False)


@pytest.mark.parametrize("kind", [EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE])
async def test_sync_reconfiguration_keeps_the_pair_inline(kind):
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=2)
    seen = []

    def replacement(message):
        seen.append(("new", message.payload))

    def original(message):
        seen.append(("old", message.payload))
        client.message_callback_add("audit/x", replacement)

    client.message_callback_add("audit/x", original)
    with _callback_errors() as errors:
        assert client._apply_message_effect_batch_inline(_messages(2, kind), 0) == 2
        assert seen == [("old", b"0"), ("new", b"1")]
        assert client._callback_worker_task is None
        assert errors == []


@pytest.mark.parametrize("path", ["single", "pair", "captured", "queued"])
async def test_stale_router_transfers_unstarted_work_and_reresolves_before_worker(path):
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=2)
    seen = []
    entered = asyncio.Event()
    release = asyncio.Event()

    def original(message):
        seen.append(("old", message.payload))

    async def superseded(message):
        seen.append(("wrong", message.payload))

    async def latest(message):
        entered.set()
        await release.wait()
        seen.append(("new", message.payload))

    client.message_callback_add("audit/x", original)
    captured = client._message_callback
    client.message_callback_add("audit/x", superseded)
    effects = _messages(2 if path == "pair" else 1, EffectKind.MESSAGE)
    with _callback_errors() as errors:
        try:
            if path == "single":
                client._delivery.dispatch_callback_inline(captured, effects[0].data)
            elif path == "captured":
                assert client._delivery.deliver_callback_messages_inline(
                    [effects[0].data], captured
                )
            elif path == "queued":
                client._delivery.spawn_callback(captured, effects[0].data)
            else:
                assert client._delivery.deliver_message_batch_inline(effects, captured) == 2
            assert seen == []
            assert client.stats().delivery.callback_queued == len(effects)
            client.on_message = latest
            client.message_callback_remove("audit/x")
            joiner = asyncio.create_task(client._callback_queue.join())
            await asyncio.wait_for(entered.wait(), timeout=2)
            assert not joiner.done()
            release.set()
            await asyncio.wait_for(joiner, timeout=2)
            assert seen == [("new", str(i).encode()) for i in range(len(effects))]
            assert client.stats().delivery.callback_queued == 0
            assert client._callback_queue.maxsize == 2
            assert errors == []
        finally:
            release.set()
            await client._shutdown_callback_worker(drain=False)


@pytest.mark.parametrize("eager", [False, True])
async def test_inline_handoff_precedes_reentrant_jobs_with_bounded_refill(eager):
    if eager and not hasattr(asyncio, "eager_task_factory"):
        pytest.skip("eager_task_factory requires Python 3.12")
    loop = asyncio.get_running_loop()
    factory = loop.get_task_factory()
    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=2)
    entered = asyncio.Event()
    release = asyncio.Event()
    seen = []

    async def replacement(message):
        seen.append("tail-start")
        entered.set()
        await release.wait()
        seen.append("tail-end")

    def original(message):
        seen.append("first")
        client.message_callback_add("audit/x", replacement)
        assert client._delivery.try_enqueue_callback(lambda: seen.append("reentrant"))
        assert client.stats().delivery.callback_queued == 2
        assert not client._delivery.try_enqueue_callback(lambda: seen.append("overflow"))

    client.message_callback_add("audit/x", original)
    with _callback_errors() as errors:
        try:
            assert (
                client._apply_message_effect_batch_inline(_messages(2, EffectKind.MESSAGE), 0) == 2
            )
            await asyncio.wait_for(entered.wait(), timeout=2)
            assert seen == ["first", "tail-start"]
            assert client._delivery.try_enqueue_callback(lambda: seen.append("refill"))
            assert not client._delivery.try_enqueue_callback(lambda: seen.append("overflow"))
            release.set()
            await asyncio.wait_for(client._callback_queue.join(), timeout=2)
            assert seen == ["first", "tail-start", "tail-end", "reentrant", "refill"]
            assert client.stats().delivery.callback_queued == 0
            assert client._callback_queue.maxsize == 2
            assert errors == []
        finally:
            release.set()
            await client._shutdown_callback_worker(drain=False)
            loop.set_task_factory(factory)


@pytest.mark.parametrize("kind", [EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE])
async def test_cancel_before_handoff_worker_starts_releases_every_reservation(kind):
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=2)
    seen = []

    async def replacement(message):
        seen.append("unexpected-tail")

    def original(message):
        seen.append("first")
        client.message_callback_add("audit/x", replacement)
        assert client._delivery.try_enqueue_callback(lambda: seen.append("unexpected-reentrant"))

    client.message_callback_add("audit/x", original)
    with _callback_errors() as errors:
        assert client._apply_message_effect_batch_inline(_messages(2, kind), 0) == 2
        assert seen == ["first"]
        assert client.stats().delivery.callback_queued == 2
        await client._shutdown_callback_worker(drain=False)
        await asyncio.wait_for(client._callback_queue.join(), timeout=2)
        assert seen == ["first"]
        assert client.stats().delivery.callback_queued == 0
        assert client._callback_queue.maxsize == 2
        assert errors == []


async def test_inline_pair_can_repeat_the_same_message_object_without_replay():
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=2)
    seen = []
    message = Message(topic="audit/x", payload=b"same")

    async def replacement(msg):
        assert msg is message
        seen.append("new")

    def original(msg):
        assert msg is message
        seen.append("old")
        client.message_callback_add("audit/x", replacement)

    client.message_callback_add("audit/x", original)
    effects = deque(
        EngineEffect(EffectKind.MESSAGE, message, requires_delivery_mark=False) for _ in range(2)
    )
    with _callback_errors() as errors:
        try:
            assert client._apply_message_effect_batch_inline(effects, 0) == 2
            await asyncio.wait_for(client._callback_queue.join(), timeout=2)
            assert seen == ["old", "new"]
            assert errors == []
        finally:
            await client._shutdown_callback_worker(drain=False)


async def test_sync_message_snapshot_survives_mutation_of_a_later_match():
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=2)
    seen = []

    async def replacement(message):
        seen.append(("new-tail", message.payload))

    def first(message):
        seen.append(("first", message.payload))
        client.message_callback_add("audit/+", replacement)

    def old_tail(message):
        seen.append(("old-tail", message.payload))

    client.message_callback_add("audit/#", first)
    client.message_callback_add("audit/+", old_tail)
    with _callback_errors() as errors:
        try:
            assert (
                client._apply_message_effect_batch_inline(_messages(2, EffectKind.MESSAGE), 0) == 2
            )
            await asyncio.wait_for(client._callback_queue.join(), timeout=2)
            assert seen == [
                ("first", b"0"),
                ("old-tail", b"0"),
                ("first", b"1"),
                ("new-tail", b"1"),
            ]
            assert errors == []
        finally:
            await client._shutdown_callback_worker(drain=False)


async def test_handoff_capacity_failure_is_transactional():
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=1)
    from mqttium.api._delivery import _CALLBACK_MESSAGE_BATCH

    job = (
        lambda _message: None,
        ([Message("audit/x", b"a"), Message("audit/x", b"b")],),
        _CALLBACK_MESSAGE_BATCH,
    )
    with pytest.raises(RuntimeError, match="reserved capacity"):
        client._delivery._prepend_callback_job(job)
    assert client._callback_queue.empty()
    assert client._callback_queue.maxsize == 1
    assert client._callback_worker_task is None
    await asyncio.wait_for(client._callback_queue.join(), timeout=2)


@pytest.mark.parametrize("eager", [False, True])
async def test_fresh_handoff_worker_installs_ownership_before_callback_disconnect(eager):
    if eager and not hasattr(asyncio, "eager_task_factory"):
        pytest.skip("eager_task_factory requires Python 3.12")
    loop = asyncio.get_running_loop()
    factory = loop.get_task_factory()
    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=2)
    seen = []

    async def replacement(message):
        assert not client._effect_pump.draining_inline
        assert client._callback_worker_task is asyncio.current_task()
        seen.append("tail")
        await client.disconnect()
        assert client._delivery._callback_stop
        seen.append("disconnected")

    def original(message):
        seen.append("first")
        client.message_callback_add("audit/x", replacement)

    client.message_callback_add("audit/x", original)
    for effect in _messages(2, EffectKind.MESSAGE):
        client._engine._emit(effect.kind, effect.data, requires_delivery_mark=False)
    client._collect_effects_locked()
    with _callback_errors() as errors:
        try:
            client._drain_effects_inline()
            assert seen == ["first"]
            assert not client._pending_effects
            await asyncio.wait_for(client._callback_queue.join(), timeout=2)
            assert seen == ["first", "tail", "disconnected"]
            assert client.stats().delivery.callback_queued == 0
            assert client._callback_queue.maxsize == 2
            assert errors == []
        finally:
            await client._shutdown_callback_worker(drain=False)
            loop.set_task_factory(factory)


@pytest.mark.parametrize("eager", [False, True])
@pytest.mark.parametrize("cancel_putter", [False, True])
async def test_handoff_preserves_waiting_producer_order_and_join(eager, cancel_putter):
    """Releasing a pair reservation must not let an awakened putter overtake it."""
    if eager and not hasattr(asyncio, "eager_task_factory"):
        pytest.skip("eager_task_factory requires Python 3.12")
    loop = asyncio.get_running_loop()
    factory = loop.get_task_factory()
    if eager:
        loop.set_task_factory(asyncio.eager_task_factory)
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=2)
    delivery = client._delivery
    seen = []
    entered = asyncio.Event()
    release = asyncio.Event()
    putters = []
    joiners = []

    async def tail(_message):
        seen.append("tail-start")
        entered.set()
        await release.wait()
        seen.append("tail-end")

    def first(_message):
        seen.append("first")
        client.message_callback_add("audit/x", tail)
        delivery.spawn_callback(lambda: seen.append("reentrant"))
        job = (lambda: seen.append("putter"), (), None)
        putters.append(asyncio.create_task(delivery.enqueue_callback_job_slow(job)))
        joiners.append(asyncio.create_task(delivery.callback_queue.join()))
        assert delivery.stats().callback_queued == 2

    client.message_callback_add("audit/x", first)
    with _callback_errors() as errors:
        try:
            client._apply_message_effect_batch_inline(_messages(2, EffectKind.MESSAGE), 0)
            if cancel_putter:
                putters[0].cancel()
            await asyncio.wait_for(entered.wait(), timeout=2)
            assert not joiners[0].done()
            await asyncio.gather(*putters, return_exceptions=cancel_putter)
            assert delivery.stats().callback_queued <= 2
            release.set()
            await asyncio.wait_for(joiners[0], timeout=2)
            assert seen == ["first", "tail-start", "tail-end", "reentrant"] + (
                [] if cancel_putter else ["putter"]
            )
            assert delivery.stats().callback_queued == 0
            assert delivery.callback_queue.maxsize == 2
            assert errors == []
        finally:
            release.set()
            for task in putters + joiners:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*putters, *joiners, return_exceptions=True)
            await delivery.shutdown_callbacks(drain=False)
            loop.set_task_factory(factory)


@pytest.mark.parametrize("kind", [EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE])
async def test_handoff_precedes_a_reentrant_reserved_batch(kind):
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=4)
    delivery = client._delivery
    seen = []
    entered = asyncio.Event()
    release = asyncio.Event()

    async def tail(_message):
        seen.append("tail")
        entered.set()
        await release.wait()

    def first(_message):
        seen.append("first")
        client.message_callback_add("audit/x", tail)
        nested = [Message("audit/x", bytes([value])) for value in range(3)]
        assert delivery.deliver_callback_messages_inline(
            nested, lambda message: seen.append(message.payload)
        )
        assert delivery.stats().callback_queued == 4

    client.message_callback_add("audit/x", first)
    with _callback_errors() as errors:
        try:
            assert client._apply_message_effect_batch_inline(_messages(2, kind), 0) == 2
            await asyncio.wait_for(entered.wait(), timeout=2)
            assert seen == ["first", "tail"]
            assert delivery.stats().callback_queued == 3
            release.set()
            await asyncio.wait_for(delivery.callback_queue.join(), timeout=2)
            assert seen == ["first", "tail", b"\x00", b"\x01", b"\x02"]
            assert delivery.stats().callback_queued == 0
            assert delivery.callback_queue.maxsize == 4
            assert errors == []
        finally:
            release.set()
            await delivery.shutdown_callbacks(drain=False)


async def test_empty_handoff_batch_is_rejected_without_mutation():
    from mqttium.api._delivery import _CALLBACK_MESSAGE_BATCH

    client = AsyncClient(message_delivery="callback", max_pending_callbacks=2)
    delivery = client._delivery
    with pytest.raises(RuntimeError, match="reserved capacity"):
        delivery._prepend_callback_job((lambda _message: None, ([],), _CALLBACK_MESSAGE_BATCH))
    assert delivery.callback_task is None
    assert delivery.callback_queue.empty()
    assert delivery.callback_queue.maxsize == 2
    assert delivery.stats().callback_queued == 0
    await asyncio.wait_for(delivery.callback_queue.join(), timeout=2)
