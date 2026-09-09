from pathlib import Path
p = Path('arm-b')
path=p/'tests/unit/test_callback_route_pressure.py'
s=path.read_text()
s=s.replace('lambda l,x: errors.append(x)','lambda _loop, context: errors.append(context)')
old='''        delivery = c.stats().delivery
        assert delivery.callback_queued <= bound
        if budget is not None:
            assert delivery.pending_high_water_bytes <= budget
'''
assert s.count(old)==1
s=s.replace(old,'        _assert_bounds(c, bound, budget)\n')
s=s.replace("@pytest.mark.parametrize('mode'",'''def _assert_bounds(client, bound, budget):
    delivery = client.stats().delivery
    assert delivery.callback_queued <= bound
    if budget is not None:
        assert delivery.pending_high_water_bytes <= budget

@pytest.mark.parametrize('mode' ''',1).replace("('mode' ,","('mode',")
path.write_text(s)
path=p/'src/mqttium/api/_delivery.py'
s=path.read_text()
s=s.replace('''        The caller has released the inline reservation; transfer it to this job
        before starting a worker (also safe with an eager task factory).
''','''        The caller has released the inline reservation. Start an idle worker
        before admission so an eager task factory parks on the empty queue,
        rather than executing user code before its task ownership is installed.
''')
old='''        self.callback_queue.put_nowait(job)
        self.callback_queue._queue.rotate(1)  # type: ignore[attr-defined]
        self._reserve_callback_batch(count)
        self.ensure_callback_worker()
'''
new='''        self.ensure_callback_worker()
        self.callback_queue.put_nowait(job)
        self.callback_queue._queue.rotate(1)  # type: ignore[attr-defined]
        self._reserve_callback_batch(count)
'''
assert s.count(old)==1
path.write_text(s.replace(old,new))
path=p/'tests/unit/test_callback_route_handoff.py'
s=path.read_text()
s+='''

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
'''
path.write_text(s)
print('refinements applied: lint and eager pre-admission ownership')
