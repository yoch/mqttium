"""Bounded lifecycle hooks outside protocol and message-delivery ownership."""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient, ReconnectPolicy
from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import ProtocolError
from mqttium.packets import PublishPacket, encode_frame
from tests.support import ScriptedBrokerTransport, wait_until


@pytest.fixture(params=["normal", "eager"])
async def task_factory(request):
    if request.param == "eager" and not hasattr(asyncio, "eager_task_factory"):
        pytest.skip("eager task factory requires Python 3.12")
    loop = asyncio.get_running_loop()
    previous = loop.get_task_factory()
    if request.param == "eager":
        loop.set_task_factory(asyncio.eager_task_factory)
    try:
        yield
    finally:
        loop.set_task_factory(previous)


def install_brokers(client):
    brokers = []

    async def factory(*args, **kwargs):
        broker = ScriptedBrokerTransport(protocol=client._engine.config.protocol)
        brokers.append(broker)
        return broker

    client._transport_factory = factory
    return brokers


async def finish(client):
    await client.disconnect()
    await wait_until(lambda: client._lifecycle_hooks.task is None)


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
async def test_connect_hook_can_subscribe_and_publish_outside_setup(task_factory, protocol):
    client = AsyncClient("lifecycle-ready", protocol=protocol)
    brokers = install_brokers(client)
    completed = asyncio.Event()

    async def connected(packet):
        assert packet.reason_code == 0
        assert client.is_connected
        assert not client._lifecycle_lock.locked()
        assert not client._engine_lock.locked()
        assert not client._effect_pump.lock.locked()
        assert client._keepalive_task is not None
        assert asyncio.current_task() is client._lifecycle_hooks.hook_task
        await client.subscribe("result/#")
        receipt = await client.publish("result/online", b"yes", qos=1)
        await receipt.wait()
        completed.set()

    client.on_connect = connected
    try:
        await client.connect("fake")
        await asyncio.wait_for(completed.wait(), 1)
        assert len(brokers[0].publishes) == 1
    finally:
        await finish(client)


async def test_disconnect_returns_after_cleanup_without_waiting_for_hook(task_factory):
    client = AsyncClient("lifecycle-cleanup")
    brokers = install_brokers(client)
    entered, release = asyncio.Event(), asyncio.Event()
    errors = []

    async def disconnected(error):
        errors.append(error)
        assert client._transport is None
        assert client._write_pump.task is None
        assert old_reader.done()
        assert not client._lifecycle_lock.locked()
        entered.set()
        await release.wait()

    client.on_disconnect = disconnected
    try:
        await client.connect("fake")
        old_reader = client._reader_task
        assert old_reader is not None
        await asyncio.wait_for(client.disconnect(), 1)
        assert brokers[0].is_closing()
        await asyncio.wait_for(entered.wait(), 1)
        assert errors == [None]
        assert client._lifecycle_hooks.hook_task is not None
        assert not client._lifecycle_hooks.hook_task.done()
    finally:
        release.set()
        await finish(client)


@pytest.mark.parametrize("active_kind", ["connect", "disconnect"])
async def test_external_connection_supersedes_running_hook(task_factory, active_kind):
    client = AsyncClient("lifecycle-replacement")
    brokers = install_brokers(client)
    entered, cancelled, replacement = (asyncio.Event() for _ in range(3))
    active = 0

    async def obsolete(_packet):
        nonlocal active
        active += 1
        assert active == 1
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            active -= 1
            cancelled.set()

    async def current(_packet):
        assert active == 0
        replacement.set()

    if active_kind == "connect":
        client.on_connect = obsolete
    else:
        client.on_disconnect = obsolete
    try:
        await client.connect("fake")
        if active_kind == "disconnect":
            await brokers[0].close()
        await asyncio.wait_for(entered.wait(), 1)
        if client.is_connected:
            await client.disconnect()
        client.on_connect = current
        client.on_disconnect = None
        await asyncio.wait_for(client.connect("fake"), 1)
        await asyncio.wait_for(cancelled.wait(), 1)
        await asyncio.wait_for(replacement.wait(), 1)
        assert len(brokers) == 2
        assert client._transport is brokers[1]
    finally:
        await finish(client)


async def test_connect_hook_can_disconnect_and_finish_itself(task_factory):
    client = AsyncClient("hook-self-disconnect")
    install_brokers(client)
    completed = asyncio.Event()
    observed = []

    async def connected(_packet):
        await client.disconnect()
        assert not client.is_connected
        completed.set()

    client.on_connect = connected
    client.on_disconnect = observed.append
    try:
        await client.connect("fake")
        await asyncio.wait_for(completed.wait(), 1)
        await wait_until(lambda: observed == [None])
    finally:
        await finish(client)


async def test_replacement_does_not_join_or_recancel_hook_cleanup(task_factory):
    client = AsyncClient("hook-cancellation-cleanup")
    brokers = install_brokers(client)
    entered, cancelling, release, replacement = (asyncio.Event() for _ in range(4))

    async def connected(_packet):
        if len(brokers) != 1:
            replacement.set()
            return
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelling.set()
            await release.wait()

    client.on_connect = connected
    try:
        await client.connect("fake")
        await asyncio.wait_for(entered.wait(), 1)
        old_hook = client._lifecycle_hooks.hook_task
        await asyncio.wait_for(client.disconnect(), 1)
        await asyncio.wait_for(cancelling.wait(), 1)
        await asyncio.wait_for(client.connect("fake"), 1)
        assert client.is_connected
        assert old_hook is not None and old_hook.cancelling() == 1
        assert client._lifecycle_hooks.hook_task is old_hook
        assert not replacement.is_set()
        release.set()
        await asyncio.wait_for(replacement.wait(), 1)
        assert old_hook.done()
    finally:
        release.set()
        await finish(client)


async def test_redundant_connect_does_not_supersede_active_connect_hook(task_factory):
    client = AsyncClient("hook-redundant-connect")
    brokers = install_brokers(client)
    entered, release, completed = (asyncio.Event() for _ in range(3))

    async def connected(_packet):
        entered.set()
        await release.wait()
        receipt = await client.publish("initial-connection", b"alive", qos=1)
        await receipt.wait()
        completed.set()

    client.on_connect = connected
    try:
        await client.connect("fake")
        await asyncio.wait_for(entered.wait(), 1)
        token = client._lifecycle_hooks.token
        hook = client._lifecycle_hooks.hook_task
        with pytest.raises(ProtocolError, match="Already connected"):
            await client.connect("other")
        assert client._lifecycle_hooks.token == token
        assert client._lifecycle_hooks.hook_task is hook
        assert hook is not None and not hook.cancelling()
        assert client._host == "fake"
        assert len(brokers) == 1
        release.set()
        await asyncio.wait_for(completed.wait(), 1)
    finally:
        release.set()
        await finish(client)


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
@pytest.mark.parametrize("automatic_connection", [False, True])
async def test_invalid_disconnect_preserves_active_hook_and_retry(
    task_factory, protocol, automatic_connection
):
    client = AsyncClient(
        "hook-invalid-disconnect",
        protocol=protocol,
        reconnect=ReconnectPolicy(initial_delay=0, max_delay=0, stable_after=60),
    )
    brokers = install_brokers(client)
    entered, release, completed = (asyncio.Event() for _ in range(3))
    active_generation = 2 if automatic_connection else 1

    async def connected(_packet):
        if len(brokers) != active_generation:
            return
        entered.set()
        await release.wait()
        receipt = await client.publish("preserved-connection", b"alive", qos=1)
        await receipt.wait()
        completed.set()

    client.on_connect = connected
    try:
        await client.connect("fake")
        if automatic_connection:
            await brokers[0].close()
        await asyncio.wait_for(entered.wait(), 1)
        hooks = client._lifecycle_hooks
        token, hook = hooks.token, hooks.hook_task
        transport, reader = client._transport, client._reader_task
        epoch = client._connection_epoch
        retry_task = client._reconnect_task
        config = client._engine.config
        endpoint = (client._host, client._port, client._ssl)
        assert client._will_reconnect()
        if automatic_connection:
            assert retry_task is not None and not retry_task.done()

        with pytest.raises(ProtocolError):
            await client.disconnect(0x01)

        assert client.is_connected
        assert client._transport is transport and not transport.is_closing()
        assert client._reader_task is reader
        assert client._connection_epoch == epoch
        assert hooks.token == token and hooks.hook_task is hook
        assert hook is not None and not hook.cancelling()
        assert client._disconnect_hook_origin is None
        assert not client._intentional_disconnect
        assert client._will_reconnect()
        assert client._reconnect_task is retry_task
        assert retry_task is None or not retry_task.cancelling()
        assert client._engine.config is config
        assert (client._host, client._port, client._ssl) == endpoint
        assert len(brokers) == active_generation
        release.set()
        await asyncio.wait_for(completed.wait(), 1)
        assert len(brokers[-1].publishes) == 1
    finally:
        release.set()
        await finish(client)


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
async def test_disconnect_without_transport_remains_idempotent(protocol):
    client = AsyncClient("disconnect-without-transport", protocol=protocol)
    await client.disconnect(0x01)
    await client.disconnect(0x01)
    assert client._transport is None
    assert client._intentional_disconnect
    assert client._delivery.closed.is_set()


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
@pytest.mark.parametrize("qos", [0, 1, 2])
async def test_application_reconnect_keeps_fresh_callbacks_and_retires_old_delivery(
    task_factory, protocol, qos
):
    class InboundBroker(ScriptedBrokerTransport):
        def __init__(self):
            super().__init__(protocol=protocol)
            self.acknowledged = []

        def handle_packet(self, raw):
            if raw.packet_type is PacketType.PUBREC:
                self.push_rx(encode_frame(PacketType.PUBREL, 2, raw.remaining[:2]))
            elif raw.packet_type in (PacketType.PUBACK, PacketType.PUBCOMP):
                self.acknowledged.append(int.from_bytes(raw.remaining[:2], "big"))
            super().handle_packet(raw)

        def publish(self, prefix, count):
            self.push_rx(
                b"".join(
                    PublishPacket(
                        topic="route/message",
                        payload=prefix + index.to_bytes(2, "big") + b"x" * 256,
                        qos=QoS(qos),
                        mid=index + 1 if qos else None,
                        retain=False,
                        dup=False,
                    ).encode(protocol)
                    for index in range(count)
                )
            )

    client = AsyncClient(
        "application-reconnect-delivery",
        protocol=protocol,
        local_receive_maximum=256,
        message_delivery="callback",
        max_pending_callbacks=1,
        max_pending_delivery_bytes=1024,
    )
    brokers = []
    work: asyncio.Task[None] | None = None
    old_worker = None
    seen = []
    completed = asyncio.Event()

    async def factory(*args, **kwargs):
        broker = InboundBroker()
        brokers.append(broker)
        return broker

    async def reconnect():
        nonlocal old_worker
        old_worker = client._delivery.callback_task
        await client.disconnect()
        assert old_worker is not None and old_worker.done()
        assert client._delivery.callback_queue.empty()
        assert client.stats().delivery.pending_bytes == 0
        await client.connect("fake")
        brokers[1].publish(b"fresh", 3)

    def on_message(message):
        nonlocal work
        seen.append(message.payload)
        if message.payload.startswith(b"old") and work is None:
            work = asyncio.create_task(reconnect())
        if sum(payload.startswith(b"fresh") for payload in seen) == 3:
            completed.set()

    client._transport_factory = factory
    client.on_message = on_message
    try:
        await client.connect("fake")
        brokers[0].publish(b"old", 256)
        await asyncio.wait_for(completed.wait(), 2)
        assert work is not None
        await work
        if qos:
            await wait_until(lambda: len(brokers[1].acknowledged) == 3)
        await client._delivery.callback_queue.join()
        assert client.is_connected
        assert client._delivery.callback_task is not old_worker
        assert len([payload for payload in seen if payload.startswith(b"old")]) < 256
        assert all(payload.startswith(b"fresh") for payload in seen[-3:])
        assert len({payload for payload in seen[-3:]}) == 3
        assert client.stats().delivery.pending_bytes == 0
        assert client.stats().inbound.inflight == 0
    finally:
        if work is not None:
            if not work.done():
                work.cancel()
            try:
                await work
            except asyncio.CancelledError:
                pass
        await finish(client)


async def test_disconnect_hook_can_connect_and_publish_itself(task_factory):
    client = AsyncClient(
        "hook-self-connect", reconnect=ReconnectPolicy(initial_delay=0, max_delay=0)
    )
    brokers = install_brokers(client)
    completed = asyncio.Event()

    async def disconnected(_error):
        if len(brokers) == 1:
            await client.connect("fake")
            receipt = await client.publish("replacement", b"alive", qos=1)
            await receipt.wait()
            completed.set()

    client.on_disconnect = disconnected
    try:
        await client.connect("fake")
        await brokers[0].close()
        await asyncio.wait_for(completed.wait(), 1)
        assert client._transport is brokers[1]
        assert len(brokers) == 2
        assert len(brokers[1].publishes) == 1
        assert client._reconnect_task is None or client._reconnect_task.done()
    finally:
        await finish(client)


async def test_rapid_self_replacements_keep_only_latest_pending_hook(task_factory):
    client = AsyncClient("hook-coalescing")
    brokers = install_brokers(client)
    completed = asyncio.Event()
    observed = []
    owner_pairs = set()

    async def connected(_packet):
        observed.append(("connect", len(brokers)))
        if len(brokers) == 1:
            for _ in range(8):
                owner_pairs.add((client._lifecycle_hooks.task, client._lifecycle_hooks.hook_task))
                await client.disconnect()
                await client.connect("fake")
                assert client._lifecycle_hooks.pending is not None
        else:
            completed.set()

    client.on_connect = connected
    client.on_disconnect = lambda _error: observed.append(("disconnect", len(brokers)))
    try:
        await client.connect("fake")
        await asyncio.wait_for(completed.wait(), 2)
        assert observed == [("connect", 1), ("connect", 9)]
        assert len(owner_pairs) == 1
        assert client._lifecycle_hooks.pending is None
    finally:
        await finish(client)


@pytest.mark.parametrize("kind", ["future", "raised", "runtime", "connect", "disconnect"])
async def test_disconnect_hook_errors_do_not_own_protocol_lifecycle(task_factory, kind):
    client = AsyncClient(
        "hook-errors",
        reconnect=ReconnectPolicy(initial_delay=0, max_delay=0, stable_after=0),
    )
    brokers = install_brokers(client)
    reports = []
    loop = asyncio.get_running_loop()
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _, context: reports.append(context))

    async def disconnected(_error):
        if len(brokers) != 1:
            return
        if kind == "connect":
            await client.connect("fake")
        elif kind == "disconnect":
            await client.disconnect()
        if kind == "future":
            cancelled = loop.create_future()
            cancelled.cancel()
            await cancelled
        if kind == "runtime":
            raise RuntimeError("notification failed")
        raise asyncio.CancelledError("notification cancelled")

    client.on_disconnect = disconnected
    try:
        await client.connect("fake")
        reader = client._reader_task
        await brokers[0].close()
        await wait_until(lambda: bool(reports))
        assert reader is not None and reader.done()
        expected = RuntimeError if kind == "runtime" else asyncio.CancelledError
        assert len(reports) == 1
        assert isinstance(reports[0]["exception"], expected)
        if kind == "disconnect":
            assert not client.is_connected
            assert len(brokers) == 1
        else:
            await wait_until(lambda: len(brokers) == 2 and client.is_connected)
    finally:
        await finish(client)
        loop.set_exception_handler(previous)


async def test_reconnect_during_stability_waits_for_new_disconnect_hook(task_factory):
    client = AsyncClient(
        "hook-retry-gate",
        reconnect=ReconnectPolicy(initial_delay=0, max_delay=0, stable_after=0.02),
    )
    brokers = install_brokers(client)
    entered, release = asyncio.Event(), asyncio.Event()

    async def disconnected(_error):
        if len(brokers) == 2:
            entered.set()
            await release.wait()

    client.on_disconnect = disconnected
    try:
        await client.connect("fake")
        await brokers[0].close()
        await wait_until(lambda: len(brokers) == 2 and client.is_connected)
        await brokers[1].close()
        await asyncio.wait_for(entered.wait(), 1)
        await asyncio.sleep(0.05)
        assert len(brokers) == 2
        release.set()
        await wait_until(lambda: len(brokers) == 3 and client.is_connected)
    finally:
        release.set()
        await finish(client)


async def test_retry_gate_closes_before_transport_cleanup_suspends(task_factory):
    client = AsyncClient(
        "hook-cleanup-retry-gate",
        reconnect=ReconnectPolicy(initial_delay=0, max_delay=0, stable_after=0.01),
    )
    closing, release_close = asyncio.Event(), asyncio.Event()
    notifying, release_hook = asyncio.Event(), asyncio.Event()
    brokers = []

    class ClosingBroker(ScriptedBrokerTransport):
        async def close(self):
            closing.set()
            await release_close.wait()
            await super().close()

    async def factory(*args, **kwargs):
        broker = ClosingBroker() if len(brokers) == 1 else ScriptedBrokerTransport()
        brokers.append(broker)
        return broker

    async def disconnected(_error):
        if len(brokers) == 2:
            notifying.set()
            await release_hook.wait()

    client._transport_factory = factory
    client.on_disconnect = disconnected
    try:
        await client.connect("fake")
        await brokers[0].close()
        await wait_until(lambda: len(brokers) == 2 and client.is_connected)
        brokers[1].push_rx(b"")
        await asyncio.wait_for(closing.wait(), 1)
        await asyncio.sleep(0.03)
        assert len(brokers) == 2
        assert not notifying.is_set()
        release_close.set()
        await asyncio.wait_for(notifying.wait(), 1)
        assert len(brokers) == 2
        release_hook.set()
        await wait_until(lambda: len(brokers) == 3 and client.is_connected)
    finally:
        release_close.set()
        release_hook.set()
        await finish(client)


async def test_refused_connack_does_not_invoke_connect_hook(task_factory):
    class RefusingBroker(ScriptedBrokerTransport):
        def handle_packet(self, raw):
            if raw.packet_type is PacketType.CONNECT:
                self.push_rx(encode_frame(PacketType.CONNACK, 0, b"\x00\x05"))
            else:
                super().handle_packet(raw)

    client = AsyncClient("hook-refusal")
    broker = RefusingBroker()
    connected = []

    async def factory(*args, **kwargs):
        return broker

    client._transport_factory = factory
    client.on_connect = connected.append
    try:
        with pytest.raises(ProtocolError, match="refused"):
            await client.connect("fake")
        await wait_until(lambda: client._lifecycle_hooks.task is None)
        assert connected == []
    finally:
        await finish(client)
