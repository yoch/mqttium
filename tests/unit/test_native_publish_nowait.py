"""Loop-bound native publish admission and adapter-boundary contracts."""

from __future__ import annotations

import asyncio
import inspect

import pytest

import mqttium.compat.paho as paho_compat
import mqttium.packets._publish as publish_v5_module
from mqttium.api import AsyncClient
from mqttium.api.models import PublishMessage
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import FlowControlError, ProtocolError
from mqttium.packets import PublishPacket
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.protocol.negotiated import NegotiatedSettings
from mqttium.types import Properties


def test_publish_nowait_requires_a_running_loop() -> None:
    client = AsyncClient()
    with pytest.raises(RuntimeError, match="event-loop thread"):
        client.publish_nowait("native/off-loop", b"x")


async def test_publish_nowait_rejects_a_different_running_loop() -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    client.publish_nowait("native/owner", b"x", qos=0)

    errors: list[BaseException] = []

    def run_other_loop() -> None:
        async def attempt() -> None:
            try:
                client.publish_nowait("native/other-loop", b"x", qos=0)
            except BaseException as exc:
                errors.append(exc)

        asyncio.run(attempt())

    await asyncio.to_thread(run_other_loop)
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert "different event loop" in str(errors[0])


async def test_publish_nowait_registers_qos1_receipt() -> None:
    client = AsyncClient()
    client._engine.state = ConnectionState.CONNECTED
    receipt = client.publish_nowait("native/qos1", b"x", qos=1)
    assert receipt.qos is QoS.AT_LEAST_ONCE
    assert receipt.mid is not None
    assert client._pop_publish_receipt(receipt.mid) is receipt


@pytest.mark.parametrize("qos", [QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
async def test_publish_nowait_qosn_hands_the_single_send_directly(qos: QoS) -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED

    receipt = client.publish_nowait("native/qosn-direct", b"payload", qos=qos)

    assert receipt.qos is qos
    assert receipt.mid is not None
    assert client._receipts[receipt.mid] is receipt
    assert client._effect_pump.batches == 0
    assert client._effect_pump.enqueued == 0
    assert not client._engine.has_pending_effects
    assert client.stats().writer.queued_messages == 1
    assert client._engine.store.get_out(receipt.mid) is not None


async def test_publish_nowait_qosn_registers_receipt_before_writer_commit(monkeypatch) -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    original_commit = client._try_enqueue_outbound
    observed: list[bool] = []

    def checking_commit(item, *, epoch=None):
        receipts = list(client._receipts.values())
        observed.append(len(receipts) == 1 and receipts[0].mid is not None)
        return original_commit(item, epoch=epoch)

    monkeypatch.setattr(client, "_try_enqueue_outbound", checking_commit)

    receipt = client.publish_nowait("native/receipt-before-wire", b"x", qos=1)

    assert observed == [True]
    assert receipt.mid is not None
    assert client._receipts[receipt.mid] is receipt


async def test_publish_nowait_qosn_writer_refusal_precedes_protocol_mutation() -> None:
    client = AsyncClient(max_outbound_messages=1, max_outbound_bytes=1024)
    client._engine.state = ConnectionState.CONNECTED
    client.publish_nowait("native/occupy-writer", b"first", qos=0)

    before = client._engine.outbound.stats()
    with pytest.raises(FlowControlError):
        client.publish_nowait("native/refused-qos1", b"second", qos=1)

    after = client._engine.outbound.stats()
    assert after.pending_messages == before.pending_messages == 0
    assert after.pending_bytes == before.pending_bytes == 0
    assert after.flow_inflight == before.flow_inflight == 0
    assert after.packet_ids_in_use == before.packet_ids_in_use == 0
    assert client._receipts == {}
    assert not client._engine.has_pending_effects
    assert not client._effect_pump.pending


async def test_publish_nowait_qosn_full_flow_falls_back_to_engine_queue(monkeypatch) -> None:
    client = AsyncClient(max_outbound_inflight=1, max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    # The focused client tests bypass CONNACK, which normally applies this cap.
    client._engine.flow.limit = 1
    first = client.publish_nowait("native/first", b"x", qos=1)

    def direct_commit_forbidden(*_args, **_kwargs):
        raise AssertionError("full flow must not use the direct writer commit")

    monkeypatch.setattr(client, "_try_enqueue_outbound", direct_commit_forbidden)
    second = client.publish_nowait("native/queued", b"y", qos=1)

    assert first.mid is not None
    assert second.mid is not None
    assert client.stats().writer.queued_messages == 1
    assert client.stats().outbound.queued_messages == 1
    assert client._engine.outbound.pending_messages == 2


async def test_publish_nowait_qosn_pending_engine_effect_disables_direct_commit() -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    client._engine._emit(EffectKind.PINGRESP)

    receipt = client.publish_nowait("native/pending-effect", b"x", qos=1)

    assert receipt.mid is not None
    assert client._effect_pump.batches == 1
    assert client._effect_pump.multi_effect_batches == 1
    assert not client._engine.has_pending_effects


async def test_publish_nowait_qosn_pending_pump_disables_direct_handoff(monkeypatch) -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    client._effect_pump.pending.append(EngineEffect(kind=EffectKind.PINGRESP, data=None))
    observed_direct_sinks: list[object] = []
    original = type(client._engine.outbound).queue_publish

    def observe_handoff(outbound, *args, **kwargs):
        observed_direct_sinks.append(kwargs.get("_direct_wires"))
        return original(outbound, *args, **kwargs)

    monkeypatch.setattr(type(client._engine.outbound), "queue_publish", observe_handoff)
    receipt = client.publish_nowait("native/pending-pump", b"x", qos=1)

    assert receipt.mid is not None
    assert observed_direct_sinks == [None]
    assert client.stats().writer.queued_messages == 1
    assert not client._effect_pump.pending


async def test_publish_nowait_qosn_topic_alias_stays_on_engine_effect_path() -> None:
    client = AsyncClient(
        protocol=MQTTProtocolVersion.MQTTv5,
        max_outbound_messages=8,
    )
    client._engine.state = ConnectionState.CONNECTED
    client._engine.negotiated = NegotiatedSettings(topic_alias_maximum=2)

    receipt = client.publish_nowait(
        "canonical/topic",
        b"payload",
        qos=1,
        properties=Properties({"topic_alias": 1}),
    )

    assert receipt.mid is not None
    assert client._effect_pump.batches == 1
    assert not client._engine.has_pending_effects


async def test_publish_nowait_qosn_reentrant_writer_refusal_falls_back_bounded(
    monkeypatch,
) -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    monkeypatch.setattr(client, "_try_enqueue_outbound", lambda *_args, **_kwargs: False)

    receipt = client.publish_nowait("native/reentrant-refusal", b"x", qos=1)

    assert receipt.mid is not None
    assert client.stats().writer.queued_messages == 0
    assert len(client._effect_pump.pending) == 1
    assert client._effect_pump.pending[0].kind is EffectKind.SEND
    assert client._receipts[receipt.mid] is receipt


async def test_publish_nowait_qosn_reentrant_flow_change_keeps_queued_receipt(
    monkeypatch,
) -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    monkeypatch.setattr(type(client._engine.flow), "try_acquire", lambda _self: False)

    receipt = client.publish_nowait("native/reentrant-flow", b"x", qos=1)

    assert receipt.mid is not None
    assert client.stats().outbound.queued_messages == 1
    assert client.stats().writer.queued_messages == 0
    assert client._receipts[receipt.mid] is receipt
    assert not client._engine.has_pending_effects


async def test_publish_nowait_qosn_reentrant_effect_preserves_generic_order(
    monkeypatch,
) -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    original = type(client._engine.outbound).queue_publish

    def queue_after_effect(outbound, *args, **kwargs):
        outbound._engine._emit(EffectKind.PINGRESP)
        return original(outbound, *args, **kwargs)

    monkeypatch.setattr(type(client._engine.outbound), "queue_publish", queue_after_effect)
    receipt = client.publish_nowait("native/reentrant-effect", b"x", qos=1)

    assert receipt.mid is not None
    assert client._effect_pump.batches == 1
    assert client._effect_pump.multi_effect_batches == 1
    assert client.stats().writer.queued_messages == 1
    assert not client._engine.has_pending_effects


async def test_publish_nowait_callback_uses_direct_writer_admission() -> None:
    client = AsyncClient(max_outbound_messages=512)
    client._engine.state = ConnectionState.CONNECTED
    seen: list[int | None] = []
    client.on_publish = lambda mid, _error: seen.append(mid)

    for _ in range(100):
        client.publish_nowait("native/qos0", b"x", qos=0)

    assert client._effect_pump.enqueued == 0
    assert client._effect_flush_task is None
    assert client.stats().writer.queued_messages == 100
    assert seen == []
    await client._callback_queue.join()
    assert seen == [None] * 100
    await client._shutdown_callback_worker(drain=False)


async def test_qos0_callback_marks_writer_admission_not_transport_drain() -> None:
    client = AsyncClient(max_outbound_messages=1, max_outbound_bytes=1024)
    client._engine.state = ConnectionState.CONNECTED
    seen: list[tuple[int | None, BaseException | None]] = []
    client.on_publish = lambda mid, error: seen.append((mid, error))

    receipt = client.publish_nowait("native/qos0-boundary", b"first", qos=0)
    assert client._effect_flush_task is None
    await client._callback_queue.join()

    assert receipt.is_done()
    assert seen == [(None, None)]
    assert client.stats().writer.queued_messages == 1
    queued_bytes = client.stats().writer.queued_bytes

    with pytest.raises(FlowControlError):
        client.publish_nowait("native/qos0-boundary", b"second", qos=0)

    assert client.stats().writer.queued_messages == 1
    assert client.stats().writer.queued_bytes == queued_bytes
    assert not client._engine.has_pending_effects
    assert not client._effect_pump.pending
    await client._shutdown_callback_worker(drain=False)


def test_paho_uses_the_async_client_adapter_boundary() -> None:
    source = inspect.getsource(paho_compat.Client)
    for forbidden in (
        "self._async._engine",
        "self._async._register_publish_receipt",
        "self._async._collect_effects_locked",
        "self._async._drain_effects_inline",
    ):
        assert forbidden not in source


def test_disconnect_metadata_boundary_is_private() -> None:
    assert not hasattr(AsyncClient, "last_disconnect")
    assert hasattr(AsyncClient, "_last_disconnect_info")


def _forbid_direct_path(client: AsyncClient, monkeypatch) -> None:
    """Make any entry into the direct QoS 0 path an explicit failure."""

    def direct_path_forbidden(*_args, **_kwargs):
        raise AssertionError("the direct QoS 0 path must stay disabled here")

    monkeypatch.setattr(type(client._engine.outbound), "prepare_qos0", direct_path_forbidden)


async def test_await_publish_qos0_uses_the_direct_path() -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED

    receipt = await client.publish("native/await-qos0", b"x", qos=0)

    assert receipt.mid is None
    assert client._effect_pump.batches == 0
    assert client._effect_pump.enqueued == 0
    assert not client._engine.has_pending_effects
    assert isinstance(client._outbound.get_nowait(), bytes)


async def test_direct_qos0_path_commits_outbound_alias_after_writer_admission() -> None:
    client = AsyncClient(protocol=MQTTProtocolVersion.MQTTv5, max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    client._engine.negotiated = NegotiatedSettings(topic_alias_maximum=2)
    properties = Properties({"topic_alias": 1})

    await client.publish("canonical/topic", b"seed", properties=properties)
    await client.publish("", b"reuse", properties=properties)

    decoder = IncrementalDecoder()
    decoder.feed(client._outbound.get_nowait())
    decoder.feed(client._outbound.get_nowait())
    publishes = [
        PublishPacket.decode(raw.flags, raw.remaining, MQTTProtocolVersion.MQTTv5)
        for raw in decoder.drain_packets()
    ]
    assert [publish.topic for publish in publishes] == ["canonical/topic", ""]


async def test_refused_direct_qos0_write_does_not_establish_alias() -> None:
    client = AsyncClient(
        protocol=MQTTProtocolVersion.MQTTv5,
        max_outbound_messages=1,
        max_outbound_bytes=1024,
    )
    client._engine.state = ConnectionState.CONNECTED
    client._engine.negotiated = NegotiatedSettings(topic_alias_maximum=2)
    client._outbound.put_nowait(b"occupied")
    client._write_pump.queued_bytes = len(b"occupied")
    client._write_pump._admit_queued()
    properties = Properties({"topic_alias": 1})

    with pytest.raises(FlowControlError):
        await client.publish(
            "canonical/topic",
            b"seed",
            properties=properties,
            nowait=True,
        )

    client._write_pump.discard()
    with pytest.raises(ProtocolError, match="Unknown outbound topic alias"):
        await client.publish("", b"reuse", properties=properties)


async def test_await_publish_qos0_callback_keeps_the_direct_path() -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    seen: list[tuple[int | None, BaseException | None]] = []
    client.on_publish = lambda mid, error: seen.append((mid, error))

    await client.publish("native/await-qos0", b"x", qos=0)

    assert client._effect_pump.batches == 0
    assert seen == []
    await client._callback_queue.join()
    assert seen == [(None, None)]
    await client._shutdown_callback_worker(drain=False)


async def test_publish_many_qos0_uses_the_direct_path() -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED

    receipt = await client.publish_many(
        [PublishMessage("native/batch", b"a", 0), PublishMessage("native/batch", b"b", 0)]
    )

    assert receipt.submitted == 2
    assert receipt.completed == 2
    assert client._effect_pump.batches == 0
    assert not client._engine.has_pending_effects
    assert isinstance(client._outbound.get_nowait(), bytes)
    assert isinstance(client._outbound.get_nowait(), bytes)


async def test_publish_many_callback_keeps_the_direct_path() -> None:
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    seen: list[tuple[int | None, BaseException | None]] = []
    client.on_publish = lambda mid, error: seen.append((mid, error))

    receipt = await client.publish_many(
        [PublishMessage("native/batch", b"a", 0), PublishMessage("native/batch", b"b", 0)]
    )

    assert receipt.submitted == 2
    assert receipt.completed == 2
    assert client._effect_pump.batches == 0
    assert client.stats().writer.queued_messages == 2
    assert seen == []
    await client._callback_queue.join()
    assert seen == [(None, None), (None, None)]
    await client._shutdown_callback_worker(drain=False)


async def test_publish_many_mixed_qos_keeps_the_effect_path(monkeypatch) -> None:
    """One non-QoS-0 request disqualifies the whole batch."""
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    _forbid_direct_path(client, monkeypatch)

    receipt = await client.publish_many(
        [PublishMessage("native/batch", b"a", 0), PublishMessage("native/batch", b"b", 1)]
    )

    assert receipt.submitted == 2
    assert client._effect_pump.batches > 0


async def test_direct_path_is_gated_on_drained_effect_queues() -> None:
    """Both pending-effect gates must independently disable the direct path."""
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED

    assert client._direct_qos0_ready() is True

    client._engine._emit(EffectKind.SUBACK, None)
    assert client._engine.has_pending_effects
    assert client._direct_qos0_ready() is False

    client._engine.take_effects()
    assert client._direct_qos0_ready() is True

    client._effect_pump.pending.append(EngineEffect(kind=EffectKind.SUBACK, data=None))
    assert client._direct_qos0_ready() is False

    client._effect_pump.pending.clear()
    assert client._direct_qos0_ready() is True


async def test_direct_path_requires_capacity_for_every_publish_callback() -> None:
    """A full callback queue sends the entire operation through the effect pump."""
    client = AsyncClient(max_outbound_messages=32, max_pending_callbacks=1)
    client._engine.state = ConnectionState.CONNECTED
    blocker_seen: list[str] = []
    client._callback_queue.put_nowait((lambda: blocker_seen.append("blocker"), (), None))

    seen: list[tuple[int | None, BaseException | None]] = []
    client.on_publish = lambda mid, error: seen.append((mid, error))
    assert client._direct_qos0_ready() is False
    receipt = client.publish_nowait("native/gate", b"x", qos=0)
    assert client._effect_pump.enqueued > 0

    await client._drain_effects()
    await client._callback_queue.join()
    assert receipt.is_done()
    assert blocker_seen == ["blocker"]
    assert seen == [(None, None)]
    await client._shutdown_callback_worker(drain=False)


async def test_direct_path_writer_refusal_does_not_enqueue_a_callback() -> None:
    """Writer admission remains the atomic boundary for callback completion."""
    client = AsyncClient(max_outbound_messages=1, max_pending_callbacks=8)
    client._engine.state = ConnectionState.CONNECTED
    seen: list[tuple[int | None, BaseException | None]] = []
    client.on_publish = lambda mid, error: seen.append((mid, error))

    client.publish_nowait("native/full", b"first", qos=0)
    with pytest.raises(FlowControlError):
        client.publish_nowait("native/full", b"second", qos=0)

    assert client.stats().writer.queued_messages == 1
    assert client._callback_queue.qsize() == 1
    assert not client._engine.has_pending_effects
    assert not client._effect_pump.pending
    await client._callback_queue.join()
    assert seen == [(None, None)]
    await client._shutdown_callback_worker(drain=False)


async def test_publish_many_callback_capacity_falls_back_atomically() -> None:
    """Insufficient callback capacity must not split a batch across paths."""
    client = AsyncClient(max_outbound_messages=8, max_pending_callbacks=2)
    client._engine.state = ConnectionState.CONNECTED
    blocker_seen: list[str] = []
    client._callback_queue.put_nowait((lambda: blocker_seen.append("blocker"), (), None))
    seen: list[tuple[int | None, BaseException | None]] = []
    client.on_publish = lambda mid, error: seen.append((mid, error))

    receipt = await client.publish_many(
        [PublishMessage("native/batch", b"a", 0), PublishMessage("native/batch", b"b", 0)]
    )

    assert client._effect_pump.batches > 0
    await client._drain_effects()
    await client._callback_queue.join()
    assert receipt.submitted == 2
    assert receipt.completed == 2
    assert blocker_seen == ["blocker"]
    assert seen == [(None, None), (None, None)]
    await client._shutdown_callback_worker(drain=False)


async def test_publish_nowait_direct_path_encodes_mqtt5_properties(monkeypatch) -> None:
    properties = Properties()
    properties.set("content_type", "application/json")
    properties.add_user_property("source", "native-fast-path")
    client = AsyncClient(protocol=MQTTProtocolVersion.MQTTv5, max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED
    original_encode = publish_v5_module.encode_publish_item_v5
    encode_calls = 0

    def counted_encode(*args, **kwargs):
        nonlocal encode_calls
        encode_calls += 1
        return original_encode(*args, **kwargs)

    monkeypatch.setattr(publish_v5_module, "encode_publish_item_v5", counted_encode)
    client._engine.outbound._encode_publish = publish_v5_module.encode_publish_item_v5

    receipt = client.publish_nowait(
        "native/mqtt5",
        b'{"value": 42}',
        qos=0,
        properties=properties,
    )

    assert receipt.mid is None
    assert encode_calls == 1
    assert client._effect_pump.batches == 0
    assert client._effect_pump.enqueued == 0
    assert not client._engine.has_pending_effects
    item = client._outbound.get_nowait()
    assert isinstance(item, bytes)
    decoder = IncrementalDecoder()
    decoder.feed(item)
    raw = decoder.next_packet()
    assert raw is not None
    assert raw.packet_type is PacketType.PUBLISH
    packet = PublishPacket.decode(raw.flags, raw.remaining, MQTTProtocolVersion.MQTTv5)
    assert packet.topic == "native/mqtt5"
    assert packet.payload == b'{"value": 42}'
    assert packet.properties == properties


@pytest.mark.parametrize("qos", [3, -1, 99])
async def test_invalid_qos_still_raises_value_error(qos: int) -> None:
    """Comparing before converting must not swallow an invalid level."""
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED

    with pytest.raises(ValueError):
        await client.publish("native/invalid", b"x", qos=qos)
    with pytest.raises(ValueError):
        client.publish_nowait("native/invalid", b"x", qos=qos)


async def test_qos1_rejection_constructs_no_qos_enum(monkeypatch) -> None:
    """QoS 1/2 publishes reach the direct-path gate and must not pay for it."""
    client = AsyncClient(max_outbound_messages=8)
    client._engine.state = ConnectionState.CONNECTED

    calls = 0
    original_new = QoS.__new__

    def counted_new(cls, value):
        nonlocal calls
        calls += 1
        return original_new(cls, value)

    monkeypatch.setattr(QoS, "__new__", counted_new)
    client._try_direct_qos0_publish(
        "native/qos1", b"x", qos=1, retain=False, properties=None, nowait=True
    )

    assert calls == 0


async def test_int_and_enum_qos0_both_take_the_direct_path() -> None:
    for level in (0, QoS.AT_MOST_ONCE):
        client = AsyncClient(max_outbound_messages=8)
        client._engine.state = ConnectionState.CONNECTED
        receipt = client.publish_nowait("native/qos0", b"x", qos=level)
        assert receipt.mid is None
        assert client._effect_pump.enqueued == 0
