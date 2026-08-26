"""Isolated scenario registry used by :mod:`paired_regression`."""

from __future__ import annotations

import asyncio
import shutil
import tempfile
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


TOPIC = "bench/sensors/temp"
PAYLOAD = b'{"t":21.5,"h":40}'


@dataclass
class ScenarioMeasurement:
    elapsed_s: float
    operations: int
    ops_per_s: float


def _measure(fn: Callable[[], object], *, operations: int, warmup: int) -> ScenarioMeasurement:
    for _ in range(warmup):
        fn()
    started = time.perf_counter()
    for _ in range(operations):
        fn()
    elapsed = time.perf_counter() - started
    return ScenarioMeasurement(elapsed, operations, operations / elapsed)


class _DiscardQueue:
    __slots__ = ()

    def qsize(self) -> int:
        return 0

    def empty(self) -> bool:
        return True

    def put_nowait(self, item: object) -> None:
        return None


def _install_discard_writer(client: Any) -> None:
    queue = _DiscardQueue()
    pump = getattr(client, "_write_pump", None)
    if pump is None:
        client._outbound = queue
        client._max_outbound_messages = 1 << 30
        client._max_outbound_bytes = 1 << 60
    else:
        pump.queue = queue
        pump.max_messages = 1 << 30
        pump.max_bytes = 1 << 60


def _encode(scenario: str) -> ScenarioMeasurement:
    from mqttium.enums import MQTTProtocolVersion, QoS
    from mqttium.packets import PublishPacket

    qos = QoS.AT_MOST_ONCE if "qos0" in scenario else QoS.AT_LEAST_ONCE
    protocol = (
        MQTTProtocolVersion.MQTTv5 if scenario.endswith("_v5") else MQTTProtocolVersion.MQTTv311
    )
    packet = PublishPacket(
        topic=TOPIC,
        payload=PAYLOAD,
        qos=qos,
        retain=False,
        dup=False,
        mid=None if qos == QoS.AT_MOST_ONCE else 42,
    )
    return _measure(lambda: packet.encode(protocol), operations=1_000_000, warmup=10_000)


def _ingress_qos0(scenario: str) -> ScenarioMeasurement:
    from mqttium.codec.buffer import IncrementalDecoder
    from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
    from mqttium.packets import PublishPacket
    from mqttium.protocol.engine import EngineConfig, ProtocolEngine

    protocol = (
        MQTTProtocolVersion.MQTTv5 if scenario.endswith("_v5") else MQTTProtocolVersion.MQTTv311
    )
    packet = PublishPacket(
        topic=TOPIC, payload=PAYLOAD, qos=QoS.AT_MOST_ONCE, retain=False, dup=False
    )
    decoder = IncrementalDecoder()
    decoder.feed(packet.encode(protocol))
    raw = decoder.drain_packets()[0]
    engine = ProtocolEngine(EngineConfig(client_id="paired-ingress", protocol=protocol))
    engine.state = ConnectionState.CONNECTED

    def run() -> None:
        engine.handle_raw(raw)
        engine.take_effects()

    return _measure(run, operations=80_000, warmup=2_000)


def _persistence_cycle(scenario: str) -> ScenarioMeasurement:
    from mqttium.codec.buffer import IncrementalDecoder
    from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
    from mqttium.packets import PubAckPacket, PubCompPacket, PubRecPacket, encode_frame
    from mqttium.persistence.memory import MemoryInflightStore
    from mqttium.persistence.sqlite import SqliteInflightStore
    from mqttium.protocol.engine import EngineConfig, ProtocolEngine

    directory = tempfile.mkdtemp(prefix="mqttium-paired-")
    store = (
        MemoryInflightStore()
        if scenario == "qos1_cycle_memory"
        else SqliteInflightStore(Path(directory) / "inflight.db")
    )
    qos = QoS.EXACTLY_ONCE if scenario == "qos2_cycle_sqlite" else QoS.AT_LEAST_ONCE
    engine = ProtocolEngine(
        EngineConfig(
            client_id="paired-cycle",
            protocol=MQTTProtocolVersion.MQTTv311,
            max_pending_outbound_messages=None,
            max_pending_outbound_bytes=None,
        ),
        store=store,
    )
    engine.begin_connect()
    decoder = IncrementalDecoder()
    decoder.feed(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
    engine.handle_raw(decoder.drain_packets()[0])
    engine.take_effects()

    def feed(wire: bytes) -> None:
        local = IncrementalDecoder()
        local.feed(wire)
        engine.handle_raw(local.drain_packets()[0])

    def qos1_cycle() -> None:
        handle = engine.queue_publish(TOPIC, b"z" * 4096, qos=1)
        engine.take_effects()
        feed(PubAckPacket(mid=handle.mid or 0).encode())
        engine.take_effects()

    def qos2_cycle() -> None:
        handle = engine.queue_publish(TOPIC, b"z" * 4096, qos=2)
        engine.take_effects()
        mid = handle.mid or 0
        feed(PubRecPacket(mid=mid).encode())
        engine.take_effects()
        feed(PubCompPacket(mid=mid).encode())
        engine.take_effects()

    try:
        operation = qos1_cycle if qos == QoS.AT_LEAST_ONCE else qos2_cycle
        operations = 50_000 if scenario == "qos1_cycle_memory" else 5_000
        return _measure(operation, operations=operations, warmup=500)
    finally:
        close = getattr(store, "close", None)
        if close is not None:
            close()
        shutil.rmtree(directory, ignore_errors=True)


def _mqtt5_puback_reason_cycle(_scenario: str) -> ScenarioMeasurement:
    from mqttium.codec.buffer import RawPacket
    from mqttium.codec.primitives import pack_u16
    from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType
    from mqttium.protocol.engine import EngineConfig, ProtocolEngine

    engine = ProtocolEngine(
        EngineConfig(
            client_id="paired-mqtt5-puback",
            protocol=MQTTProtocolVersion.MQTTv5,
            max_pending_outbound_messages=None,
            max_pending_outbound_bytes=None,
        )
    )
    engine.state = ConnectionState.CONNECTED

    def run() -> None:
        handle = engine.queue_publish(TOPIC, b"x", qos=1)
        engine.take_effects()
        mid = handle.mid
        if mid is None:
            raise RuntimeError("QoS 1 publish returned no MID")
        engine.handle_raw(RawPacket(PacketType.PUBACK, 0, pack_u16(mid) + b"\x10"))
        engine.take_effects()

    return _measure(run, operations=60_000, warmup=2_000)


def _ingress_qos1(_scenario: str) -> ScenarioMeasurement:
    from mqttium.codec.buffer import IncrementalDecoder
    from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
    from mqttium.packets import PublishPacket, encode_frame
    from mqttium.persistence.memory import MemoryInflightStore
    from mqttium.protocol.engine import EngineConfig, ProtocolEngine

    engine = ProtocolEngine(
        EngineConfig(client_id="paired-ingress-qos1", protocol=MQTTProtocolVersion.MQTTv311),
        store=MemoryInflightStore(),
    )
    engine.begin_connect()
    decoder = IncrementalDecoder()
    decoder.feed(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
    engine.handle_raw(decoder.drain_packets()[0])
    engine.take_effects()
    inbound = IncrementalDecoder()
    inbound.feed(
        PublishPacket(
            topic=TOPIC,
            payload=PAYLOAD,
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            dup=False,
            mid=17,
        ).encode(MQTTProtocolVersion.MQTTv311)
    )
    raw = inbound.drain_packets()[0]

    def run() -> None:
        engine.handle_raw(raw)
        engine.take_effects()

    return _measure(run, operations=60_000, warmup=2_000)


def _writer(scenario: str) -> ScenarioMeasurement:
    from mqttium.api.async_client import AsyncClient

    client = AsyncClient(client_id="paired-writer")
    _install_discard_writer(client)
    if scenario == "writer_try_enqueue":

        def enqueue() -> None:
            if not client._try_enqueue_outbound(b"x"):
                raise RuntimeError("writer try-enqueue unexpectedly refused")

        return _measure(enqueue, operations=160_000, warmup=3_000)

    async def enqueue_async() -> ScenarioMeasurement:
        warmup = 2_000
        operations = 60_000
        started = 0.0
        for index in range(warmup + operations):
            if index == warmup:
                started = time.perf_counter()
            await client._enqueue_outbound(b"x")
        elapsed = time.perf_counter() - started
        return ScenarioMeasurement(elapsed, operations, operations / elapsed)

    return asyncio.run(enqueue_async())


def _native_publish(scenario: str) -> ScenarioMeasurement:
    from mqttium.api.async_client import AsyncClient
    from mqttium.enums import ConnectionState

    callback = scenario == "native_publish_nowait_qos0_callback"
    client = AsyncClient(
        client_id="paired-native-publish",
        max_pending_callbacks=4_096,
    )
    _install_discard_writer(client)
    client._engine.state = ConnectionState.CONNECTED
    if callback:
        client.on_publish = lambda _mid, _reason: None

    async def run() -> ScenarioMeasurement:
        if callback:
            batch_size = 64
            warmup_batches = 32
            measured_batches = 2_000
            started = 0.0
            for batch in range(warmup_batches + measured_batches):
                if batch == warmup_batches:
                    started = time.perf_counter()
                for _ in range(batch_size):
                    client.publish_nowait(TOPIC, b"x", qos=0)
                await client._drain_effects()
                await client._callback_queue.join()
            elapsed = time.perf_counter() - started
            await client._shutdown_callback_worker(drain=False)
            operations = measured_batches * batch_size
            return ScenarioMeasurement(elapsed, operations, operations / elapsed)

        warmup = 2_000
        operations = 60_000
        started = 0.0
        for index in range(warmup + operations):
            if index == warmup:
                started = time.perf_counter()
            if scenario == "native_publish_nowait_qos0" and hasattr(client, "publish_nowait"):
                client.publish_nowait(TOPIC, b"x", qos=0)
            else:
                await client.publish(TOPIC, b"x", qos=0, nowait=True)
        elapsed = time.perf_counter() - started
        return ScenarioMeasurement(elapsed, operations, operations / elapsed)

    return asyncio.run(run())


def _native_publish_fixed_costs(_scenario: str) -> ScenarioMeasurement:
    """Measure the synchronous wrapper without protocol or writer work."""
    from mqttium.api.async_client import AsyncClient
    from mqttium.api.models import PublishReceipt
    from mqttium.enums import ConnectionState, QoS

    client = AsyncClient(client_id="paired-native-fixed-costs")
    client._engine.state = ConnectionState.CONNECTED

    async def run() -> ScenarioMeasurement:
        client._owner_loop = asyncio.get_running_loop()
        client._try_direct_qos0_publish = lambda *_args, **_kwargs: None
        client._queue_publish_on_loop = lambda *_args, **_kwargs: PublishReceipt(
            mid=1, qos=QoS.AT_LEAST_ONCE
        )
        client._finalize_loop_commands = lambda: None
        return _measure(
            lambda: client.publish_nowait(TOPIC, b"x", qos=1),
            operations=500_000,
            warmup=10_000,
        )

    return asyncio.run(run())


def _nowait_empty_writer_capacity(_scenario: str) -> ScenarioMeasurement:
    """Isolate the no-preview capacity check used by the common idle writer."""
    import inspect

    from mqttium.api.async_client import AsyncClient
    from mqttium.enums import ConnectionState

    client = AsyncClient(client_id="paired-nowait-capacity")
    client._engine.state = ConnectionState.CONNECTED
    check = client._check_nowait_publish_capacity
    # The candidate removes the unused retain argument. Select the source
    # signature before timing so one benchmark works against both revisions.
    if len(inspect.signature(check).parameters) == 5:

        def run_check() -> None:
            check(TOPIC, b"x", 1, False, None)

    else:

        def run_check() -> None:
            check(TOPIC, b"x", 1, None)

    return _measure(run_check, operations=1_000_000, warmup=20_000)


def _compat_qos1(_scenario: str) -> ScenarioMeasurement:
    from mqttium.compat.paho import CallbackAPIVersion, Client
    from mqttium.enums import ConnectionState

    client = Client(
        CallbackAPIVersion.VERSION2,
        client_id="paired-compat-qos1",
        max_pending_outbound_messages=None,
        max_pending_outbound_bytes=None,
    )
    client.loop_start()
    try:
        client._run_loop_mutation(
            lambda: setattr(client._async._engine, "state", ConnectionState.CONNECTED)
        )

        def publish() -> None:
            if client.publish(TOPIC, b"x", qos=1).mid is None:
                raise RuntimeError("QoS 1 publish returned no MID")

        return _measure(publish, operations=2_000, warmup=200)
    finally:
        client.loop_stop()


def _compat_qos0(_scenario: str) -> ScenarioMeasurement:
    from mqttium.compat.paho import CallbackAPIVersion, Client
    from mqttium.enums import ConnectionState

    client = Client(
        CallbackAPIVersion.VERSION2,
        client_id="paired-compat-qos0",
        max_pending_outbound_messages=None,
        max_pending_outbound_bytes=None,
    )
    client._async._max_outbound_messages = 100_000
    client._async._max_outbound_bytes = 8 * 1024 * 1024
    client.loop_start()
    client._run_loop_mutation(
        lambda: setattr(client._async._engine, "state", ConnectionState.CONNECTED)
    )

    def submit_and_drain(count: int) -> float:
        started = time.perf_counter()
        for _ in range(count):
            client.publish(TOPIC, b"x", qos=0)
        drained = threading.Event()

        def fence() -> None:
            with client._publish_schedule_lock:
                idle = (
                    not client._publish_drain_scheduled
                    and client._publish_spillover is None
                    and client._publish_pending.empty()
                )
            if idle:
                drained.set()
            else:
                assert client._loop is not None
                client._loop.call_soon(fence)

        assert client._loop is not None
        client._loop.call_soon_threadsafe(fence)
        if not drained.wait(timeout=10):
            raise RuntimeError("QoS 0 compatibility batch did not drain")
        return time.perf_counter() - started

    try:
        submit_and_drain(1_000)
        operations = 20_000
        elapsed = submit_and_drain(operations)
        return ScenarioMeasurement(elapsed, operations, operations / elapsed)
    finally:
        client.loop_stop()


def _effects(scenario: str) -> ScenarioMeasurement:
    from mqttium.api.async_client import AsyncClient
    from mqttium.enums import ConnectionState
    from mqttium.protocol.effects import EffectKind

    client = AsyncClient(client_id="paired-effects")
    _install_discard_writer(client)
    client._engine.state = ConnectionState.CONNECTED
    if scenario in ("effect_send_inline", "effect_batch_inline"):
        batch_size = 1 if scenario == "effect_send_inline" else 8

        def collect() -> None:
            for _ in range(batch_size):
                client._engine._emit(EffectKind.SEND, b"x")
            client._collect_effects_locked()

        return _measure(collect, operations=100_000 if batch_size == 1 else 30_000, warmup=2_000)

    reordered = scenario == "effect_batch_reordered"

    def collect_ordered() -> None:
        if reordered:
            for _ in range(4):
                client._engine._emit(EffectKind.PUBLISH_COMPLETE, None)
                client._engine._emit(EffectKind.SEND, b"x")
        else:
            for _ in range(4):
                client._engine._emit(EffectKind.SEND, b"x")
            for _ in range(4):
                client._engine._emit(EffectKind.PUBLISH_COMPLETE, None)
        client._collect_effects_locked()
        client._effect_pump.pending.clear()

    return _measure(collect_ordered, operations=30_000, warmup=2_000)


def _delivery(scenario: str) -> ScenarioMeasurement:
    from mqttium.api import AsyncClient
    from mqttium.protocol.effects import EffectKind, EngineEffect
    from mqttium.types import Message

    mode: Any = scenario.removeprefix("delivery_")
    client = AsyncClient(
        message_delivery=mode,
        max_pending_messages=100_000,
        max_pending_callbacks=100_000,
        # Delivery dispatch is measured independently from byte-accounting;
        # bounded-memory costs have dedicated scenarios and profiles.
        max_pending_delivery_bytes=None,
    )
    if mode in ("callback", "both"):
        client.on_message = lambda _message: None
    effect = EngineEffect(EffectKind.MESSAGE, Message(topic=TOPIC, payload=b"x"))

    async def run() -> ScenarioMeasurement:
        warmup = 1_000
        operations = 30_000
        started = 0.0
        for index in range(warmup + operations):
            if index == warmup:
                started = time.perf_counter()
            await client._apply_effect(effect, nowait=False)
            if mode in ("iterator", "both"):
                client._messages.get_nowait()
        if mode in ("callback", "both"):
            await client._callback_queue.join()
        elapsed = time.perf_counter() - started
        await client._shutdown_callback_worker(drain=False)
        return ScenarioMeasurement(elapsed, operations, operations / elapsed)

    return asyncio.run(run())


def _single_message_effect(_scenario: str) -> ScenarioMeasurement:
    from mqttium.api import AsyncClient
    from mqttium.protocol.effects import EffectKind
    from mqttium.types import Message

    client = AsyncClient(
        message_delivery="callback",
        max_pending_callbacks=4_096,
        max_pending_delivery_bytes=None,
    )
    client.on_message = lambda _message: None
    message = Message(topic=TOPIC, payload=b"x")

    async def run() -> ScenarioMeasurement:
        warmup = 1_000
        operations = 30_000
        started = 0.0
        for index in range(warmup + operations):
            if index == warmup:
                started = time.perf_counter()
            client._engine._emit(
                EffectKind.MESSAGE,
                message,
                requires_delivery_mark=False,
            )
            client._collect_effects_locked()
            await client._drain_effects()
        await client._callback_queue.join()
        elapsed = time.perf_counter() - started
        await client._shutdown_callback_worker(drain=False)
        return ScenarioMeasurement(elapsed, operations, operations / elapsed)

    return asyncio.run(run())


def _websocket_mask(_scenario: str) -> ScenarioMeasurement:
    from mqttium.transport.websocket import _mask_payload

    payload = b"x" * 4096
    return _measure(lambda: _mask_payload(payload, b"abcd"), operations=20_000, warmup=500)


def _receipt(scenario: str) -> ScenarioMeasurement:
    from mqttium.api.models import PublishReceipt
    from mqttium.enums import QoS

    def allocate() -> PublishReceipt:
        return PublishReceipt(mid=1, qos=QoS.AT_LEAST_ONCE)

    operation: Callable[[], object] = allocate
    if scenario == "receipt_settle_unawaited":

        def settle() -> None:
            PublishReceipt(mid=1, qos=QoS.AT_LEAST_ONCE)._settle()

        operation = settle
    return _measure(operation, operations=500_000, warmup=10_000)


def _publish_completion(scenario: str) -> ScenarioMeasurement:
    from mqttium.api import AsyncClient
    from mqttium.api.models import PublishReceipt
    from mqttium.enums import QoS
    from mqttium.protocol.effects import EffectKind

    client = AsyncClient(
        client_id="paired-publish-completion",
        max_pending_callbacks=4_096,
    )
    callback = scenario.endswith("callback")
    if callback:
        client.on_publish = lambda _mid, _reason: None

    def complete() -> None:
        receipt = PublishReceipt(mid=1, qos=QoS.AT_LEAST_ONCE)
        client._register_publish_receipt(1, receipt)
        client._engine._emit(EffectKind.PUBLISH_COMPLETE, 1)
        client._collect_effects_locked()
        if not callback and not receipt.is_done():
            raise RuntimeError("inline receipt completion did not settle")

    if not callback:
        return _measure(complete, operations=100_000, warmup=2_000)

    async def run_callback() -> ScenarioMeasurement:
        batch_size = 64
        warmup_batches = 32
        measured_batches = 2_000
        started = 0.0
        for batch in range(warmup_batches + measured_batches):
            if batch == warmup_batches:
                started = time.perf_counter()
            for _ in range(batch_size):
                complete()
            await client._drain_effects()
            await client._callback_queue.join()
        elapsed = time.perf_counter() - started
        await client._shutdown_callback_worker(drain=False)
        operations = measured_batches * batch_size
        return ScenarioMeasurement(elapsed, operations, operations / elapsed)

    return asyncio.run(run_callback())


def _prime_process_wide_tables() -> None:
    """Build lazily-initialised module tables before anything is profiled.

    `websocket._xor_tables()` allocates a 256x256 XOR table on first use: one
    time per process, ~65 500 profiler-visible calls. Whichever measurement
    touched it first absorbed that cost, which made `websocket_mask_4k` report
    either 8.5 or 11.8 calls/op on identical code. Priming at import time keeps
    it out of every profiled window instead of an arbitrary one.
    """
    from mqttium.transport.websocket import _xor_tables

    _xor_tables()


_prime_process_wide_tables()


REGISTRY: dict[str, Callable[[str], ScenarioMeasurement]] = {
    "encode_qos0": _encode,
    "encode_qos1": _encode,
    "encode_qos0_v5": _encode,
    "encode_qos1_v5": _encode,
    "ingress_engine_qos0": _ingress_qos0,
    "ingress_engine_qos0_v5": _ingress_qos0,
    "qos1_cycle_memory": _persistence_cycle,
    "qos1_cycle_sqlite": _persistence_cycle,
    "qos2_cycle_sqlite": _persistence_cycle,
    "mqtt5_puback_reason_cycle": _mqtt5_puback_reason_cycle,
    "ingress_publish_qos1": _ingress_qos1,
    "writer_try_enqueue": _writer,
    "writer_enqueue_async": _writer,
    "async_publish_nowait_qos0": _native_publish,
    "native_publish_nowait_qos0": _native_publish,
    "native_publish_nowait_qos0_callback": _native_publish,
    "native_publish_nowait_fixed_costs": _native_publish_fixed_costs,
    "nowait_empty_writer_capacity": _nowait_empty_writer_capacity,
    "compat_publish_qos1": _compat_qos1,
    "compat_publish_qos0_batch": _compat_qos0,
    "effect_send_inline": _effects,
    "effect_batch_inline": _effects,
    "effect_batch_ordered": _effects,
    "effect_batch_reordered": _effects,
    "delivery_callback": _delivery,
    "delivery_iterator": _delivery,
    "delivery_both": _delivery,
    "effect_single_message_callback": _single_message_effect,
    "websocket_mask_4k": _websocket_mask,
    "receipt_allocate": _receipt,
    "receipt_settle_unawaited": _receipt,
    "publish_complete_receipt": _publish_completion,
    "publish_complete_callback": _publish_completion,
}


def run_scenario(scenario: str) -> ScenarioMeasurement:
    try:
        runner = REGISTRY[scenario]
    except KeyError as exc:
        raise ValueError(f"unknown scenario: {scenario}") from exc
    return runner(scenario)
