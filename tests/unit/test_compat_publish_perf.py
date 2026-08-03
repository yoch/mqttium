"""Regression: compat publish must not await the writer; mid reuse must settle receipts."""

from __future__ import annotations

import threading
import time

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.compat.paho import CallbackAPIVersion, Client
from mqttium.enums import ConnectionState, MQTTProtocolVersion, OutboundQoSState, QoS
from mqttium.packets import PubAckPacket
from mqttium.protocol.engine import EffectKind, ProtocolEngine
from mqttium.types import OutboundMessage


def test_engine_puback_emits_complete_before_mid_reuse() -> None:
    """PUBLISH_COMPLETE must be queued before the packet id returns to the pool."""
    engine = ProtocolEngine()
    engine.state = ConnectionState.CONNECTED
    mid = engine.packet_ids.allocate()
    assert engine.flow.try_acquire()
    engine.store.put_out(
        OutboundMessage(
            mid=mid,
            topic="t",
            payload=b"x",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBACK,
        )
    )
    engine.take_effects()

    decoder = IncrementalDecoder()
    decoder.feed(PubAckPacket(mid=mid).encode())
    for raw in decoder.drain_packets():
        engine.handle_raw(raw)

    effects = engine.take_effects()
    assert any(e.kind is EffectKind.PUBLISH_COMPLETE and e.data == mid for e in effects)
    # Mid must be free again after the handler returns.
    assert engine.packet_ids.allocate() == mid


def test_compat_off_loop_publish_qos0_throughput_floor() -> None:
    """Off-loop QoS0 publish must not wait on the writer (was ~5k/s, target >> that)."""
    client = Client(
        CallbackAPIVersion.VERSION2,
        client_id="perf",
        protocol=MQTTProtocolVersion.MQTTv311,
    )
    connected = threading.Event()
    completed = 0
    lock = threading.Lock()

    def on_connect(*_a, **_k):
        connected.set()

    def on_publish(*_a, **_k):
        nonlocal completed
        with lock:
            completed += 1

    client.on_connect = on_connect
    client.on_publish = on_publish
    client.loop_start()
    try:
        assert client.connect("127.0.0.1", 11883) == 0
        assert connected.wait(5)
        n = 8000
        t0 = time.perf_counter()
        for _ in range(n):
            client.publish("perf/qos0", b"x" * 256, qos=0)
        # Submission rate — must not be gated on TCP writes.
        submit_rate = n / (time.perf_counter() - t0)
        deadline = time.perf_counter() + 10
        while completed < n and time.perf_counter() < deadline:
            time.sleep(0.01)
        assert completed == n
        # Paho submits at ~30–40k/s on this host; the old await-_flush_effects /
        # per-publish loop handoff capped near ~5–7k/s. Require a clear recovery.
        assert submit_rate > 18000, f"submit_rate={submit_rate:.0f} too low"
    finally:
        client.disconnect()
        client.loop_stop()
