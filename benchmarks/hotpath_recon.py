#!/usr/bin/env python3
"""MQTTium-only hot-path reconnaissance (no runtime optimisation).

Reuses the in-process fake-broker shape from ``paired_qos1_rtt.py`` (#418).
Monkeypatches live instances so product code is unchanged.  Separate
instrumentation modes keep counter/tracing overhead out of the baseline.

This host is not a reference machine. Numbers are internal cost ratios, not
cross-client claims.
"""

from __future__ import annotations

import argparse
import asyncio
import cProfile
import gc
import json
import os
import pstats
import sys
import time
import tracemalloc
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

try:
    import resource
except ImportError:  # pragma: no cover - Windows has no POSIX resource module
    resource = None  # type: ignore[assignment]

import mqttium.protocol.inbound as inbound_mod
from mqttium.api import AsyncClient
from mqttium.api._delivery import ApplicationDelivery
from mqttium.api._writer import WritePump
from mqttium.codec.buffer import IncrementalDecoder, RawPacket
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PublishPacket, encode_frame
from mqttium.packets._publish import decode_qos12_fields_v311, decode_publish_fields_v5
from mqttium.protocol.effects import EffectKind
from mqttium.protocol.engine import EngineConfig, ProtocolEngine

TOPIC_PUB = "bench/pub"
TOPIC_REQUEST = "bench/request"
TOPIC_REPLY = "bench/reply"
PAYLOAD = b'{"t":21.5,"h":40}'

LOADS: dict[str, dict[str, int]] = {
    "low": {"outstanding": 1, "burst": 1, "count": 4_000, "warmup": 200},
    "medium": {"outstanding": 8, "burst": 8, "count": 12_000, "warmup": 400},
    "high": {"outstanding": 32, "burst": 32, "count": 24_000, "warmup": 800},
}


@dataclass
class PathCounters:
    qos0_direct_hits: int = 0
    qos0_direct_misses: int = 0
    qos0_direct_attempts: int = 0
    qos1_v311_field_decodes: int = 0
    qos1_v5_field_decodes: int = 0
    callback_inline: int = 0
    callback_worker_jobs: int = 0
    eager_data_hits: int = 0
    eager_data_misses: int = 0
    eager_ack_hits: int = 0
    eager_ack_misses: int = 0
    writer_queue_puts: int = 0
    call_soon: int = 0
    tasks_created: int = 0
    effect_collect_calls: int = 0
    effect_collect_single_inline: int = 0
    effect_multi_batches: int = 0
    effect_reordered_batches: int = 0
    engine_effects_taken: int = 0
    send_ack_effects: int = 0
    message_effects: int = 0
    decoder_next_packet: int = 0
    decoder_borrowed_qos0: int = 0
    eager_rearms: int = 0


def _ratio(hits: int, total: int) -> float | None:
    if total <= 0:
        return None
    return hits / total


class InProcessBroker:
    """Packet-aware in-process transport. Optional coalesced reads mimic TCP."""

    def __init__(
        self,
        *,
        protocol: MQTTProtocolVersion,
        coalesce_reads: bool = False,
        auto_puback: bool = True,
    ) -> None:
        self._decoder = IncrementalDecoder()
        self._rx: asyncio.Queue[bytes] = asyncio.Queue()
        self._closed = False
        self.protocol = protocol
        self.coalesce_reads = coalesce_reads
        self.auto_puback = auto_puback
        self.writes = 0
        self.write_bytes = 0
        self.eager_writes = 0
        self.queued_writes = 0
        self.pubacks_sent = 0
        self.publishes_seen = 0

    def _feed_client_bytes(self, data: bytes, path: str) -> None:
        self.writes += 1
        self.write_bytes += len(data)
        if path == "eager":
            self.eager_writes += 1
        else:
            self.queued_writes += 1
        self._decoder.feed(data)
        for raw in self._decoder.drain_packets():
            packet_type = raw.packet_type
            if packet_type is PacketType.CONNECT:
                remaining = (
                    b"\x00\x00\x00" if self.protocol is MQTTProtocolVersion.MQTTv5 else b"\x00\x00"
                )
                self._rx.put_nowait(encode_frame(PacketType.CONNACK, 0, remaining))
                continue
            if packet_type is PacketType.PUBLISH:
                self.publishes_seen += 1
                if not self.auto_puback:
                    continue
                flags = raw.flags
                qos = (flags >> 1) & 0x03
                if qos == 0:
                    continue
                remaining = raw.remaining
                topic_len = (remaining[0] << 8) | remaining[1]
                mid_pos = 2 + topic_len
                if qos:
                    mid = (remaining[mid_pos] << 8) | remaining[mid_pos + 1]
                    ack = bytes((0x40, 2, mid >> 8, mid & 0xFF))
                    self.pubacks_sent += 1
                    asyncio.get_running_loop().call_soon(self._rx.put_nowait, ack)

    def write_nowait(self, data: bytes) -> bool:
        self._feed_client_bytes(data, "eager")
        return True

    async def write(self, data: bytes) -> None:
        self._feed_client_bytes(data, "queued")

    async def write_many(self, parts: list[bytes]) -> None:
        for part in parts:
            self._feed_client_bytes(part, "queued")

    async def read(self, _n: int = 65536) -> bytes:
        first = await self._rx.get()
        if not first or not self.coalesce_reads or self._rx.empty():
            return first
        chunks = [first]
        while True:
            try:
                extra = self._rx.get_nowait()
            except asyncio.QueueEmpty:
                break
            chunks.append(extra)
            if not extra:
                break
        return b"".join(chunks)

    async def close(self) -> None:
        self._closed = True
        self._rx.put_nowait(b"")

    def is_closing(self) -> bool:
        return self._closed

    def push_qos1(self, sequence: int, *, protocol: MQTTProtocolVersion) -> None:
        packet = PublishPacket(
            topic=TOPIC_REQUEST,
            payload=sequence.to_bytes(8, "big"),
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            dup=False,
            mid=(sequence % 65535) + 1,
        )
        self._rx.put_nowait(packet.encode(protocol))

    def push_qos1_batch(self, start: int, count: int, *, protocol: MQTTProtocolVersion) -> None:
        parts = []
        for sequence in range(start, start + count):
            packet = PublishPacket(
                topic=TOPIC_REQUEST,
                payload=sequence.to_bytes(8, "big"),
                qos=QoS.AT_LEAST_ONCE,
                retain=False,
                dup=False,
                mid=(sequence % 65535) + 1,
            )
            parts.append(packet.encode(protocol))
        self._rx.put_nowait(b"".join(parts))


class _PatchSet:
    """Bind/restore instance or class attributes for recon counters."""

    def __init__(self) -> None:
        self._originals: list[tuple[Any, str, Any]] = []

    def bind(self, obj: Any, name: str, wrapper: Callable[..., Any]) -> None:
        original = getattr(obj, name)
        setattr(obj, name, wrapper)
        self._originals.append((obj, name, original))

    def restore(self) -> None:
        for obj, name, orig in self._originals:
            setattr(obj, name, orig)


def _wrap_qos0_direct(patches: _PatchSet, client: AsyncClient, counters: PathCounters) -> None:
    orig_direct = client._try_direct_qos0_publish

    def direct_qos0(*args: Any, **kwargs: Any) -> Any:
        counters.qos0_direct_attempts += 1
        result = orig_direct(*args, **kwargs)
        if result is None:
            counters.qos0_direct_misses += 1
        else:
            counters.qos0_direct_hits += 1
        return result

    patches.bind(client, "_try_direct_qos0_publish", direct_qos0)


def _wrap_decode_paths(patches: _PatchSet, counters: PathCounters) -> None:
    orig_v311 = decode_qos12_fields_v311
    orig_v5 = decode_publish_fields_v5

    def v311_fields(raw: RawPacket) -> Any:
        counters.qos1_v311_field_decodes += 1
        return orig_v311(raw)

    def v5_fields(raw: RawPacket, qos: QoS) -> Any:
        counters.qos1_v5_field_decodes += 1
        return orig_v5(raw, qos)

    patches.bind(inbound_mod, "decode_qos12_fields_v311", v311_fields)
    patches.bind(inbound_mod, "decode_publish_fields_v5", v5_fields)


def _wrap_callbacks(patches: _PatchSet, counters: PathCounters) -> None:
    orig_inline = ApplicationDelivery.dispatch_callback_inline
    orig_enqueue_cb = ApplicationDelivery.try_enqueue_callback
    orig_batch = ApplicationDelivery._enqueue_message_batch

    def inline_cb(self: ApplicationDelivery, callback: Any, *args: Any) -> None:
        counters.callback_inline += 1
        orig_inline(self, callback, *args)

    def enqueue_cb(self: ApplicationDelivery, callback: Any, *args: Any) -> bool:
        result = orig_enqueue_cb(self, callback, *args)
        if result:
            counters.callback_worker_jobs += 1
        return result

    def enqueue_batch(
        self: ApplicationDelivery, callback: Any, messages: Any, **kwargs: Any
    ) -> None:
        counters.callback_worker_jobs += len(messages)
        orig_batch(self, callback, messages, **kwargs)

    patches.bind(ApplicationDelivery, "dispatch_callback_inline", inline_cb)
    patches.bind(ApplicationDelivery, "try_enqueue_callback", enqueue_cb)
    patches.bind(ApplicationDelivery, "_enqueue_message_batch", enqueue_batch)


def _wrap_writer(patches: _PatchSet, client: AsyncClient, counters: PathCounters) -> None:
    orig_data_eager = WritePump._try_write_data_eager
    orig_ack_eager = WritePump._try_write_ack_eager
    orig_rearm = WritePump._schedule_eager_rearm
    orig_try = client._try_enqueue_outbound
    orig_ack = client._try_enqueue_outbound_ack

    def data_eager(self: WritePump, item: Any) -> bool:
        hit = orig_data_eager(self, item)
        if hit:
            counters.eager_data_hits += 1
        return hit

    def ack_eager(self: WritePump, item: bytes) -> bool:
        hit = orig_ack_eager(self, item)
        if hit:
            counters.eager_ack_hits += 1
        return hit

    def try_enq(item: Any, *, epoch: int | None = None) -> bool:
        before = counters.eager_data_hits
        result = orig_try(item, epoch=epoch)
        if result and counters.eager_data_hits == before:
            counters.eager_data_misses += 1
            counters.writer_queue_puts += 1
        return result

    def try_enq_ack(item: bytes, *, epoch: int | None = None) -> bool:
        before = counters.eager_ack_hits
        result = orig_ack(item, epoch=epoch)
        if result and counters.eager_ack_hits == before:
            counters.eager_ack_misses += 1
            counters.writer_queue_puts += 1
        return result

    def schedule_rearm(self: WritePump) -> None:
        counters.eager_rearms += 1
        orig_rearm(self)

    patches.bind(WritePump, "_try_write_data_eager", data_eager)
    patches.bind(WritePump, "_try_write_ack_eager", ack_eager)
    patches.bind(WritePump, "_schedule_eager_rearm", schedule_rearm)
    patches.bind(client, "_try_enqueue_outbound", try_enq)
    patches.bind(client, "_try_enqueue_outbound_ack", try_enq_ack)


def _wrap_effects(patches: _PatchSet, client: AsyncClient, counters: PathCounters) -> None:
    orig_collect = client._collect_effects_locked
    orig_take = client._engine.take_effects

    def collect() -> None:
        pump = client._effect_pump
        counters.effect_collect_calls += 1
        pending_before = len(pump.pending)
        inline_before = pump.inline_effects
        multi_before = pump.multi_effect_batches
        reorder_before = pump.reordered_batches
        orig_collect()
        if pump.inline_effects > inline_before and len(pump.pending) == pending_before:
            counters.effect_collect_single_inline += 1
        counters.effect_multi_batches += pump.multi_effect_batches - multi_before
        counters.effect_reordered_batches += pump.reordered_batches - reorder_before

    def take() -> Any:
        effects = orig_take()
        counters.engine_effects_taken += len(effects)
        for effect in effects:
            if effect.kind is EffectKind.SEND_ACK:
                counters.send_ack_effects += 1
            elif effect.kind in (EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE):
                counters.message_effects += 1
        return effects

    patches.bind(client, "_collect_effects_locked", collect)
    patches.bind(client._engine, "take_effects", take)


def _wrap_decoder(patches: _PatchSet, client: AsyncClient, counters: PathCounters) -> None:
    client_decoder = client._decoder
    orig_next = IncrementalDecoder.next_packet
    orig_direct_batch = client._process_direct_qos0_batch

    def next_packet(self: IncrementalDecoder) -> Any:
        packet = orig_next(self)
        if packet is not None and self is client_decoder:
            counters.decoder_next_packet += 1
        return packet

    def direct_batch() -> Any:
        result = orig_direct_batch()
        counters.decoder_borrowed_qos0 += len(result[3])
        return result

    patches.bind(IncrementalDecoder, "next_packet", next_packet)
    patches.bind(client, "_process_direct_qos0_batch", direct_batch)


def _install_counters(client: AsyncClient, counters: PathCounters) -> Callable[[], None]:
    """Wrap hot-path instance methods. Returns an uninstall callback."""
    patches = _PatchSet()
    loop = asyncio.get_running_loop()
    orig_call_soon = loop.call_soon
    orig_create_task = loop.create_task
    loop_patched = False
    try:
        _wrap_qos0_direct(patches, client, counters)
        _wrap_decode_paths(patches, counters)
        _wrap_callbacks(patches, counters)
        _wrap_writer(patches, client, counters)
        _wrap_effects(patches, client, counters)
        _wrap_decoder(patches, client, counters)

        def call_soon(*args: Any, **kwargs: Any) -> Any:
            counters.call_soon += 1
            return orig_call_soon(*args, **kwargs)

        def create_task(*args: Any, **kwargs: Any) -> Any:
            counters.tasks_created += 1
            return orig_create_task(*args, **kwargs)

        loop.call_soon = call_soon  # type: ignore[method-assign]
        loop.create_task = create_task  # type: ignore[method-assign]
        loop_patched = True
    except BaseException:
        patches.restore()
        if loop_patched:
            loop.call_soon = orig_call_soon  # type: ignore[method-assign]
            loop.create_task = orig_create_task  # type: ignore[method-assign]
        raise

    def uninstall() -> None:
        patches.restore()
        loop.call_soon = orig_call_soon  # type: ignore[method-assign]
        loop.create_task = orig_create_task  # type: ignore[method-assign]

    return uninstall


async def _connect(
    *,
    protocol: MQTTProtocolVersion,
    coalesce: bool,
    auto_puback: bool,
    inflight: int,
) -> tuple[AsyncClient, InProcessBroker]:
    broker = InProcessBroker(protocol=protocol, coalesce_reads=coalesce, auto_puback=auto_puback)

    async def factory(_host: str, _port: int, *, ssl: object = None) -> InProcessBroker:
        del ssl
        return broker

    client = AsyncClient(
        client_id=f"hotpath-recon-{os.getpid()}-{id(broker)}",
        protocol=protocol,
        message_delivery="callback",
        max_outbound_inflight=inflight,
        max_pending_outbound_messages=max(1024, inflight * 8),
    )
    client._transport_factory = factory  # type: ignore[method-assign]
    await client.connect("in-process", 1883, timeout=5.0)
    await asyncio.sleep(0)
    return client, broker


def _cpu_times() -> tuple[float, float]:
    if resource is None:
        return 0.0, 0.0
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime, usage.ru_stime


def _snapshot_stats(client: AsyncClient) -> dict[str, Any]:
    stats = client.stats()
    return {
        "writer_eager_writes": stats.writer.eager_writes,
        "writer_batches": stats.writer.batches,
        "writer_batched_items": stats.writer.batched_items,
        "writer_enqueue_suspensions": stats.writer.enqueue_suspensions,
        "effect_batches": stats.effects.batches,
        "effect_multi_effect_batches": stats.effects.multi_effect_batches,
        "effect_reordered_batches": stats.effects.reordered_batches,
        "effect_inline_effects": stats.effects.inline_effects,
        "effect_enqueued": stats.effects.enqueued,
        "effect_applied": stats.effects.applied,
        "effect_apply_suspensions": stats.effects.apply_suspensions,
    }


def _summarize_counters(counters: PathCounters | None, operations: int) -> dict[str, Any]:
    if counters is None:
        return {}
    data_total = counters.eager_data_hits + counters.eager_data_misses
    ack_total = counters.eager_ack_hits + counters.eager_ack_misses
    qos0_total = counters.qos0_direct_hits + counters.qos0_direct_misses
    cb_total = counters.callback_inline + counters.callback_worker_jobs
    payload = asdict(counters)
    payload.update(
        {
            "eager_data_hit_rate": _ratio(counters.eager_data_hits, data_total),
            "eager_ack_hit_rate": _ratio(counters.eager_ack_hits, ack_total),
            "qos0_direct_hit_rate": _ratio(counters.qos0_direct_hits, qos0_total),
            "callback_inline_rate": _ratio(counters.callback_inline, cb_total),
            "call_soon_per_op": counters.call_soon / operations if operations else None,
            "writer_queue_puts_per_op": (
                counters.writer_queue_puts / operations if operations else None
            ),
            "effects_taken_per_op": (
                counters.engine_effects_taken / operations if operations else None
            ),
            "decoder_next_packet_per_op": (
                counters.decoder_next_packet / operations if operations else None
            ),
        }
    )
    return payload


async def _publish_qos0_burst(client: AsyncClient, burst: int, rounds: int) -> int:
    published = 0
    for _ in range(rounds):
        for _item in range(burst):
            client.publish_nowait(TOPIC_PUB, PAYLOAD, qos=0)
        published += burst
        await asyncio.sleep(0)
    return published


async def _publish_qos1_window(client: AsyncClient, outstanding: int, count: int) -> None:
    inflight: list[Any] = []
    for _sequence in range(count):
        inflight.append(client.publish_nowait(TOPIC_PUB, PAYLOAD, qos=1))
        if len(inflight) >= outstanding:
            await inflight.pop(0).wait()
    while inflight:
        await inflight.pop(0).wait()


async def _run_qos0_publish(
    *,
    protocol: MQTTProtocolVersion,
    burst: int,
    count: int,
    warmup: int,
    instrument: bool,
) -> dict[str, Any]:
    client, broker = await _connect(
        protocol=protocol, coalesce=False, auto_puback=False, inflight=20
    )
    uninstall: Callable[[], None] | None = None
    counters: PathCounters | None = None
    try:
        await _publish_qos0_burst(client, burst, max(1, warmup // max(burst, 1)))
        if instrument:
            counters = PathCounters()
            uninstall = _install_counters(client, counters)
        gc_before = gc.get_count()
        cpu0 = _cpu_times()
        started = time.perf_counter()
        published = await _publish_qos0_burst(client, burst, max(1, count // max(burst, 1)))
        elapsed = time.perf_counter() - started
        cpu1 = _cpu_times()
        gc_after = gc.get_count()
        stats = _snapshot_stats(client)
    finally:
        if uninstall is not None:
            uninstall()
        await client.disconnect()
    cpu_s = (cpu1[0] - cpu0[0]) + (cpu1[1] - cpu0[1])
    return {
        "scenario": "qos0_publish",
        "protocol": protocol.name,
        "operations": published,
        "elapsed_s": elapsed,
        "ops_per_s": published / elapsed if elapsed else 0.0,
        "cpu_us_per_op": (cpu_s * 1e6 / published) if published else None,
        "wall_us_per_op": (elapsed * 1e6 / published) if published else None,
        "gc_count_delta": [a - b for a, b in zip(gc_after, gc_before, strict=True)],
        "broker_writes": broker.writes,
        "broker_eager_writes": broker.eager_writes,
        "broker_queued_writes": broker.queued_writes,
        "client_stats": stats,
        "counters": _summarize_counters(counters, published),
    }


async def _run_qos1_publish(
    *,
    protocol: MQTTProtocolVersion,
    outstanding: int,
    count: int,
    warmup: int,
    instrument: bool,
) -> dict[str, Any]:
    client, broker = await _connect(
        protocol=protocol,
        coalesce=False,
        auto_puback=True,
        inflight=max(outstanding, 20),
    )
    uninstall: Callable[[], None] | None = None
    counters: PathCounters | None = None
    try:
        await _publish_qos1_window(client, outstanding, warmup)
        if instrument:
            counters = PathCounters()
            uninstall = _install_counters(client, counters)
        gc_before = gc.get_count()
        cpu0 = _cpu_times()
        started = time.perf_counter()
        await _publish_qos1_window(client, outstanding, count)
        elapsed = time.perf_counter() - started
        cpu1 = _cpu_times()
        gc_after = gc.get_count()
        stats = _snapshot_stats(client)
        operations = count
    finally:
        if uninstall is not None:
            uninstall()
        await client.disconnect()
    cpu_s = (cpu1[0] - cpu0[0]) + (cpu1[1] - cpu0[1])
    return {
        "scenario": "qos1_publish",
        "protocol": protocol.name,
        "operations": operations,
        "elapsed_s": elapsed,
        "ops_per_s": operations / elapsed if elapsed else 0.0,
        "cpu_us_per_op": (cpu_s * 1e6 / operations) if operations else None,
        "wall_us_per_op": (elapsed * 1e6 / operations) if operations else None,
        "gc_count_delta": [a - b for a, b in zip(gc_after, gc_before, strict=True)],
        "broker_writes": broker.writes,
        "broker_pubacks": broker.pubacks_sent,
        "client_stats": stats,
        "counters": _summarize_counters(counters, operations),
    }


async def _run_qos1_inbound_reply(
    *,
    protocol: MQTTProtocolVersion,
    outstanding: int,
    count: int,
    warmup: int,
    coalesce: bool,
    instrument: bool,
) -> dict[str, Any]:
    client, broker = await _connect(
        protocol=protocol,
        coalesce=coalesce,
        auto_puback=True,
        inflight=max(outstanding, 20),
    )
    done: asyncio.Queue[Any] = asyncio.Queue()

    def on_message(message: Any) -> None:
        receipt = client.publish_nowait(TOPIC_REPLY, message.payload, qos=1)
        done.put_nowait(receipt)

    client.on_message = on_message
    uninstall: Callable[[], None] | None = None
    counters: PathCounters | None = None
    try:
        next_seq = 0

        async def issue(n: int) -> None:
            nonlocal next_seq
            if coalesce and n > 1:
                broker.push_qos1_batch(next_seq, n, protocol=protocol)
                next_seq += n
                return
            for _ in range(n):
                broker.push_qos1(next_seq, protocol=protocol)
                next_seq += 1

        async def collect(n: int) -> None:
            for _ in range(n):
                receipt = await done.get()
                await receipt.wait()

        await issue(warmup)
        await collect(warmup)
        if instrument:
            counters = PathCounters()
            uninstall = _install_counters(client, counters)
        gc_before = gc.get_count()
        cpu0 = _cpu_times()
        started = time.perf_counter()
        remaining = count
        in_flight = 0
        async with asyncio.timeout(max(15.0, count / 50.0)):
            while remaining > 0 or in_flight > 0:
                room = outstanding - in_flight
                if remaining > 0 and room > 0:
                    step = min(room, remaining)
                    await issue(step)
                    remaining -= step
                    in_flight += step
                if in_flight:
                    receipt = await done.get()
                    await receipt.wait()
                    in_flight -= 1
        elapsed = time.perf_counter() - started
        cpu1 = _cpu_times()
        gc_after = gc.get_count()
        stats = _snapshot_stats(client)
        operations = count
    finally:
        if uninstall is not None:
            uninstall()
        await client.disconnect()
    cpu_s = (cpu1[0] - cpu0[0]) + (cpu1[1] - cpu0[1])
    return {
        "scenario": "qos1_inbound_reply_coalesced" if coalesce else "qos1_inbound_reply",
        "protocol": protocol.name,
        "operations": operations,
        "elapsed_s": elapsed,
        "ops_per_s": operations / elapsed if elapsed else 0.0,
        "cpu_us_per_op": (cpu_s * 1e6 / operations) if operations else None,
        "wall_us_per_op": (elapsed * 1e6 / operations) if operations else None,
        "gc_count_delta": [a - b for a, b in zip(gc_after, gc_before, strict=True)],
        "broker_writes": broker.writes,
        "client_stats": stats,
        "counters": _summarize_counters(counters, operations),
    }


def _run_engine_qos1_ingress(*, protocol: MQTTProtocolVersion, count: int) -> dict[str, Any]:
    packet = PublishPacket(
        topic=TOPIC_REQUEST,
        payload=PAYLOAD,
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        dup=False,
        mid=7,
    )
    wire = packet.encode(protocol)
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.drain_packets()[0]
    engine = ProtocolEngine(EngineConfig(client_id="recon-engine", protocol=protocol))
    engine.state = ConnectionState.CONNECTED
    for _ in range(200):
        engine.handle_raw(raw)
        engine.take_effects()

    gc.collect()
    gc_before = gc.get_count()
    alloc0 = sys.getallocatedblocks() if hasattr(sys, "getallocatedblocks") else None
    cpu0 = _cpu_times()
    started = time.perf_counter()
    for _ in range(count):
        engine.handle_raw(raw)
        effects = engine.take_effects()
        if len(effects) != 2:
            raise AssertionError(f"expected SEND_ACK+MESSAGE, got {[e.kind for e in effects]}")
    elapsed = time.perf_counter() - started
    cpu1 = _cpu_times()
    alloc1 = sys.getallocatedblocks() if hasattr(sys, "getallocatedblocks") else None
    gc_after = gc.get_count()
    cpu_s = (cpu1[0] - cpu0[0]) + (cpu1[1] - cpu0[1])
    return {
        "scenario": "engine_qos1_ingress",
        "protocol": protocol.name,
        "operations": count,
        "elapsed_s": elapsed,
        "ops_per_s": count / elapsed if elapsed else 0.0,
        "cpu_us_per_op": (cpu_s * 1e6 / count) if count else None,
        "wall_us_per_op": (elapsed * 1e6 / count) if count else None,
        "allocated_blocks_delta": None if alloc0 is None else alloc1 - alloc0,
        "allocated_blocks_per_op": (
            None if alloc0 is None else (alloc1 - alloc0) / count  # type: ignore[operator]
        ),
        "gc_count_delta": [a - b for a, b in zip(gc_after, gc_before, strict=True)],
        "effects_per_ingress": 2,
    }


def _run_engine_qos1_alloc_trace(*, protocol: MQTTProtocolVersion, count: int) -> dict[str, Any]:
    packet = PublishPacket(
        topic=TOPIC_REQUEST,
        payload=PAYLOAD,
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        dup=False,
        mid=11,
    )
    wire = packet.encode(protocol)
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.drain_packets()[0]
    engine = ProtocolEngine(EngineConfig(client_id="recon-engine-alloc", protocol=protocol))
    engine.state = ConnectionState.CONNECTED
    for _ in range(50):
        engine.handle_raw(raw)
        engine.take_effects()
    tracemalloc.start()
    for _ in range(count):
        engine.handle_raw(raw)
        engine.take_effects()
    current, peak = tracemalloc.get_traced_memory()
    snapshot = tracemalloc.take_snapshot()
    tracemalloc.stop()
    stats = snapshot.statistics("lineno")[:12]
    return {
        "scenario": "engine_qos1_ingress_tracemalloc",
        "protocol": protocol.name,
        "operations": count,
        "current_bytes": current,
        "peak_bytes": peak,
        "current_bytes_per_op": current / count,
        "top_lines": [
            {
                "file": str(item.traceback[0].filename),
                "line": item.traceback[0].lineno,
                "size": item.size,
                "count": item.count,
            }
            for item in stats
        ],
    }


async def _run_cell(
    *,
    scenario: str,
    protocol: MQTTProtocolVersion,
    load: str,
    instrument: bool,
) -> dict[str, Any]:
    spec = LOADS[load]
    if scenario == "qos0_publish":
        result = await _run_qos0_publish(
            protocol=protocol,
            burst=spec["burst"],
            count=spec["count"],
            warmup=spec["warmup"],
            instrument=instrument,
        )
    elif scenario == "qos1_publish":
        result = await _run_qos1_publish(
            protocol=protocol,
            outstanding=spec["outstanding"],
            count=spec["count"],
            warmup=spec["warmup"],
            instrument=instrument,
        )
    elif scenario == "qos1_inbound_reply":
        result = await _run_qos1_inbound_reply(
            protocol=protocol,
            outstanding=spec["outstanding"],
            count=spec["count"],
            warmup=spec["warmup"],
            coalesce=False,
            instrument=instrument,
        )
    elif scenario == "qos1_inbound_reply_coalesced":
        result = await _run_qos1_inbound_reply(
            protocol=protocol,
            outstanding=spec["outstanding"],
            count=spec["count"],
            warmup=spec["warmup"],
            coalesce=True,
            instrument=instrument,
        )
    else:
        raise ValueError(f"unknown scenario {scenario}")
    result["load"] = load
    result["outstanding"] = spec["outstanding"]
    result["instrument"] = instrument
    return result


def _profile_cell(scenario: str, protocol: MQTTProtocolVersion, load: str) -> dict[str, Any]:
    profiler = cProfile.Profile()

    def run() -> dict[str, Any]:
        return asyncio.run(
            _run_cell(scenario=scenario, protocol=protocol, load=load, instrument=False)
        )

    measurement = profiler.runcall(run)
    stats = pstats.Stats(profiler)
    hottest = sorted(
        (
            {
                "file": key[0],
                "line": key[1],
                "function": key[2],
                "primitive_calls": value[0],
                "calls": value[1],
                "total_s": value[2],
                "cumulative_s": value[3],
            }
            for key, value in stats.stats.items()  # type: ignore[attr-defined]
        ),
        key=lambda item: float(item["total_s"]),
        reverse=True,
    )[:15]
    operations = int(measurement["operations"])
    return {
        "scenario": scenario,
        "protocol": protocol.name,
        "load": load,
        "operations": operations,
        "profiled_elapsed_s": measurement["elapsed_s"],
        "ops_per_s": measurement["ops_per_s"],
        "total_calls": stats.total_calls,
        "primitive_calls": stats.prim_calls,
        "calls_per_op": stats.total_calls / operations if operations else None,
        "primitive_calls_per_op": stats.prim_calls / operations if operations else None,
        "top_functions": hottest,
    }


def _host_info() -> dict[str, Any]:
    governor = None
    path = Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
    if path.exists():
        governor = path.read_text(encoding="utf-8").strip()
    return {
        "python": sys.version,
        "platform": sys.platform,
        "cpus": os.cpu_count(),
        "cpu_governor": governor,
        "pid": os.getpid(),
        "note": (
            "This is not a reference host. Governor is typically unreadable in "
            "this environment. Use numbers as internal cost maps, not rankings."
        ),
    }


SCENARIOS = (
    "qos0_publish",
    "qos1_publish",
    "qos1_inbound_reply",
    "qos1_inbound_reply_coalesced",
)

PROTOCOLS = (MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5)


async def _campaign(*, quick: bool, instrument_only: bool) -> dict[str, Any]:
    loads = ("low",) if quick else tuple(LOADS)
    scenarios = SCENARIOS
    cells: list[dict[str, Any]] = []
    for scenario in scenarios:
        for protocol in PROTOCOLS:
            for load in loads:
                if not instrument_only:
                    cells.append(
                        await _run_cell(
                            scenario=scenario,
                            protocol=protocol,
                            load=load,
                            instrument=False,
                        )
                    )
                cells.append(
                    await _run_cell(
                        scenario=scenario,
                        protocol=protocol,
                        load=load,
                        instrument=True,
                    )
                )
    engine_cells = [
        _run_engine_qos1_ingress(protocol=protocol, count=20_000 if not quick else 4_000)
        for protocol in PROTOCOLS
    ]
    engine_alloc = [
        _run_engine_qos1_alloc_trace(protocol=protocol, count=8_000 if not quick else 2_000)
        for protocol in PROTOCOLS
    ]
    return {
        "host": _host_info(),
        "cells": cells,
        "engine_ingress": engine_cells,
        "engine_alloc": engine_alloc,
    }


def _overhead_table(cells: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, dict[str, Any]]] = {}
    for cell in cells:
        key = (cell["scenario"], cell["protocol"], cell["load"])
        grouped.setdefault(key, {})["instrument" if cell["instrument"] else "baseline"] = cell
    rows = []
    for (scenario, protocol, load), pair in grouped.items():
        if "baseline" not in pair or "instrument" not in pair:
            continue
        base = pair["baseline"]["ops_per_s"]
        inst = pair["instrument"]["ops_per_s"]
        rows.append(
            {
                "scenario": scenario,
                "protocol": protocol,
                "load": load,
                "baseline_ops_per_s": base,
                "instrumented_ops_per_s": inst,
                "relative_throughput": None if not base else inst / base,
                "estimated_overhead": None if not base else 1.0 - (inst / base),
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument(
        "--mode",
        choices=("campaign", "cprofile", "instrument-only"),
        default="campaign",
    )
    parser.add_argument("--scenario", choices=SCENARIOS)
    parser.add_argument("--load", choices=tuple(LOADS), default="medium")
    parser.add_argument(
        "--protocol",
        choices=("MQTTv311", "MQTTv5"),
        default="MQTTv311",
    )
    args = parser.parse_args()
    protocol = MQTTProtocolVersion[args.protocol]
    if args.mode == "cprofile":
        scenario = args.scenario or "qos1_inbound_reply"
        payload: dict[str, Any] = {
            "host": _host_info(),
            "profile": _profile_cell(scenario, protocol, args.load),
        }
    else:
        payload = asyncio.run(
            _campaign(quick=args.quick, instrument_only=args.mode == "instrument-only")
        )
        payload["instrumentation_overhead"] = _overhead_table(payload["cells"])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"wrote": str(args.output), "mode": args.mode}, indent=2))


if __name__ == "__main__":
    main()
