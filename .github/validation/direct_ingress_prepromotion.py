#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import resource
import socket
import struct
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from mqttium.api.async_client import AsyncClient as StandardClient
from mqttium.enums import MQTTProtocolVersion
from mqttium.protocol.reconnect import ReconnectPolicy


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_port(port, timeout=5):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.02)
    raise TimeoutError(port)


class Broker:
    def __init__(self, root, port):
        self.port = port
        self.proc = None
        self.conf = Path(root) / "mosquitto.conf"
        self.log = Path(root) / "mosquitto.log"
        self.conf.write_text(
            f"listener {port} 127.0.0.1\nprotocol mqtt\nallow_anonymous true\n"
            "persistence false\nconnection_messages false\nlog_type error\n"
            "max_inflight_messages 1000\nmax_queued_messages 10000\n"
            "max_packet_size 33554432\n"
        )

    def start(self):
        log = self.log.open("ab")
        self.proc = subprocess.Popen(
            ["mosquitto", "-c", str(self.conf)], stdout=log, stderr=subprocess.STDOUT
        )
        wait_port(self.port)

    def stop(self, hard=False):
        if self.proc is None:
            return
        if self.proc.poll() is None:
            self.proc.kill() if hard else self.proc.terminate()
            try:
                self.proc.wait(3)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(3)
        self.proc = None


async def safe_disconnect(client):
    try:
        await client.disconnect()
    except (Exception, asyncio.CancelledError):
        pass


async def wait_until(pred, timeout, label):
    end = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < end:
        if pred():
            return
        await asyncio.sleep(0.02)
    raise TimeoutError(label)


def install_direct():
    from mqttium._direct_decoder_ingress_prototype import install

    return install()


async def qos_matrix(Direct, port):
    out = []
    for proto in (MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5):
        got = []
        ready = asyncio.Event()

        def callback(message):
            assert isinstance(message.payload, bytes)
            got.append(message.payload)
            if len(got) == 3:
                ready.set()

        name = "v5" if proto is MQTTProtocolVersion.MQTTv5 else "v311"
        topic = f"prepromotion/{name}/{os.getpid()}"
        sub = Direct(f"d-{name}", protocol=proto, message_delivery="callback")
        pub = StandardClient(f"p-{name}", protocol=proto, max_outbound_bytes=8 << 20)
        sub.on_message = callback
        try:
            await sub.connect("127.0.0.1", port, timeout=3)
            from mqttium._direct_decoder_ingress_prototype import DirectIngressTcpTransport

            assert isinstance(sub._transport, DirectIngressTcpTransport)
            await sub.subscribe(topic, qos=2, timeout=3)
            await pub.connect("127.0.0.1", port, timeout=3)
            expected = []
            for qos in (0, 1, 2):
                payload = f"{name}-q{qos}".encode()
                expected.append(payload)
                receipt = await pub.publish(topic, payload, qos=qos)
                await asyncio.wait_for(receipt.wait(), 3)
            await asyncio.wait_for(ready.wait(), 3)
            assert set(got) == set(expected)
            stats = sub._transport.receive_stats()
            assert stats["recv_callbacks"] > 0
            out.append({"protocol": name, "stats": stats})
        finally:
            await safe_disconnect(pub)
            await safe_disconnect(sub)
    return out


async def reconnect_case(Direct, broker):
    topic = f"prepromotion/reconnect/{os.getpid()}"
    got = []
    first = asyncio.Event()
    second = asyncio.Event()

    def callback(message):
        got.append(message.payload)
        (first if len(got) == 1 else second).set()

    policy = ReconnectPolicy(
        enabled=True,
        initial_delay=0.05,
        multiplier=1,
        max_delay=0.05,
        max_retries=40,
        stable_after=0.05,
        connect_timeout=1,
    )
    sub = Direct(
        "direct-reconnect",
        reconnect=policy,
        message_delivery="callback",
        maximum_packet_size=4 << 20,
        max_pending_delivery_bytes=16 << 20,
    )
    sub.on_message = callback
    pub = StandardClient("std-reconnect", max_outbound_bytes=8 << 20)
    try:
        await sub.connect("127.0.0.1", broker.port, timeout=3)
        await sub.subscribe(topic, qos=1, timeout=3)
        await pub.connect("127.0.0.1", broker.port, timeout=3)
        receipt = await pub.publish(topic, b"L" * 900_000, qos=1)
        await asyncio.wait_for(receipt.wait(), 3)
        await asyncio.wait_for(first.wait(), 3)
        decoder = sub._direct_ingress_decoder
        assert decoder is not None
        capacity = decoder.capacity
        assert capacity >= 1 << 20

        broker.stop(hard=True)
        await wait_until(lambda: not sub.is_connected, 3, "loss not observed")
        await safe_disconnect(pub)
        await asyncio.sleep(0.12)
        broker.start()
        await wait_until(lambda: sub.is_connected, 5, "reconnect failed")
        assert sub._direct_ingress_decoder is decoder
        assert decoder.capacity == capacity

        await sub.subscribe(topic, qos=1, timeout=3)
        pub = StandardClient("std-reconnect-2")
        await pub.connect("127.0.0.1", broker.port, timeout=3)
        receipt = await pub.publish(topic, b"after", qos=1)
        await asyncio.wait_for(receipt.wait(), 3)
        await asyncio.wait_for(second.wait(), 3)
        return {"capacity": capacity, "decoder_reused": True, "received": [len(x) for x in got]}
    finally:
        await safe_disconnect(pub)
        await safe_disconnect(sub)


def connack(proto):
    return b"\x20\x03\x00\x00\x00" if proto is MQTTProtocolVersion.MQTTv5 else b"\x20\x02\x00\x00"


def one_publish(proto):
    return b"\x30\x05\x00\x01a\x00x" if proto is MQTTProtocolVersion.MQTTv5 else b"\x30\x04\x00\x01ax"


async def eof_case(Direct, proto):
    event = asyncio.Event()
    got = []

    def callback(message):
        got.append(message.payload)
        event.set()

    async def server(reader, writer):
        await reader.read(4096)
        writer.write(connack(proto))
        await writer.drain()
        await asyncio.sleep(0.03)
        writer.write(one_publish(proto))
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    srv = await asyncio.start_server(server, "127.0.0.1", 0)
    port = srv.sockets[0].getsockname()[1]
    client = Direct(f"eof-{port}", protocol=proto, message_delivery="callback")
    client.on_message = callback
    try:
        await client.connect("127.0.0.1", port, timeout=2)
        await asyncio.wait_for(event.wait(), 2)
        assert got == [b"x"]
        return proto.name
    finally:
        await safe_disconnect(client)
        srv.close()
        await srv.wait_closed()


async def reset_case(Direct):
    event = asyncio.Event()
    errors = []
    got = []

    def disconnected(error):
        errors.append(type(error).__name__ if error else None)
        event.set()

    async def server(reader, writer):
        await reader.read(4096)
        writer.write(connack(MQTTProtocolVersion.MQTTv311))
        await writer.drain()
        await asyncio.sleep(0.03)
        writer.write(b"\x30\x7f\x00\x01a" + b"x" * 20)
        await writer.drain()
        raw = writer.get_extra_info("socket")
        if raw is not None:
            raw.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        writer.transport.abort()

    srv = await asyncio.start_server(server, "127.0.0.1", 0)
    port = srv.sockets[0].getsockname()[1]
    client = Direct(f"rst-{port}", message_delivery="callback")
    client.on_disconnect = disconnected
    client.on_message = lambda message: got.append(message.payload)
    try:
        await client.connect("127.0.0.1", port, timeout=2)
        await asyncio.wait_for(event.wait(), 2)
        assert not got and errors[-1] is not None
        return errors[-1]
    finally:
        await safe_disconnect(client)
        srv.close()
        await srv.wait_closed()


async def slow_case(Direct, port):
    gate = asyncio.Event()
    started = asyncio.Event()

    async def callback(message):
        started.set()
        await gate.wait()

    topic = f"prepromotion/slow/{os.getpid()}"
    sub = Direct(
        "direct-slow",
        message_delivery="callback",
        maximum_packet_size=4 << 20,
        max_pending_callbacks=64,
        max_pending_delivery_bytes=2 << 20,
    )
    pub = StandardClient("std-slow", max_outbound_bytes=16 << 20)
    sub.on_message = callback
    try:
        await sub.connect("127.0.0.1", port, timeout=3)
        await sub.subscribe(topic, qos=0, timeout=3)
        await pub.connect("127.0.0.1", port, timeout=3)
        big = b"B" * 1_500_000
        await pub.publish(topic, big, qos=0)
        await asyncio.wait_for(started.wait(), 3)
        for _ in range(16):
            await pub.publish(topic, b"s" * 64_000, qos=0)
        await asyncio.sleep(0.25)
        stats = sub._transport.receive_stats()
        assert stats["pause_count"] >= 1
        assert stats["decoder_high_water"] <= len(big) + (512 << 10)
        assert stats["decoder_capacity"] <= 4 << 20
        return stats
    finally:
        gate.set()
        await safe_disconnect(pub)
        await safe_disconnect(sub)


async def run_e2e(output):
    Direct = install_direct()
    with tempfile.TemporaryDirectory() as tmp:
        broker = Broker(tmp, free_port())
        broker.start()
        try:
            data = {
                "python": sys.version,
                "implementation": sys.implementation.name,
                "qos": await qos_matrix(Direct, broker.port),
                "reconnect": await reconnect_case(Direct, broker),
                "slow": await slow_case(Direct, broker.port),
                "eof": [
                    await eof_case(Direct, MQTTProtocolVersion.MQTTv311),
                    await eof_case(Direct, MQTTProtocolVersion.MQTTv5),
                ],
                "reset_error": await reset_case(Direct),
            }
        finally:
            broker.stop()
    Path(output).write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    print(json.dumps(data, indent=2, sort_keys=True))


def rss_pss():
    try:
        text = Path("/proc/self/smaps_rollup").read_text()
    except OSError:
        return None, None
    vals = {}
    for line in text.splitlines():
        if line.startswith(("Rss:", "Pss:")):
            k, v, _ = line.split()
            vals[k[:-1]] = int(v)
    return vals.get("Rss"), vals.get("Pss")


def commit(decoder, data):
    view = decoder.writable_buffer()
    try:
        assert len(data) <= len(view)
        view[: len(data)] = data
    finally:
        view.release()
    decoder.commit_written(len(data))


def memory_case(case):
    from mqttium._direct_decoder_ingress_prototype import DirectIngressDecoder
    from mqttium.codec.vbi import encode_vbi

    before = rss_pss()
    maxrss0 = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if case == "small":
        dec = DirectIngressDecoder(4 << 20)
        for _ in range(2000):
            commit(dec, b"\xd0\x00")
            assert dec.next_packet() is not None
    elif case == "mixed":
        dec = DirectIngressDecoder(4 << 20)
        for n in (0, 64_000, 0, 180_000, 0, 64_000, 0):
            wire = b"\xd0\x00" if n == 0 else b"\x30" + encode_vbi(n) + b"x" * n
            for off in range(0, len(wire), 128 << 10):
                commit(dec, wire[off : off + (128 << 10)])
                while dec.next_packet() is not None:
                    pass
    else:
        chunk = int(case.split("-")[-1]) << 10
        limit = 8 << 20
        remaining = limit - 5
        dec = DirectIngressDecoder(limit)
        commit(dec, b"\x30" + encode_vbi(remaining))
        left = remaining
        while left:
            n = min(chunk, left)
            commit(dec, b"x" * n)
            left -= n
            if left:
                assert dec.peek_packet_bounds() is None
        bounds = dec.peek_packet_bounds()
        assert bounds is not None
        dec.consume_peeked_packet(bounds[2])
    after = rss_pss()
    return {
        "case": case,
        "high_water": dec.high_water,
        "capacity": dec.capacity,
        "retained_after_drain": dec.capacity,
        "growth_count": dec.growth_count,
        "compaction_count": dec.compaction_count,
        "capacity_over_high_water": dec.capacity / max(1, dec.high_water),
        "rss_before_after_kib": before[0:1] + after[0:1],
        "pss_before_after_kib": before[1:2] + after[1:2],
        "ru_maxrss_before_after_kib": [
            maxrss0,
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        ],
    }


def run_memory(output):
    cases = ["small", "mixed", "near-64", "near-127", "near-192", "near-255"]
    rows = []
    for case in cases:
        p = subprocess.run(
            [sys.executable, __file__, "memory-child", case],
            text=True,
            capture_output=True,
            check=True,
        )
        rows.append(json.loads(p.stdout))
    data = {"python": sys.version, "implementation": sys.implementation.name, "cases": rows}
    Path(output).write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    print(json.dumps(data, indent=2, sort_keys=True))


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("e2e", "memory"):
        q = sub.add_parser(name)
        q.add_argument("--output", required=True)
    q = sub.add_parser("memory-child")
    q.add_argument("case")
    args = p.parse_args()
    if args.cmd == "e2e":
        asyncio.run(run_e2e(args.output))
    elif args.cmd == "memory":
        run_memory(args.output)
    else:
        print(json.dumps(memory_case(args.case), sort_keys=True))


if __name__ == "__main__":
    main()
