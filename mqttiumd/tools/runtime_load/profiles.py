#!/usr/bin/env python3
"""Bounded loopback MQTT load probes for the delivery and reader profiles."""
import argparse
import asyncio
import contextlib
import json
import math
from pathlib import Path
import resource
import socket
import ssl
import statistics
import struct
import subprocess
import sys
import tempfile
import time

from wire import (
    ROOT, append_json, broker, close, connect, expect, mqtt, proc_sample, read_packet, write_json,
)

sys.path.insert(0, str(ROOT / "tools/arm64_bench"))
from run_arm64_bench import make_pki

TOPIC = "atelier/load"


def distribution(values):
    if not values:
        return {"count": 0}
    ordered = sorted(values)
    return {"count": len(values), "min": ordered[0], "p50": statistics.median(ordered),
            "p99": ordered[math.ceil(.99 * len(ordered)) - 1], "max": ordered[-1]}


def tls_context(pki, name):
    if not pki:
        return None
    ctx = ssl.create_default_context(cafile=str(pki / "ca.crt"))
    ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    ctx.load_cert_chain(pki / f"{name}.crt", pki / f"{name}.key")
    return ctx


async def ping(reader, writer):
    start = time.monotonic_ns()
    writer.write(mqtt.pingreq())
    await expect(reader, 0xd0, b"")
    return (time.monotonic_ns() - start) / 1e6


async def bounds(args, port, proc):
    sessions = []
    admission_ms = []
    release_ms = []
    ping_ms = []
    bound = 8
    qos = b"".join(mqtt.publish(TOPIC, b"q", qos=2, packet_id=i) for i in range(1, bound + 1))
    releases = b"".join(mqtt.pubrel(i) for i in range(1, bound + 1))

    async def acquire(index):
        start = time.monotonic_ns()
        read, write = await connect(port, f"bound-{index}")
        write.write(qos)
        observed = [await read_packet(read) for _ in range(bound)]
        if sorted(observed) != [(0x50, i.to_bytes(2, "big")) for i in range(1, bound + 1)]:
            raise ValueError("PUBREC identifiers differ")
        admission_ms.append((time.monotonic_ns() - start) / 1e6)
        return read, write

    async def release(pair):
        read, write = pair
        start = time.monotonic_ns()
        write.write(releases)
        observed = [await read_packet(read) for _ in range(bound)]
        if sorted(observed) != [(0x70, i.to_bytes(2, "big")) for i in range(1, bound + 1)]:
            raise ValueError("PUBCOMP identifiers differ")
        release_ms.append((time.monotonic_ns() - start) / 1e6)

    memory = {"empty": proc_sample(proc.pid)}
    start = time.monotonic()
    try:
        for offset in range(0, args.sessions, 16):
            group = await asyncio.gather(*(acquire(i) for i in
                                         range(offset, min(offset + 16, args.sessions))))
            sessions.extend(group)
            if len(sessions) % 256 == 0:
                append_json(args.out / "progress.jsonl", {"at_bound": len(sessions),
                            "elapsed": time.monotonic() - start, **proc_sample(proc.pid)})
        memory["at_bound"] = proc_sample(proc.pid)
        for _ in range(5):
            ping_ms.append(await ping(*sessions[0]))
        for offset in range(0, args.sessions, 16):
            await asyncio.gather(*(release(s) for s in sessions[offset:offset + 16]))
        memory["released"] = proc_sample(proc.pid)
        r, w = sessions[0]
        w.write(mqtt.publish(TOPIC, b"after", qos=2, packet_id=9))
        await expect(r, 0x50, b"\0\x09")
        w.write(mqtt.pubrel(9))
        await expect(r, 0x70, b"\0\x09")
    finally:
        for offset in range(0, len(sessions), 16):
            await asyncio.gather(*(close(w) for _, w in sessions[offset:offset + 16]),
                                 return_exceptions=True)
    await asyncio.sleep(.5)
    memory["closed"] = proc_sample(proc.pid)
    return {"sessions": len(sessions), "pubrec_held_together": len(sessions) * bound,
            "completed": len(release_ms) * bound, "post_release_publications_completed": 1,
            "session_fill_including_connect_ms": distribution(admission_ms),
            "session_release_ms": distribution(release_ms), "full_table_ping_ms": distribution(ping_ms),
            "resources": memory, "elapsed_seconds": time.monotonic() - start}


async def waiting(args, port, proc):
    # Read progress at a held publication: a normal 3.1.1 pipeline needing PUBREL replies.
    pr, pw = await connect(port, "reader-producer")
    sr, sw = await connect(port, "reader-consumer")
    wr, ww = await connect(port, "reader-witness")
    sw.write(mqtt.subscribe(1, [(TOPIC, 0)]))
    await expect(sr, 0x90, b"\0\1\0")
    depth = args.depth
    completed_ms = []
    unrelated_ms = []
    copies = 0
    measured_start = time.monotonic()
    for turn in range(args.rounds):
        base = turn * depth
        burst = bytearray()
        for i in range(depth):
            burst.extend(mqtt.publish(TOPIC, struct.pack("!I", base + i), qos=2, packet_id=i + 1))
        start = time.monotonic_ns()
        pw.write(burst)

        async def finish():
            receipts, completions = set(), set()
            while len(completions) < depth:
                head, body = await read_packet(pr)
                ident = int.from_bytes(body, "big")
                if not 1 <= ident <= depth or len(body) != 2:
                    raise ValueError("unexpected packet identifier")
                if head == 0x50 and ident not in receipts:
                    receipts.add(ident)
                    pw.write(mqtt.pubrel(ident))
                elif head == 0x70 and ident in receipts and ident not in completions:
                    completions.add(ident)
                    completed_ms.append((time.monotonic_ns() - start) / 1e6)
                else:
                    raise ValueError("unexpected publisher response")

        async def receive():
            nonlocal copies
            for i in range(depth):
                head, body = await read_packet(sr)
                if head != 0x30 or body != mqtt.utf8(TOPIC) + struct.pack("!I", base + i):
                    raise ValueError("subscriber sequence differs")
                copies += 1

        _, _, delay = await asyncio.gather(finish(), receive(), ping(wr, ww))
        unrelated_ms.append(delay)
    measured_seconds = time.monotonic() - measured_start
    write_json(args.out / "timings-ms.json", {"pubcomp_since_burst": completed_ms,
                                             "separate_client_ping": unrelated_ms})
    await asyncio.gather(close(pw), close(sw), close(ww))
    return {"measured_seconds": measured_seconds, "completed_per_second": copies / measured_seconds,
            "pipeline": depth, "rounds": args.rounds, "ordered_unique": copies,
            "pubcomp_since_burst_ms": distribution(completed_ms),
            "separate_client_ping_ms": distribution(unrelated_ms), "process": proc_sample(proc.pid)}


def close_cost(args, port, proc, pki):
    ctx = tls_context(pki, "sensor")
    handshakes, connect_ms, closes, trials = [], [], [], []
    baseline = proc_sample(proc.pid)
    time.sleep(args.idle_seconds)
    idle_before = proc_sample(proc.pid)
    for _ in range(args.rounds):
        with socket.create_connection(("127.0.0.1", port), timeout=4) as tcp:
            tcp.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            start = time.monotonic_ns()
            with contextlib.ExitStack() as stack:
                stream = tcp
                if ctx:
                    stream = stack.enter_context(ctx.wrap_socket(tcp, server_hostname="127.0.0.1",
                                                suppress_ragged_eofs=False))
                    handshakes.append((time.monotonic_ns() - start) / 1e6)
                before_connack = time.monotonic_ns()
                stream.sendall(mqtt.connect("sensor-0", keepalive=0))
                ack = b""
                while len(ack) < 4:
                    piece = stream.recv(4 - len(ack))
                    if not piece:
                        raise ValueError("EOF before CONNACK")
                    ack += piece
                if ack != mqtt.connack():
                    raise ValueError(f"unexpected CONNACK {ack!r}")
                connect_ms.append((time.monotonic_ns() - before_connack) / 1e6)
                disconnect = time.monotonic_ns()
                stream.sendall(mqtt.disconnect())
                if stream.recv(1) != b"":
                    raise ValueError("expected EOF")
                end = time.monotonic_ns()
                closes.append((end - disconnect) / 1e6)
                trials.append((disconnect, end))
    busy = proc_sample(proc.pid)
    time.sleep(args.idle_seconds)
    idle_after = proc_sample(proc.pid)
    events = [json.loads(line) for line in (args.out / "events.jsonl").read_text().splitlines()]
    closed = [e for e in events if e["kind"] == "connection_closed"]
    if len(closed) != len(trials) or any(e["details"]["code"] != "client_disconnect" for e in closed):
        raise ValueError("close events do not match the trials")
    observed = [int(e["details"]["observed_monotonic_ns"]) for e in closed]
    write_json(args.out / "timings-ms.json", {"tls_handshake": handshakes, "mqtt_connect": connect_ms,
                                             "peer_eof": closes, "broker_observed_ns": observed,
                                             "disconnect_and_eof_ns": trials})
    return {"tls": bool(ctx), "trials": len(trials), "tls_handshake_ms": distribution(handshakes),
            "mqtt_connect_ms": distribution(connect_ms), "disconnect_to_peer_eof_ms": distribution(closes),
            "disconnect_to_broker_observation_ms": distribution(
                [(ts - start) / 1e6 for ts, (start, _) in zip(observed, trials, strict=True)]),
            "idle_before_cpu_seconds": idle_before["cpu_seconds"] - baseline["cpu_seconds"],
            "idle_after_cpu_seconds": idle_after["cpu_seconds"] - busy["cpu_seconds"],
            "idle_window_seconds": args.idle_seconds, "idle_before_samples": [baseline, idle_before],
            "idle_after_samples": [busy, idle_after]}


async def qos0(args, port, proc, pki):
    sensor = tls_context(pki, "sensor")
    dashboard = tls_context(pki, "dashboard")
    peers = await asyncio.gather(*(connect(port, f"dashboard-{i}", tls=dashboard)
                                  for i in range(args.fanout)))
    for r, w in peers:
        w.write(mqtt.subscribe(1, [(TOPIC, 0)]))
        await expect(r, 0x90, b"\0\1\0")
    pr, pw = await connect(port, "sensor-0", tls=sensor)
    total = args.duration * args.rate
    padding = b"p" * (args.payload - 12)
    prefix = mqtt.utf8(TOPIC)
    latency = [[] for _ in peers]
    fenced = [False] * args.fanout
    unique = [0] * args.fanout
    window = [0] * args.fanout
    duplicates = [0] * args.fanout
    start = time.monotonic_ns()
    end = start + args.duration * 1_000_000_000
    async def receive(i):
        seen = bytearray(total)
        while True:
            head, body = await read_packet(peers[i][0])
            now = time.monotonic_ns()
            if head != 0x30 or len(body) != len(prefix) + args.payload or not body.startswith(prefix):
                raise ValueError("unexpected QoS 0 frame")
            seq, stamp = struct.unpack_from("!IQ", body, len(prefix))
            if seq == total:
                fenced[i] = True
                return
            if seq > total:
                raise ValueError("unexpected sequence number")
            if seen[seq]:
                duplicates[i] += 1
            else:
                seen[seq] = 1
                unique[i] += 1
                window[i] += int(now <= end)
                latency[i].append((now - stamp) / 1e6)
    readers = [asyncio.create_task(receive(i)) for i in range(args.fanout)]
    try:
        for i in range(total):
            delay = (start + i * 1_000_000_000 // args.rate - time.monotonic_ns()) / 1e9
            if delay > 0:
                await asyncio.sleep(delay)
            payload = struct.pack("!IQ", i, time.monotonic_ns()) + padding
            pw.write(mqtt.publish(TOPIC, payload))
            await pw.drain()
        finished = time.monotonic_ns()
        pw.write(mqtt.publish(TOPIC, struct.pack("!IQ", total, finished) + padding))
        pw.write(mqtt.pingreq())
        await expect(pr, 0xd0, b"")
        pending = await asyncio.wait(readers, timeout=max(0, (end - finished) / 1e9) + 5)
        for task in pending[0]:
            task.result()
    finally:
        for task in readers:
            task.cancel()
        await asyncio.gather(*readers, return_exceptions=True)
        await asyncio.gather(close(pw), *(close(w) for _, w in peers))
    write_json(args.out / "latencies-ms.json", latency)
    return {"payload_bytes": args.payload, "subscribers": len(peers), "transport": "mTLS" if sensor else "TCP",
            "offered": total, "rate": args.rate, "duration_seconds": args.duration,
            "send_window_seconds": (finished - start) / 1e9, "copies_expected": total * args.fanout,
            "in_window_by_subscriber": window, "after_drain_by_subscriber": unique,
            "duplicates_by_subscriber": duplicates, "end_marker_by_subscriber": fenced,
            "delivered_in_window_per_second": sum(window) / args.duration,
            "delivered_latency_ms": distribution([v for subscriber in latency for v in subscriber]),
            "latency_ms_by_subscriber": [distribution(samples) for samples in latency],
            "process": proc_sample(proc.pid),
            "competitor_cpu": args.competitor_cpu}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("profile", choices=("bounds", "waiting", "close", "qos0"))
    ap.add_argument("--binary", required=True, type=Path)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--broker-cpus", help="taskset CPU list for the broker")
    ap.add_argument("--shards", type=int, choices=(0, 2, 3), default=0)
    ap.add_argument("--sessions", type=int, default=1)
    ap.add_argument("--rounds", type=int, default=30)
    ap.add_argument("--depth", type=int, default=12)
    ap.add_argument("--tls", action="store_true")
    ap.add_argument("--idle-seconds", type=int, default=2)
    ap.add_argument("--fanout", type=int, default=1)
    ap.add_argument("--duration", type=int, default=3)
    ap.add_argument("--rate", type=int, default=100)
    ap.add_argument("--payload", type=int, choices=(256, 65536), default=256)
    ap.add_argument("--competitor-cpu", type=int, help="one fixed CPU shared with broker for a QoS 0 trial")
    args = ap.parse_args()
    if not (1 <= args.sessions <= 4096 and 1 <= args.rounds <= 200 and 1 <= args.depth <= 41
            and 1 <= args.fanout <= 64 and 1 <= args.duration <= 10 and 1 <= args.rate <= 1000
            and 1 <= args.idle_seconds <= 10):
        ap.error("profile outside the documented load budget")
    if args.sessions + 32 > resource.getrlimit(resource.RLIMIT_NOFILE)[0]:
        ap.error("insufficient file descriptors for requested session count")
    if args.fanout * args.payload * args.rate > 64 * 1024**2:
        ap.error("offered fanout exceeds 64 MiB/s")
    if args.sessions >= 1024:
        memory_info = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
        if int(memory_info["MemAvailable"].split()[0]) < 2 * 1024**2:
            ap.error("large session profile requires at least 2 GiB available memory")
    if args.tls and args.profile not in ("qos0", "close"):
        ap.error("TLS is available for qos0 and close")
    if args.competitor_cpu is not None and args.profile != "qos0":
        ap.error("the bounded co-tenant applies to the qos0 profile")
    with tempfile.TemporaryDirectory(prefix="mqttiumd-runtime-load-") as tmp:
        pki = make_pki(Path(tmp), args.fanout) if args.tls else None
        extra = ["--delivery-shards", str(args.shards)]
        if pki:
            extra.extend(("--tls-cert", str(pki / "server.crt"), "--tls-key", str(pki / "server.key"),
                          "--client-ca", str(pki / "ca.crt"), "--identities", str(pki / "identities.json")))
        with broker(args, extra) as (port, proc), contextlib.ExitStack() as stack:
            if args.competitor_cpu is not None:
                # A bounded checksum loop uses one logical CPU, independently of the load generator.
                command = ["taskset", "-c", str(args.competitor_cpu), sys.executable, "-c",
                           "import hashlib,time; start=time.monotonic(); data=bytes(65536)\n"
                           "while time.monotonic()-start<30: hashlib.sha256(data).digest()"]
                competitor = subprocess.Popen(command)
                stack.callback(competitor.wait)
                stack.callback(competitor.terminate)
                competitor_start = proc_sample(competitor.pid)
            if args.profile == "close":
                result = close_cost(args, port, proc, pki)
            else:
                coro = (qos0(args, port, proc, pki) if args.profile == "qos0" else
                        bounds(args, port, proc) if args.profile == "bounds" else waiting(args, port, proc))
                result = asyncio.run(asyncio.wait_for(coro, timeout=600 if args.profile == "bounds" else 90))
            if args.competitor_cpu is not None:
                if competitor.poll() is not None:
                    raise RuntimeError("CPU competitor exited during the trial")
                competitor_end = proc_sample(competitor.pid)
                result["competitor_cpu_seconds"] = competitor_end["cpu_seconds"] - competitor_start["cpu_seconds"]
        if args.profile == "waiting":
            # The ops journal flushes its final interval at broker shutdown.
            events = (json.loads(line) for line in (args.out / "events.jsonl").read_text().splitlines())
            result["publish_waiting_wire"] = sum(
                int(event["details"]["counts"].get("publish_waiting_wire", 0))
                for event in events if event["kind"] == "counters"
            )
        write_json(args.out / "result.json", result)


if __name__ == "__main__":
    main()
