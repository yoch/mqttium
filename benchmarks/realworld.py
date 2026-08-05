"""Fresh-process confirmed-delivery MQTT benchmark.

A real ``mosquitto_sub`` process validates every payload. Publisher ACK
throughput and subscriber delivery throughput are reported separately. Latency
is measured from immediately before the application calls ``publish`` and
therefore includes local admission and queue residence; sweep ``--window`` when
diagnosing latency/throughput trade-offs.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import json
import os
import platform
import resource
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import paho.mqtt.client as mqtt

from mqttium.api import AsyncClient
from mqttium.protocol.reconnect import ReconnectPolicy


HEADER_HEX_BYTES = 32


@dataclass
class Result:
    library: str
    transport: str
    profile: str
    payload_bytes: int
    count: int
    window: int
    publisher_ack_msg_s: float
    delivered_msg_s: float
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    cpu_seconds: float
    max_rss_mib: float


def package_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def environment(args: argparse.Namespace) -> dict[str, object]:
    return {
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "host": args.host,
        "port": args.port,
        "transport": "tls" if args.cafile else "tcp",
        "profile": args.profile,
        "window": args.window,
        "latency_origin": "immediately_before_publish_call",
        "versions": {
            "mqttium": package_version("mqttium"),
            "paho-mqtt": package_version("paho-mqtt"),
        },
    }


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, round((len(ordered) - 1) * fraction))
    return ordered[index]


def start_subscriber(
    host: str,
    port: int,
    topic: str,
    count: int,
    cafile: str | None,
):
    command = [
        "mosquitto_sub",
        "-h",
        host,
        "-p",
        str(port),
        "-t",
        topic,
        "-q",
        "1",
        "-C",
        str(count),
        "-F",
        "%p",
    ]
    if cafile:
        command.extend(["--cafile", cafile])
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    arrivals: list[tuple[bytes, int]] = []

    def read() -> None:
        assert process.stdout is not None
        for line in process.stdout:
            arrivals.append((line.rstrip(b"\n"), time.monotonic_ns()))

    thread = threading.Thread(target=read, daemon=True)
    thread.start()
    time.sleep(0.35)
    if process.poll() is not None:
        assert process.stderr is not None
        raise RuntimeError(process.stderr.read().decode(errors="replace"))
    return process, arrivals, thread


def make_payload(sequence: int, size: int) -> bytes:
    header = f"{sequence:016x}{time.monotonic_ns():016x}".encode("ascii")
    return header + b"x" * max(0, size - len(header))


async def publish_mqttium(
    host: str,
    port: int,
    topic: str,
    count: int,
    size: int,
    context,
    window: int,
) -> float:
    client = AsyncClient(
        client_id=f"real-mn-{os.getpid()}",
        max_outbound_inflight=window,
        reconnect=ReconnectPolicy(enabled=False),
    )
    await client.connect(host, port, ssl=context, timeout=10.0)
    pending = []
    started = time.perf_counter()
    for sequence in range(count):
        pending.append(await client.publish(topic, make_payload(sequence, size), qos=1))
        if len(pending) >= window:
            await pending.pop(0).wait()
    await asyncio.gather(*(receipt.wait() for receipt in pending))
    elapsed = time.perf_counter() - started
    await client.disconnect()
    return elapsed


def publish_paho(
    host: str,
    port: int,
    topic: str,
    count: int,
    size: int,
    cafile: str | None,
    window: int,
) -> float:
    connected = threading.Event()
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"real-paho-{os.getpid()}",
    )
    client.max_inflight_messages_set(window)
    client.max_queued_messages_set(window * 2)
    if cafile:
        client.tls_set(ca_certs=cafile)

    def on_connect(_client, _userdata, _flags, reason_code, _properties) -> None:
        if reason_code == 0:
            connected.set()

    client.on_connect = on_connect
    client.connect(host, port, keepalive=60)
    client.loop_start()
    if not connected.wait(10.0):
        client.loop_stop()
        raise TimeoutError("Paho did not connect")
    pending = []
    started = time.perf_counter()
    for sequence in range(count):
        info = client.publish(topic, make_payload(sequence, size), qos=1)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            client.loop_stop()
            raise RuntimeError(f"Paho publish failed rc={info.rc}")
        pending.append(info)
        if len(pending) >= window:
            pending.pop(0).wait_for_publish(timeout=30.0)
    for info in pending:
        info.wait_for_publish(timeout=30.0)
    elapsed = time.perf_counter() - started
    client.disconnect()
    client.loop_stop()
    return elapsed


def child(args: argparse.Namespace) -> None:
    import ssl

    topic = f"bench/real/{args.library}/{os.getpid()}/{time.time_ns()}"
    process, arrivals, thread = start_subscriber(
        args.host,
        args.port,
        topic,
        args.count,
        args.cafile,
    )
    delivery_started = time.perf_counter()
    cpu_started = time.process_time()
    if args.library == "mqttium":
        context = ssl.create_default_context(cafile=args.cafile) if args.cafile else None
        ack_seconds = asyncio.run(
            publish_mqttium(
                args.host,
                args.port,
                topic,
                args.count,
                args.payload_bytes,
                context,
                args.window,
            )
        )
    else:
        ack_seconds = publish_paho(
            args.host,
            args.port,
            topic,
            args.count,
            args.payload_bytes,
            args.cafile,
            args.window,
        )
    try:
        process.wait(timeout=args.subscriber_timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    thread.join(timeout=2.0)
    if len(arrivals) != args.count:
        assert process.stderr is not None
        detail = process.stderr.read().decode(errors="replace")
        raise TimeoutError(
            f"subscriber incomplete {len(arrivals)}/{args.count} "
            f"after {args.subscriber_timeout:.1f}s: {detail}"
        )
    delivery_seconds = time.perf_counter() - delivery_started
    latencies = []
    sequences = []
    for raw, arrived_ns in arrivals:
        if len(raw) < HEADER_HEX_BYTES:
            raise RuntimeError(f"invalid payload length {len(raw)}")
        try:
            sequence = int(raw[:16], 16)
            sent_ns = int(raw[16:HEADER_HEX_BYTES], 16)
        except ValueError as exc:
            raise RuntimeError("invalid benchmark payload header") from exc
        sequences.append(sequence)
        latencies.append((arrived_ns - sent_ns) / 1_000_000)
    if sorted(sequences) != list(range(args.count)):
        raise RuntimeError("subscriber payload sequence mismatch")
    usage = resource.getrusage(resource.RUSAGE_SELF)
    result = Result(
        library=args.library,
        transport="tls" if args.cafile else "tcp",
        profile=args.profile,
        payload_bytes=args.payload_bytes,
        count=args.count,
        window=args.window,
        publisher_ack_msg_s=args.count / ack_seconds,
        delivered_msg_s=args.count / delivery_seconds,
        latency_p50_ms=statistics.median(latencies),
        latency_p95_ms=percentile(latencies, 0.95),
        latency_p99_ms=percentile(latencies, 0.99),
        cpu_seconds=time.process_time() - cpu_started,
        max_rss_mib=usage.ru_maxrss / 1024,
    )
    print(json.dumps(asdict(result)))


def write_results(
    output: Path,
    args: argparse.Namespace,
    results: list[dict[str, object]],
) -> None:
    payload = {"metadata": environment(args), "results": results}
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2))


def parent(args: argparse.Namespace) -> None:
    results: list[dict[str, object]] = []
    write_results(args.output, args, results)
    scenarios = ((64, args.count), (4096, max(500, args.count // 3)))
    for payload_bytes, count in scenarios:
        for library in ("mqttium", "paho"):
            command = [
                sys.executable,
                __file__,
                "--child",
                "--library",
                library,
                "--host",
                args.host,
                "--port",
                str(args.port),
                "--payload-bytes",
                str(payload_bytes),
                "--count",
                str(count),
                "--profile",
                args.profile,
                "--subscriber-timeout",
                str(args.subscriber_timeout),
                "--window",
                str(args.window),
            ]
            if args.cafile:
                command.extend(["--cafile", args.cafile])
            try:
                completed = subprocess.run(
                    command,
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=args.subscriber_timeout + 60,
                )
            except subprocess.TimeoutExpired as exc:
                raise TimeoutError(
                    f"benchmark child process exceeded timeout: {command!r}"
                ) from exc
            if completed.returncode != 0:
                raise RuntimeError(
                    f"benchmark child failed rc={completed.returncode}\n"
                    f"command={command!r}\n"
                    f"stdout:\n{completed.stdout}\n"
                    f"stderr:\n{completed.stderr}"
                )
            lines = [line for line in completed.stdout.splitlines() if line.strip()]
            if not lines:
                raise RuntimeError("benchmark child returned no result")
            result = json.loads(lines[-1])
            results.append(result)
            write_results(args.output, args, results)
            print(
                f"{library:8s} {payload_bytes:5d} B "
                f"ack={result['publisher_ack_msg_s']:9.1f} msg/s "
                f"delivered={result['delivered_msg_s']:9.1f} msg/s "
                f"p95={result['latency_p95_ms']:8.2f} ms"
            )


def parse() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", action="store_true")
    parser.add_argument("--library", choices=("mqttium", "paho"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11883)
    parser.add_argument("--cafile")
    parser.add_argument("--profile", default="local")
    parser.add_argument("--payload-bytes", type=int, default=64)
    parser.add_argument("--count", type=int, default=3_000)
    parser.add_argument("--window", type=int, default=100)
    parser.add_argument("--subscriber-timeout", type=float, default=120.0)
    parser.add_argument("--output", type=Path, default=Path("/tmp/realworld.json"))
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse()
    if arguments.child:
        child(arguments)
    else:
        parent(arguments)
