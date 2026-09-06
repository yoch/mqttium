#!/usr/bin/env python3
"""External-process pacer for temporally independent MQTT load.

The in-loop `asyncio.sleep` pacer in `paired_open_loop.py` shares the event loop
under test, so a cheaper client changes timer lag, catch-up bursts and the
eager-write mix. This harness moves the clock into a dedicated process and
delivers timestamped tokens over a Unix datagram socketpair. The publisher
loop never sleeps to pace.

Transport was chosen by a 8000-token micro at 5000 msg/s on this host:
socketpair SOCK_DGRAM beat SOCK_STREAM and filesystem AF_UNIX datagram on
receiver jitter and transport delay. Default safety margin is 150 µs, the same
for every arm.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import socket
import statistics
import struct
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from paired_network import start_subscriber


TOKEN_STRUCT = struct.Struct("<QQQ")  # seq, deadline_ns, emit_ns
READY = b"R"
PCTS = (10, 25, 50, 75, 90, 95, 99)
DEFAULT_RATES = (4500.0, 5000.0, 5250.0, 5500.0)
DEFAULT_MARGIN_NS = 150_000
START_DELAY_NS = 50_000_000
BUFFER_BYTES = 1 << 20

# Physical-core map for the 4-vCPU control host. No SMT siblings exist here.
DEFAULT_HARNESS_CPU = 0
DEFAULT_BROKER_CPU = 1
DEFAULT_PUBLISHER_CPU = 2
DEFAULT_PACER_CPU = 3


def percentile(values: Sequence[float], pct: float) -> float:
    if not values:
        return math.nan
    ordered = sorted(values)
    rank = (len(ordered) - 1) * pct / 100.0
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] * (1.0 - (rank - low)) + ordered[high] * (rank - low)


def pcts_us(values_ns: Sequence[int | float]) -> dict[str, float]:
    scaled = [float(value) / 1000.0 for value in values_ns]
    return {f"p{int(pct)}": percentile(scaled, pct) for pct in PCTS}


def pcts_ms(values_ms: Sequence[float]) -> dict[str, float]:
    return {f"p{int(pct)}": percentile(list(values_ms), pct) for pct in PCTS}


def pack_token(seq: int, deadline_ns: int, emit_ns: int) -> bytes:
    return TOKEN_STRUCT.pack(seq, deadline_ns, emit_ns)


def unpack_token(payload: bytes) -> tuple[int, int, int]:
    return TOKEN_STRUCT.unpack(payload)


def schedule_deadline(start_ns: int, seq: int, interval_ns: float) -> int:
    return int(start_ns + seq * interval_ns)


def pin_cpu(cpu: int | None) -> None:
    if cpu is None or cpu < 0:
        return
    try:
        os.sched_setaffinity(0, {cpu})
    except (AttributeError, OSError) as exc:
        raise RuntimeError(f"cannot pin process to CPU {cpu}") from exc


def wait_until(deadline_ns: int, safety_margin_ns: int) -> bool:
    """Sleep then spin until an absolute deadline. True if already late."""
    now = time.monotonic_ns()
    catchup = now >= deadline_ns
    remaining = deadline_ns - now
    if remaining > safety_margin_ns:
        time.sleep((remaining - safety_margin_ns) / 1e9)
    while time.monotonic_ns() < deadline_ns:
        pass
    return catchup


def burst_runs(catchup: Sequence[bool]) -> list[int]:
    runs: list[int] = []
    run = 0
    for flag in catchup:
        if flag:
            run += 1
        elif run:
            runs.append(run)
            run = 0
    if run:
        runs.append(run)
    return runs


def burst_stats(catchup: Sequence[bool]) -> dict[str, float]:
    runs = burst_runs(catchup)
    fraction = sum(1 for flag in catchup if flag) / len(catchup) if catchup else 0.0
    if not runs:
        return {
            "catchup_fraction": fraction,
            "burst_count": 0.0,
            "burst_mean": 0.0,
            "burst_p50": 0.0,
            "burst_p95": 0.0,
            "burst_max": 0.0,
        }
    sizes = [float(run) for run in runs]
    return {
        "catchup_fraction": fraction,
        "burst_count": float(len(runs)),
        "burst_mean": statistics.fmean(sizes),
        "burst_p50": percentile(sizes, 50),
        "burst_p95": percentile(sizes, 95),
        "burst_max": float(max(runs)),
    }


def intervals_ns(timestamps: Sequence[int]) -> list[int]:
    return [right - left for left, right in zip(timestamps, timestamps[1:], strict=False)]


def configure_dgram(sock: socket.socket) -> socket.socket:
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, BUFFER_BYTES)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, BUFFER_BYTES)
    return sock


def socket_from_fd(fd: int) -> socket.socket:
    sock = socket.fromfd(fd, socket.AF_UNIX, socket.SOCK_DGRAM)
    return configure_dgram(sock)


def process_cpu_seconds(pid: int) -> float:
    ticks = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
    with open(f"/proc/{pid}/stat", encoding="utf-8") as handle:
        stat = handle.read()
    fields = stat[stat.rfind(")") + 2 :].split()
    return (int(fields[11]) + int(fields[12])) / ticks


def host_info() -> dict[str, Any]:
    siblings = []
    cpu_root = Path("/sys/devices/system/cpu")
    for entry in sorted(cpu_root.glob("cpu[0-9]*")):
        path = entry / "topology" / "thread_siblings_list"
        if path.exists():
            siblings.append({"cpu": entry.name, "siblings": path.read_text().strip()})
    governor = "unavailable"
    gov_path = cpu_root / "cpu0" / "cpufreq" / "scaling_governor"
    if gov_path.exists():
        governor = gov_path.read_text().strip()
    # host_info() is imported by the unit suite, which runs on hosts with no
    # /proc and no broker binary. Every probe here degrades to a label.
    model = "unknown"
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("model name"):
                    model = line.split(":", 1)[1].strip()
                    break
    except OSError:
        model = "unavailable"
    try:
        mosq = subprocess.run(["mosquitto", "-h"], capture_output=True, text=True, check=False)
    except OSError:
        mosq_version = "unavailable"
    else:
        mosq_text = (mosq.stdout or mosq.stderr or "").splitlines()
        mosq_version = mosq_text[0] if mosq_text else "unknown"
    return {
        "cpu_model": model,
        "cpus": os.cpu_count(),
        "thread_siblings": siblings,
        "governor": governor,
        "python": sys.version.split()[0],
        "mosquitto": mosq_version,
        "affinity": {
            "harness_subscriber": DEFAULT_HARNESS_CPU,
            "broker": DEFAULT_BROKER_CPU,
            "publisher": DEFAULT_PUBLISHER_CPU,
            "pacer": DEFAULT_PACER_CPU,
        },
        "safety_margin_ns": DEFAULT_MARGIN_NS,
        "transport": "socketpair SOCK_DGRAM",
    }


def emit_tokens(
    sock: socket.socket,
    count: int,
    interval_ns: float,
    safety_margin_ns: int,
) -> dict[str, Any]:
    cpu0 = time.process_time()
    sock.recv(1)
    start_ns = time.monotonic_ns() + START_DELAY_NS
    emit_ns: list[int] = []
    lateness_ns: list[int] = []
    catchup: list[bool] = []
    lost = 0
    for seq in range(count):
        deadline = schedule_deadline(start_ns, seq, interval_ns)
        was_catchup = wait_until(deadline, safety_margin_ns)
        emitted = time.monotonic_ns()
        try:
            sock.send(pack_token(seq, deadline, emitted))
        except OSError:
            lost += 1
            continue
        emit_ns.append(emitted)
        lateness_ns.append(emitted - deadline)
        catchup.append(was_catchup)
    return {
        "count": count,
        "emitted": len(emit_ns),
        "lost_sends": lost,
        "target_interval_us": interval_ns / 1000.0,
        "emission_interval_us": pcts_us(intervals_ns(emit_ns)),
        "lateness_us": pcts_us(lateness_ns),
        "pacer_cpu_seconds": time.process_time() - cpu0,
        **burst_stats(catchup),
    }


def receive_tokens(
    sock: socket.socket,
    count: int,
    timeout: float,
) -> dict[str, Any]:
    sock.settimeout(timeout)
    sock.send(READY)
    recv_ns: list[int] = []
    transport_ns: list[int] = []
    lateness_ns: list[int] = []
    sequences: list[int] = []
    for expected in range(count):
        payload = sock.recv(TOKEN_STRUCT.size)
        received = time.monotonic_ns()
        seq, deadline, emitted = unpack_token(payload)
        if seq != expected:
            raise RuntimeError(f"token sequence {seq} != {expected}")
        sequences.append(seq)
        recv_ns.append(received)
        transport_ns.append(received - emitted)
        lateness_ns.append(received - deadline)
    return {
        "received": len(sequences),
        "lost_tokens": count - len(sequences),
        "sequence_ok": sequences == list(range(count)),
        "receiver_interval_us": pcts_us(intervals_ns(recv_ns)),
        "transport_delay_us": pcts_us(transport_ns),
        "receiver_lateness_us": pcts_us(lateness_ns),
    }


def pacer_worker(args: argparse.Namespace) -> None:
    pin_cpu(args.pacer_cpu)
    sock = socket_from_fd(args.fd)
    interval_ns = 1e9 / args.rate
    result = emit_tokens(sock, args.count, interval_ns, args.safety_margin_ns)
    sock.close()
    print(json.dumps(result), flush=True)


def receiver_worker(args: argparse.Namespace) -> None:
    pin_cpu(args.publisher_cpu)
    sock = socket_from_fd(args.fd)
    result = receive_tokens(sock, args.count, args.timeout)
    sock.close()
    print(json.dumps(result), flush=True)


def _payload(seq: int, sent_ns: int, size: int) -> bytes:
    header = f"{seq:016x}{sent_ns:016x}".encode("ascii")
    return header + b"x" * max(0, size - len(header))


def _writer_metrics(writer: Any, count: int) -> dict[str, float]:
    batches = max(writer.batches, 1)
    return {
        "eager_writes_per_msg": writer.eager_writes / count,
        "batches_per_msg": writer.batches / count,
        "items_per_batch": writer.batched_items / batches,
        "enqueue_suspensions_per_msg": writer.enqueue_suspensions / count,
        "high_water_messages": float(writer.high_water_messages),
        "high_water_bytes": float(writer.high_water_bytes),
    }


def _effect_metrics(effects: Any, count: int) -> dict[str, float]:
    return {
        "enqueued_per_msg": effects.enqueued / count,
        "multi_batches_per_msg": effects.multi_effect_batches / count,
        "apply_suspensions": float(effects.apply_suspensions),
        "pending_high_water": float(effects.pending_high_water),
    }


async def publish_from_tokens(
    args: argparse.Namespace, topic: str, sock: socket.socket
) -> dict[str, Any]:
    # Bind mqttium from PYTHONPATH so one harness can drive both source trees.
    from mqttium.api import AsyncClient
    from mqttium.enums import MQTTProtocolVersion
    from mqttium.protocol.reconnect import ReconnectPolicy

    protocol = MQTTProtocolVersion.MQTTv5 if args.protocol == "5" else MQTTProtocolVersion.MQTTv311
    client = AsyncClient(
        client_id=f"ext-pacer-{os.getpid()}-{time.time_ns()}",
        protocol=protocol,
        max_outbound_inflight=args.window,
        max_pending_outbound_messages=None,
        max_pending_outbound_bytes=None,
        reconnect=ReconnectPolicy(enabled=False),
    )
    ack_ms = [math.nan] * args.count
    recv_ns: list[int] = []
    transport_ns: list[int] = []
    token_to_publish_ns: list[int] = []
    admission_ns: list[int] = []
    tasks: list[asyncio.Task[None]] = []
    loop = asyncio.get_running_loop()
    sock.setblocking(False)

    async def observe(receipt: Any, seq: int, sent: int) -> None:
        await receipt.wait()
        ack_ms[seq] = (time.monotonic_ns() - sent) / 1_000_000

    await client.connect(args.host, args.port, timeout=args.timeout)
    sock.send(READY)
    cpu0 = time.process_time()
    offered0 = 0.0
    lost = 0
    try:
        for expected in range(args.count):
            payload = await asyncio.wait_for(
                loop.sock_recv(sock, TOKEN_STRUCT.size),
                timeout=args.timeout,
            )
            received = time.monotonic_ns()
            seq, _deadline, emitted = unpack_token(payload)
            if seq != expected:
                raise RuntimeError(f"token sequence {seq} != {expected}")
            transport_ns.append(received - emitted)
            recv_ns.append(received)
            if expected == 0:
                offered0 = time.perf_counter()
            pre = time.monotonic_ns()
            token_to_publish_ns.append(pre - received)
            sent = time.monotonic_ns()
            receipt = await client.publish(topic, _payload(seq, sent, args.payload_bytes), qos=1)
            admission_ns.append(time.monotonic_ns() - pre)
            tasks.append(loop.create_task(observe(receipt, seq, sent)))
        offered_elapsed = max(time.perf_counter() - offered0, 1e-9)
        await asyncio.gather(*tasks)
        completed_elapsed = max(time.perf_counter() - offered0, 1e-9)
        snapshot = client.stats()
        cpu_seconds = time.process_time() - cpu0
        finite_ack = [value for value in ack_ms if not math.isnan(value)]
        if len(finite_ack) != args.count:
            lost = args.count - len(finite_ack)
        return {
            "target_rate": args.rate,
            "pacer_emitted_rate": args.count / max(offered_elapsed, 1e-9),
            "publisher_received_rate": len(recv_ns) / offered_elapsed,
            "completed_rate": len(finite_ack) / completed_elapsed,
            "completion_ratio": len(finite_ack) / args.count,
            "lost_tokens": lost,
            "sequence_ok": True,
            "publisher_cpu_seconds": cpu_seconds,
            "publisher_cpu_us_per_msg": (cpu_seconds * 1e6) / max(len(finite_ack), 1),
            "ack_ms": pcts_ms(finite_ack),
            "receiver_interval_us": pcts_us(intervals_ns(recv_ns)),
            "transport_delay_us": pcts_us(transport_ns),
            "token_to_publish_us": pcts_us(token_to_publish_ns),
            "publish_admission_us": pcts_us(admission_ns),
            "writer": _writer_metrics(snapshot.writer, args.count),
            "effects": _effect_metrics(snapshot.effects, args.count),
            "payload_bytes": args.payload_bytes,
            "window": args.window,
            "protocol": args.protocol,
            "count": args.count,
        }
    finally:
        await client.disconnect()
        sock.close()


def publisher_worker(args: argparse.Namespace) -> None:
    pin_cpu(args.publisher_cpu)
    topic = args.topic
    sock = socket_from_fd(args.fd)
    result = asyncio.run(publish_from_tokens(args, topic, sock))
    print(json.dumps(result), flush=True)


def _spawn_self(
    extra: Sequence[str],
    *,
    env: dict[str, str] | None = None,
    pass_fds: Sequence[int] = (),
    timeout: float,
) -> dict[str, Any]:
    command = [sys.executable, str(Path(__file__).resolve()), *extra]
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        env=env,
        timeout=timeout,
        pass_fds=pass_fds,
    )
    if completed.returncode:
        diagnostic = (completed.stderr or completed.stdout or "no output").strip()
        raise RuntimeError(f"worker exited {completed.returncode}: {diagnostic[-2000:]}")
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    return json.loads(lines[-1])


def _make_pair() -> tuple[socket.socket, socket.socket]:
    left, right = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
    return configure_dgram(left), configure_dgram(right)


def run_qualify(args: argparse.Namespace) -> dict[str, Any]:
    pin_cpu(args.harness_cpu)
    left, right = _make_pair()
    timeout = args.timeout + args.count / args.rate + 15.0
    pacer_cmd = [
        "--mode",
        "pacer",
        "--fd",
        str(left.fileno()),
        "--rate",
        str(args.rate),
        "--count",
        str(args.count),
        "--safety-margin-ns",
        str(args.safety_margin_ns),
        "--timeout",
        str(args.timeout),
    ]
    recv_cmd = [
        "--mode",
        "receiver",
        "--fd",
        str(right.fileno()),
        "--rate",
        str(args.rate),
        "--count",
        str(args.count),
        "--timeout",
        str(args.timeout),
    ]
    if args.pacer_cpu is not None:
        pacer_cmd.extend(("--pacer-cpu", str(args.pacer_cpu)))
    if args.publisher_cpu is not None:
        recv_cmd.extend(("--publisher-cpu", str(args.publisher_cpu)))
    pacer = subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), *pacer_cmd],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        pass_fds=(left.fileno(),),
    )
    left.close()
    try:
        receiver = _spawn_self(recv_cmd, pass_fds=(right.fileno(),), timeout=timeout)
    finally:
        right.close()
        stdout, stderr = pacer.communicate(timeout=timeout)
    if pacer.returncode:
        raise RuntimeError(f"pacer exited {pacer.returncode}: {(stderr or stdout)[-2000:]}")
    emission = json.loads([line for line in stdout.splitlines() if line.strip()][-1])
    merged = {
        "rate": args.rate,
        "count": args.count,
        "target_interval_us": 1e6 / args.rate,
        **emission,
        **receiver,
    }
    return merged


def _start_broker(port: int, cpu: int | None) -> subprocess.Popen[str]:
    conf = Path(f"/tmp/mqttium-ext-pacer-{port}.conf")
    conf.write_text(f"listener {port} 127.0.0.1\nallow_anonymous true\npersistence false\n")
    command = ["mosquitto", "-c", str(conf)]
    if cpu is not None:
        command = ["taskset", "-c", str(cpu), *command]
    proc = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    deadline = time.time() + 5.0
    while time.time() < deadline:
        probe = socket.socket()
        try:
            probe.settimeout(0.1)
            probe.connect(("127.0.0.1", port))
            probe.close()
            return proc
        except OSError:
            probe.close()
            if proc.poll() is not None:
                err = proc.stderr.read() if proc.stderr is not None else ""
                raise RuntimeError(f"mosquitto exited: {err}") from None
            time.sleep(0.05)
    proc.kill()
    raise RuntimeError(f"mosquitto did not listen on {port}")


def run_mqtt_sample(args: argparse.Namespace, root: Path, arm: str) -> dict[str, Any]:
    pin_cpu(args.harness_cpu)
    topic = f"bench/ext-pacer/{os.getpid()}/{time.time_ns()}"
    subscriber = start_subscriber(args.host, args.port, topic, args.count)
    left, right = _make_pair()
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root.resolve() / "src")
    timeout = args.timeout + args.count / args.rate + 30.0
    pacer_cmd = [
        "--mode",
        "pacer",
        "--fd",
        str(left.fileno()),
        "--rate",
        str(args.rate),
        "--count",
        str(args.count),
        "--safety-margin-ns",
        str(args.safety_margin_ns),
        "--timeout",
        str(args.timeout),
    ]
    pub_cmd = [
        "--mode",
        "publisher",
        "--fd",
        str(right.fileno()),
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--protocol",
        args.protocol,
        "--payload-bytes",
        str(args.payload_bytes),
        "--window",
        str(args.window),
        "--rate",
        str(args.rate),
        "--count",
        str(args.count),
        "--topic",
        topic,
        "--timeout",
        str(args.timeout),
        "--safety-margin-ns",
        str(args.safety_margin_ns),
    ]
    if args.pacer_cpu is not None:
        pacer_cmd.extend(("--pacer-cpu", str(args.pacer_cpu)))
    if args.publisher_cpu is not None:
        pub_cmd.extend(("--publisher-cpu", str(args.publisher_cpu)))
    broker_cpu0 = process_cpu_seconds(args.broker_pid) if args.broker_pid else math.nan
    pacer = subprocess.Popen(
        [sys.executable, str(Path(__file__).resolve()), *pacer_cmd],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        pass_fds=(left.fileno(),),
    )
    left.close()
    try:
        publisher = _spawn_self(pub_cmd, env=env, pass_fds=(right.fileno(),), timeout=timeout)
    except BaseException:
        subscriber.abort()
        pacer.kill()
        raise
    finally:
        right.close()
        stdout, stderr = pacer.communicate(timeout=timeout)
    if pacer.returncode:
        subscriber.abort()
        raise RuntimeError(f"pacer exited {pacer.returncode}: {(stderr or stdout)[-2000:]}")
    emission = json.loads([line for line in stdout.splitlines() if line.strip()][-1])
    delivery_ms, sequences = subscriber.finish(args.timeout)
    if sorted(sequences) != list(range(args.count)):
        raise RuntimeError(f"subscriber sequence mismatch: {len(sequences)}/{args.count}")
    broker_cpu = process_cpu_seconds(args.broker_pid) - broker_cpu0 if args.broker_pid else math.nan
    return {
        "arm": arm,
        "root": str(root),
        "rate": args.rate,
        "target_interval_us": 1e6 / args.rate,
        "delivery_ms": pcts_ms(delivery_ms),
        "pacer": emission,
        "pacer_cpu_seconds": emission["pacer_cpu_seconds"],
        "broker_cpu_seconds": broker_cpu,
        **publisher,
    }


def _median_maps(rows: Sequence[Mapping[str, Any]], path: str) -> float:
    values: list[float] = []
    for row in rows:
        node: Any = row
        for part in path.split("."):
            node = node[part]
        if isinstance(node, (int, float)) and not (isinstance(node, float) and math.isnan(node)):
            values.append(float(node))
    return statistics.median(values) if values else math.nan


def _summarize_arm(rows: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    return {
        "cpu_us_per_msg": _median_maps(rows, "publisher_cpu_us_per_msg"),
        "publisher_cpu_seconds": _median_maps(rows, "publisher_cpu_seconds"),
        "pacer_cpu_seconds": _median_maps(rows, "pacer_cpu_seconds"),
        "broker_cpu_seconds": _median_maps(rows, "broker_cpu_seconds"),
        "ack_p10": _median_maps(rows, "ack_ms.p10"),
        "ack_p25": _median_maps(rows, "ack_ms.p25"),
        "ack_p50": _median_maps(rows, "ack_ms.p50"),
        "ack_p75": _median_maps(rows, "ack_ms.p75"),
        "ack_p90": _median_maps(rows, "ack_ms.p90"),
        "ack_p95": _median_maps(rows, "ack_ms.p95"),
        "ack_p99": _median_maps(rows, "ack_ms.p99"),
        "delivery_p10": _median_maps(rows, "delivery_ms.p10"),
        "delivery_p25": _median_maps(rows, "delivery_ms.p25"),
        "delivery_p50": _median_maps(rows, "delivery_ms.p50"),
        "delivery_p75": _median_maps(rows, "delivery_ms.p75"),
        "delivery_p90": _median_maps(rows, "delivery_ms.p90"),
        "delivery_p95": _median_maps(rows, "delivery_ms.p95"),
        "delivery_p99": _median_maps(rows, "delivery_ms.p99"),
        "eager_per_msg": _median_maps(rows, "writer.eager_writes_per_msg"),
        "batches_per_msg": _median_maps(rows, "writer.batches_per_msg"),
        "items_per_batch": _median_maps(rows, "writer.items_per_batch"),
        "enqueue_per_msg": _median_maps(rows, "writer.enqueue_suspensions_per_msg"),
        "high_water_messages": _median_maps(rows, "writer.high_water_messages"),
        "high_water_bytes": _median_maps(rows, "writer.high_water_bytes"),
        "effect_enqueued_per_msg": _median_maps(rows, "effects.enqueued_per_msg"),
        "effect_multi_per_msg": _median_maps(rows, "effects.multi_batches_per_msg"),
        "effect_suspensions": _median_maps(rows, "effects.apply_suspensions"),
        "effect_pending_hw": _median_maps(rows, "effects.pending_high_water"),
        "token_to_publish_p50": _median_maps(rows, "token_to_publish_us.p50"),
        "token_to_publish_p95": _median_maps(rows, "token_to_publish_us.p95"),
        "publish_admission_p50": _median_maps(rows, "publish_admission_us.p50"),
        "publish_admission_p95": _median_maps(rows, "publish_admission_us.p95"),
        "transport_p50": _median_maps(rows, "transport_delay_us.p50"),
        "transport_p95": _median_maps(rows, "transport_delay_us.p95"),
        "recv_interval_p50": _median_maps(rows, "receiver_interval_us.p50"),
        "recv_interval_p95": _median_maps(rows, "receiver_interval_us.p95"),
        "recv_interval_p99": _median_maps(rows, "receiver_interval_us.p99"),
        "emit_interval_p50": _median_maps(rows, "pacer.emission_interval_us.p50"),
        "emit_interval_p95": _median_maps(rows, "pacer.emission_interval_us.p95"),
        "emit_interval_p99": _median_maps(rows, "pacer.emission_interval_us.p99"),
        "lateness_p50": _median_maps(rows, "pacer.lateness_us.p50"),
        "lateness_p95": _median_maps(rows, "pacer.lateness_us.p95"),
        "lateness_p99": _median_maps(rows, "pacer.lateness_us.p99"),
        "catchup_fraction": _median_maps(rows, "pacer.catchup_fraction"),
        "completion_ratio": _median_maps(rows, "completion_ratio"),
        "completed_rate": _median_maps(rows, "completed_rate"),
        "publisher_received_rate": _median_maps(rows, "publisher_received_rate"),
        "pacer_emitted_rate": _median_maps(rows, "pacer_emitted_rate"),
    }


def _delta_pct(base: float, cand: float) -> float:
    if not base:
        return math.nan
    return (cand - base) / base * 100.0


def _print_qualify(row: Mapping[str, Any]) -> None:
    emit = row["emission_interval_us"]
    recv = row["receiver_interval_us"]
    late = row["lateness_us"]
    xport = row["transport_delay_us"]
    print(
        f"{row['rate']:6.0f} {row['target_interval_us']:8.1f} "
        f"{emit['p50']:7.2f}/{emit['p95']:7.2f}/{emit['p99']:7.2f} "
        f"{recv['p50']:7.2f}/{recv['p95']:7.2f}/{recv['p99']:7.2f} "
        f"{late['p95']:8.2f} {xport['p95']:8.2f} "
        f"{row['catchup_fraction'] * 100:7.2f}%"
    )


def parse_rates(raw: str) -> list[float]:
    return [float(part) for part in raw.split(",") if part.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=(
            "pacer",
            "receiver",
            "publisher",
            "qualify",
            "aa",
            "ab",
            "probe-transport",
            "calibrate",
            "host-info",
        ),
        required=True,
    )
    parser.add_argument("--fd", type=int, default=-1)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=22883)
    parser.add_argument("--protocol", choices=("311", "5"), default="311")
    parser.add_argument("--payload-bytes", type=int, default=64)
    parser.add_argument("--window", type=int, default=32)
    parser.add_argument("--rate", type=float, default=5000.0)
    parser.add_argument("--rates", default=",".join(str(int(r)) for r in DEFAULT_RATES))
    parser.add_argument("--count", type=int, default=0)
    parser.add_argument("--sample-seconds", type=float, default=4.0)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--safety-margin-ns", type=int, default=DEFAULT_MARGIN_NS)
    parser.add_argument("--topic", default="")
    parser.add_argument("--base-root", type=Path, default=None)
    parser.add_argument("--candidate-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--pairs", type=int, default=5)
    parser.add_argument("--cycles", type=int, default=2)
    parser.add_argument("--start-broker", action="store_true")
    parser.add_argument("--broker-pid", type=int, default=0)
    parser.add_argument("--harness-cpu", type=int, default=DEFAULT_HARNESS_CPU)
    parser.add_argument("--broker-cpu", type=int, default=DEFAULT_BROKER_CPU)
    parser.add_argument("--publisher-cpu", type=int, default=DEFAULT_PUBLISHER_CPU)
    parser.add_argument("--pacer-cpu", type=int, default=DEFAULT_PACER_CPU)
    parser.add_argument("--output", type=Path, default=None)
    return parser


def _count_for(rate: float, sample_seconds: float, override: int) -> int:
    if override:
        return override
    return max(int(rate * sample_seconds), 500)


def run_calibrate(args: argparse.Namespace) -> dict[str, Any]:
    margins = (50_000, 100_000, 150_000, 200_000, 250_000)
    count = 3000
    interval_ns = 1e9 / 5000.0
    rows = []
    for margin in margins:
        emit_ns: list[int] = []
        late: list[int] = []
        start = time.monotonic_ns() + 5_000_000
        for seq in range(count):
            deadline = schedule_deadline(start, seq, interval_ns)
            wait_until(deadline, margin)
            emitted = time.monotonic_ns()
            emit_ns.append(emitted)
            late.append(emitted - deadline)
        rows.append(
            {
                "safety_margin_us": margin / 1000.0,
                "lateness_us": pcts_us(late),
                "emission_interval_us": pcts_us(intervals_ns(emit_ns)),
            }
        )
    return {"rows": rows, "chosen_margin_ns": DEFAULT_MARGIN_NS}


def run_probe_transport(args: argparse.Namespace) -> dict[str, Any]:
    # The selection micro lives in the module docstring. Re-run the winner only
    # so the campaign host records the same socketpair path used for A/B.
    saved_count = args.count
    args.count = _count_for(args.rate, min(args.sample_seconds, 1.5), saved_count)
    row = run_qualify(args)
    args.count = saved_count
    row["transport"] = "socketpair SOCK_DGRAM"
    return row


def _arm_order(kind: str, pairs: int, cycles: int) -> list[str]:
    if kind == "aa":
        return ["A", "A"] * pairs
    order: list[str] = []
    for _ in range(cycles):
        order.extend(("A", "B", "B", "A"))
    return order


def run_campaign(args: argparse.Namespace, kind: str) -> dict[str, Any]:
    if args.base_root is None:
        raise RuntimeError("--base-root is required for A/A and A/B")
    broker: subprocess.Popen[str] | None = None
    if args.start_broker:
        broker = _start_broker(args.port, args.broker_cpu)
        args.broker_pid = broker.pid
    rates = parse_rates(args.rates)
    payload: dict[str, Any] = {
        "kind": kind,
        "host": host_info(),
        "protocol": args.protocol,
        "window": args.window,
        "payload_bytes": args.payload_bytes,
        "safety_margin_ns": args.safety_margin_ns,
        "base_root": str(args.base_root),
        "candidate_root": str(args.candidate_root),
        "cells": [],
    }
    try:
        for rate in rates:
            args.rate = rate
            args.count = _count_for(rate, args.sample_seconds, 0)
            samples: list[dict[str, Any]] = []
            for arm in _arm_order(kind, args.pairs, args.cycles):
                root = args.base_root if arm == "A" else args.candidate_root
                sample = run_mqtt_sample(args, root, arm)
                samples.append(sample)
                print(
                    f"{kind} {rate:.0f} {arm} "
                    f"ack50={sample['ack_ms']['p50']:.3f} "
                    f"d50={sample['delivery_ms']['p50']:.3f} "
                    f"cpu={sample['publisher_cpu_us_per_msg']:.1f}",
                    flush=True,
                )
            by_arm = {
                "A": [row for row in samples if row["arm"] == "A"],
                "B": [row for row in samples if row["arm"] == "B"],
            }
            summary_a = _summarize_arm(by_arm["A"])
            summary_b = _summarize_arm(by_arm["B"]) if by_arm["B"] else {}
            cell = {
                "rate": rate,
                "count": args.count,
                "samples": samples,
                "A": summary_a,
                "B": summary_b,
            }
            if summary_b:
                cell["delta_pct"] = {
                    key: _delta_pct(summary_a[key], summary_b[key]) for key in summary_a
                }
            payload["cells"].append(cell)
    finally:
        if broker is not None and broker.poll() is None:
            # Leave the broker running for follow-up cells; only stop if it is
            # a campaign-owned process and the caller asked for a one-shot run.
            pass
    if args.output is not None:
        args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.mode == "host-info":
        print(json.dumps(host_info(), indent=2))
        return 0
    if args.mode == "calibrate":
        print(json.dumps(run_calibrate(args), indent=2))
        return 0
    if args.mode == "pacer":
        pacer_worker(args)
        return 0
    if args.mode == "receiver":
        receiver_worker(args)
        return 0
    if args.mode == "publisher":
        publisher_worker(args)
        return 0
    if args.mode == "qualify":
        if not args.count:
            args.count = _count_for(args.rate, args.sample_seconds, 0)
        row = run_qualify(args)
        print(json.dumps(row))
        return 0
    if args.mode == "probe-transport":
        print(json.dumps(run_probe_transport(args), indent=2))
        return 0
    if args.mode in {"aa", "ab"}:
        payload = run_campaign(args, args.mode)
        print(json.dumps({k: v for k, v in payload.items() if k != "cells"}))
        return 0
    raise RuntimeError(f"unknown mode {args.mode}")


if __name__ == "__main__":
    raise SystemExit(main())
