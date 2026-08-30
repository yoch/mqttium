"""Paired, source-isolated TCP measurements for inbound protocol responses.

Unlike :mod:`paired_protocol_responses`, this uses a real ``TcpTransport`` and
a packet-aware peer in a separate process.  It deliberately distinguishes the
three cases that matter to the eager design:

* ``idle``: an inbound QoS 1 PUBLISH while the ordinary eager state is ready;
* ``collision``: a QoS 0 callback publishes, then an inbound QoS 1 PUBLISH in
  the same broker write needs its PUBACK;
* ``qos2`` and ``burst``: the remaining QoS responses and the coalescing path.

The peer is a minimal MQTT 3.1.1 broker, not an in-memory transport.  It only
implements the packets required by this probe and is intentionally kept here
instead of production code.  Parent mode alternates fresh base/candidate
workers (ABBA) and emits one JSON artifact suitable for ARM64 runner evidence.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


_DEFAULT_TIMEOUT = 20.0
_PUBLISH = 0x30
_PUBACK = 0x40
_PUBREC = 0x50
_PUBREL = 0x60
_PUBCOMP = 0x70
_SUBSCRIBE = 0x80
_DISCONNECT = 0xE0


def _encode_remaining_length(value: int) -> bytes:
    encoded = bytearray()
    while True:
        digit = value % 128
        value //= 128
        if value:
            digit |= 0x80
        encoded.append(digit)
        if not value:
            return bytes(encoded)


def _publish(topic: str, payload: bytes, *, qos: int, mid: int | None = None) -> bytes:
    topic_bytes = topic.encode("utf-8")
    body = len(topic_bytes).to_bytes(2, "big") + topic_bytes
    if qos:
        if mid is None:
            raise ValueError("QoS PUBLISH requires a packet id")
        body += mid.to_bytes(2, "big")
    body += payload
    return bytes((_PUBLISH | (qos << 1),)) + _encode_remaining_length(len(body)) + body


async def _read_packet(reader: asyncio.StreamReader) -> tuple[int, bytes]:
    first = (await reader.readexactly(1))[0]
    multiplier = 1
    remaining = 0
    for _ in range(4):
        digit = (await reader.readexactly(1))[0]
        remaining += (digit & 0x7F) * multiplier
        if not digit & 0x80:
            return first, await reader.readexactly(remaining)
        multiplier *= 128
    raise RuntimeError("malformed MQTT remaining length")


def _percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    index = round((len(ordered) - 1) * fraction)
    return ordered[index]


def _pin(cpu: int | None) -> None:
    if cpu is None:
        return
    try:
        os.sched_setaffinity(0, {cpu})
    except (AttributeError, OSError) as exc:
        raise RuntimeError(f"cannot pin worker to CPU {cpu}") from exc


@dataclass(slots=True)
class PhaseResult:
    name: str
    count: int
    p50_us: float
    p95_us: float
    p99_us: float
    elapsed_seconds: float
    completed_per_second: float


async def _expect(
    reader: asyncio.StreamReader,
    *,
    packet_type: int,
    mid: int | None,
    timeout: float,
) -> None:
    """Read until the expected response; QoS 0 callback publishes are legal."""
    while True:
        first, body = await asyncio.wait_for(_read_packet(reader), timeout=timeout)
        kind = first & 0xF0
        if kind == packet_type:
            received_mid = int.from_bytes(body[:2], "big") if len(body) >= 2 else None
            if received_mid != mid:
                raise RuntimeError(f"expected mid {mid}, received {received_mid}")
            return
        if kind == _PUBLISH:
            # The collision phase deliberately causes this from the callback.
            continue
        if kind == _DISCONNECT:
            raise RuntimeError("client disconnected before protocol response")
        raise RuntimeError(f"unexpected client packet 0x{first:02x}")


async def _handshake(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, timeout: float
) -> None:
    first, _body = await asyncio.wait_for(_read_packet(reader), timeout=timeout)
    if first & 0xF0 != 0x10:
        raise RuntimeError(f"expected CONNECT, got 0x{first:02x}")
    writer.write(b"\x20\x02\x00\x00")
    await writer.drain()
    first, body = await asyncio.wait_for(_read_packet(reader), timeout=timeout)
    if first & 0xF0 != _SUBSCRIBE or len(body) < 2:
        raise RuntimeError(f"expected SUBSCRIBE, got 0x{first:02x}")
    writer.write(b"\x90\x03" + body[:2] + b"\x02")
    await writer.drain()


async def _wait_control(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    expected: bytes,
    timeout: float,
) -> None:
    while True:
        first, body = await asyncio.wait_for(_read_packet(reader), timeout=timeout)
        if first & 0xF0 != _PUBLISH:
            raise RuntimeError(f"expected control PUBLISH, got 0x{first:02x}")
        topic_length = int.from_bytes(body[:2], "big")
        payload = body[2 + topic_length :]
        if payload != expected:
            continue
        if expected == b"ready":
            writer.write(_publish("probe/control", b"ready", qos=0))
            await writer.drain()
        return


async def _measure_qos1(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    name: str,
    count: int,
    collision: bool,
    timeout: float,
) -> PhaseResult:
    latencies: list[float] = []
    started = time.perf_counter()
    for mid in range(1, count + 1):
        if collision:
            wire = _publish("probe/inbound", b"trigger", qos=0) + _publish(
                "probe/inbound", b"collision", qos=1, mid=mid
            )
        else:
            wire = _publish("probe/inbound", b"idle", qos=1, mid=mid)
        writer.write(wire)
        await writer.drain()
        sent_ns = time.perf_counter_ns()
        await _expect(reader, packet_type=_PUBACK, mid=mid, timeout=timeout)
        latencies.append((time.perf_counter_ns() - sent_ns) / 1_000.0)
    elapsed = time.perf_counter() - started
    return PhaseResult(
        name=name,
        count=count,
        p50_us=_percentile(latencies, 0.50),
        p95_us=_percentile(latencies, 0.95),
        p99_us=_percentile(latencies, 0.99),
        elapsed_seconds=elapsed,
        completed_per_second=count / max(elapsed, 1e-9),
    )


async def _measure_qos2(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, count: int, timeout: float
) -> list[PhaseResult]:
    pubrec: list[float] = []
    pubcomp: list[float] = []
    started = time.perf_counter()
    for mid in range(1, count + 1):
        writer.write(_publish("probe/inbound", b"qos2", qos=2, mid=mid))
        await writer.drain()
        sent_ns = time.perf_counter_ns()
        await _expect(reader, packet_type=_PUBREC, mid=mid, timeout=timeout)
        pubrec.append((time.perf_counter_ns() - sent_ns) / 1_000.0)
        writer.write(b"\x62\x02" + mid.to_bytes(2, "big"))
        await writer.drain()
        sent_ns = time.perf_counter_ns()
        await _expect(reader, packet_type=_PUBCOMP, mid=mid, timeout=timeout)
        pubcomp.append((time.perf_counter_ns() - sent_ns) / 1_000.0)
    elapsed = time.perf_counter() - started
    return [
        PhaseResult(
            "qos2_pubrec",
            count,
            _percentile(pubrec, 0.50),
            _percentile(pubrec, 0.95),
            _percentile(pubrec, 0.99),
            elapsed,
            count / max(elapsed, 1e-9),
        ),
        PhaseResult(
            "qos2_pubcomp",
            count,
            _percentile(pubcomp, 0.50),
            _percentile(pubcomp, 0.95),
            _percentile(pubcomp, 0.99),
            elapsed,
            count / max(elapsed, 1e-9),
        ),
    ]


async def _measure_burst(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, size: int, timeout: float
) -> PhaseResult:
    wire = b"".join(
        _publish("probe/inbound", b"burst", qos=1, mid=mid) for mid in range(1, size + 1)
    )
    started = time.perf_counter()
    writer.write(wire)
    await writer.drain()
    sent_ns = time.perf_counter_ns()
    latencies: list[float] = []
    for mid in range(1, size + 1):
        await _expect(reader, packet_type=_PUBACK, mid=mid, timeout=timeout)
        latencies.append((time.perf_counter_ns() - sent_ns) / 1_000.0)
    elapsed = time.perf_counter() - started
    return PhaseResult(
        f"burst_{size}",
        size,
        _percentile(latencies, 0.50),
        _percentile(latencies, 0.95),
        _percentile(latencies, 0.99),
        elapsed,
        size / max(elapsed, 1e-9),
    )


async def broker(args: argparse.Namespace) -> None:
    _pin(args.cpu)
    complete: asyncio.Future[dict[str, object]] = asyncio.get_running_loop().create_future()

    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await _handshake(reader, writer, args.timeout)
            await _wait_control(reader, writer, b"ready", args.timeout)
            await _wait_control(reader, writer, b"start", args.timeout)
            phases = [
                await _measure_qos1(
                    reader,
                    writer,
                    name="idle",
                    count=args.count,
                    collision=False,
                    timeout=args.timeout,
                ),
                await _measure_qos1(
                    reader,
                    writer,
                    name="collision",
                    count=args.count,
                    collision=True,
                    timeout=args.timeout,
                ),
                *await _measure_qos2(reader, writer, args.count, args.timeout),
                await _measure_burst(reader, writer, 16, args.timeout),
                await _measure_burst(reader, writer, 64, args.timeout),
            ]
            writer.write(_publish("probe/control", b"done", qos=0))
            await writer.drain()
            complete.set_result({"phases": [asdict(phase) for phase in phases]})
        except BaseException as exc:
            if not complete.done():
                complete.set_exception(exc)
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handler, "127.0.0.1", args.port)
    port = server.sockets[0].getsockname()[1]
    Path(args.ready).write_text(json.dumps({"port": port}) + "\n", encoding="utf-8")
    try:
        result = await asyncio.wait_for(complete, timeout=args.timeout + args.count * 0.01)
    finally:
        server.close()
        await server.wait_closed()
    Path(args.result).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


async def client(args: argparse.Namespace) -> None:
    _pin(args.cpu)
    from mqttium.api import AsyncClient
    from mqttium.enums import QoS
    from mqttium.protocol.reconnect import ReconnectPolicy

    ready = asyncio.Event()
    done = asyncio.Event()
    client = AsyncClient(
        client_id=f"response-socket-{os.getpid()}", reconnect=ReconnectPolicy(enabled=False)
    )

    def on_message(message: Any) -> None:
        if message.payload == b"trigger":
            client.publish_nowait("probe/outbound", b"x", qos=QoS.AT_MOST_ONCE)
        elif message.payload == b"ready":
            ready.set()
        elif message.payload == b"done":
            done.set()

    client.on_message = on_message
    await client.connect("127.0.0.1", args.port, timeout=args.timeout)
    await client.subscribe("probe/#", qos=QoS.EXACTLY_ONCE, timeout=args.timeout)
    client.publish_nowait("probe/control", b"ready", qos=QoS.AT_MOST_ONCE)
    await asyncio.wait_for(ready.wait(), timeout=args.timeout)
    client.publish_nowait("probe/control", b"start", qos=QoS.AT_MOST_ONCE)
    baseline = client._write_pump.stats()
    await asyncio.wait_for(done.wait(), timeout=args.timeout * (args.count * 3 + 10))
    end = client._write_pump.stats()
    result = {
        "eager_writes": end.eager_writes - baseline.eager_writes,
        "batched_items": end.batched_items - baseline.batched_items,
        "enqueue_suspensions": end.enqueue_suspensions - baseline.enqueue_suspensions,
    }
    try:
        await client.disconnect()
    except Exception:
        # The probe peer closes as soon as it has emitted the completion marker.
        # A terminal transport loss after that marker cannot invalidate the
        # already-collected response measurements.
        pass
    Path(args.result).write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


def _run_sample(script: Path, root: Path, args: argparse.Namespace) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="mqttium-response-socket-") as temp:
        directory = Path(temp)
        ready = directory / "ready.json"
        broker_result = directory / "broker.json"
        client_result = directory / "client.json"
        broker_command = [
            sys.executable,
            str(script),
            "--broker",
            "--port",
            "0",
            "--ready",
            str(ready),
            "--result",
            str(broker_result),
            "--count",
            str(args.count),
            "--timeout",
            str(args.timeout),
        ]
        if args.broker_cpu is not None:
            broker_command.extend(("--cpu", str(args.broker_cpu)))
        broker_process = subprocess.Popen(
            broker_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        deadline = time.monotonic() + args.timeout
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        if not ready.exists():
            broker_process.kill()
            _out, error = broker_process.communicate()
            raise RuntimeError(f"broker did not become ready: {error[-2000:]}")
        port = int(json.loads(ready.read_text(encoding="utf-8"))["port"])
        env = os.environ.copy()
        env["PYTHONPATH"] = str(root / "src")
        client_command = [
            sys.executable,
            str(script),
            "--client",
            "--port",
            str(port),
            "--result",
            str(client_result),
            "--count",
            str(args.count),
            "--timeout",
            str(args.timeout),
        ]
        if args.client_cpu is not None:
            client_command.extend(("--cpu", str(args.client_cpu)))
        worker_timeout = args.timeout + args.count * 0.01 + 10.0
        client_process = subprocess.run(
            client_command,
            capture_output=True,
            text=True,
            env=env,
            timeout=worker_timeout,
        )
        _out, broker_error = broker_process.communicate(timeout=worker_timeout)
        if client_process.returncode:
            raise RuntimeError(
                f"client failed: {(client_process.stderr or client_process.stdout)[-2000:]}"
            )
        if broker_process.returncode:
            raise RuntimeError(f"broker failed: {broker_error[-2000:]}")
        return {
            "broker": json.loads(broker_result.read_text(encoding="utf-8")),
            "client": json.loads(client_result.read_text(encoding="utf-8")),
        }


def _phase(sample: dict[str, object], name: str) -> dict[str, object]:
    broker = sample["broker"]
    assert isinstance(broker, dict)
    phases = broker["phases"]
    assert isinstance(phases, list)
    for phase in phases:
        assert isinstance(phase, dict)
        if phase["name"] == name:
            return phase
    raise AssertionError(f"missing phase {name}")


def _cv(values: list[float]) -> float:
    mean = statistics.fmean(values)
    return statistics.stdev(values) / mean if len(values) > 1 and mean else 0.0


def _eligibility(path: Path | None) -> dict[str, object]:
    if path is None:
        return {"eligible": None, "failures": ["no runner preflight supplied"]}
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {"eligible": False, "failures": [f"cannot read runner preflight: {exc}"]}
    return {
        "eligible": report.get("eligible") is True,
        "failures": list(report.get("failures", [])),
        "report": str(path),
    }


def parent(args: argparse.Namespace) -> int:
    script = Path(__file__).resolve()
    roots = {"base": args.base_root.resolve(), "candidate": args.candidate_root.resolve()}
    aa_control = roots["base"] == roots["candidate"]
    eligibility = _eligibility(args.preflight_report)
    payload: dict[str, object] = {
        "repeat": args.repeat,
        "count": args.count,
        "policy": args.policy,
        "aa_control": aa_control,
        "eligibility": eligibility,
        "thresholds": {
            "max_cv": args.max_cv,
            "max_aa_ratio_deviation": args.max_aa_ratio_deviation,
            "collision_max_p50_ratio": args.collision_max_p50_ratio,
            "collision_min_delta_us": args.collision_min_delta_us,
            "idle_max_ratio_deviation": args.idle_max_ratio_deviation,
            "max_tail_ratio": args.max_tail_ratio,
        },
        "pairs": [],
        "invalidations": [],
        "regressions": [],
        "status": "running",
    }
    if args.policy == "strict" and eligibility["eligible"] is not True:
        payload["status"] = "invalid"
        payload["invalidations"] = list(eligibility["failures"])
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        return 2
    pairs: list[dict[str, object]] = []
    for index in range(args.repeat):
        order = ("base", "candidate") if index % 2 == 0 else ("candidate", "base")
        measured = {variant: _run_sample(script, roots[variant], args) for variant in order}
        pairs.append({"order": list(order), **measured})
    payload["pairs"] = pairs
    summary: dict[str, object] = {}
    invalidations: list[str] = []
    regressions: list[str] = []
    for name in ("idle", "collision", "qos2_pubrec", "qos2_pubcomp", "burst_16", "burst_64"):
        base_p50 = [float(_phase(pair["base"], name)["p50_us"]) for pair in pairs]
        candidate_p50 = [float(_phase(pair["candidate"], name)["p50_us"]) for pair in pairs]
        ratios = [
            candidate / max(base, 1e-9)
            for base, candidate in zip(base_p50, candidate_p50, strict=True)
        ]
        p95_ratios = [
            float(_phase(pair["candidate"], name)["p95_us"])
            / max(float(_phase(pair["base"], name)["p95_us"]), 1e-9)
            for pair in pairs
        ]
        p99_ratios = [
            float(_phase(pair["candidate"], name)["p99_us"])
            / max(float(_phase(pair["base"], name)["p99_us"]), 1e-9)
            for pair in pairs
        ]
        ratio = statistics.median(ratios)
        base_median = statistics.median(base_p50)
        candidate_median = statistics.median(candidate_p50)
        summary[name] = {
            "median_candidate_over_base_p50": ratio,
            "base_median_p50_us": base_median,
            "candidate_median_p50_us": candidate_median,
            "p50_delta_us": base_median - candidate_median,
            "base_p50_cv": _cv(base_p50),
            "candidate_p50_cv": _cv(candidate_p50),
            "median_candidate_over_base_p95": statistics.median(p95_ratios),
            "median_candidate_over_base_p99": statistics.median(p99_ratios),
            "pairs_favouring_candidate": sum(ratio < 1.0 for ratio in ratios),
        }
        if _cv(base_p50) > args.max_cv or _cv(candidate_p50) > args.max_cv:
            invalidations.append(f"{name}: p50 variability exceeds {args.max_cv:.0%}")
        if aa_control and abs(ratio - 1.0) > args.max_aa_ratio_deviation:
            invalidations.append(f"{name}: A/A p50 ratio {ratio:.4f} is unstable")
        if (
            not aa_control
            and max(statistics.median(p95_ratios), statistics.median(p99_ratios))
            > args.max_tail_ratio
        ):
            regressions.append(f"{name}: p95/p99 ratio exceeds {args.max_tail_ratio:.4f}")

    if not aa_control:
        collision = summary["collision"]
        assert isinstance(collision, dict)
        if float(collision["median_candidate_over_base_p50"]) > args.collision_max_p50_ratio:
            regressions.append("collision: p50 gain is below the required 10%")
        if float(collision["p50_delta_us"]) < args.collision_min_delta_us:
            regressions.append("collision: p50 gain is below the required absolute delta")
        if int(collision["pairs_favouring_candidate"]) < args.min_favourable_pairs:
            regressions.append("collision: fewer than the required favourable pairs")
        idle = summary["idle"]
        assert isinstance(idle, dict)
        if abs(float(idle["median_candidate_over_base_p50"]) - 1.0) > args.idle_max_ratio_deviation:
            regressions.append("idle: candidate is outside the no-regression band")
        for pair in pairs:
            candidate = pair["candidate"]
            assert isinstance(candidate, dict)
            client = candidate["client"]
            assert isinstance(client, dict)
            if int(client["batched_items"]) < 78:
                regressions.append("burst: response frames did not return to writer coalescing")
                break
    payload["summary"] = summary
    payload["invalidations"] = invalidations
    payload["regressions"] = regressions
    payload["status"] = "invalid" if invalidations else "regressed" if regressions else "passed"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, sort_keys=True))
    return 2 if args.policy == "strict" and (invalidations or regressions) else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--broker", action="store_true")
    mode.add_argument("--client", action="store_true")
    parser.add_argument("--base-root", type=Path)
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--port", type=int)
    parser.add_argument("--ready", type=Path)
    parser.add_argument("--result", type=Path)
    parser.add_argument(
        "--output", type=Path, default=Path("/tmp/paired-protocol-response-socket.json")
    )
    parser.add_argument("--count", type=int, default=10_000)
    parser.add_argument("--repeat", type=int, default=8)
    parser.add_argument("--timeout", type=float, default=_DEFAULT_TIMEOUT)
    parser.add_argument("--policy", choices=("advisory", "strict"), default="advisory")
    parser.add_argument("--preflight-report", type=Path)
    parser.add_argument("--max-cv", type=float, default=0.10)
    parser.add_argument("--max-aa-ratio-deviation", type=float, default=0.05)
    parser.add_argument("--collision-max-p50-ratio", type=float, default=0.90)
    parser.add_argument("--collision-min-delta-us", type=float, default=5.0)
    parser.add_argument("--idle-max-ratio-deviation", type=float, default=0.05)
    parser.add_argument("--max-tail-ratio", type=float, default=1.05)
    parser.add_argument("--min-favourable-pairs", type=int, default=7)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--broker-cpu", type=int)
    parser.add_argument("--client-cpu", type=int)
    args = parser.parse_args()
    if args.count <= 0 or args.repeat <= 0 or args.repeat % 2:
        parser.error("count must be positive and repeat must be positive and even")
    if args.timeout <= 0:
        parser.error("timeout must be positive")
    if args.max_cv < 0 or args.max_aa_ratio_deviation < 0 or args.idle_max_ratio_deviation < 0:
        parser.error("variability and equivalence thresholds must be non-negative")
    if args.broker or args.client:
        if args.port is None or args.result is None:
            parser.error("workers require --port and --result")
        if args.broker and args.ready is None:
            parser.error("broker requires --ready")
    elif args.base_root is None or args.candidate_root is None:
        parser.error("parent mode requires --base-root and --candidate-root")
    return args


if __name__ == "__main__":
    arguments = parse_args()
    if arguments.broker:
        asyncio.run(broker(arguments))
    elif arguments.client:
        asyncio.run(client(arguments))
    else:
        raise SystemExit(parent(arguments))
