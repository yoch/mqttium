"""Paired benchmark for inbound QoS protocol-response writer latency.

The engine produces one real PUBACK, PUBREC and PUBCOMP effect per cycle.  The
runtime applies each effect while the ordinary producer eager path is
deliberately disarmed, modelling a response generated after a write in the same
reader turn.  The old path must wake the writer task; the response-aware path
may append to the transport synchronously when all FIFO guards allow it.

Every worker also races a response against an in-flight segmented write before
the timed phase.  That invariant check is deliberately outside the timing
window: a faster result is invalid if the response can split header and payload.

Run an A/A control before A/B on the same quiet runner::

    python benchmarks/paired_protocol_responses.py \
      --base-root base --candidate-root base --repeat 8 --policy strict
    python benchmarks/paired_protocol_responses.py \
      --base-root base --candidate-root candidate --repeat 8 --policy strict

The parent alternates fresh source-isolated worker processes.  A strict run can
also consume the JSON emitted by ``runner_probe.py`` through
``--preflight-report``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


_EXPECTED_TYPES = ("PUBACK", "PUBREC", "PUBCOMP")


@dataclass(slots=True)
class ResponseResult:
    count: int
    elapsed_seconds: float
    completed_rate: float
    cpu_seconds: float
    latency_p50_us: float
    latency_p95_us: float
    latency_p99_us: float
    immediate_writes: int
    queued_writes: int
    marked_effect_types: list[str]
    wire_packet_types: list[str]
    fifo_segment_check: bool


class _RecordingTransport:
    def __init__(self) -> None:
        self.written: list[bytes] = []

    def write_nowait(self, data: bytes) -> bool:
        self.written.append(data)
        return True

    async def write(self, data: bytes) -> None:
        # StreamTransport.write normally completes without suspending while its
        # buffer remains below high-water.  The writer-task wakeup, not an
        # artificial sleep in this transport, is what this benchmark measures.
        self.written.append(data)

    async def write_many(self, parts: list[bytes]) -> None:
        self.written.extend(parts)

    async def read(self, n: int = 65536) -> bytes:
        return b""

    async def close(self) -> None:
        return None

    def is_closing(self) -> bool:
        return False


class _SegmentGateTransport(_RecordingTransport):
    def __init__(self) -> None:
        super().__init__()
        self.header_started = asyncio.Event()
        self.release_header = asyncio.Event()

    async def write(self, data: bytes) -> None:
        self.written.append(data)
        if data == b"segmented-header":
            self.header_started.set()
            await self.release_header.wait()


async def _no_failure(exc: BaseException) -> None:
    raise AssertionError(f"unexpected writer failure: {exc!r}")


def _percentile(ordered: list[float], q: float) -> float:
    if not ordered:
        return 0.0
    rank = q * (len(ordered) - 1)
    lo = int(rank)
    hi = min(lo + 1, len(ordered) - 1)
    fraction = rank - lo
    return ordered[lo] * (1.0 - fraction) + ordered[hi] * fraction


def _feed(engine: Any, wire: bytes) -> None:
    from mqttium.codec.buffer import IncrementalDecoder

    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    if raw is None:
        raise AssertionError("benchmark packet did not decode")
    engine.handle_raw(raw)


def _take_send(engine: Any, expected_type: Any) -> Any:
    from mqttium.protocol.effects import EffectKind

    effects = engine.take_effects()
    response_kind = getattr(EffectKind, "SEND_PROTOCOL_RESPONSE", EffectKind.SEND)
    sends = [effect for effect in effects if effect.kind in (EffectKind.SEND, response_kind)]
    if len(sends) != 1:
        raise AssertionError(f"expected one SEND effect, got {len(sends)}")
    effect = sends[0]
    data = effect.data
    if not isinstance(data, bytes) or data[0] & 0xF0 != expected_type:
        raise AssertionError(f"expected {expected_type.name}, got {data!r}")
    return effect


def _build_response_effects() -> list[Any]:
    """Exercise each engine transition that owns a protocol response."""
    from mqttium.enums import MQTTProtocolVersion, PacketType, QoS
    from mqttium.packets import PubRelPacket, PublishPacket, encode_frame
    from mqttium.protocol.config import EngineConfig
    from mqttium.protocol.engine import ProtocolEngine

    protocol = MQTTProtocolVersion.MQTTv5
    engine = ProtocolEngine(EngineConfig(client_id="paired-protocol-response", protocol=protocol))
    engine.begin_connect()
    _feed(engine, encode_frame(PacketType.CONNACK, 0, b"\x00\x00\x00"))
    engine.take_effects()

    inbound_qos1 = PublishPacket(
        topic="bench/response/qos1",
        payload=b"x",
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        dup=False,
        mid=101,
    )
    _feed(engine, inbound_qos1.encode(protocol))
    puback = _take_send(engine, PacketType.PUBACK)

    inbound_qos2 = PublishPacket(
        topic="bench/response/qos2",
        payload=b"x",
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        dup=False,
        mid=102,
    )
    _feed(engine, inbound_qos2.encode(protocol))
    pubrec = _take_send(engine, PacketType.PUBREC)
    _feed(engine, PubRelPacket(mid=102).encode(protocol))
    pubcomp = _take_send(engine, PacketType.PUBCOMP)

    # Keep the wire-order sequence stable and visible in every artefact.
    return [puback, pubrec, pubcomp]


def _response_admitter(pump: Any):
    return getattr(pump, "try_enqueue_protocol_response", pump.try_enqueue)


async def _assert_segmented_fifo(effect: Any) -> None:
    """A protocol response must remain behind both halves of a segmented item."""
    from mqttium.api._writer import WritePump

    pump = WritePump(max_bytes=1 << 20, max_messages=16, on_failure=_no_failure)
    transport = _SegmentGateTransport()
    pump.start(transport)
    try:
        pump._eager_armed = False
        if not pump.try_enqueue((b"segmented-header", b"segmented-payload")):
            raise AssertionError("segmented prelude was refused")
        await asyncio.wait_for(transport.header_started.wait(), timeout=1.0)
        if not _response_admitter(pump)(effect.data):
            raise AssertionError("protocol response was refused")
        if transport.written != [b"segmented-header"]:
            raise AssertionError("protocol response overtook an in-flight segment")
        transport.release_header.set()
        await asyncio.wait_for(pump.join(), timeout=1.0)
        expected = [b"segmented-header", b"segmented-payload", effect.data]
        if transport.written != expected:
            raise AssertionError(f"segmented wire order changed: {transport.written!r}")
    finally:
        transport.release_header.set()
        await pump.stop()


async def _run_phase(count: int, *, turn_batch: int = 1) -> ResponseResult:
    from mqttium.api import AsyncClient
    from mqttium.enums import PacketType
    from mqttium.protocol.effects import EffectKind

    effects = _build_response_effects()
    response_kind = getattr(EffectKind, "SEND_PROTOCOL_RESPONSE", EffectKind.SEND)
    packet_types = [PacketType.from_byte(effect.data[0]).name for effect in effects]
    if tuple(packet_types) != _EXPECTED_TYPES:
        raise AssertionError(f"response corpus changed: {packet_types!r}")
    marked = [
        packet_type
        for packet_type, effect in zip(packet_types, effects, strict=True)
        if effect.kind is response_kind
    ]
    await _assert_segmented_fifo(effects[0])

    client = AsyncClient(client_id="paired-protocol-response-runtime")
    pump = client._write_pump
    transport = _RecordingTransport()
    pump.start(transport)
    latencies_us: list[float] = []
    immediate = 0
    cpu_started = time.process_time()
    started = time.perf_counter()
    try:
        for index in range(count):
            effect = effects[index % len(effects)]
            pump._eager_armed = False
            before = len(transport.written)
            response_started = time.perf_counter_ns()
            if not client._apply_effect_inline(effect, client._connection_epoch):
                raise AssertionError("protocol response did not apply inline")
            if len(transport.written) == before + 1:
                immediate += 1
            else:
                await pump.join()
            latencies_us.append((time.perf_counter_ns() - response_started) / 1_000.0)
            # Model paced request/response traffic: each isolated inbound
            # packet gets a loop turn in which the response permit can re-arm.
            # Burst coalescing is pinned separately by the writer tests.
            if (index + 1) % turn_batch == 0:
                await asyncio.sleep(0)
        elapsed = time.perf_counter() - started
        cpu_seconds = time.process_time() - cpu_started
        await pump.join()
    finally:
        await pump.stop()

    expected_wire = [effects[index % len(effects)].data for index in range(count)]
    if transport.written != expected_wire:
        raise AssertionError("protocol response wire FIFO changed")
    latencies_us.sort()
    return ResponseResult(
        count=count,
        elapsed_seconds=elapsed,
        completed_rate=count / max(elapsed, 1e-9),
        cpu_seconds=cpu_seconds,
        latency_p50_us=_percentile(latencies_us, 0.50),
        latency_p95_us=_percentile(latencies_us, 0.95),
        latency_p99_us=_percentile(latencies_us, 0.99),
        immediate_writes=immediate,
        queued_writes=count - immediate,
        marked_effect_types=marked,
        wire_packet_types=packet_types,
        fifo_segment_check=True,
    )


async def _sample(args: argparse.Namespace) -> ResponseResult:
    if args.warmup_count:
        await _run_phase(args.warmup_count)
    return await _run_phase(args.count)


def worker(args: argparse.Namespace) -> None:
    if args.cpu is not None:
        try:
            os.sched_setaffinity(0, {args.cpu})
        except (AttributeError, OSError) as exc:
            raise RuntimeError(f"cannot pin response worker to CPU {args.cpu}") from exc
    print(json.dumps(asdict(asyncio.run(_sample(args)))))


def _run_worker(script: Path, root: Path, args: argparse.Namespace) -> ResponseResult:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(root.resolve() / "src")
    command = [
        sys.executable,
        str(script),
        "--worker",
        "--warmup-count",
        str(args.warmup_count),
        "--count",
        str(args.count),
    ]
    if args.cpu is not None:
        command.extend(("--cpu", str(args.cpu)))
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            env=environment,
            timeout=args.timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"protocol-response worker timed out after {args.timeout:.1f}s") from exc
    if completed.returncode:
        diagnostic = (completed.stderr or completed.stdout or "no output").strip()
        raise RuntimeError(
            f"protocol-response worker exited {completed.returncode}: {diagnostic[-2000:]}"
        )
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    try:
        return ResponseResult(**json.loads(lines[-1]))
    except (IndexError, TypeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"malformed worker output: {completed.stdout[-2000:]!r}") from exc


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
        "failures": report.get("failures", []),
        "report": str(path),
    }


def parent(args: argparse.Namespace) -> int:
    script = Path(__file__).resolve()
    roots = {"base": args.base_root.resolve(), "candidate": args.candidate_root.resolve()}
    aa_control = roots["base"] == roots["candidate"]
    eligibility = _eligibility(args.preflight_report)
    payload: dict[str, object] = {
        "base_root": str(roots["base"]),
        "candidate_root": str(roots["candidate"]),
        "policy": args.policy,
        "eligibility": eligibility,
        "harness": {
            "mode": "engine_effect_to_write_pump_protocol_responses",
            "response_types": list(_EXPECTED_TYPES),
            "count": args.count,
            "warmup_count": args.warmup_count,
            "turn_batch": 256,
            "repeat": args.repeat,
            "cpu": args.cpu,
            "aa_control": aa_control,
        },
        "thresholds": {
            "max_cv": args.max_cv,
            "max_aa_ratio_deviation": args.max_aa_ratio_deviation,
            "min_rate_ratio": args.min_rate_ratio,
            "max_p50_ratio": args.max_p50_ratio,
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

    base_rates: list[float] = []
    candidate_rates: list[float] = []
    base_p50: list[float] = []
    candidate_p50: list[float] = []
    pairs = payload["pairs"]
    assert isinstance(pairs, list)
    try:
        for index in range(args.repeat):
            order = ("base", "candidate") if index % 2 == 0 else ("candidate", "base")
            measured = {variant: _run_worker(script, roots[variant], args) for variant in order}
            base = measured["base"]
            candidate = measured["candidate"]
            base_rates.append(base.completed_rate)
            candidate_rates.append(candidate.completed_rate)
            base_p50.append(base.latency_p50_us)
            candidate_p50.append(candidate.latency_p50_us)
            pairs.append(
                {
                    "order": list(order),
                    "base": asdict(base),
                    "candidate": asdict(candidate),
                    "candidate_over_base_rate": candidate.completed_rate / base.completed_rate,
                    "candidate_over_base_p50": candidate.latency_p50_us / base.latency_p50_us,
                }
            )
    except RuntimeError as exc:
        payload["status"] = "invalid"
        payload["invalidations"] = [str(exc)]
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        print(str(exc), file=sys.stderr)
        return 2 if args.policy == "strict" else 0

    rate_ratios = [
        candidate / base for base, candidate in zip(base_rates, candidate_rates, strict=True)
    ]
    p50_ratios = [candidate / base for base, candidate in zip(base_p50, candidate_p50, strict=True)]
    rate_ratio = statistics.median(rate_ratios)
    p50_ratio = statistics.median(p50_ratios)
    base_rate_cv = _cv(base_rates)
    candidate_rate_cv = _cv(candidate_rates)
    invalidations: list[str] = []
    regressions: list[str] = []
    if base_rate_cv > args.max_cv:
        invalidations.append(f"baseline rate CV {base_rate_cv:.2%} exceeds {args.max_cv:.2%}")
    if candidate_rate_cv > args.max_cv:
        invalidations.append(f"candidate rate CV {candidate_rate_cv:.2%} exceeds {args.max_cv:.2%}")
    if aa_control:
        if abs(rate_ratio - 1.0) > args.max_aa_ratio_deviation:
            invalidations.append(
                f"A/A rate ratio {rate_ratio:.4f} outside 1+/-{args.max_aa_ratio_deviation:.2%}"
            )
    else:
        candidate_samples = [pair["candidate"] for pair in pairs]
        if any(
            sample["marked_effect_types"] != list(_EXPECTED_TYPES) for sample in candidate_samples
        ):
            regressions.append("candidate engine did not mark all four protocol response types")
        if any(sample["immediate_writes"] != args.count for sample in candidate_samples):
            regressions.append("candidate left an idle protocol response behind the eager throttle")
        if rate_ratio < args.min_rate_ratio:
            regressions.append(
                f"candidate/base rate {rate_ratio:.4f} below {args.min_rate_ratio:.4f}"
            )
        if p50_ratio > args.max_p50_ratio:
            regressions.append(f"candidate/base p50 {p50_ratio:.4f} above {args.max_p50_ratio:.4f}")

    payload["summary"] = {
        "median_candidate_over_base_rate": rate_ratio,
        "median_candidate_over_base_p50": p50_ratio,
        "base_rate_cv": base_rate_cv,
        "candidate_rate_cv": candidate_rate_cv,
        "pairs_favouring_candidate_rate": sum(ratio > 1.0 for ratio in rate_ratios),
        "pairs_favouring_candidate_p50": sum(ratio < 1.0 for ratio in p50_ratios),
    }
    payload["invalidations"] = invalidations
    payload["regressions"] = regressions
    payload["status"] = "invalid" if invalidations else "regressed" if regressions else "passed"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(
        f"protocol responses candidate/base rate={rate_ratio:.4f} p50={p50_ratio:.4f} "
        f"base_cv={base_rate_cv:.2%} candidate_cv={candidate_rate_cv:.2%}",
        flush=True,
    )
    if args.policy == "strict" and (invalidations or regressions):
        return 2
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--base-root", type=Path)
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--warmup-count", type=int, default=3_999)
    parser.add_argument("--count", type=int, default=99_999)
    parser.add_argument("--repeat", type=int, default=8)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--policy", choices=("advisory", "strict"), default="advisory")
    parser.add_argument("--preflight-report", type=Path)
    parser.add_argument("--max-cv", type=float, default=0.05)
    parser.add_argument("--max-aa-ratio-deviation", type=float, default=0.03)
    parser.add_argument("--min-rate-ratio", type=float, default=1.10)
    parser.add_argument("--max-p50-ratio", type=float, default=0.85)
    parser.add_argument("--output", type=Path, default=Path("/tmp/paired-protocol-responses.json"))
    args = parser.parse_args()
    if not args.worker and (args.base_root is None or args.candidate_root is None):
        parser.error("--base-root and --candidate-root are required")
    if args.repeat <= 0 or args.repeat % 2:
        parser.error("--repeat must be a positive even count (complete ABBA cycles)")
    corpus_size = len(_EXPECTED_TYPES)
    if args.warmup_count < 0 or args.count <= 0 or args.count % corpus_size:
        parser.error("--count must be a positive multiple of three; warmup may be zero")
    if args.warmup_count % corpus_size:
        parser.error("--warmup-count must be a multiple of three")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.max_cv < 0 or args.max_aa_ratio_deviation < 0:
        parser.error("CV and A/A deviation thresholds must be non-negative")
    return args


if __name__ == "__main__":
    arguments = parse_args()
    if arguments.worker:
        worker(arguments)
    else:
        raise SystemExit(parent(arguments))
