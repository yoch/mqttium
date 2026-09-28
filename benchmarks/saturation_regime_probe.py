#!/usr/bin/env python3
"""Explain why one commit's saturated throughput settles on distinct levels (#493).

On the dedicated Pi 5, the closed-loop capacity of the same code lands on
distinct levels (about 20.5k, 22.3k and 26k msgs/s for 64 B QoS 1) at a
constant CPU frequency, and fixed-rate cells above capacity inherit that
dispersion. Paired A/A controls then fail their equivalence budget. This probe
runs one runtime many times per cell and records, around each unchanged
`paired_open_loop.py` worker sample, what the client, the broker, the kernel
network path and TCP were doing:

- worker (publisher plus its subscriber) CPU and context switches, from
  `RUSAGE_CHILDREN`;
- broker CPU and context switches, from its process;
- per-CPU `NET_RX` / `NET_TX` softirq counts;
- one `ss -tin` snapshot of the broker connections mid-sample;
- per-CPU frequency before and during the sample.

`--summarise` groups each cell's samples into throughput levels and ranks the
indicators by how cleanly they separate those levels. Diagnostic only: it
changes no runtime and asserts no A/B claim.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import resource
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
ENGINE = SCRIPT_DIR / "paired_open_loop.py"
# A throughput gap between sorted samples larger than this share of the cell
# median starts a new level.
DEFAULT_LEVEL_GAP = 0.02
SEPARATION_FLOOR = 0.01
_UNIT = {"": 1.0, "K": 1e3, "M": 1e6, "G": 1e9}


@dataclass(frozen=True)
class Cell:
    protocol: str
    payload_bytes: int
    rate: float

    @property
    def key(self) -> str:
        rate = "unpaced" if self.rate <= 0 else f"{self.rate:.0f}"
        return f"protocol={self.protocol} payload={self.payload_bytes} rate={rate}"


# --- system readers ---------------------------------------------------------


def parse_softirqs(text: str) -> dict[str, list[int]]:
    """Per-CPU counts of the NET_RX and NET_TX rows of /proc/softirqs."""
    rows: dict[str, list[int]] = {}
    for line in text.splitlines():
        name, _, values = line.partition(":")
        name = name.strip()
        if name in ("NET_RX", "NET_TX"):
            rows[name] = [int(value) for value in values.split()]
    return rows


def read_softirqs(path: Path = Path("/proc/softirqs")) -> dict[str, list[int]]:
    try:
        return parse_softirqs(path.read_text(encoding="ascii"))
    except OSError:
        return {}


def _rate(value: str) -> float | None:
    match = re.fullmatch(r"([\d.]+)([KMG]?)bps", value)
    return float(match.group(1)) * _UNIT[match.group(2)] if match else None


def _ms(value: str) -> float | None:
    match = re.match(r"([\d.]+)ms", value)
    return float(match.group(1)) if match else None


def parse_ss(text: str) -> list[dict[str, float]]:
    """Numeric TCP_INFO fields of each connection in `ss -tinH` output."""
    connections: list[dict[str, float]] = []
    for line in text.splitlines():
        if not line.startswith((" ", "\t")):
            continue
        fields: dict[str, float] = {}
        tokens = line.split()
        for index, token in enumerate(tokens):
            key, sep, value = token.partition(":")
            if not sep:
                if token in ("send", "pacing_rate", "delivery_rate") and index + 1 < len(tokens):
                    rate = _rate(tokens[index + 1])
                    if rate is not None:
                        fields[token.replace("send", "send_rate")] = rate
                continue
            if key == "rtt":
                fields["rtt_ms"] = float(value.split("/")[0])
            elif key in ("busy", "rwnd_limited", "sndbuf_limited"):
                parsed = _ms(value)
                if parsed is not None:
                    fields[f"{key}_ms"] = parsed
            elif key in ("cwnd", "unacked", "notsent", "retrans", "lost", "rcv_space"):
                number = re.match(r"[\d.]+", value)
                if number:
                    fields[key] = float(number.group(0))
        if fields:
            connections.append(fields)
    return connections


def snapshot_ss(port: int) -> list[dict[str, float]]:
    command = ["ss", "-tinH", "state", "established", f"( sport = :{port} or dport = :{port} )"]
    try:
        completed = subprocess.run(command, capture_output=True, text=True, timeout=5, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return []
    return parse_ss(completed.stdout)


def read_frequencies() -> dict[str, int]:
    frequencies: dict[str, int] = {}
    for path in sorted(Path("/sys/devices/system/cpu").glob("cpu[0-9]*/cpufreq/scaling_cur_freq")):
        try:
            frequencies[path.parent.parent.name] = int(path.read_text(encoding="ascii"))
        except (OSError, ValueError):
            continue
    return frequencies


def broker_process(pid_file: Path | None) -> Any:
    try:
        import psutil
    except ImportError:  # pragma: no cover - the runner installs psutil
        return None
    if pid_file is not None:
        try:
            return psutil.Process(int(pid_file.read_text(encoding="ascii").strip()))
        except (OSError, ValueError, psutil.Error):
            return None
    for process in psutil.process_iter(["name"]):
        if process.info.get("name") == "mosquitto":
            return process
    return None


def _broker_counters(process: Any) -> dict[str, float]:
    if process is None:
        return {}
    try:
        times = process.cpu_times()
        switches = process.num_ctx_switches()
    except Exception:  # psutil errors are all diagnostic-only here
        return {}
    return {
        "cpu": times.user + times.system,
        "voluntary": switches.voluntary,
        "involuntary": switches.involuntary,
    }


def _children() -> dict[str, float]:
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return {
        "user": usage.ru_utime,
        "system": usage.ru_stime,
        "voluntary": usage.ru_nvcsw,
        "involuntary": usage.ru_nivcsw,
    }


# --- acquisition ------------------------------------------------------------


def sample_count(cell: Cell, args: argparse.Namespace) -> int:
    if cell.rate <= 0:
        return args.count_small if cell.payload_bytes <= 256 else args.count_large
    return max(1_000, min(args.max_count, math.ceil(cell.rate * args.sample_seconds)))


def acquire(cell: Cell, args: argparse.Namespace, broker: Any) -> dict[str, Any]:
    from paired_open_loop import _run_worker

    count = sample_count(cell, args)
    expected = count / cell.rate if cell.rate > 0 else args.sample_seconds
    mid: dict[str, Any] = {}

    def snapshot() -> None:
        time.sleep(min(max(expected / 2, 0.2), 30.0))
        mid["tcp"] = snapshot_ss(args.port)
        mid["frequencies"] = read_frequencies()

    before = {
        "children": _children(),
        "broker": _broker_counters(broker),
        "softirqs": read_softirqs(),
        "frequencies": read_frequencies(),
    }
    probe = threading.Thread(target=snapshot, daemon=True)
    probe.start()
    result = _run_worker(
        ENGINE,
        args.root,
        args,
        mode="calibrate" if cell.rate <= 0 else "sample",
        protocol=cell.protocol,
        payload_bytes=cell.payload_bytes,
        completion="receipt",
        window=args.window,
        count=count,
        target_rate=cell.rate,
    )
    probe.join(timeout=35)
    after = {
        "children": _children(),
        "broker": _broker_counters(broker),
        "softirqs": read_softirqs(),
    }
    return {
        "cell": cell.key,
        "protocol": cell.protocol,
        "payload_bytes": cell.payload_bytes,
        "rate": cell.rate,
        "count": count,
        "result": result,
        "system": {
            "children": _delta(before["children"], after["children"]),
            "broker": _delta(before["broker"], after["broker"]),
            "softirqs": {
                name: [
                    b - a for a, b in zip(before["softirqs"].get(name, []), values, strict=False)
                ]
                for name, values in after["softirqs"].items()
            },
            "frequencies_before": before["frequencies"],
            "frequencies_mid": mid.get("frequencies", {}),
            "tcp": mid.get("tcp", []),
        },
    }


def _delta(before: dict[str, float], after: dict[str, float]) -> dict[str, float]:
    return {key: after[key] - before[key] for key in after if key in before}


# --- analysis ---------------------------------------------------------------


def features(sample: dict[str, Any]) -> dict[str, float]:
    """Flatten one sample into per-message indicators."""
    result = sample["result"]
    system = sample["system"]
    count = float(sample["count"])
    out: dict[str, float] = {}

    def put(name: str, value: Any) -> None:
        if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
            out[name] = float(value)

    put("completed_rate", result.get("completed_rate"))
    target = result.get("target_rate") or 0.0
    if target:
        put("offered_over_target", result.get("offered_rate", 0.0) / target)
    for name in (
        "loop_lag_p95_ms",
        "ack_latency_p50_ms",
        "delivery_latency_p50_ms",
        "observer_join_tail_seconds",
        "pending_receipts_high_water",
    ):
        put(name, result.get(name))
    per_message = {
        "worker_cpu_us": result.get("cpu_seconds"),
        "writer_batches": result.get("writer_batches"),
        "writer_eager_writes": result.get("writer_eager_writes"),
        "writer_enqueue_suspensions": result.get("writer_enqueue_suspensions"),
        "effect_suspensions": result.get("effect_suspensions"),
    }
    for name, value in per_message.items():
        if isinstance(value, (int, float)):
            put(f"{name}_per_msg", value / count * (1e6 if name.endswith("_us") else 1.0))
    batches = result.get("writer_batches") or 0
    if batches:
        put("writer_items_per_batch", result.get("writer_batched_items", 0) / batches)
    children = system.get("children", {})
    put(
        "children_cpu_us_per_msg",
        (children.get("user", 0) + children.get("system", 0)) / count * 1e6,
    )
    put(
        "children_system_share",
        children.get("system", 0) / max(children.get("user", 0) + children.get("system", 0), 1e-9),
    )
    put("children_voluntary_per_msg", children.get("voluntary", 0) / count)
    put("children_involuntary_per_msg", children.get("involuntary", 0) / count)
    broker = system.get("broker", {})
    if broker:
        put("broker_cpu_us_per_msg", broker.get("cpu", 0) / count * 1e6)
        elapsed = result.get("measurement_seconds") or 0
        if elapsed:
            put("broker_busy_share", broker.get("cpu", 0) / elapsed)
        put("broker_voluntary_per_msg", broker.get("voluntary", 0) / count)
        put("broker_involuntary_per_msg", broker.get("involuntary", 0) / count)
    for name, values in system.get("softirqs", {}).items():
        put(f"{name.lower()}_per_msg", sum(values) / count)
        for cpu, value in enumerate(values[:4]):
            put(f"{name.lower()}_cpu{cpu}_per_msg", value / count)
    tcp = system.get("tcp", [])
    for key in (
        "cwnd",
        "rtt_ms",
        "unacked",
        "notsent",
        "delivery_rate",
        "busy_ms",
        "rwnd_limited_ms",
        "sndbuf_limited_ms",
    ):
        values = [connection[key] for connection in tcp if key in connection]
        if values:
            put(f"tcp_max_{key}", max(values))
    frequencies = list(system.get("frequencies_mid", {}).values())
    if frequencies:
        put("min_cpu_khz_mid", min(frequencies))
    return out


def split_levels(values: list[float], *, gap: float = DEFAULT_LEVEL_GAP) -> list[list[int]]:
    """Group sample indices into levels separated by relative throughput gaps."""
    if not values:
        return []
    order = sorted(range(len(values)), key=values.__getitem__)
    scale = statistics.median(values)
    levels = [[order[0]]]
    for previous, current in zip(order, order[1:], strict=False):
        if values[current] - values[previous] > gap * scale:
            levels.append([])
        levels[-1].append(current)
    return levels


def _mad(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    centre = statistics.median(values)
    return statistics.median(abs(value - centre) for value in values)


def separation(levels: list[list[dict[str, float]]], name: str) -> float | None:
    """Distance between the extreme levels' medians over their pooled spread."""
    groups = [[sample[name] for sample in level if name in sample] for level in levels]
    groups = [group for group in groups if group]
    if len(groups) < 2:
        return None
    low, high = groups[0], groups[-1]
    low_median, high_median = statistics.median(low), statistics.median(high)
    # A level of one or two samples has no usable spread: never divide by less
    # than 1 % of the larger median, so singletons cannot rank first by accident.
    floor = SEPARATION_FLOOR * max(abs(low_median), abs(high_median), 1e-12)
    spread = max(_mad(low), _mad(high), floor)
    return abs(high_median - low_median) / spread


def summarise(samples: list[dict[str, Any]], *, gap: float = DEFAULT_LEVEL_GAP) -> dict[str, Any]:
    by_cell: dict[str, list[dict[str, float]]] = {}
    for sample in samples:
        by_cell.setdefault(sample["cell"], []).append(features(sample))
    cells: list[dict[str, Any]] = []
    for key, rows in by_cell.items():
        rates = [row.get("completed_rate", math.nan) for row in rows]
        groups = split_levels(rates, gap=gap)
        levels = [[rows[index] for index in group] for group in groups]
        names = sorted({name for row in rows for name in row} - {"completed_rate"})
        ranked = sorted(
            ((name, score) for name in names if (score := separation(levels, name)) is not None),
            key=lambda item: item[1],
            reverse=True,
        )
        cells.append(
            {
                "cell": key,
                "samples": len(rows),
                "levels": [
                    {
                        "samples": len(level),
                        "completed_rate_median": statistics.median(
                            row["completed_rate"] for row in level
                        ),
                        "medians": {
                            name: statistics.median(row[name] for row in level if name in row)
                            for name in names
                            if any(name in row for row in level)
                        },
                    }
                    for level in levels
                ],
                "separating_indicators": [
                    {"indicator": name, "separation": score} for name, score in ranked[:12]
                ],
            }
        )
    return {"level_gap": gap, "cells": cells}


def markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Saturation regime probe (#493)",
        "",
        f"Levels split where sorted completed throughput jumps by more than "
        f"{summary['level_gap']:.0%} of the cell median. Separation is the distance "
        "between the lowest and highest levels' medians over their larger median "
        "absolute deviation.",
    ]
    for cell in summary["cells"]:
        lines += [
            "",
            f"## {cell['cell']} ({cell['samples']} samples)",
            "",
            "| Level | Samples | Completed msgs/s |",
            "| ---: | ---: | ---: |",
        ]
        for index, level in enumerate(cell["levels"], start=1):
            lines.append(f"| {index} | {level['samples']} | {level['completed_rate_median']:.0f} |")
        if len(cell["levels"]) > 1:
            lines += [
                "",
                "| Indicator | Separation | Lowest level | Highest level |",
                "| --- | ---: | ---: | ---: |",
            ]
            low, high = cell["levels"][0]["medians"], cell["levels"][-1]["medians"]
            for item in cell["separating_indicators"]:
                name = item["indicator"]
                lines.append(
                    f"| {name} | {item['separation']:.1f} | {low.get(name, math.nan):.4g} "
                    f"| {high.get(name, math.nan):.4g} |"
                )
    return "\n".join(lines) + "\n"


# --- command line -----------------------------------------------------------


def _cells(args: argparse.Namespace) -> list[Cell]:
    return [
        Cell(protocol, int(payload), float(rate))
        for protocol in args.protocols.split(",")
        for payload in args.payloads.split(",")
        for rate in args.rates.split(",")
    ]


def acquire_all(args: argparse.Namespace) -> list[dict[str, Any]]:
    broker = broker_process(args.broker_pid_file)
    cells = _cells(args)
    samples: list[dict[str, Any]] = []
    # Interleave cells so slow drift spreads over every cell instead of one.
    for round_index in range(args.samples):
        for cell in cells:
            record = acquire(cell, args, broker)
            record["round"] = round_index
            samples.append(record)
            completed = record["result"].get("completed_rate", math.nan)
            print(f"round={round_index} {cell.key} completed={completed:.0f}", flush=True)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(
                json.dumps({"root": str(args.root), "samples": samples}, indent=1) + "\n",
                encoding="utf-8",
            )
    return samples


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", type=Path, help="runtime source root to measure")
    parser.add_argument(
        "--summarise", type=Path, nargs="*", help="summarise retained probe JSON instead"
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11883)
    parser.add_argument("--protocols", default="311,5")
    parser.add_argument("--payloads", default="64,4096")
    parser.add_argument("--rates", default="0,20000,24000,26000", help="0 means unpaced")
    parser.add_argument("--samples", type=int, default=12)
    parser.add_argument("--window", type=int, default=100)
    parser.add_argument("--sample-seconds", type=float, default=2.0)
    parser.add_argument("--count-small", type=int, default=50_000)
    parser.add_argument("--count-large", type=int, default=40_000)
    parser.add_argument("--max-count", type=int, default=60_000)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--observer-retention", choices=("pending", "all"), default="pending")
    parser.add_argument("--broker-pid-file", type=Path)
    parser.add_argument("--level-gap", type=float, default=DEFAULT_LEVEL_GAP)
    parser.add_argument("--output", type=Path, default=Path("/tmp/saturation-regimes.json"))
    parser.add_argument("--summary-output", type=Path)
    args = parser.parse_args(argv)
    if args.summarise is None and args.root is None:
        parser.error("--root is required unless --summarise is given")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.summarise is not None:
        samples: list[dict[str, Any]] = []
        for path in args.summarise or [args.output]:
            samples.extend(json.loads(path.read_text(encoding="utf-8"))["samples"])
    else:
        sys.path.insert(0, str(SCRIPT_DIR))
        samples = acquire_all(args)
    summary = summarise(samples, gap=args.level_gap)
    text = markdown(summary)
    if args.summary_output is not None:
        args.summary_output.write_text(text, encoding="utf-8")
        args.summary_output.with_name(f"{args.summary_output.stem}-summary.json").write_text(
            json.dumps(summary, indent=2) + "\n", encoding="utf-8"
        )
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
