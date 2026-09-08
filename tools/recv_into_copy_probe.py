#!/usr/bin/env python3
"""Fresh-process allocator probe for recv_into() plus #445-style bytes copy.

This is diagnostic evidence, not a product benchmark.  Each child receives full
buffers through ``recv_into(..., MSG_WAITALL)`` and optionally materializes the
same immutable ``bytes(memoryview(buffer)[:nbytes])`` object that #445 creates in
``buffer_updated()``.  The parent repeats fresh processes under normal ASLR so a
layout-sensitive allocator regime is visible as a distribution.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import resource
import socket
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

SIZES = (65536, 98304, 122880, 131072, 163840, 262144)
MODES = ("recv_into", "recv_into_copy")


def usage() -> tuple[int, int, float, float]:
    r = resource.getrusage(resource.RUSAGE_SELF)
    return int(r.ru_minflt), int(r.ru_majflt), float(r.ru_stime), float(r.ru_utime)


def child(size: int, iterations: int, warmup: int, copy_chunk: bool) -> dict:
    tx, rx = socket.socketpair()
    payload = b"x" * size
    buf = bytearray(size)
    view = memoryview(buf)
    total = warmup + iterations
    sender_error: list[BaseException] = []

    def send_all() -> None:
        try:
            for _ in range(total):
                tx.sendall(payload)
        except BaseException as exc:  # diagnostic helper
            sender_error.append(exc)

    sender = threading.Thread(target=send_all, daemon=True)
    sender.start()
    checksum = 0

    def one() -> None:
        nonlocal checksum
        n = rx.recv_into(view, size, socket.MSG_WAITALL)
        if n != size:
            raise RuntimeError(f"short recv_into: {n} != {size}")
        if copy_chunk:
            chunk = bytes(view[:n])
            checksum ^= len(chunk) ^ chunk[0]
        else:
            checksum ^= n

    try:
        for _ in range(warmup):
            one()
        before = usage()
        started = time.perf_counter_ns()
        for _ in range(iterations):
            one()
        elapsed_ns = time.perf_counter_ns() - started
        after = usage()
    finally:
        rx.close()
        tx.close()
        sender.join(timeout=5)
    if sender_error:
        raise sender_error[0]

    return {
        "pid": os.getpid(),
        "size": size,
        "mode": "recv_into_copy" if copy_chunk else "recv_into",
        "iterations": iterations,
        "warmup": warmup,
        "ru_minflt": after[0] - before[0],
        "ru_majflt": after[1] - before[1],
        "ru_stime_s": after[2] - before[2],
        "ru_utime_s": after[3] - before[3],
        "minflt_per_op": (after[0] - before[0]) / iterations,
        "ns_per_op": elapsed_ns / iterations,
        "checksum": checksum,
        "aslr": Path("/proc/sys/kernel/randomize_va_space").read_text().strip(),
        "personality": Path("/proc/self/personality").read_text().strip(),
    }


def run_fresh(size: int, mode: str, iterations: int, warmup: int) -> dict:
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--child",
        "--size",
        str(size),
        "--iterations",
        str(iterations),
        "--warmup",
        str(warmup),
        "--mode",
        mode,
    ]
    completed = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return json.loads(completed.stdout)


def summarize(rows: list[dict]) -> dict:
    grouped: dict[str, dict] = {}
    for size in SIZES:
        for mode in MODES:
            subset = [r for r in rows if r["size"] == size and r["mode"] == mode]
            if not subset:
                continue
            faults = [float(r["minflt_per_op"]) for r in subset]
            times = [float(r["ns_per_op"]) for r in subset]
            key = f"{mode}:{size}"
            grouped[key] = {
                "n": len(subset),
                "fault_heavy_ge_0_5_per_op": sum(v >= 0.5 for v in faults),
                "minflt_per_op": {
                    "min": min(faults),
                    "median": statistics.median(faults),
                    "max": max(faults),
                },
                "ns_per_op": {
                    "min": min(times),
                    "median": statistics.median(times),
                    "max": max(times),
                },
            }
    return grouped


def parent(samples: int, iterations: int, warmup: int, seed: int) -> dict:
    plan = [(size, mode, sample) for size in SIZES for mode in MODES for sample in range(samples)]
    random.Random(seed).shuffle(plan)
    rows = []
    for index, (size, mode, sample) in enumerate(plan, 1):
        row = run_fresh(size, mode, iterations, warmup)
        row["sample"] = sample
        row["execution_index"] = index
        rows.append(row)
        print(
            f"{index:03d}/{len(plan)} {mode:14s} {size//1024:3d} KiB "
            f"minflt/op={row['minflt_per_op']:.4f} ns/op={row['ns_per_op']:.0f}",
            file=sys.stderr,
            flush=True,
        )
    return {
        "schema_version": 1,
        "probe": "recv_into_plus_bytes_copy",
        "official_benchmark": False,
        "samples_per_cell": samples,
        "iterations": iterations,
        "warmup": warmup,
        "sizes": list(SIZES),
        "modes": list(MODES),
        "summary": summarize(rows),
        "rows": rows,
    }


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--child", action="store_true")
    p.add_argument("--size", type=int, default=65536)
    p.add_argument("--mode", choices=MODES, default="recv_into_copy")
    p.add_argument("--samples", type=int, default=16)
    p.add_argument("--iterations", type=int, default=500)
    p.add_argument("--warmup", type=int, default=10)
    p.add_argument("--seed", type=int, default=445)
    p.add_argument("--output")
    args = p.parse_args()
    if args.child:
        payload = child(
            args.size,
            args.iterations,
            args.warmup,
            copy_chunk=args.mode == "recv_into_copy",
        )
    else:
        payload = parent(args.samples, args.iterations, args.warmup, args.seed)
    text = json.dumps(payload, indent=None if args.child else 2, sort_keys=True)
    if args.output and not args.child:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
