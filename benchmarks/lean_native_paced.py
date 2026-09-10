#!/usr/bin/env python3
"""Fixed-offered-load comparison with a separate process owning the clock.

Rates are frozen from the reference arm's long-lot A/A measurements. Each sample
retains planned arrival, pacer emission, call, admission-return and delivery
clocks. Admission-return is an observable API boundary, not an internal commit
hook: an early delivery is recorded rather than assigned a negative residence.
Raw results and profiles belong outside Git.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import itertools
import json
import math
import os
import socket
import struct
import subprocess
import sys
import time
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from external_pacer import TOKEN_STRUCT, configure_dgram, emit_tokens
from lean_native_compare import _git


def _percentiles(ns: list[int]) -> dict[str, float]:
    ordered = sorted(ns)
    return {
        f"p{p}_us": ordered[min(len(ordered) - 1, math.ceil(len(ordered) * p / 100) - 1)] / 1000
        for p in (50, 95, 99)
    }


async def _sample(args: argparse.Namespace) -> dict[str, Any]:  # noqa: C901 - one timed lifecycle
    from mqttium.api import AsyncClient
    from mqttium.enums import MQTTProtocolVersion

    spec = json.loads(args.scenario)
    count = args.count
    protocol = MQTTProtocolVersion.MQTTv5 if spec["protocol"] == 5 else MQTTProtocolVersion.MQTTv311
    topic = f"lean-paced/{os.getpid()}/{time.monotonic_ns()}"
    warmup = 32
    # seq, planned, emitted, called, admission-return, delivery, receipt-finished
    clocks = [[index, 0, 0, 0, 0, 0, 0] for index in range(count)]
    received = 0
    progress = asyncio.Event()
    errors: list[str] = []
    suffix = b"x" * 248

    def observe(message: Any) -> None:
        nonlocal received
        sequence = struct.unpack_from("!Q", message.payload)[0]
        if sequence != received or message.payload[8:] != suffix:
            errors.append(f"delivery {received}: unexpected sequence or payload {sequence}")
        if warmup <= sequence < warmup + count:
            clocks[sequence - warmup][5] = time.monotonic_ns()
        received += 1
        progress.set()

    async def wait_received(total: int) -> None:
        while received < total:
            progress.clear()
            await progress.wait()
        if errors:
            raise AssertionError(errors)

    client = AsyncClient(
        f"lean-paced-{os.getpid()}",
        protocol=protocol,
        message_delivery=spec["mode"],
        max_outbound_inflight=20,
        max_pending_outbound_messages=10_000,
        max_pending_outbound_bytes=64 * 1024**2,
        max_outbound_messages=10_000,
        max_outbound_bytes=1024**2,
        max_pending_messages=1024,
        max_pending_callbacks=1024,
        max_pending_delivery_bytes=64 * 1024**2,
        delivery_timeout=5,
        keepalive=0,
    )
    if spec["mode"] == "callback":
        client.on_message = observe
    consumer = None
    local = remote = None
    pacer = None
    loop = asyncio.get_running_loop()
    prior_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _, context: errors.append(str(context)))

    async def consume() -> None:
        async for message in client.messages():
            observe(message)

    try:
        await client.connect(args.host, args.port, timeout=5)
        if spec["mode"] == "iterator":
            consumer = asyncio.create_task(consume())
        suback = await client.subscribe(topic, qos=spec["qos"])
        if list(suback.reason_codes) != [spec["qos"]]:
            raise AssertionError("unexpected subscription grant")
        for seq in range(warmup):
            receipt = await client.publish(topic, struct.pack("!Q", seq) + suffix, qos=spec["qos"])
            await receipt.wait()
            await wait_received(seq + 1)
        local, remote = socket.socketpair(type=socket.SOCK_DGRAM)
        configure_dgram(local)
        configure_dgram(remote)
        local.setblocking(False)
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "--pacer-fd",
            str(remote.fileno()),
            "--count",
            str(count),
            "--rate",
            str(args.rate),
            "--pacer-cpu",
            str(args.pacer_cpu),
        ]
        pacer = subprocess.Popen(
            command,
            pass_fds=(remote.fileno(),),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        remote.close()
        remote = None
        cpu_start = time.process_time_ns()
        local.send(b"R")
        pending = deque()
        for seq in range(count):
            token = await loop.sock_recv(local, TOKEN_STRUCT.size)
            actual, planned, emitted = TOKEN_STRUCT.unpack(token)
            if actual != seq:
                raise AssertionError(f"pacer sequence {actual} != {seq}")
            row = clocks[seq]
            row[1:3] = [planned, emitted]
            row[3] = time.monotonic_ns()
            receipt = await client.publish(
                topic, struct.pack("!Q", seq + warmup) + suffix, qos=spec["qos"]
            )
            row[4] = time.monotonic_ns()
            pending.append((receipt, row))
            # One bounded observer loop; no task per receipt and no pacing sleep
            # in the client loop. A completed receipt is polled immediately.
            while pending and pending[0][0].is_done():
                done, submitted = pending.popleft()
                await done.wait()
                submitted[6] = time.monotonic_ns()
        for receipt, row in pending:
            await receipt.wait()
            row[6] = time.monotonic_ns()
        await wait_received(warmup + count)
        await client._write_pump.join()
        elapsed_ns = max(row[5] for row in clocks) - clocks[0][1]
        cpu_ns = time.process_time_ns() - cpu_start
        if received != warmup + count or errors:
            raise AssertionError(errors or "delivery count mismatch")
        if any(
            not (0 < row[1] <= row[2] <= row[3] <= row[4] <= row[6]) or row[5] < row[3]
            for row in clocks
        ):
            raise AssertionError("missing or inconsistent message clocks")
        if client.stats().outbound.pending_messages or client.stats().inbound.inflight:
            raise AssertionError("pending protocol state after completion")
        # The pacer has emitted every token before the last receive; reading its
        # final tiny report cannot delay a timed publication or delivery.
        stdout, stderr = pacer.communicate(timeout=10)
        if pacer.returncode:
            raise RuntimeError(stderr)
        pacer_result = json.loads(stdout)
        if pacer_result["emitted"] != count or pacer_result["lost_sends"]:
            raise AssertionError("pacer did not deliver the prescribed load")
        return {
            "count": count,
            "delivered": received - warmup,
            "target_rate": args.rate,
            "elapsed_s": elapsed_ns / 1e9,
            "completed_per_s": count / (elapsed_ns / 1e9),
            "cpu_us_per_message": cpu_ns / count / 1000,
            "scheduled_to_call": _percentiles([row[3] - row[1] for row in clocks]),
            "call_to_admission_return": _percentiles([row[4] - row[3] for row in clocks]),
            "residual_after_return": _percentiles([max(0, row[5] - row[4]) for row in clocks]),
            "scheduled_to_delivery": _percentiles([row[5] - row[1] for row in clocks]),
            "early_delivery_count": sum(row[5] < row[4] for row in clocks),
            "pacer": pacer_result,
            "clocks_ns": clocks,
        }
    finally:
        await client.disconnect()
        if consumer is not None:
            if not consumer.done():
                consumer.cancel()
            try:
                await consumer
            except asyncio.CancelledError:
                pass
        if local is not None:
            local.close()
        if remote is not None:
            remote.close()
        if pacer is not None and pacer.poll() is None:
            pacer.terminate()
            try:
                pacer.communicate(timeout=3)
            except subprocess.TimeoutExpired:
                pacer.kill()
                pacer.communicate()
        loop.set_exception_handler(prior_handler)


def _run_worker(args, root, spec, count, rate):
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--source-root",
        str(root),
        "--scenario",
        json.dumps(spec),
        "--count",
        str(count),
        "--rate",
        str(rate),
        "--cpu",
        str(args.cpu),
        "--pacer-cpu",
        str(args.pacer_cpu),
        "--host",
        args.host,
        "--port",
        str(args.port),
    ]
    result = subprocess.run(
        command,
        cwd="/tmp",
        text=True,
        capture_output=True,
        timeout=150,
        env={**os.environ, "PYTHONHASHSEED": "0", "PYTHONDONTWRITEBYTECODE": "1"},
    )
    if result.returncode:
        raise RuntimeError(f"{root} {spec}: {result.stderr}")
    return json.loads(result.stdout)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--pacer-fd", type=int)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--base-root", type=Path)
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--reference-json", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--scenario")
    parser.add_argument("--count", type=int)
    parser.add_argument("--rate", type=float)
    parser.add_argument("--cpu", type=int, default=4)
    parser.add_argument("--pacer-cpu", type=int, default=5)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11884)
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument("--aa-cycles", type=int, default=2)
    parser.add_argument("--seconds", type=float, default=2)
    args = parser.parse_args()
    if args.count is not None and args.count <= 0:
        parser.error("count must be positive")
    if args.rate is not None and (not math.isfinite(args.rate) or args.rate <= 0):
        parser.error("rate must be finite and positive")
    if (
        args.cycles < 1
        or args.aa_cycles < 1
        or not math.isfinite(args.seconds)
        or args.seconds <= 0
    ):
        parser.error("cycles, aa-cycles and seconds must be positive")
    if args.pacer_fd is not None:
        os.sched_setaffinity(0, {args.pacer_cpu})
        with socket.socket(fileno=args.pacer_fd) as sock:
            result = emit_tokens(sock, args.count, 1e9 / args.rate, 150_000)
        print(json.dumps(result))
        return
    if args.worker:
        sys.path.insert(0, str(args.source_root.resolve() / "src"))
        os.sched_setaffinity(0, {args.cpu})

        async def run():
            async with asyncio.timeout(120):
                return await _sample(args)

        print(json.dumps(asyncio.run(run())))
        return
    if any(
        value is None
        for value in (args.base_root, args.candidate_root, args.reference_json, args.output)
    ):
        parser.error("base-root, candidate-root, reference-json and output are required")
    reference = json.loads(args.reference_json.read_text())
    if "completed_utc" not in reference["metadata"]:
        parser.error("reference calibration must be complete")
    roots = {"A": args.base_root.resolve(), "B": args.candidate_root.resolve()}
    for root in roots.values():
        _git(root, "diff", "--exit-code", "HEAD", "--", "src")
    if _git(roots["A"], "rev-parse", "HEAD") != reference["metadata"]["commits"]["A"]:
        parser.error("reference rates must belong to the exact control source")
    metadata = {
        "started_utc": datetime.now(UTC).isoformat(),
        "commits": {arm: _git(root, "rev-parse", "HEAD") for arm, root in roots.items()},
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "pacer_sha256": hashlib.sha256(
            Path(__file__).with_name("external_pacer.py").read_bytes()
        ).hexdigest(),
        "reference_sha256": hashlib.sha256(args.reference_json.read_bytes()).hexdigest(),
        "python": sys.version,
        "broker": f"{args.host}:{args.port}",
        "cpu": args.cpu,
        "pacer_cpu": args.pacer_cpu,
        "load_start": os.getloadavg(),
        "clock_columns": [
            "sequence",
            "planned_arrival",
            "pacer_emission",
            "call",
            "admission_return",
            "delivery",
            "receipt_observed_done",
        ],
        "rate_definition": "fraction of reference long-lot A/A median delivered rate, frozen for both arms",
        "ab_cycles": args.cycles,
        "aa_cycles": args.aa_cycles,
    }
    output = {"metadata": metadata, "cells": []}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    clock_directory = args.output.with_name(args.output.stem + "-clocks")
    clock_directory.mkdir(exist_ok=True)
    metadata["clock_directory"] = clock_directory.name
    controls = [
        c
        for c in reference["cells"]
        if c["spec"]["burst"] == "long"
        and c["spec"]["qos"] in (0, 1)
        and c["spec"]["store"] == "memory"
    ]
    for control, fraction in itertools.product(controls, (0.50, 0.75, 0.90)):
        spec = {**control["spec"], "fraction": fraction}
        capacity = control["summary_AA"]["delivered_per_s"]["A_median"]
        rate = capacity * fraction
        count = args.count or max(1024, min(20_000, math.ceil(args.seconds * rate)))
        cell = {
            "spec": spec,
            "reference_rate": capacity,
            "offered_rate": rate,
            "count": count,
            "AA": [],
            "AB": [],
        }
        # Save every returned sample, including earlier samples of a failed cell.
        output["cells"].append(cell)
        for stage, cycles in (("AA", args.aa_cycles), ("AB", args.cycles)):
            for cycle in range(cycles):
                for position, arm in enumerate("ABBA"):
                    root = roots["A"] if stage == "AA" else roots[arm]
                    sample = _run_worker(args, root, spec, count, rate)
                    clock_file = clock_directory / (
                        f"{len(output['cells']):02d}-{stage}-{cycle}-{position}-{arm}.json"
                    )
                    clock_bytes = (json.dumps(sample.pop("clocks_ns")) + "\n").encode()
                    clock_file.write_bytes(clock_bytes)
                    sample["clock_file"] = clock_file.name
                    sample["clock_sha256"] = hashlib.sha256(clock_bytes).hexdigest()
                    cell[stage].append(
                        {"cycle": cycle, "position": position, "arm": arm, "result": sample}
                    )
                    metadata["updated_utc"] = datetime.now(UTC).isoformat()
                    args.output.write_text(json.dumps(output) + "\n")
        print(f"{len(output['cells'])}/{len(controls) * 3} {spec} rate={rate:.1f}", flush=True)
    metadata["completed_utc"] = datetime.now(UTC).isoformat()
    metadata["load_end"] = os.getloadavg()
    args.output.write_text(json.dumps(output) + "\n")


if __name__ == "__main__":
    main()
