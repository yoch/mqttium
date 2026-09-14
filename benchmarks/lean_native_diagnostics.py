#!/usr/bin/env python3
"""Isolate native publication, ingress, callback and routing orchestration.

This packet-aware in-memory diagnostic is not a network throughput claim.
Routing candidates use fresh-process A/A and ABBA comparisons. Optional cProfile
output counts Python calls/resumptions, not context switches or system calls.
"""

from __future__ import annotations

import argparse
import asyncio
import cProfile
import hashlib
import json
import math
import os
import pstats
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from lean_native_compare import _git

SCENARIOS = (
    "publish",
    "receive_iterator",
    "receive_callback",
    "callback_only",
    "route_exact",
    "route_overlap",
    "route_fallback",
    "route_error",
    "publish_qos1_individual",
    "publish_qos1_batch",
)


async def _phase(scenario: str, count: int) -> dict[str, Any]:  # noqa: C901 - scenarios share teardown
    from mqttium.api import AsyncClient, Message, PublishMessage
    from mqttium.enums import QoS
    from mqttium.packets import PublishPacket
    from tests.support import ScriptedBrokerTransport, transport_factory

    class DiagnosticBroker(ScriptedBrokerTransport):
        async def write(self, data: bytes) -> None:
            # A writer batch can contain more than drain_packets()'s default
            # limit. Consume every complete frame before reporting completion.
            self.decoder.feed(data)
            while (raw := self.decoder.next_packet()) is not None:
                self.handle_packet(raw)

    qos1_publication = scenario in ("publish_qos1_individual", "publish_qos1_batch")
    mode = "iterator" if scenario == "receive_iterator" else "callback"
    publication_options = {"max_outbound_inflight": 20} if qos1_publication else {}
    client = AsyncClient(
        "lean-diagnostic",
        message_delivery=mode,
        max_pending_callbacks=1024,
        max_pending_messages=1024,
        keepalive=0,
        **publication_options,
    )
    broker = DiagnosticBroker()
    client._transport_factory = transport_factory(broker)
    seen = 0
    prefix: list[int] = []
    errors = 0
    unexpected: list[str] = []
    progress = asyncio.Event()
    message = Message("a/b", b"x" * 256)
    route = scenario.startswith("route_")
    loop = asyncio.get_running_loop()
    old_handler = loop.get_exception_handler()

    def report(_loop, context):
        nonlocal errors
        if scenario != "route_error" or not isinstance(context.get("exception"), ValueError):
            unexpected.append(str(context))
            return
        errors += 1

    def observe(_message):
        nonlocal seen
        seen += 1
        progress.set()

    def callback(index):
        def handle(_message):
            nonlocal seen
            if len(prefix) < 12:
                prefix.append(index)
            seen += 1
            if scenario == "route_error" and index == 0:
                raise ValueError("expected isolated failure")

        return handle

    callback_zero = callback(0)
    if route:
        client.on_message = callback(9)
        if scenario == "route_overlap":
            for index, topic in enumerate(("a/+", "a/b", "a/#")):
                client.message_callback_add(topic, callback(index))
        elif scenario == "route_fallback":
            client.message_callback_add("elsewhere", callback_zero)
        elif scenario == "route_error":
            client.message_callback_add("a/+", callback_zero)
            client.message_callback_add("a/b", callback(1))
        else:
            client.message_callback_add("a/b", callback_zero)
    elif mode == "callback":
        client.on_message = observe
    consumer = None
    individual_receipts = []
    batch_receipt = None
    loop.set_exception_handler(report)

    async def consume():
        async for incoming in client.messages():
            observe(incoming)

    async def wait_seen(total):
        while seen < total:
            progress.clear()
            await progress.wait()

    try:
        await client.connect("fake")
        if scenario == "receive_iterator":
            consumer = asyncio.create_task(consume())
        wire = PublishPacket(
            topic="a/b", payload=message.payload, qos=QoS.AT_MOST_ONCE, retain=False, dup=False
        ).encode()
        cpu_start = time.process_time_ns()
        started = time.perf_counter_ns()
        if route or scenario == "callback_only":
            for _ in range(count):
                await client._delivery.accept(message, client._message_callback)
            await client._delivery.callback_queue.join()
        elif scenario == "publish":
            receipt = await client.publish_many(
                PublishMessage("a/b", message.payload) for _ in range(count)
            )
            await receipt.wait()
            await client._write_pump.join()
            if len(broker.publishes) != count:
                raise AssertionError(f"publication count mismatch: {len(broker.publishes)}/{count}")
        elif qos1_publication:
            if scenario == "publish_qos1_individual":
                for _ in range(count):
                    individual_receipts.append(await client.publish("a/b", message.payload, qos=1))
                for receipt in individual_receipts:
                    await receipt.wait()
            else:
                batch_receipt = await client.publish_many(
                    PublishMessage("a/b", message.payload, qos=1) for _ in range(count)
                )
                await batch_receipt.wait()
            await client._write_pump.join()
        else:
            for start in range(0, count, 256):
                stop = min(start + 256, count)
                broker.push_rx(wire * (stop - start))
                await wait_seen(stop)
        elapsed = (time.perf_counter_ns() - started) / 1e9
        cpu = (time.process_time_ns() - cpu_start) / count / 1000
        matches = 3 if scenario == "route_overlap" else 2 if scenario == "route_error" else 1
        if (
            scenario != "publish"
            and not qos1_publication
            and seen != count * (matches if route else 1)
        ):
            raise AssertionError("callback/delivery count mismatch")
        if qos1_publication:
            if len(broker.publishes) != count:
                raise AssertionError(f"publication count mismatch: {len(broker.publishes)}/{count}")
            if any(
                packet.qos is not QoS.AT_LEAST_ONCE
                or packet.mid is None
                or packet.topic != "a/b"
                or packet.payload != message.payload
                for packet in broker.publishes
            ):
                raise AssertionError("QoS 1 publication content mismatch")
            if scenario == "publish_qos1_individual":
                if len(individual_receipts) != count or any(
                    not receipt.is_done()
                    or receipt.qos is not QoS.AT_LEAST_ONCE
                    or receipt.mid != packet.mid
                    for receipt, packet in zip(individual_receipts, broker.publishes, strict=True)
                ):
                    raise AssertionError("individual receipt completion mismatch")
            elif (
                batch_receipt is None
                or not batch_receipt.is_done()
                or batch_receipt.submitted != count
                or batch_receipt.completed != count
                or batch_receipt.pending_count
                or batch_receipt.failure_count
            ):
                raise AssertionError("batch receipt completion mismatch")
            stats = client.stats()
            if (
                stats.receipts.publish
                or stats.receipts.publish_batches
                or stats.outbound.pending_messages
            ):
                raise AssertionError("publication completion retained pending state")
        if route:
            expected = (
                [0, 1, 2]
                if matches == 3
                else [0, 1]
                if matches == 2
                else [9]
                if scenario == "route_fallback"
                else [0]
            )
            if prefix != (expected * count)[:12]:
                raise AssertionError("routing order mismatch")
        if unexpected:
            raise AssertionError(unexpected)
        if errors != (count if scenario == "route_error" else 0):
            raise AssertionError("callback error isolation mismatch")
        if client.stats().delivery.pending_bytes:
            raise AssertionError("delivery bytes retained")
        result = {
            "count": count,
            "operations_per_s": count / elapsed,
            "cpu_us_per_message": cpu,
            "elapsed_s": elapsed,
            "callbacks": seen,
            "errors": errors,
        }
        if qos1_publication:
            result.update(
                qos=1,
                max_outbound_inflight=20,
                submitted=count,
                completed=count,
                wire_count=len(broker.publishes),
            )
        return result
    finally:
        await client.disconnect()
        if consumer is not None:
            if not consumer.done():
                consumer.cancel()
            try:
                await consumer
            except asyncio.CancelledError:
                pass
        loop.set_exception_handler(old_handler)


def _worker(args):
    sys.path[:0] = [str(args.source_root / "src"), str(args.source_root)]
    os.sched_setaffinity(0, {args.cpu})
    asyncio.run(_phase(args.scenario, 64))
    result = asyncio.run(_phase(args.scenario, args.count))
    if args.profile:
        profiler = cProfile.Profile()
        profiler.runcall(asyncio.run, _phase(args.scenario, min(args.count, 2048)))
        stats: Any = pstats.Stats(profiler)
        result["profile_count"] = min(args.count, 2048)
        result["profile"] = [
            {
                "file": key[0],
                "line": key[1],
                "function": key[2],
                "primitive_calls": value[0],
                "calls": value[1],
                "total_s": value[2],
                "cumulative_s": value[3],
            }
            for key, value in sorted(stats.stats.items(), key=lambda item: item[1][3], reverse=True)
        ]
    print(json.dumps(result))


def _run(args, root, scenario, count, *, profile=False):
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--source-root",
        str(root),
        "--scenario",
        scenario,
        "--count",
        str(count),
        "--cpu",
        str(args.cpu),
    ]
    if profile:
        command.append("--profile")
    process = subprocess.run(
        command,
        cwd="/tmp",
        text=True,
        capture_output=True,
        timeout=90,
        env={**os.environ, "PYTHONHASHSEED": "0", "PYTHONDONTWRITEBYTECODE": "1"},
    )
    if process.returncode:
        raise RuntimeError(f"{root} {scenario}: {process.stderr}")
    return json.loads(process.stdout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--profile", action="store_true")
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--base-root", type=Path)
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--scenario", choices=SCENARIOS)
    parser.add_argument("--scenarios", default=",".join(SCENARIOS))
    parser.add_argument("--count", type=int)
    parser.add_argument("--cpu", type=int, default=4)
    parser.add_argument("--seconds", type=float, default=0.5)
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument("--aa-cycles", type=int, default=2)
    args = parser.parse_args()
    if args.count is not None and args.count <= 0:
        parser.error("count must be positive")
    if (
        args.cycles < 1
        or args.aa_cycles < 1
        or not math.isfinite(args.seconds)
        or args.seconds <= 0
    ):
        parser.error("cycles, aa-cycles and seconds must be positive")
    if args.worker:
        _worker(args)
        return
    if any(value is None for value in (args.base_root, args.candidate_root, args.output)):
        parser.error("base-root, candidate-root and output are required")
    roots = {"A": args.base_root.resolve(), "B": args.candidate_root.resolve()}
    for root in roots.values():
        _git(root, "diff", "--exit-code", "HEAD", "--", "src")
    metadata = {
        "started_utc": datetime.now(UTC).isoformat(),
        "commits": {arm: _git(root, "rev-parse", "HEAD") for arm, root in roots.items()},
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "python": sys.version,
        "cpu": args.cpu,
        "load_start": os.getloadavg(),
        "ab_cycles": args.cycles,
        "aa_cycles": args.aa_cycles,
        "interpretation": "packet-aware in-memory diagnostics, not network measurements",
        "routing_delivery": "synchronous routes in the bounded message callback worker",
    }
    output = {"metadata": metadata, "cells": []}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    for scenario in args.scenarios.split(","):
        pilot = _run(args, roots["A"], scenario, 1024)
        count = args.count or max(
            2048, min(200_000, math.ceil(args.seconds * pilot["operations_per_s"]))
        )
        cell = {"scenario": scenario, "count": count, "pilot": pilot, "AA": [], "AB": []}
        output["cells"].append(cell)
        for stage, cycles in (("AA", args.aa_cycles), ("AB", args.cycles)):
            for cycle in range(cycles):
                for position, arm in enumerate("ABBA"):
                    root = roots["A"] if stage == "AA" else roots[arm]
                    result = _run(args, root, scenario, count)
                    cell[stage].append(
                        {"cycle": cycle, "position": position, "arm": arm, "result": result}
                    )
                    args.output.write_text(json.dumps(output, indent=2) + "\n")
        if args.profile:
            cell["profiles"] = {
                arm: _run(args, root, scenario, 2048, profile=True) for arm, root in roots.items()
            }
        for stage in ("AA", "AB"):
            summary = {}
            for metric in ("operations_per_s", "cpu_us_per_message"):
                ratios = []
                for cycle in sorted({row["cycle"] for row in cell[stage]}):
                    arms = {
                        arm: [
                            row["result"][metric]
                            for row in cell[stage]
                            if row["cycle"] == cycle and row["arm"] == arm
                        ]
                        for arm in ("A", "B")
                    }
                    ratios.append(
                        statistics.geometric_mean(arms["B"]) / statistics.geometric_mean(arms["A"])
                    )
                summary[metric] = {
                    "cycle_ratios": ratios,
                    "cycle_ratio_geomean": statistics.geometric_mean(ratios),
                }
            cell[f"summary_{stage}"] = summary
        print(scenario, cell["summary_AB"]["operations_per_s"], flush=True)
    metadata["completed_utc"] = datetime.now(UTC).isoformat()
    metadata["load_end"] = os.getloadavg()
    args.output.write_text(json.dumps(output, indent=2) + "\n")


if __name__ == "__main__":
    main()
