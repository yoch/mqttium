"""Same-runner A/B benchmark for AsyncClient.publish_many.

The benchmark uses a fresh Python process and an independent mosquitto_sub for
every sample. It compares the best practical single-publish sliding-window
baseline against the aggregate batch pipeline, while verifying exact delivery.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import resource
import statistics
import subprocess
import sys
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass
from pathlib import Path

from mqttium.api import AsyncClient, PublishMessage
from mqttium.protocol.reconnect import ReconnectPolicy

WINDOW = 100
CHUNK = 256


@dataclass(slots=True)
class Sample:
    method: str
    qos: int
    payload_bytes: int
    count: int
    publisher_msg_s: float
    delivered_msg_s: float
    cpu_us_per_message: float
    max_rss_mib: float
    first_delivery_ms: float


def start_subscriber(host: str, port: int, topic: str, qos: int, count: int):
    process = subprocess.Popen(
        [
            "mosquitto_sub",
            "-h",
            host,
            "-p",
            str(port),
            "-t",
            topic,
            "-q",
            str(qos),
            "-C",
            str(count),
            "-F",
            "%p",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    arrivals: list[float] = []

    def read() -> None:
        assert process.stdout is not None
        for _line in process.stdout:
            arrivals.append(time.perf_counter())

    thread = threading.Thread(target=read, daemon=True)
    thread.start()
    time.sleep(0.25)
    if process.poll() is not None:
        assert process.stderr is not None
        raise RuntimeError(process.stderr.read().decode(errors="replace"))
    return process, arrivals, thread


async def run_publisher(args: argparse.Namespace, topic: str) -> tuple[float, float]:
    payload = b"x" * args.payload_bytes
    messages = [PublishMessage(topic, payload, qos=args.qos) for _ in range(args.count)]
    client = AsyncClient(
        client_id=f"publish-many-ab-{args.method}-{os.getpid()}",
        max_outbound_inflight=WINDOW,
        reconnect=ReconnectPolicy(enabled=False),
    )
    await client.connect(args.host, args.port, timeout=10.0)
    cpu_started = time.process_time()
    started = time.perf_counter()

    if args.method == "baseline":
        if args.qos == 0:
            for message in messages:
                await client.publish(
                    message.topic,
                    message.payload,
                    qos=message.qos,
                    retain=message.retain,
                    properties=message.properties,
                )
        else:
            pending = deque()
            for message in messages:
                pending.append(
                    await client.publish(
                        message.topic,
                        message.payload,
                        qos=message.qos,
                        retain=message.retain,
                        properties=message.properties,
                    )
                )
                if len(pending) >= WINDOW:
                    await pending.popleft().wait()
            while pending:
                await pending.popleft().wait()
    else:
        receipt = await client.publish_many(
            messages,
            chunk_size=CHUNK,
        )
        await receipt.wait()

    await client._outbound.join()
    elapsed = time.perf_counter() - started
    cpu = time.process_time() - cpu_started
    await client.disconnect()
    return elapsed, cpu


def run_child(args: argparse.Namespace) -> None:
    topic = f"bench/publish-many/{args.qos}/{args.payload_bytes}/{args.method}/{os.getpid()}"
    process, arrivals, thread = start_subscriber(
        args.host,
        args.port,
        topic,
        args.qos,
        args.count,
    )
    delivery_started = time.perf_counter()
    publisher_elapsed, cpu = asyncio.run(run_publisher(args, topic))
    try:
        process.wait(timeout=args.timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    thread.join(timeout=2.0)
    if len(arrivals) != args.count:
        assert process.stderr is not None
        detail = process.stderr.read().decode(errors="replace")
        raise TimeoutError(f"subscriber received {len(arrivals)}/{args.count}: {detail}")
    delivered_elapsed = arrivals[-1] - delivery_started
    usage = resource.getrusage(resource.RUSAGE_SELF)
    sample = Sample(
        method=args.method,
        qos=args.qos,
        payload_bytes=args.payload_bytes,
        count=args.count,
        publisher_msg_s=args.count / publisher_elapsed,
        delivered_msg_s=args.count / delivered_elapsed,
        cpu_us_per_message=cpu * 1_000_000 / args.count,
        max_rss_mib=usage.ru_maxrss / 1024,
        first_delivery_ms=(arrivals[0] - delivery_started) * 1000,
    )
    print(json.dumps(asdict(sample)))


def child_sample(
    args: argparse.Namespace,
    method: str,
    qos: int,
    payload_bytes: int,
    count: int,
) -> dict[str, float | int | str]:
    command = [
        sys.executable,
        __file__,
        "--child",
        "--method",
        method,
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--qos",
        str(qos),
        "--payload-bytes",
        str(payload_bytes),
        "--count",
        str(count),
        "--timeout",
        str(args.timeout),
    ]
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=args.timeout + 60,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"child failed rc={completed.returncode}\n"
            f"command={command!r}\nstdout={completed.stdout}\nstderr={completed.stderr}"
        )
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError("benchmark child returned no JSON")
    return json.loads(lines[-1])


def geometric_mean(values: list[float]) -> float:
    return math.exp(statistics.fmean(math.log(value) for value in values))


def summarize(samples: list[dict[str, float | int | str]]) -> dict[str, object]:
    scenarios: list[dict[str, object]] = []
    qos_ratios: dict[int, list[float]] = {0: [], 1: [], 2: []}
    qos_cpu_ratios: dict[int, list[float]] = {0: [], 1: [], 2: []}
    qos_rss_ratios: dict[int, list[float]] = {0: [], 1: [], 2: []}

    keys = sorted({(int(s["qos"]), int(s["payload_bytes"])) for s in samples})
    for qos, payload_bytes in keys:
        baseline = [
            s
            for s in samples
            if s["method"] == "baseline"
            and int(s["qos"]) == qos
            and int(s["payload_bytes"]) == payload_bytes
        ]
        batch = [
            s
            for s in samples
            if s["method"] == "batch"
            and int(s["qos"]) == qos
            and int(s["payload_bytes"]) == payload_bytes
        ]
        if len(baseline) != len(batch):
            raise RuntimeError("unpaired benchmark samples")
        throughput_ratios = [
            float(batch[index]["publisher_msg_s"]) / float(baseline[index]["publisher_msg_s"])
            for index in range(len(batch))
        ]
        delivered_ratios = [
            float(batch[index]["delivered_msg_s"]) / float(baseline[index]["delivered_msg_s"])
            for index in range(len(batch))
        ]
        cpu_ratios = [
            float(batch[index]["cpu_us_per_message"]) / float(baseline[index]["cpu_us_per_message"])
            for index in range(len(batch))
        ]
        rss_ratios = [
            float(batch[index]["max_rss_mib"]) / float(baseline[index]["max_rss_mib"])
            for index in range(len(batch))
        ]
        first_delivery_ratios = [
            float(batch[index]["first_delivery_ms"])
            / max(float(baseline[index]["first_delivery_ms"]), 0.001)
            for index in range(len(batch))
        ]
        scenario = {
            "qos": qos,
            "payload_bytes": payload_bytes,
            "baseline_publisher_msg_s": statistics.median(
                float(s["publisher_msg_s"]) for s in baseline
            ),
            "batch_publisher_msg_s": statistics.median(float(s["publisher_msg_s"]) for s in batch),
            "publisher_ratio": statistics.median(throughput_ratios),
            "delivered_ratio": statistics.median(delivered_ratios),
            "cpu_ratio": statistics.median(cpu_ratios),
            "rss_ratio": statistics.median(rss_ratios),
            "first_delivery_ratio": statistics.median(first_delivery_ratios),
        }
        scenarios.append(scenario)
        qos_ratios[qos].append(float(scenario["publisher_ratio"]))
        qos_cpu_ratios[qos].append(float(scenario["cpu_ratio"]))
        qos_rss_ratios[qos].append(float(scenario["rss_ratio"]))

    decisions: dict[str, object] = {}
    for qos in (0, 1, 2):
        ratios = qos_ratios[qos]
        cpu = qos_cpu_ratios[qos]
        rss = qos_rss_ratios[qos]
        decisions[str(qos)] = {
            "publisher_geomean_ratio": geometric_mean(ratios),
            "cpu_geomean_ratio": geometric_mean(cpu),
            "rss_max_ratio": max(rss),
            "keep": (
                geometric_mean(ratios) >= (1.10 if qos == 0 else 1.05)
                and min(ratios) >= (1.03 if qos == 0 else 1.02)
                and max(cpu) <= 1.02
                and max(rss) <= 1.25
            ),
        }
    decisions["keep_qos12"] = bool(decisions["1"]["keep"] and decisions["2"]["keep"])
    return {"scenarios": scenarios, "decisions": decisions}


def run_parent(args: argparse.Namespace) -> None:
    counts = {
        (0, 64): 50_000,
        (1, 64): 20_000,
        (2, 64): 10_000,
        (0, 4096): 15_000,
        (1, 4096): 8_000,
        (2, 4096): 4_000,
    }
    samples: list[dict[str, float | int | str]] = []
    for qos in (0, 1, 2):
        for payload_bytes in (64, 4096):
            count = counts[(qos, payload_bytes)]
            # One unrecorded warmup per implementation.
            child_sample(args, "baseline", qos, payload_bytes, max(500, count // 10))
            child_sample(args, "batch", qos, payload_bytes, max(500, count // 10))
            for repetition in range(args.repeats):
                order = ("baseline", "batch") if repetition % 2 == 0 else ("batch", "baseline")
                pair: dict[str, dict[str, float | int | str]] = {}
                for method in order:
                    pair[method] = child_sample(args, method, qos, payload_bytes, count)
                # Store paired samples in a stable order for ratio calculation.
                samples.append(pair["baseline"])
                samples.append(pair["batch"])
                print(
                    f"qos={qos} payload={payload_bytes:4d} repeat={repetition + 1} "
                    f"baseline={float(pair['baseline']['publisher_msg_s']):9.1f} "
                    f"batch={float(pair['batch']['publisher_msg_s']):9.1f}"
                )

    summary = summarize(samples)
    output = {
        "window": WINDOW,
        "chunk": CHUNK,
        "repeats": args.repeats,
        "samples": samples,
        **summary,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2))
    print(json.dumps(summary["decisions"], indent=2))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--child", action="store_true")
    parser.add_argument("--method", choices=("baseline", "batch"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11883)
    parser.add_argument("--qos", type=int, choices=(0, 1, 2), default=1)
    parser.add_argument("--payload-bytes", type=int, default=64)
    parser.add_argument("--count", type=int, default=10_000)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--output", type=Path, default=Path("/tmp/publish-many-ab.json"))
    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    if arguments.child:
        run_child(arguments)
    else:
        run_parent(arguments)
