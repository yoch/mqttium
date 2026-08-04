"""A/B benchmark for Paho-compatible QoS 1 cross-thread submission.

The benchmark isolates the synchronous cost of obtaining a MID from a live
compatibility loop with a CONNECTED protocol engine and no broker I/O. It
compares the pre-PR coroutine handoff, a safe one-callback-per-message handoff,
and the shipped coalesced queue.

Usage::

    PYTHONPATH=src python benchmarks/compat_qosn_submit_ab.py
    PYTHONPATH=src python benchmarks/compat_qosn_submit_ab.py \
        --count 4000 --workers 8 --output /tmp/compat-qosn-submit.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from mqttium.api.models import PublishReceipt
from mqttium.compat.paho import CallbackAPIVersion, Client, MQTTMessageInfo
from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS

_TOPIC = "bench/compat/qos1"


@dataclass(frozen=True, slots=True)
class Sample:
    messages_per_second: float
    latency_p50_us: float
    latency_p95_us: float
    latency_p99_us: float


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = round((len(ordered) - 1) * percentile)
    return ordered[index]


def _sample(count: int, elapsed: float, latencies: list[float]) -> Sample:
    return Sample(
        messages_per_second=count / elapsed,
        latency_p50_us=_percentile(latencies, 0.50) * 1_000_000,
        latency_p95_us=_percentile(latencies, 0.95) * 1_000_000,
        latency_p99_us=_percentile(latencies, 0.99) * 1_000_000,
    )


def _new_client(name: str) -> Client:
    client = Client(
        CallbackAPIVersion.VERSION2,
        client_id=f"compat-submit-{name}",
        protocol=MQTTProtocolVersion.MQTTv311,
        max_pending_outbound_messages=None,
        max_pending_outbound_bytes=None,
    )
    client.loop_start()
    client._async._engine.state = ConnectionState.CONNECTED
    return client


def _commit_one_on_loop(client: Client, payload: bytes) -> int:
    handle = client._async._engine.queue_publish(
        _TOPIC,
        payload,
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
    )
    assert handle.mid is not None
    receipt = PublishReceipt(
        mid=handle.mid,
        qos=handle.qos,
        _event=asyncio.Event(),
    )
    client._async._register_publish_receipt(handle.mid, receipt)
    client._async._collect_effects_locked()
    client._async._drain_effects_inline()
    return handle.mid


def _publish_coroutine_handoff(client: Client, payload: bytes) -> int:
    assert client._loop is not None
    handoff: dict[str, Any] = {}
    done = threading.Event()

    async def commit() -> None:
        try:
            handoff["mid"] = _commit_one_on_loop(client, payload)
        except BaseException as exc:
            handoff["error"] = exc
        finally:
            done.set()

    task = asyncio.run_coroutine_threadsafe(commit(), client._loop)
    if not done.wait(timeout=5.0):
        task.cancel()
        raise RuntimeError("coroutine handoff timed out")
    error = handoff.get("error")
    if error is not None:
        raise error
    mid = handoff["mid"]
    assert isinstance(mid, int)
    return mid


def _publish_callback_handoff(client: Client, payload: bytes) -> int:
    assert client._loop is not None
    result: Future[int] = Future()

    def commit() -> None:
        if not result.set_running_or_notify_cancel():
            return
        try:
            result.set_result(_commit_one_on_loop(client, payload))
        except BaseException as exc:
            result.set_exception(exc)

    client._loop.call_soon_threadsafe(commit)
    return result.result(timeout=5.0)


def _publish_coalesced(client: Client, payload: bytes) -> int:
    info: MQTTMessageInfo = client.publish(_TOPIC, payload, qos=1)
    assert info.mid is not None
    return info.mid


Publisher = Callable[[Client, bytes], int]
PUBLISHERS: dict[str, Publisher] = {
    "coroutine": _publish_coroutine_handoff,
    "callback": _publish_callback_handoff,
    "coalesced": _publish_coalesced,
}


def _run_single(name: str, count: int) -> Sample:
    client = _new_client(f"single-{name}")
    publish = PUBLISHERS[name]
    latencies: list[float] = []
    try:
        started = time.perf_counter()
        for index in range(count):
            before = time.perf_counter()
            publish(client, str(index).encode())
            latencies.append(time.perf_counter() - before)
        return _sample(count, time.perf_counter() - started, latencies)
    finally:
        client.loop_stop()


def _run_multi(name: str, count: int, workers: int) -> Sample:
    if count % workers:
        raise ValueError("count must be divisible by workers")
    client = _new_client(f"multi-{name}")
    publish = PUBLISHERS[name]
    per_worker = count // workers
    barrier = threading.Barrier(workers + 1)
    latencies: list[float] = []
    errors: list[BaseException] = []
    result_lock = threading.Lock()

    def worker(worker_id: int) -> None:
        local: list[float] = []
        try:
            barrier.wait()
            base = worker_id * per_worker
            for index in range(per_worker):
                before = time.perf_counter()
                publish(client, str(base + index).encode())
                local.append(time.perf_counter() - before)
        except BaseException as exc:
            with result_lock:
                errors.append(exc)
        finally:
            with result_lock:
                latencies.extend(local)

    threads = [
        threading.Thread(target=worker, args=(worker_id,), daemon=True)
        for worker_id in range(workers)
    ]
    try:
        for thread in threads:
            thread.start()
        barrier.wait()
        started = time.perf_counter()
        for thread in threads:
            thread.join(timeout=60.0)
        elapsed = time.perf_counter() - started
        if any(thread.is_alive() for thread in threads):
            raise RuntimeError("benchmark worker did not finish")
        if errors:
            raise errors[0]
        return _sample(count, elapsed, latencies)
    finally:
        client.loop_stop()


def _median(samples: list[Sample], field: str) -> float:
    return statistics.median(getattr(sample, field) for sample in samples)


def _print_results(title: str, results: dict[str, list[Sample]]) -> None:
    print(f"\n{title}")
    print(f"{'strategy':<12} {'msg/s':>12} {'p50 us':>12} {'p95 us':>12} {'p99 us':>12}")
    for name, samples in results.items():
        print(
            f"{name:<12} "
            f"{_median(samples, 'messages_per_second'):12,.0f} "
            f"{_median(samples, 'latency_p50_us'):12,.1f} "
            f"{_median(samples, 'latency_p95_us'):12,.1f} "
            f"{_median(samples, 'latency_p99_us'):12,.1f}"
        )
    coroutine_rate = _median(results["coroutine"], "messages_per_second")
    callback_rate = _median(results["callback"], "messages_per_second")
    coalesced_rate = _median(results["coalesced"], "messages_per_second")
    print(f"callback / coroutine  = {callback_rate / coroutine_rate:.2f}x")
    print(f"coalesced / coroutine = {coalesced_rate / coroutine_rate:.2f}x")
    print(f"coalesced / callback  = {coalesced_rate / callback_rate:.2f}x")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=4000, help="publishes per sample")
    parser.add_argument("--repeat", type=int, default=5, help="samples per strategy")
    parser.add_argument("--warmup", type=int, default=400, help="warmup publishes")
    parser.add_argument("--workers", type=int, default=8, help="multi-thread worker count")
    parser.add_argument("--output", type=Path, help="optional JSON output path")
    args = parser.parse_args()
    if args.count <= 0 or args.repeat <= 0 or args.workers <= 0:
        raise SystemExit("count, repeat, and workers must be positive")
    if args.count % args.workers:
        raise SystemExit("count must be divisible by workers")

    single: dict[str, list[Sample]] = {name: [] for name in PUBLISHERS}
    multi: dict[str, list[Sample]] = {name: [] for name in PUBLISHERS}
    for name in PUBLISHERS:
        if args.warmup:
            _run_single(name, args.warmup)
        single[name] = [_run_single(name, args.count) for _ in range(args.repeat)]
        multi[name] = [_run_multi(name, args.count, args.workers) for _ in range(args.repeat)]

    _print_results("single-thread submit", single)
    _print_results(f"multi-thread submit ({args.workers} workers)", multi)

    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "count": args.count,
            "repeat": args.repeat,
            "warmup": args.warmup,
            "workers": args.workers,
            "single": {
                name: [asdict(sample) for sample in samples] for name, samples in single.items()
            },
            "multi": {
                name: [asdict(sample) for sample in samples] for name, samples in multi.items()
            },
        }
        args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
