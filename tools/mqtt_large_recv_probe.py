#!/usr/bin/env python3
"""Large-response MQTT probe for PR #445 receive-buffer sizing.

Runs one request/response pair at a time through a real broker.  The responder
receives a tiny request and publishes a large QoS0 response; the initiator's
receive path therefore dominates the buffer-size question.  Fresh processes are
used by the workflow.  Receive high/low water marks stay fixed while only the
reusable recv_into buffer size changes.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import resource
import statistics
import time


def usage() -> tuple[int, int, float, float]:
    r = resource.getrusage(resource.RUSAGE_SELF)
    return int(r.ru_minflt), int(r.ru_majflt), float(r.ru_stime), float(r.ru_utime)


async def run(args) -> dict:
    # Patch only experimental module constants before constructing any transport.
    # The backpressure budget is identical for every recv-chunk variant.
    from mqttium.transport import _buffered

    _buffered._READ_CHUNK = args.recv_chunk
    _buffered._READ_HIGH_WATER = args.high_water
    _buffered._READ_LOW_WATER = args.low_water

    peaks: dict[int, int] = {}
    pauses: dict[int, int] = {}
    original_updated = _buffered.BufferedSocketProtocol.buffer_updated

    def tracked_updated(self, nbytes: int) -> None:
        was_paused = self._paused_reading
        original_updated(self, nbytes)
        peaks[id(self)] = max(peaks.get(id(self), 0), self._buffered_bytes)
        if not was_paused and self._paused_reading:
            pauses[id(self)] = pauses.get(id(self), 0) + 1

    _buffered.BufferedSocketProtocol.buffer_updated = tracked_updated

    from mqttium.api import AsyncClient

    pid = os.getpid()
    req_topic = f"bench/recv/{pid}/req"
    resp_topic = f"bench/recv/{pid}/resp"
    response = b"R" * args.payload_size
    responses: asyncio.Queue[int] = asyncio.Queue()
    response_counter = 0

    responder = AsyncClient(f"recv-responder-{pid}", message_delivery="callback")
    initiator = AsyncClient(f"recv-initiator-{pid}", message_delivery="callback")

    def on_request(_message) -> None:
        responder.publish_nowait(resp_topic, response, qos=0)

    def on_response(_message) -> None:
        nonlocal response_counter
        response_counter += 1
        responses.put_nowait(time.perf_counter_ns())

    responder.on_message = on_request
    initiator.on_message = on_response

    await responder.connect(args.host, args.port)
    await initiator.connect(args.host, args.port)
    await responder.subscribe(req_topic, qos=0)
    await initiator.subscribe(resp_topic, qos=0)

    async def one() -> float:
        sent = time.perf_counter_ns()
        initiator.publish_nowait(req_topic, b"x", qos=0)
        received = await asyncio.wait_for(responses.get(), timeout=5.0)
        return (received - sent) / 1e6

    try:
        warm_deadline = time.monotonic() + args.warmup_s
        while time.monotonic() < warm_deadline:
            await one()

        before = usage()
        latencies: list[float] = []
        started = time.perf_counter()
        deadline = started + args.duration_s
        while time.perf_counter() < deadline:
            latencies.append(await one())
        elapsed = time.perf_counter() - started
        after = usage()
    finally:
        await initiator.disconnect()
        await responder.disconnect()
        _buffered.BufferedSocketProtocol.buffer_updated = original_updated

    completed = len(latencies)
    payload_bytes = completed * args.payload_size
    return {
        "pid": pid,
        "recv_chunk": args.recv_chunk,
        "high_water": args.high_water,
        "low_water": args.low_water,
        "payload_size": args.payload_size,
        "duration_s": elapsed,
        "completed": completed,
        "payload_mib_s": payload_bytes / elapsed / (1024 * 1024),
        "rtt_p50_ms": statistics.median(latencies),
        "rtt_p95_ms": sorted(latencies)[max(0, int(len(latencies) * 0.95) - 1)],
        "ru_minflt": after[0] - before[0],
        "ru_majflt": after[1] - before[1],
        "ru_stime_s": after[2] - before[2],
        "ru_utime_s": after[3] - before[3],
        "minflt_per_response": (after[0] - before[0]) / completed,
        "peak_buffered_bytes": max(peaks.values(), default=0),
        "pause_count": sum(pauses.values()),
        "responses_seen": response_counter,
    }


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=11883)
    p.add_argument("--recv-chunk", type=int, required=True)
    p.add_argument("--payload-size", type=int, required=True)
    p.add_argument("--high-water", type=int, default=524288)
    p.add_argument("--low-water", type=int, default=262144)
    p.add_argument("--warmup-s", type=float, default=0.5)
    p.add_argument("--duration-s", type=float, default=2.0)
    args = p.parse_args()
    payload = asyncio.run(run(args))
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
