"""Paired same-runner microbenchmark for source-level performance changes.

The parent alternates a base checkout and a candidate checkout. Each sample runs
in a fresh interpreter with only that checkout on ``PYTHONPATH`` so imports and
module globals cannot leak across variants.
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
from collections.abc import Callable


@dataclass
class WorkerResult:
    scenario: str
    elapsed_s: float
    operations: int
    ops_per_s: float


SCENARIOS = (
    "encode_qos0",
    "encode_qos1",
    "ingress_engine_qos0",
    "effect_send_inline",
    "effect_batch_inline",
    "writer_try_enqueue",
    "writer_enqueue_async",
    "async_publish_nowait_qos0",
    "native_publish_nowait_qos0",
    "compat_publish_qos1",
    "compat_publish_qos0_batch",
    "qos1_cycle_memory",
    "qos1_cycle_sqlite",
    "qos2_cycle_sqlite",
    "ingress_publish_qos1",
)


def _pin(cpu: int | None) -> None:
    if cpu is not None and hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, {cpu})


def _measure(fn: Callable[[], None], *, operations: int, warmup: int) -> WorkerResult:
    for _ in range(warmup):
        fn()
    started = time.perf_counter()
    for _ in range(operations):
        fn()
    elapsed = time.perf_counter() - started
    return WorkerResult("", elapsed, operations, operations / elapsed)


class _DiscardQueue:
    """Queue-shaped sink so admission benchmarks do not time cleanup."""

    __slots__ = ()

    def qsize(self) -> int:
        return 0

    def empty(self) -> bool:
        return True

    def put_nowait(self, item: object) -> None:
        return None


def _install_discard_writer(client: object) -> None:
    queue = _DiscardQueue()
    pump = getattr(client, "_write_pump", None)
    if pump is None:
        client._outbound = queue
        client._max_outbound_messages = 1 << 30
        client._max_outbound_bytes = 1 << 60
    else:
        pump.queue = queue
        pump.max_messages = 1 << 30
        pump.max_bytes = 1 << 60


def _worker(args: argparse.Namespace) -> None:  # noqa: C901
    _pin(args.cpu)
    from mqttium.api.async_client import AsyncClient
    from mqttium.codec.buffer import IncrementalDecoder
    from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
    from mqttium.packets import PublishPacket
    from mqttium.protocol.effects import EffectKind
    from mqttium.protocol.engine import EngineConfig, ProtocolEngine

    topic = "bench/sensors/temp"
    payload = b'{"t":21.5,"h":40}'
    scenario = args.scenario

    if scenario.startswith("encode_"):
        qos = QoS.AT_MOST_ONCE if scenario == "encode_qos0" else QoS.AT_LEAST_ONCE
        packet = PublishPacket(
            topic=topic,
            payload=payload,
            qos=qos,
            retain=False,
            dup=False,
            mid=None if qos == QoS.AT_MOST_ONCE else 42,
        )
        result = _measure(
            lambda: packet.encode(MQTTProtocolVersion.MQTTv311),
            operations=120_000,
            warmup=2_000,
        )
    elif scenario == "ingress_engine_qos0":
        packet = PublishPacket(
            topic=topic,
            payload=payload,
            qos=QoS.AT_MOST_ONCE,
            retain=False,
            dup=False,
        )
        wire = packet.encode(MQTTProtocolVersion.MQTTv311)
        decoder = IncrementalDecoder()
        decoder.feed(wire)
        raw = decoder.drain_packets()[0]
        engine = ProtocolEngine(
            EngineConfig(client_id="paired-ingress", protocol=MQTTProtocolVersion.MQTTv311)
        )
        engine.state = ConnectionState.CONNECTED

        def run_ingress() -> None:
            engine.handle_raw(raw)
            engine.take_effects()

        result = _measure(run_ingress, operations=80_000, warmup=2_000)
    elif scenario in ("qos1_cycle_memory", "qos1_cycle_sqlite", "qos2_cycle_sqlite"):
        # One full publish/acknowledge cycle: admission, launch, and the
        # settlement path that has to release the store row, the flow slot, the
        # packet id and the byte budget.
        import shutil
        import tempfile

        from mqttium.packets import PubAckPacket, PubCompPacket, PubRecPacket, encode_frame
        from mqttium.persistence.memory import MemoryInflightStore
        from mqttium.persistence.sqlite import SqliteInflightStore

        directory = tempfile.mkdtemp(prefix="mqttium-paired-")
        store: object
        if scenario == "qos1_cycle_memory":
            store = MemoryInflightStore()
        else:
            store = SqliteInflightStore(Path(directory) / "inflight.db")
        payload_4k = b"z" * 4096
        qos = QoS.EXACTLY_ONCE if scenario == "qos2_cycle_sqlite" else QoS.AT_LEAST_ONCE
        engine = ProtocolEngine(
            EngineConfig(
                client_id="paired-cycle",
                protocol=MQTTProtocolVersion.MQTTv311,
                max_pending_outbound_messages=None,
                max_pending_outbound_bytes=None,
            ),
            store=store,  # type: ignore[arg-type]
        )
        engine.begin_connect()
        decoder = IncrementalDecoder()
        decoder.feed(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
        engine.handle_raw(decoder.drain_packets()[0])
        engine.take_effects()

        def feed(wire: bytes) -> None:
            local = IncrementalDecoder()
            local.feed(wire)
            engine.handle_raw(local.drain_packets()[0])

        if qos == QoS.AT_LEAST_ONCE:

            def run_cycle() -> None:
                handle = engine.queue_publish(topic, payload_4k, qos=1)
                engine.take_effects()
                feed(PubAckPacket(mid=handle.mid or 0).encode())
                engine.take_effects()
        else:

            def run_cycle() -> None:
                handle = engine.queue_publish(topic, payload_4k, qos=2)
                engine.take_effects()
                mid = handle.mid or 0
                feed(PubRecPacket(mid=mid).encode())
                engine.take_effects()
                feed(PubCompPacket(mid=mid).encode())
                engine.take_effects()

        try:
            result = _measure(run_cycle, operations=5_000, warmup=500)
        finally:
            close = getattr(store, "close", None)
            if close is not None:
                close()
            shutil.rmtree(directory, ignore_errors=True)
    elif scenario == "ingress_publish_qos1":
        from mqttium.packets import encode_frame
        from mqttium.persistence.memory import MemoryInflightStore

        engine = ProtocolEngine(
            EngineConfig(client_id="paired-ingress-qos1", protocol=MQTTProtocolVersion.MQTTv311),
            store=MemoryInflightStore(),
        )
        engine.begin_connect()
        decoder = IncrementalDecoder()
        decoder.feed(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
        engine.handle_raw(decoder.drain_packets()[0])
        engine.take_effects()
        inbound_wire = PublishPacket(
            topic=topic,
            payload=payload,
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            dup=False,
            mid=17,
        ).encode(MQTTProtocolVersion.MQTTv311)
        inbound_decoder = IncrementalDecoder()
        inbound_decoder.feed(inbound_wire)
        inbound_raw = inbound_decoder.drain_packets()[0]

        def run_inbound() -> None:
            engine.handle_raw(inbound_raw)
            engine.take_effects()

        result = _measure(run_inbound, operations=60_000, warmup=2_000)
    elif scenario == "writer_try_enqueue":
        client = AsyncClient(client_id="paired-writer-try")
        _install_discard_writer(client)
        item = b"x"

        def run_writer_try() -> None:
            if not client._try_enqueue_outbound(item):
                raise RuntimeError("writer try-enqueue unexpectedly refused")

        result = _measure(run_writer_try, operations=160_000, warmup=3_000)
    elif scenario == "writer_enqueue_async":
        client = AsyncClient(client_id="paired-writer-async")
        _install_discard_writer(client)
        item = b"x"

        async def run_writer_async() -> WorkerResult:
            warmup = 2_000
            operations = 60_000
            for index in range(warmup + operations):
                if index == warmup:
                    started = time.perf_counter()
                await client._enqueue_outbound(item)
            elapsed = time.perf_counter() - started
            return WorkerResult(scenario, elapsed, operations, operations / elapsed)

        result = asyncio.run(run_writer_async())
    elif scenario == "async_publish_nowait_qos0":
        client = AsyncClient(client_id="paired-async-nowait")
        _install_discard_writer(client)
        client._engine.state = ConnectionState.CONNECTED
        publish_payload = b"x"

        async def run_async_nowait() -> WorkerResult:
            warmup = 2_000
            operations = 60_000
            for index in range(warmup + operations):
                if index == warmup:
                    started = time.perf_counter()
                await client.publish(topic, publish_payload, qos=0, nowait=True)
            elapsed = time.perf_counter() - started
            return WorkerResult(scenario, elapsed, operations, operations / elapsed)

        result = asyncio.run(run_async_nowait())
    elif scenario == "native_publish_nowait_qos0":
        client = AsyncClient(client_id="paired-native-nowait")
        _install_discard_writer(client)
        client._engine.state = ConnectionState.CONNECTED
        payload = b"x"
        use_native = hasattr(client, "publish_nowait")

        async def run_native() -> WorkerResult:
            warmup = 2_000
            operations = 60_000
            for index in range(warmup + operations):
                if index == warmup:
                    started = time.perf_counter()
                if use_native:
                    client.publish_nowait(topic, payload, qos=0)
                else:
                    await client.publish(topic, payload, qos=0, nowait=True)
            elapsed = time.perf_counter() - started
            return WorkerResult(scenario, elapsed, operations, operations / elapsed)

        result = asyncio.run(run_native())
    elif scenario == "compat_publish_qos1":
        from mqttium.compat.paho import CallbackAPIVersion, Client

        client = Client(
            CallbackAPIVersion.VERSION2,
            client_id="paired-compat-qos1",
            max_pending_outbound_messages=None,
            max_pending_outbound_bytes=None,
        )
        client.loop_start()
        try:
            client._run_loop_mutation(
                lambda: setattr(client._async._engine, "state", ConnectionState.CONNECTED)
            )

            def run_compat_qos1() -> None:
                info = client.publish(topic, b"x", qos=1)
                if info.mid is None:
                    raise RuntimeError("QoS 1 publish returned no MID")

            result = _measure(run_compat_qos1, operations=2_000, warmup=200)
        finally:
            client.loop_stop()
    elif scenario == "compat_publish_qos0_batch":
        import threading

        from mqttium.compat.paho import CallbackAPIVersion, Client

        client = Client(
            CallbackAPIVersion.VERSION2,
            client_id="paired-compat-qos0",
            max_pending_outbound_messages=None,
            max_pending_outbound_bytes=None,
        )
        client._async._max_outbound_messages = 100_000
        client._async._max_outbound_bytes = 8 * 1024 * 1024
        client.loop_start()
        try:
            client._run_loop_mutation(
                lambda: setattr(client._async._engine, "state", ConnectionState.CONNECTED)
            )

            def submit_and_drain(count: int) -> float:
                started = time.perf_counter()
                for _ in range(count):
                    client.publish(topic, b"x", qos=0)
                drained = threading.Event()

                def fence() -> None:
                    with client._publish_schedule_lock:
                        idle = (
                            not client._publish_drain_scheduled
                            and client._publish_spillover is None
                            and client._publish_pending.empty()
                        )
                    if idle:
                        drained.set()
                    else:
                        assert client._loop is not None
                        client._loop.call_soon(fence)

                assert client._loop is not None
                client._loop.call_soon_threadsafe(fence)
                if not drained.wait(timeout=10.0):
                    raise RuntimeError("QoS 0 compatibility batch did not drain")
                return time.perf_counter() - started

            submit_and_drain(1_000)
            operations = 20_000
            elapsed = submit_and_drain(operations)
            result = WorkerResult(scenario, elapsed, operations, operations / elapsed)
        finally:
            client.loop_stop()
    elif scenario in ("effect_send_inline", "effect_batch_inline"):
        client = AsyncClient(client_id="paired-effects")
        _install_discard_writer(client)
        client._engine.state = ConnectionState.CONNECTED
        batch_size = 1 if scenario == "effect_send_inline" else 8

        def run_effects() -> None:
            for _ in range(batch_size):
                client._engine._emit(EffectKind.SEND, b"x")
            client._collect_effects_locked()

        result = _measure(
            run_effects,
            operations=100_000 if batch_size == 1 else 30_000,
            warmup=2_000,
        )
    else:
        raise ValueError(f"unknown scenario: {scenario}")

    result.scenario = scenario
    print(json.dumps(asdict(result)))


def _run_worker(
    script: Path,
    root: Path,
    scenario: str,
    *,
    cpu: int | None,
) -> WorkerResult:
    env = os.environ.copy()
    source = str(root.resolve() / "src")
    env["PYTHONPATH"] = source
    command = [
        sys.executable,
        str(script),
        "--worker",
        "--scenario",
        scenario,
    ]
    if cpu is not None:
        command.extend(["--cpu", str(cpu)])
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    return WorkerResult(**json.loads(lines[-1]))


def _median(values: list[float]) -> float:
    return statistics.median(values)


def _parent(args: argparse.Namespace) -> None:
    script = Path(__file__).resolve()
    base = args.base_root.resolve()
    candidate = args.candidate_root.resolve()
    payload: dict[str, object] = {
        "base_root": str(base),
        "candidate_root": str(candidate),
        "repeat": args.repeat,
        "cpu": args.cpu,
        "scenarios": [],
    }

    scenarios = (args.scenario,) if args.scenario is not None else SCENARIOS
    for scenario in scenarios:
        pairs: list[dict[str, object]] = []
        ratios: list[float] = []
        for index in range(args.repeat):
            order = ("base", "candidate") if index % 2 == 0 else ("candidate", "base")
            measured: dict[str, WorkerResult] = {}
            for variant in order:
                root = base if variant == "base" else candidate
                measured[variant] = _run_worker(script, root, scenario, cpu=args.cpu)
            base_rate = measured["base"].ops_per_s
            candidate_rate = measured["candidate"].ops_per_s
            ratio = candidate_rate / base_rate
            ratios.append(ratio)
            pairs.append(
                {
                    "order": list(order),
                    "base_ops_per_s": base_rate,
                    "candidate_ops_per_s": candidate_rate,
                    "candidate_over_base": ratio,
                }
            )
        scenario_result = {
            "name": scenario,
            "median_candidate_over_base": _median(ratios),
            "min_candidate_over_base": min(ratios),
            "max_candidate_over_base": max(ratios),
            "pairs": pairs,
        }
        payload["scenarios"].append(scenario_result)  # type: ignore[index]
        print(
            f"{scenario:24s} candidate/base={_median(ratios):.4f} "
            f"range=[{min(ratios):.4f}, {max(ratios):.4f}]"
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--scenario", choices=SCENARIOS)
    parser.add_argument("--base-root", type=Path)
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--repeat", type=int, default=9)
    parser.add_argument("--cpu", type=int)
    parser.add_argument("--output", type=Path, default=Path("/tmp/paired-regression.json"))
    args = parser.parse_args()
    if args.worker and args.scenario is None:
        parser.error("--scenario is required with --worker")
    if not args.worker and (args.base_root is None or args.candidate_root is None):
        parser.error("--base-root and --candidate-root are required")
    return args


if __name__ == "__main__":
    arguments = parse_args()
    if arguments.worker:
        _worker(arguments)
    else:
        _parent(arguments)
