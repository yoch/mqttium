from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time
import tracemalloc
from pathlib import Path
from typing import Any


def _worker(args: argparse.Namespace) -> None:
    from mqttium.codec.buffer import RawPacket
    from mqttium.enums import (
        ConnectionState,
        InboundQoSState,
        MQTTProtocolVersion,
        OutboundQoSState,
        PacketType,
        QoS,
    )
    from mqttium.persistence.memory import MemoryInflightStore
    from mqttium.persistence.sqlite import SqliteInflightStore
    from mqttium.protocol.engine import EffectKind, EngineConfig, ProtocolEngine
    from mqttium.types import InboundMessage, OutboundMessage

    class CounterMixin:
        counts: dict[str, int]

        def _count(self, name: str) -> None:
            self.counts[name] = self.counts.get(name, 0) + 1

        def reset_counts(self) -> None:
            self.counts.clear()

        def get_out(self, *a: Any, **kw: Any) -> Any:
            self._count("get_out")
            return super().get_out(*a, **kw)  # type: ignore[misc]

        def complete_out(self, *a: Any, **kw: Any) -> Any:
            self._count("complete_out")
            return super().complete_out(*a, **kw)  # type: ignore[misc]

        def transition_out(self, *a: Any, **kw: Any) -> Any:
            self._count("transition_out")
            return super().transition_out(*a, **kw)  # type: ignore[misc]

        def out_summary_pages(self, *a: Any, **kw: Any) -> Any:
            self._count("out_summary_pages")
            yield from super().out_summary_pages(*a, **kw)  # type: ignore[misc]

        def get_in(self, *a: Any, **kw: Any) -> Any:
            self._count("get_in")
            return super().get_in(*a, **kw)  # type: ignore[misc]

        def in_index_pages(self, *a: Any, **kw: Any) -> Any:
            self._count("in_index_pages")
            yield from super().in_index_pages(*a, **kw)  # type: ignore[misc]

        def in_replay_pages(self, *a: Any, **kw: Any) -> Any:
            self._count("in_replay_pages")
            yield from super().in_replay_pages(*a, **kw)  # type: ignore[misc]

        def update_out(self, *a: Any, **kw: Any) -> Any:
            self._count("update_out")
            method = getattr(super(), "update_out", None)
            if method is None:
                raise AssertionError("runtime called retired update_out()")
            return method(*a, **kw)

    class CountingMemory(CounterMixin, MemoryInflightStore):
        def __init__(self) -> None:
            self.counts = {}
            super().__init__()

    class CountingSqlite(CounterMixin, SqliteInflightStore):
        def __init__(self, path: Path) -> None:
            self.counts = {}
            super().__init__(path)

    def make_store(kind: str, path: Path) -> Any:
        return CountingMemory() if kind == "memory" else CountingSqlite(path)

    def close_store(store: Any) -> None:
        close = getattr(store, "close", None)
        if close is not None:
            close()

    def outbound_message(mid: int, payload: bytes) -> Any:
        topic = "bench/outbound"
        message = OutboundMessage(
            mid=mid,
            topic=topic,
            payload=payload,
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBACK,
        )
        message.logical_size = len(topic) + len(payload)
        return message

    def inbound_message(mid: int, payload: bytes) -> Any:
        topic = "bench/inbound"
        message = InboundMessage(
            mid=mid,
            topic=topic,
            payload=payload,
            qos=QoS.EXACTLY_ONCE,
            retain=False,
            state=InboundQoSState.WAIT_PUBREL,
            delivered=False,
        )
        message.logical_size = len(topic) + len(payload)
        return message

    def engine_for(store: Any) -> Any:
        return ProtocolEngine(
            EngineConfig(
                client_id="pr434-bench",
                protocol=MQTTProtocolVersion.MQTTv311,
                clean_start=False,
                max_outbound_inflight=65535,
                max_pending_outbound_messages=None,
                max_pending_outbound_bytes=None,
            ),
            store=store,
        )

    def seed_out(store: Any, count: int, payload: bytes) -> None:
        with store.batch():
            for mid in range(1, count + 1):
                store.put_out(outbound_message(mid, payload))

    def seed_in(store: Any, count: int, payload: bytes) -> None:
        with store.batch():
            for mid in range(1, count + 1):
                store.put_in(inbound_message(mid, payload))

    def ack_case(kind: str, count: int) -> dict[str, Any]:
        with tempfile.TemporaryDirectory(prefix=f"pr434-ack-{kind}-") as td:
            store = make_store(kind, Path(td) / "store.db")
            seed_out(store, count, b"x")
            engine = engine_for(store)
            engine.state = ConnectionState.CONNECTED
            engine.take_effects()
            store.reset_counts()
            packets = [
                RawPacket(
                    packet_type=PacketType.PUBACK,
                    flags=0,
                    remaining=mid.to_bytes(2, "big"),
                )
                for mid in range(1, count + 1)
            ]
            tracemalloc.start()
            started = time.perf_counter()
            with store.batch():
                for raw in packets:
                    engine.handle_raw(raw)
            elapsed = time.perf_counter() - started
            _current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            effects = engine.take_effects()
            complete = sum(e.kind is EffectKind.PUBLISH_COMPLETE for e in effects)
            assert complete == count, (kind, complete, count)
            assert store.counts.get("complete_out", 0) == count, store.counts
            assert store.counts.get("get_out", 0) == 0, store.counts
            result = {
                "ops_s": count / elapsed,
                "elapsed_ms": elapsed * 1000.0,
                "peak_kib": peak / 1024.0,
                "counts": dict(store.counts),
            }
            close_store(store)
            return result

    def replay_out_case(kind: str, count: int, payload_bytes: int) -> dict[str, Any]:
        with tempfile.TemporaryDirectory(prefix=f"pr434-replay-out-{kind}-") as td:
            path = Path(td) / "store.db"
            payload = b"y" * payload_bytes
            store = make_store(kind, path)
            seed_out(store, count, payload)
            if kind == "sqlite":
                close_store(store)
                store = make_store(kind, path)

            tracemalloc.start()
            startup_started = time.perf_counter()
            engine = engine_for(store)
            startup_ms = (time.perf_counter() - startup_started) * 1000.0
            _current, startup_peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            engine.take_effects()
            store.reset_counts()

            tracemalloc.start()
            started = time.perf_counter()
            engine.outbound.replay_session()
            elapsed = time.perf_counter() - started
            _current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            effects = engine.take_effects()
            sends = sum(e.kind is EffectKind.SEND for e in effects)
            assert sends == count, (kind, sends, count)
            assert store.counts.get("get_out", 0) == count, store.counts
            result = {
                "startup_ms": startup_ms,
                "startup_peak_kib": startup_peak / 1024.0,
                "replay_ms": elapsed * 1000.0,
                "replay_peak_kib": peak / 1024.0,
                "counts": dict(store.counts),
                "sends": sends,
            }
            close_store(store)
            return result

    def replay_in_case(kind: str, count: int, payload_bytes: int) -> dict[str, Any]:
        with tempfile.TemporaryDirectory(prefix=f"pr434-replay-in-{kind}-") as td:
            path = Path(td) / "store.db"
            payload = b"z" * payload_bytes
            store = make_store(kind, path)
            seed_in(store, count, payload)
            if kind == "sqlite":
                close_store(store)
                store = make_store(kind, path)

            tracemalloc.start()
            startup_started = time.perf_counter()
            engine = engine_for(store)
            startup_ms = (time.perf_counter() - startup_started) * 1000.0
            _current, startup_peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            engine.take_effects()
            store.reset_counts()

            delivered = 0
            tracemalloc.start()
            started = time.perf_counter()
            engine.inbound.replay_session()
            while True:
                delivered += sum(e.kind is EffectKind.MESSAGE for e in engine.take_effects())
                if not engine.inbound.replay_pending:
                    break
                engine.continue_inbound_replay()
            elapsed = time.perf_counter() - started
            _current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            assert delivered == count, (kind, delivered, count)
            result = {
                "startup_ms": startup_ms,
                "startup_peak_kib": startup_peak / 1024.0,
                "replay_ms": elapsed * 1000.0,
                "replay_peak_kib": peak / 1024.0,
                "counts": dict(store.counts),
                "delivered": delivered,
            }
            close_store(store)
            return result

    payload = {
        "ack_memory": ack_case("memory", args.ack_memory),
        "ack_sqlite": ack_case("sqlite", args.ack_sqlite),
        "replay_out_memory": replay_out_case("memory", args.replay_memory, args.payload),
        "replay_out_sqlite": replay_out_case("sqlite", args.replay_sqlite, args.payload),
        "replay_in_memory": replay_in_case("memory", args.replay_memory, args.payload),
        "replay_in_sqlite": replay_in_case("sqlite", args.replay_sqlite, args.payload),
    }
    print(json.dumps(payload, sort_keys=True))


def _metric(sample: dict[str, Any], path: str) -> float:
    node: Any = sample
    for part in path.split("."):
        node = node[part]
    return float(node)


def _parent(args: argparse.Namespace) -> None:
    labels = {"A1": args.base_root, "A2": args.base_root, "B": args.candidate_root}
    samples: dict[str, list[dict[str, Any]]] = {label: [] for label in labels}
    script = str(Path(__file__).resolve())
    for repeat in range(args.repeat):
        order = ["A1", "A2", "B"]
        order = order[repeat % 3 :] + order[: repeat % 3]
        for label in order:
            env = os.environ.copy()
            env["PYTHONPATH"] = str(Path(labels[label]) / "src")
            command = [
                sys.executable,
                script,
                "--worker",
                "--ack-memory",
                str(args.ack_memory),
                "--ack-sqlite",
                str(args.ack_sqlite),
                "--replay-memory",
                str(args.replay_memory),
                "--replay-sqlite",
                str(args.replay_sqlite),
                "--payload",
                str(args.payload),
            ]
            proc = subprocess.run(command, env=env, text=True, capture_output=True, check=True)
            sample = json.loads(proc.stdout.strip().splitlines()[-1])
            samples[label].append(sample)
            print(f"repeat={repeat + 1} label={label} complete", flush=True)

    metric_specs = {
        "ack_memory_ops_s": "ack_memory.ops_s",
        "ack_sqlite_ops_s": "ack_sqlite.ops_s",
        "out_memory_startup_ms": "replay_out_memory.startup_ms",
        "out_sqlite_startup_ms": "replay_out_sqlite.startup_ms",
        "out_memory_startup_peak_kib": "replay_out_memory.startup_peak_kib",
        "out_sqlite_startup_peak_kib": "replay_out_sqlite.startup_peak_kib",
        "out_memory_replay_ms": "replay_out_memory.replay_ms",
        "out_sqlite_replay_ms": "replay_out_sqlite.replay_ms",
        "out_memory_replay_peak_kib": "replay_out_memory.replay_peak_kib",
        "out_sqlite_replay_peak_kib": "replay_out_sqlite.replay_peak_kib",
        "in_memory_startup_ms": "replay_in_memory.startup_ms",
        "in_sqlite_startup_ms": "replay_in_sqlite.startup_ms",
        "in_memory_startup_peak_kib": "replay_in_memory.startup_peak_kib",
        "in_sqlite_startup_peak_kib": "replay_in_sqlite.startup_peak_kib",
        "in_memory_replay_ms": "replay_in_memory.replay_ms",
        "in_sqlite_replay_ms": "replay_in_sqlite.replay_ms",
        "in_memory_replay_peak_kib": "replay_in_memory.replay_peak_kib",
        "in_sqlite_replay_peak_kib": "replay_in_sqlite.replay_peak_kib",
    }
    summary: dict[str, Any] = {
        "base_sha": args.base_sha,
        "candidate_sha": args.candidate_sha,
        "repeat": args.repeat,
        "ack_memory": args.ack_memory,
        "ack_sqlite": args.ack_sqlite,
        "replay_memory": args.replay_memory,
        "replay_sqlite": args.replay_sqlite,
        "payload": args.payload,
        "metrics": {},
        "call_counts": {},
        "samples": samples,
    }

    print("\nmetric                              A/A control    B vs A      A median       B median")
    print("-" * 92)
    for name, path in metric_specs.items():
        a1 = [_metric(sample, path) for sample in samples["A1"]]
        a2 = [_metric(sample, path) for sample in samples["A2"]]
        b = [_metric(sample, path) for sample in samples["B"]]
        control = [100.0 * (y / x - 1.0) for x, y in zip(a1, a2, strict=True)]
        paired = [
            100.0 * (candidate / ((x + y) / 2.0) - 1.0)
            for x, y, candidate in zip(a1, a2, b, strict=True)
        ]
        abase = statistics.median(a1 + a2)
        bmed = statistics.median(b)
        control_med = statistics.median(control)
        paired_med = statistics.median(paired)
        summary["metrics"][name] = {
            "baseline_median": abase,
            "candidate_median": bmed,
            "aa_control_delta_pct_median": control_med,
            "candidate_delta_pct_median_paired": paired_med,
        }
        print(
            f"{name:34s} {control_med:+9.2f}% {paired_med:+9.2f}% "
            f"{abase:13.3f} {bmed:13.3f}"
        )

    for scenario in (
        "ack_memory",
        "ack_sqlite",
        "replay_out_memory",
        "replay_out_sqlite",
        "replay_in_memory",
        "replay_in_sqlite",
    ):
        for label in ("A1", "A2", "B"):
            keys = set().union(*(sample[scenario]["counts"].keys() for sample in samples[label]))
            medians = {
                key: statistics.median(
                    sample[scenario]["counts"].get(key, 0) for sample in samples[label]
                )
                for key in sorted(keys)
            }
            summary["call_counts"][f"{scenario}.{label}"] = medians

    # The central architectural proof: base replay writes every retransmission
    # through update_out; the simplified candidate must never call it.
    for scenario in ("replay_out_memory", "replay_out_sqlite"):
        candidate_updates = [
            sample[scenario]["counts"].get("update_out", 0) for sample in samples["B"]
        ]
        if any(candidate_updates):
            raise AssertionError(f"candidate unexpectedly called update_out in {scenario}")

    print("\nCALL_COUNTS")
    for key, value in summary["call_counts"].items():
        print(f"{key}: {value}")
    print("\nRESULT_JSON=" + json.dumps(summary, sort_keys=True))
    if args.output:
        Path(args.output).write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--base-root")
    parser.add_argument("--candidate-root")
    parser.add_argument("--base-sha", default="")
    parser.add_argument("--candidate-sha", default="")
    parser.add_argument("--repeat", type=int, default=7)
    parser.add_argument("--ack-memory", type=int, default=20_000)
    parser.add_argument("--ack-sqlite", type=int, default=3_000)
    parser.add_argument("--replay-memory", type=int, default=4_000)
    parser.add_argument("--replay-sqlite", type=int, default=2_000)
    parser.add_argument("--payload", type=int, default=4096)
    parser.add_argument("--output")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.worker:
        _worker(args)
    else:
        if not args.base_root or not args.candidate_root:
            raise SystemExit("--base-root and --candidate-root are required")
        _parent(args)


if __name__ == "__main__":
    main()
