"""Rotated A/B of the durable `seq` indices under a realistic mixed profile.

The microbenchmark that motivated this script commits once per statement, which
no real client does: `AsyncClient` wraps a whole ingress lot in `store.batch()`,
so a segment carrying many PUBACKs settles inside a single transaction. This
harness reproduces that structure — publications commit individually, incoming
acknowledgements settle in batches — and adds the other half of the trade-off,
the reconnect replay the indices exist for.

Each variant is measured in the same process on the same store shape, with the
variant order rotated between repeats and medians reported, per
`docs/benchmarking.md`. Results are build artefacts: do not commit them.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import statistics
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import InboundQoSState, MQTTProtocolVersion, PacketType, QoS
from mqttium.packets import PubAckPacket, encode_frame
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.engine import EngineConfig, ProtocolEngine
from mqttium.types import InboundMessage

VARIANTS = ("both", "inbound", "none")


@dataclass
class Sample:
    variant: str
    publish_ack_ops_per_s: float
    loop_lag_p95_ms: float
    loop_lag_p99_ms: float
    outbound_replay_ms: float
    inbound_replay_ms: float
    startup_ms: float


def make_store(path: Path, variant: str) -> SqliteInflightStore:
    """Open a store and add the `seq` indices the variant is testing.

    `none` is what MQTTium actually ships: ordered pagination is served by one
    sorted metadata pass over a payload-last schema, which measured faster than
    the indexed form while costing nothing on every publish. The other two
    variants recreate the indexed design so the decision stays falsifiable — if
    a future schema or query change flips the trade-off, this harness shows it.
    """
    store = SqliteInflightStore(path)
    if variant in ("both", "inbound"):
        store._conn.execute("CREATE INDEX IF NOT EXISTS inbound_seq_idx ON inbound(seq)")
    if variant == "both":
        store._conn.execute("CREATE INDEX IF NOT EXISTS outbound_seq_idx ON outbound(seq)")
    store._conn.commit()
    return store


def connected_engine(store: SqliteInflightStore, *, session_present: bool) -> ProtocolEngine:
    engine = ProtocolEngine(
        EngineConfig(
            client_id="persistence-ab",
            protocol=MQTTProtocolVersion.MQTTv311,
            clean_start=False,
            max_pending_outbound_messages=None,
            max_pending_outbound_bytes=None,
        ),
        store=store,
    )
    engine.begin_connect()
    engine.take_effects()
    feed(engine, encode_frame(PacketType.CONNACK, 0, bytes((int(session_present), 0))))
    return engine


def feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    for raw in decoder.drain_packets():
        engine.handle_raw(raw)


class LagSampler:
    """Event-loop delay, sampled the way a production stall would show up."""

    def __init__(self, interval: float = 0.005) -> None:
        self.interval = interval
        self.samples: list[float] = []
        self._stop = False

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._stop:
            before = loop.time()
            await asyncio.sleep(self.interval)
            self.samples.append((loop.time() - before - self.interval) * 1000.0)

    def stop(self) -> None:
        self._stop = True

    def percentile(self, fraction: float) -> float:
        if not self.samples:
            return 0.0
        ordered = sorted(self.samples)
        index = min(len(ordered) - 1, int(fraction * len(ordered)))
        return ordered[index]


async def publish_ack_profile(
    engine: ProtocolEngine,
    store: SqliteInflightStore,
    *,
    count: int,
    payload_bytes: int,
    ack_batch: int,
) -> float:
    """Sustained QoS 1 publishing with acknowledgements arriving in segments."""
    payload = b"z" * payload_bytes
    pending: list[int] = []
    started = time.perf_counter()
    for _index in range(count):
        handle = engine.queue_publish("bench/durable", payload, qos=1)
        engine.take_effects()
        if handle.mid is not None:
            pending.append(handle.mid)
        if len(pending) >= ack_batch:
            # One ingress lot: the client holds a single transaction open for
            # every acknowledgement carried by one transport read.
            with store.batch():
                for mid in pending:
                    feed(engine, PubAckPacket(mid=mid).encode())
            engine.take_effects()
            pending.clear()
            await asyncio.sleep(0)
    if pending:
        with store.batch():
            for mid in pending:
                feed(engine, PubAckPacket(mid=mid).encode())
        engine.take_effects()
    return count / (time.perf_counter() - started)


def seed_session(
    store: SqliteInflightStore,
    *,
    outbound_records: int,
    inbound_records: int,
    payload_bytes: int,
) -> None:
    payload = b"y" * payload_bytes
    engine = connected_engine(store, session_present=False)
    with store.batch():
        for _ in range(outbound_records):
            engine.queue_publish("bench/session", payload, qos=1)
    engine.take_effects()
    with store.batch():
        for mid in range(1, inbound_records + 1):
            store.put_in(
                InboundMessage(
                    mid=mid,
                    topic="bench/inbound",
                    payload=payload,
                    qos=QoS.EXACTLY_ONCE,
                    retain=False,
                    state=InboundQoSState.WAIT_PUBREL,
                    delivered=False,
                )
            )


def replay_profile(path: Path, variant: str) -> tuple[float, float, float]:
    """Startup hydration, outbound replay and inbound replay-to-completion."""
    started = time.perf_counter()
    store = make_store(path, variant)
    engine = ProtocolEngine(
        EngineConfig(client_id="persistence-ab-replay", clean_start=False),
        store=store,
    )
    startup_ms = (time.perf_counter() - started) * 1000.0

    engine.begin_connect()
    engine.take_effects()
    outbound_started = time.perf_counter()
    engine.outbound.replay_session()
    outbound_ms = (time.perf_counter() - outbound_started) * 1000.0
    engine.take_effects()

    inbound_started = time.perf_counter()
    engine.inbound.replay_session()
    engine.take_effects()
    while engine.inbound.replay_pending:
        engine.continue_inbound_replay()
        engine.take_effects()
    inbound_ms = (time.perf_counter() - inbound_started) * 1000.0
    store.close()
    return startup_ms, outbound_ms, inbound_ms


async def measure(variant: str, args: argparse.Namespace) -> Sample:
    with tempfile.TemporaryDirectory(prefix="mqttium-index-ab-") as directory:
        hot_path = Path(directory) / "hot.db"
        store = make_store(hot_path, variant)
        engine = connected_engine(store, session_present=False)
        sampler = LagSampler()
        sampler_task = asyncio.create_task(sampler.run())
        try:
            ops = await publish_ack_profile(
                engine,
                store,
                count=args.publishes,
                payload_bytes=args.payload,
                ack_batch=args.ack_batch,
            )
        finally:
            sampler.stop()
            await sampler_task
        store.close()

        session_path = Path(directory) / "session.db"
        seed_store = make_store(session_path, variant)
        seed_session(
            seed_store,
            outbound_records=args.session_records,
            inbound_records=args.session_records,
            payload_bytes=args.payload,
        )
        seed_store.close()
        startup_ms, outbound_ms, inbound_ms = replay_profile(session_path, variant)

    return Sample(
        variant=variant,
        publish_ack_ops_per_s=ops,
        loop_lag_p95_ms=sampler.percentile(0.95),
        loop_lag_p99_ms=sampler.percentile(0.99),
        outbound_replay_ms=outbound_ms,
        inbound_replay_ms=inbound_ms,
        startup_ms=startup_ms,
    )


async def run(args: argparse.Namespace) -> dict[str, object]:
    per_variant: dict[str, list[Sample]] = {variant: [] for variant in VARIANTS}
    for repeat in range(args.repeat):
        # Rotate the variant order so warm caches cannot favour one of them.
        order = VARIANTS[repeat % len(VARIANTS) :] + VARIANTS[: repeat % len(VARIANTS)]
        for variant in order:
            per_variant[variant].append(await measure(variant, args))

    summary: list[dict[str, object]] = []
    for variant, samples in per_variant.items():
        summary.append(
            {
                "variant": variant,
                "publish_ack_ops_per_s": statistics.median(
                    s.publish_ack_ops_per_s for s in samples
                ),
                "loop_lag_p95_ms": statistics.median(s.loop_lag_p95_ms for s in samples),
                "loop_lag_p99_ms": statistics.median(s.loop_lag_p99_ms for s in samples),
                "outbound_replay_ms": statistics.median(s.outbound_replay_ms for s in samples),
                "inbound_replay_ms": statistics.median(s.inbound_replay_ms for s in samples),
                "startup_ms": statistics.median(s.startup_ms for s in samples),
                "samples": [asdict(s) for s in samples],
            }
        )
    return {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "publishes": args.publishes,
        "payload_bytes": args.payload,
        "ack_batch": args.ack_batch,
        "session_records": args.session_records,
        "repeat": args.repeat,
        "variants": summary,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--publishes", type=int, default=5_000)
    parser.add_argument("--payload", type=int, default=4096)
    parser.add_argument("--ack-batch", type=int, default=32)
    parser.add_argument("--session-records", type=int, default=10_000)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = asyncio.run(run(args))
    header = (
        f"{'variant':8s} {'pub+ack/s':>10s} {'lag p95':>9s} {'lag p99':>9s} "
        f"{'out replay':>11s} {'in replay':>10s} {'startup':>9s}"
    )
    print(header)
    for entry in payload["variants"]:  # type: ignore[index]
        print(
            f"{entry['variant']:8s} {entry['publish_ack_ops_per_s']:10.0f} "
            f"{entry['loop_lag_p95_ms']:8.2f}m {entry['loop_lag_p99_ms']:8.2f}m "
            f"{entry['outbound_replay_ms']:10.1f}m {entry['inbound_replay_ms']:9.1f}m "
            f"{entry['startup_ms']:8.1f}m"
        )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, indent=2) + "\n")


if __name__ == "__main__":
    main()
