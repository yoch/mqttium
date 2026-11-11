#!/usr/bin/env python3
"""Measure the event-loop pause of outbound replay on a resumed session.

Replaying unacknowledged publications when a session is resumed, and hydrating
the store when a client is constructed, are synchronous and linear in the
number of records. This reports both durations at engine level, so the figure
quoted in the sizing guide can be reproduced on the reference runner.

No broker and no event loop are involved: the time reported is the time the
loop would be blocked.
"""

from __future__ import annotations

import argparse
import json
import statistics
import tempfile
import time
from pathlib import Path

from mqttium.codec import properties as property_codec
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import MQTTProtocolVersion, PacketType
from mqttium.packets import encode_frame
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.protocol.config import EngineConfig
from mqttium.protocol.engine import ProtocolEngine
from mqttium.types import Properties

# (store, records, payload bytes, Receive Maximum of the resumed connection)
SCENARIOS = (
    ("sqlite", 10_000, 256, 100),
    ("sqlite", 10_000, 256, 65_535),
    ("sqlite", 10_000, 4096, 65_535),
    ("sqlite", 50_000, 256, 100),
    ("memory", 10_000, 256, 100),
    ("memory", 10_000, 256, 65_535),
    ("memory", 10_000, 4096, 65_535),
    ("memory", 50_000, 256, 100),
)


def _feed(engine: ProtocolEngine, wire: bytes) -> None:
    decoder = IncrementalDecoder()
    decoder.feed(wire)
    raw = decoder.next_packet()
    assert raw is not None
    engine.handle_raw(raw)


def _connack(session_present: bool, receive_maximum: int) -> bytes:
    properties = property_codec.encode_properties(
        Properties({"receive_maximum": receive_maximum}), property_codec.CONNACK
    )
    return encode_frame(PacketType.CONNACK, 0, bytes((int(session_present), 0)) + properties)


def _config() -> EngineConfig:
    return EngineConfig(
        client_id="replay-pause",
        protocol=MQTTProtocolVersion.MQTTv5,
        clean_start=False,
        max_unacknowledged_messages=None,
        max_unacknowledged_bytes=None,
    )


def measure(store_kind: str, records: int, payload: int, receive_maximum: int) -> dict[str, float]:
    """One fresh session: fill it, then time construction and CONNACK processing."""
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "inflight.sqlite"
        store = SqliteInflightStore(path) if store_kind == "sqlite" else MemoryInflightStore()
        engine = ProtocolEngine(_config(), store)
        engine.begin_connect()
        # Every record is sent on the first connection when the window allows.
        _feed(engine, _connack(False, min(receive_maximum, records)))
        engine.take_effects()
        with store.batch():
            for index in range(records):
                engine.queue_publish(f"replay/{index}", b"x" * payload, qos=1)
        engine.take_effects()
        if store_kind == "sqlite":
            store.close()
            store = SqliteInflightStore(path)
        started = time.perf_counter()
        resumed = ProtocolEngine(_config(), store)
        hydrated = time.perf_counter()
        resumed.begin_connect()
        before_connack = time.perf_counter()
        _feed(resumed, _connack(True, receive_maximum))
        replayed = time.perf_counter()
        resent = len(resumed.take_effects()) - 1
        if store_kind == "sqlite":
            store.close()
    return {
        "construct_ms": (hydrated - started) * 1e3,
        "connack_ms": (replayed - before_connack) * 1e3,
        "resent": float(resent),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    results = []
    for store_kind, records, payload, receive_maximum in SCENARIOS:
        runs = [measure(store_kind, records, payload, receive_maximum) for _ in range(args.repeat)]
        row = {
            "store": store_kind,
            "records": records,
            "payload_bytes": payload,
            "receive_maximum": receive_maximum,
            "resent": int(runs[0]["resent"]),
            "construct_ms_median": statistics.median(run["construct_ms"] for run in runs),
            "construct_ms_max": max(run["construct_ms"] for run in runs),
            "connack_ms_median": statistics.median(run["connack_ms"] for run in runs),
            "connack_ms_max": max(run["connack_ms"] for run in runs),
        }
        results.append(row)
        print(
            f"{store_kind:6} records={records:6} payload={payload:5} resent={row['resent']:6}: "
            f"construct {row['construct_ms_median']:7.1f} ms (max {row['construct_ms_max']:.1f}), "
            f"CONNACK {row['connack_ms_median']:7.1f} ms (max {row['connack_ms_max']:.1f})"
        )
    if args.output is not None:
        args.output.write_text(
            json.dumps({"repeat": args.repeat, "results": results}, indent=2) + "\n",
            encoding="utf-8",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
