"""Isolated performance sprint harness for mqttium.

Measures each track separately so optimizations can be accepted/rejected on data.

Tracks:
  A  codec_encode / codec_decode  (CPU, no broker)
  B  ingress_batch                (CPU, IncrementalDecoder + handle_raw)
  C  e2e_qos{0,1,2}               (Mosquitto)
  D  pipeline_window              (e2e QoS1/2 vs max_outbound_inflight)

Usage:
  PYTHONPATH=mqttium/src python3 mqttium/benchmarks/perf_sprint.py
  PYTHONPATH=mqttium/src python3 mqttium/benchmarks/perf_sprint.py --tag baseline
  PYTHONPATH=mqttium/src python3 mqttium/benchmarks/perf_sprint.py --only A,C
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.packets import PublishPacket
from mqttium.protocol.engine import EngineConfig, ProtocolEngine

BROKER = ("127.0.0.1", 11883)
RESULTS = Path(__file__).resolve().parent / "results" / "perf_sprint"
TOPIC = "bench/sensors/temp"
PAYLOAD = b'{"t":21.5,"h":40}'


@dataclass
class Sample:
    track: str
    name: str
    ops_per_s: float
    unit: str
    notes: str = ""


def _median_rate(fn, iterations: int, runs: int = 9, warmup: int = 300) -> float:
    for _ in range(warmup):
        fn()
    rates: list[float] = []
    for _ in range(runs):
        t0 = time.perf_counter()
        for _ in range(iterations):
            fn()
        rates.append(iterations / (time.perf_counter() - t0))
    return statistics.median(rates)


def track_A_codec() -> list[Sample]:
    samples: list[Sample] = []
    pkt0 = PublishPacket(
        topic=TOPIC, payload=PAYLOAD, qos=QoS.AT_MOST_ONCE, retain=False, dup=False
    )
    pkt1 = PublishPacket(
        topic=TOPIC,
        payload=PAYLOAD,
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        dup=False,
        mid=42,
    )
    pkt5 = PublishPacket(
        topic=TOPIC,
        payload=PAYLOAD,
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
    )
    wire311 = pkt0.encode(MQTTProtocolVersion.MQTTv311)
    wire5 = pkt5.encode(MQTTProtocolVersion.MQTTv5)

    samples.append(
        Sample(
            "A",
            "encode_qos0_mqtt311",
            _median_rate(lambda: pkt0.encode(MQTTProtocolVersion.MQTTv311), 40_000),
            "msg/s",
        )
    )
    samples.append(
        Sample(
            "A",
            "encode_qos1_mqtt311",
            _median_rate(lambda: pkt1.encode(MQTTProtocolVersion.MQTTv311), 40_000),
            "msg/s",
        )
    )
    samples.append(
        Sample(
            "A",
            "encode_qos0_mqtt5_empty_props",
            _median_rate(lambda: pkt5.encode(MQTTProtocolVersion.MQTTv5), 40_000),
            "msg/s",
        )
    )

    def decode_one(wire: bytes, proto: MQTTProtocolVersion) -> None:
        # Fixed header is 1 + VBI; remaining is body after that.
        # Wire layout: type|flags, VBI..., body
        from mqttium.codec.vbi import decode_vbi

        rl, pos = decode_vbi(wire, 1)
        body = wire[pos : pos + rl]
        flags = wire[0] & 0x0F
        PublishPacket.decode(flags, body, proto)

    samples.append(
        Sample(
            "A",
            "decode_qos0_mqtt311",
            _median_rate(lambda: decode_one(wire311, MQTTProtocolVersion.MQTTv311), 40_000),
            "msg/s",
        )
    )
    samples.append(
        Sample(
            "A",
            "decode_qos0_mqtt5_empty_props",
            _median_rate(lambda: decode_one(wire5, MQTTProtocolVersion.MQTTv5), 40_000),
            "msg/s",
        )
    )
    return samples


def track_B_ingress() -> list[Sample]:
    samples: list[Sample] = []
    pkt = PublishPacket(topic=TOPIC, payload=PAYLOAD, qos=QoS.AT_MOST_ONCE, retain=False, dup=False)
    wire = pkt.encode()
    batch50 = wire * 50
    batch500 = wire * 500

    def decode_batch(blob: bytes) -> None:
        dec = IncrementalDecoder()
        dec.feed(blob)
        while True:
            got = dec.drain_packets(limit=100)
            if not got:
                break

    samples.append(
        Sample(
            "B",
            "ingress_decode_50x",
            _median_rate(lambda: decode_batch(batch50), 8_000),
            "batch/s",
        )
    )
    samples.append(
        Sample(
            "B",
            "ingress_decode_500x",
            _median_rate(lambda: decode_batch(batch500), 800),
            "batch/s",
        )
    )

    # Engine handle_raw on pre-decoded frames (no socket).
    engine = ProtocolEngine(
        EngineConfig(client_id="bench-ingress", protocol=MQTTProtocolVersion.MQTTv311)
    )
    # Force connected-like path for PUBLISH delivery effects without CONNACK.
    from mqttium.enums import ConnectionState

    engine.state = ConnectionState.CONNECTED

    def feed_engine(blob: bytes) -> None:
        dec = IncrementalDecoder()
        dec.feed(blob)
        while True:
            got = dec.drain_packets(limit=100)
            if not got:
                break
            for raw in got:
                engine.handle_raw(raw)
            engine.take_effects()

    samples.append(
        Sample(
            "B",
            "ingress_engine_50x_qos0",
            _median_rate(lambda: feed_engine(batch50), 2_000),
            "batch/s",
        )
    )
    return samples


async def _e2e_pub(qos: int, count: int, payload: bytes, *, outbound_window: int = 20) -> float:
    from mqttium.protocol.reconnect import ReconnectPolicy

    client = AsyncClient(
        client_id=f"sprint-q{qos}-w{outbound_window}-{time.time_ns() % 1_000_000}",
        local_receive_maximum=100,
        max_outbound_inflight=outbound_window,
        reconnect=ReconnectPolicy(enabled=False),
    )
    await client.connect(*BROKER)
    try:
        t0 = time.perf_counter()
        if qos == 0:
            for _ in range(count):
                await client.publish(TOPIC, payload, qos=0)
        else:
            receipts = []
            for _ in range(count):
                receipts.append(await client.publish(TOPIC, payload, qos=qos))
            await asyncio.gather(*(r.wait() for r in receipts))
        elapsed = time.perf_counter() - t0
        return count / elapsed
    finally:
        await client.disconnect()


def track_C_e2e() -> list[Sample]:
    samples: list[Sample] = []
    counts = {0: 80_000, 1: 8_000, 2: 4_000}

    async def run_all() -> None:
        for qos in (0, 1, 2):
            rates: list[float] = []
            for _ in range(5):
                rates.append(await _e2e_pub(qos, counts[qos], PAYLOAD, outbound_window=20))
            samples.append(
                Sample(
                    "C",
                    f"e2e_pub_qos{qos}_p64_w20",
                    statistics.median(rates),
                    "msg/s",
                    notes=f"count={counts[qos]}",
                )
            )

    asyncio.run(run_all())
    return samples


def track_D_pipeline() -> list[Sample]:
    samples: list[Sample] = []
    windows = (20, 100, 500, 2000)

    async def run_all() -> None:
        for qos in (1, 2):
            count = 8_000 if qos == 1 else 4_000
            for window in windows:
                rates: list[float] = []
                for _ in range(4):
                    rates.append(await _e2e_pub(qos, count, PAYLOAD, outbound_window=window))
                samples.append(
                    Sample(
                        "D",
                        f"e2e_qos{qos}_w{window}",
                        statistics.median(rates),
                        "msg/s",
                        notes=f"count={count}",
                    )
                )

    asyncio.run(run_all())
    return samples


def _write_results(tag: str, samples: list[Sample]) -> Path:
    RESULTS.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    out = RESULTS / f"{tag}_{stamp}.json"
    md = RESULTS / f"{tag}_{stamp}.md"
    payload = {
        "tag": tag,
        "samples": [asdict(s) for s in samples],
    }
    out.write_text(json.dumps(payload, indent=2) + "\n")
    lines = [
        f"# Perf sprint — `{tag}`",
        "",
        f"Date: {stamp}",
        "",
        "| Track | Name | Rate | Unit | Notes |",
        "| --- | --- | ---: | --- | --- |",
    ]
    for s in samples:
        lines.append(f"| {s.track} | `{s.name}` | {s.ops_per_s:,.1f} | {s.unit} | {s.notes} |")
    md.write_text("\n".join(lines) + "\n")
    print(md.read_text())
    print(f"Wrote {out}")
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tag", default="run")
    parser.add_argument("--only", default="A,B,C,D", help="Comma tracks: A,B,C,D")
    args = parser.parse_args()
    only = {x.strip().upper() for x in args.only.split(",") if x.strip()}
    samples: list[Sample] = []
    if "A" in only:
        print("=== Track A: codec ===")
        samples.extend(track_A_codec())
    if "B" in only:
        print("=== Track B: ingress ===")
        samples.extend(track_B_ingress())
    if "C" in only:
        print("=== Track C: e2e ===")
        samples.extend(track_C_e2e())
    if "D" in only:
        print("=== Track D: outbound inflight window ===")
        samples.extend(track_D_pipeline())
    _write_results(args.tag, samples)


if __name__ == "__main__":
    main()
