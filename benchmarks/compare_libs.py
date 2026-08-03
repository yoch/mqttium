"""Comparable MQTT publisher benchmarks.

Micro results are only shown where libraries expose equivalent isolated APIs.
QoS 1 compares mqttium and Paho with the same sliding window and waits for
PUBACK. gmqtt is reported as N/A because the pinned public API has no
equivalent per-publish completion contract.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import json
import os
import platform
import statistics
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import paho.mqtt.client as mqtt
from gmqtt import Message as GMessage
from gmqtt.mqtt.constants import MQTTv311 as G_MQTTv311
from gmqtt.mqtt.package import PublishPacket as GPublish
from gmqtt.mqtt.utils import IdGenerator

from mqttium.api import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import QoS as MqQoS
from mqttium.packets import PublishPacket as MqPublish
from mqttium.protocol.reconnect import ReconnectPolicy


BROKER = ("127.0.0.1", 11883)
WINDOW = 100


@dataclass
class Sample:
    name: str
    library: str
    ops_per_s: float | None
    unit: str
    notes: str = ""


def package_version(name: str) -> str:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


def metadata() -> dict[str, object]:
    return {
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "versions": {
            "mqttium": package_version("mqttium"),
            "paho-mqtt": package_version("paho-mqtt"),
            "gmqtt": package_version("gmqtt"),
        },
    }


def median_ops(fn, iterations: int, runs: int = 7, warmup: int = 200) -> float:
    for _ in range(warmup):
        fn()
    rates = []
    for _ in range(runs):
        started = time.perf_counter()
        for _ in range(iterations):
            fn()
        rates.append(iterations / (time.perf_counter() - started))
    return statistics.median(rates)


class GProto:
    def __init__(self) -> None:
        self.proto_ver = G_MQTTv311
        self.id_generator = IdGenerator()


def micro_encode() -> list[Sample]:
    topic = "bench/sensors/temp"
    payload = b'{"t":21.5,"h":40}'
    packet = MqPublish(
        topic=topic,
        payload=payload,
        qos=MqQoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
    )
    proto = GProto()
    message = GMessage(topic, payload, qos=0)
    return [
        Sample(
            "publish_encode_qos0_small",
            "mqttium",
            median_ops(packet.encode, 20_000),
            "msg/s",
        ),
        Sample(
            "publish_encode_qos0_small",
            "gmqtt",
            median_ops(lambda: GPublish.build_package(message, proto), 20_000),
            "msg/s",
        ),
        Sample(
            "publish_encode_qos0_small",
            "paho",
            None,
            "msg/s",
            "N/A: no isolated public codec API",
        ),
    ]


def micro_decode() -> list[Sample]:
    wire = MqPublish(
        topic="bench/sensors/temp",
        payload=b'{"t":21.5}',
        qos=MqQoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
    ).encode()
    blob = wire * 50

    def decode() -> None:
        decoder = IncrementalDecoder()
        decoder.feed(blob)
        while decoder.next_packet() is not None:
            pass

    return [
        Sample(
            "ingress_decode_50x_qos0_small",
            "mqttium",
            median_ops(decode, 3_000),
            "batch/s",
        ),
        Sample(
            "ingress_decode_50x_qos0_small",
            "gmqtt",
            None,
            "batch/s",
            "N/A: decoder is coupled to the client protocol",
        ),
        Sample(
            "ingress_decode_50x_qos0_small",
            "paho",
            None,
            "batch/s",
            "N/A: decoder is coupled to the network loop",
        ),
    ]


async def mqttium_puback(count: int, payload: bytes) -> float:
    client = AsyncClient(
        client_id=f"mn-{time.time_ns()}",
        max_outbound_inflight=WINDOW,
        reconnect=ReconnectPolicy(enabled=False),
    )
    await client.connect(*BROKER, timeout=5.0)
    pending = []
    started = time.perf_counter()
    for _ in range(count):
        pending.append(await client.publish("bench/mqttium", payload, qos=1))
        if len(pending) >= WINDOW:
            await pending.pop(0).wait()
    await asyncio.gather(*(receipt.wait() for receipt in pending))
    elapsed = time.perf_counter() - started
    await client.disconnect()
    return count / elapsed


def paho_puback(count: int, payload: bytes) -> float:
    connected = threading.Event()
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"paho-{time.time_ns()}",
    )
    client.max_inflight_messages_set(WINDOW)
    client.max_queued_messages_set(WINDOW * 2)

    def on_connect(_client, _userdata, _flags, reason_code, _properties) -> None:
        if reason_code == 0:
            connected.set()

    client.on_connect = on_connect
    client.connect(*BROKER, keepalive=60)
    client.loop_start()
    if not connected.wait(5.0):
        client.loop_stop()
        raise TimeoutError("Paho did not connect")

    pending = []
    started = time.perf_counter()
    for _ in range(count):
        info = client.publish("bench/paho", payload, qos=1)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            client.loop_stop()
            raise RuntimeError(f"Paho publish failed rc={info.rc}")
        pending.append(info)
        if len(pending) >= WINDOW:
            pending.pop(0).wait_for_publish(timeout=30.0)
    for info in pending:
        info.wait_for_publish(timeout=30.0)
    elapsed = time.perf_counter() - started
    client.disconnect()
    client.loop_stop()
    return count / elapsed


async def e2e(count: int) -> list[Sample]:
    payload = b"x" * 64
    rates = {"mqttium": [], "paho": []}
    orders = (("mqttium", "paho"), ("paho", "mqttium"), ("mqttium", "paho"))
    for order in orders:
        for library in order:
            if library == "mqttium":
                rate = await mqttium_puback(count, payload)
            else:
                rate = await asyncio.to_thread(paho_puback, count, payload)
            rates[library].append(rate)
    name = f"publisher_puback_qos1_p64_w{WINDOW}"
    return [
        Sample(
            name,
            "mqttium",
            statistics.median(rates["mqttium"]),
            "msg/s",
            f"count={count}; sliding window={WINDOW}",
        ),
        Sample(
            name,
            "gmqtt",
            None,
            "msg/s",
            "N/A: no equivalent public per-publish completion contract",
        ),
        Sample(
            name,
            "paho",
            statistics.median(rates["paho"]),
            "msg/s",
            f"count={count}; sliding window={WINDOW}",
        ),
    ]


def markdown(samples: list[Sample]) -> str:
    rows: dict[str, dict[str, Sample]] = {}
    for sample in samples:
        rows.setdefault(sample.name, {})[sample.library] = sample
    lines = [
        "| Scenario | mqttium | gmqtt | paho | mqttium/paho |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for name, row in rows.items():
        values = {
            library: row.get(library, Sample(name, library, None, "")).ops_per_s
            for library in ("mqttium", "gmqtt", "paho")
        }
        ratio = (
            values["mqttium"] / values["paho"]
            if values["mqttium"] is not None and values["paho"]
            else None
        )

        def fmt(value: float | None) -> str:
            if value is None:
                return "n/a"
            return f"{value:,.0f}" if value >= 1000 else f"{value:,.2f}"

        lines.append(
            f"| `{name}` | {fmt(values['mqttium'])} | "
            f"{fmt(values['gmqtt'])} | {fmt(values['paho'])} | "
            f"{fmt(ratio)}× |"
        )
    return "\n".join(lines)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=8_000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    samples = [*micro_encode(), *micro_decode(), *(await e2e(args.count))]
    table = markdown(samples)
    payload = {
        "metadata": metadata(),
        "broker": list(BROKER),
        "window": WINDOW,
        "samples": [asdict(sample) for sample in samples],
        "table_md": table,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2))
    print(table)


if __name__ == "__main__":
    asyncio.run(main())
