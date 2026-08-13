"""Temporary paired native QoS1 ingress review for PR #204."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path


def _publish(host: str, port: int, topic: str, count: int, payload: bytes, protocol: str) -> None:
    import paho.mqtt.client as mqtt

    connected = threading.Event()
    proto = mqtt.MQTTv5 if protocol == "5" else mqtt.MQTTv311
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"pr204-pub-{os.getpid()}-{time.time_ns()}",
        protocol=proto,
    )
    client.max_inflight_messages_set(32)
    client.max_queued_messages_set(0)

    def on_connect(_client, _userdata, _flags, reason_code, _properties=None):
        if reason_code.is_failure:
            raise RuntimeError(f"publisher connection refused: {reason_code}")
        connected.set()

    client.on_connect = on_connect
    client.connect(host, port, keepalive=30)
    client.loop_start()
    try:
        if not connected.wait(timeout=10):
            raise TimeoutError("publisher CONNACK timeout")
        last = None
        for _ in range(count):
            last = client.publish(topic, payload, qos=1)
            if last.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError(f"publisher refused message: rc={last.rc}")
        assert last is not None
        last.wait_for_publish(timeout=20)
        if not last.is_published():
            raise TimeoutError("publisher did not finish QoS1 drain")
    finally:
        client.disconnect()
        client.loop_stop()


async def _worker(args: argparse.Namespace) -> dict[str, object]:
    from mqttium.api import AsyncClient
    from mqttium.enums import MQTTProtocolVersion
    from mqttium.protocol.reconnect import ReconnectPolicy

    protocol = MQTTProtocolVersion.MQTTv5 if args.protocol == "5" else MQTTProtocolVersion.MQTTv311
    client = AsyncClient(
        client_id=f"pr204-sub-{os.getpid()}-{time.time_ns()}",
        protocol=protocol,
        local_receive_maximum=256,
        message_delivery="callback",
        max_pending_callbacks=max(4096, args.count + 256),
        max_pending_delivery_bytes=None,
        reconnect=ReconnectPolicy(enabled=False),
    )
    received = 0
    complete = asyncio.Event()

    def on_message(_message) -> None:
        nonlocal received
        received += 1
        if received == args.count:
            complete.set()

    client.on_message = on_message
    await client.connect(args.host, args.port, timeout=10.0)
    topic = f"bench/pr204/{os.getpid()}/{time.time_ns()}"
    await client.subscribe(topic, qos=1)
    payload = b"x" * args.payload_bytes
    started = time.perf_counter()
    publish_task = asyncio.create_task(
        asyncio.to_thread(
            _publish,
            args.host,
            args.port,
            topic,
            args.count,
            payload,
            args.protocol,
        )
    )
    try:
        await asyncio.wait_for(complete.wait(), timeout=20.0)
        await publish_task
    except BaseException:
        if not publish_task.done():
            publish_task.cancel()
        raise
    elapsed = time.perf_counter() - started
    stats = client.stats()
    await client.disconnect()
    return {
        "protocol": args.protocol,
        "payload_bytes": args.payload_bytes,
        "count": args.count,
        "elapsed_s": elapsed,
        "msg_s": args.count / elapsed,
        "received": received,
        "effect_inline": stats.effects.inline_effects,
        "effect_enqueued": stats.effects.enqueued,
        "effect_suspensions": stats.effects.apply_suspensions,
    }


def _run_variant(
    script: Path,
    root: Path,
    args: argparse.Namespace,
    protocol: str,
    payload: int,
    count: int,
) -> dict[str, object]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root.resolve() / "src")
    command = [
        sys.executable,
        str(script),
        "--worker",
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--protocol",
        protocol,
        "--payload-bytes",
        str(payload),
        "--count",
        str(count),
    ]
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        env=env,
        timeout=35,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"worker failed rc={completed.returncode}\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    return json.loads(lines[-1])


def _parent(args: argparse.Namespace) -> None:
    script = Path(__file__).resolve()
    roots = {"base": args.base_root, "candidate": args.candidate_root}
    scenarios = [
        ("311", 64, 1500),
        ("5", 64, 1500),
        ("311", 4096, 750),
        ("5", 4096, 750),
    ]
    report: dict[str, object] = {"repeat": args.repeat, "scenarios": []}
    output = report["scenarios"]
    assert isinstance(output, list)
    for protocol, payload, count in scenarios:
        ratios: list[float] = []
        pairs: list[dict[str, object]] = []
        for index in range(args.repeat):
            order = ("base", "candidate") if index % 2 == 0 else ("candidate", "base")
            measured = {
                variant: _run_variant(script, roots[variant], args, protocol, payload, count)
                for variant in order
            }
            base_rate = float(measured["base"]["msg_s"])
            candidate_rate = float(measured["candidate"]["msg_s"])
            ratio = candidate_rate / base_rate
            ratios.append(ratio)
            pairs.append(
                {
                    "order": order,
                    "base_msg_s": base_rate,
                    "candidate_msg_s": candidate_rate,
                    "candidate_over_base": ratio,
                }
            )
        row = {
            "protocol": protocol,
            "payload_bytes": payload,
            "count": count,
            "median_candidate_over_base": statistics.median(ratios),
            "min_candidate_over_base": min(ratios),
            "max_candidate_over_base": max(ratios),
            "pairs_favouring_candidate": sum(ratio > 1.0 for ratio in ratios),
            "pairs": pairs,
        }
        output.append(row)
        print(
            f"MQTT {protocol} payload={payload}: candidate/base="
            f"{row['median_candidate_over_base']:.4f}, positive="
            f"{row['pairs_favouring_candidate']}/{args.repeat}, range="
            f"[{row['min_candidate_over_base']:.4f}, {row['max_candidate_over_base']:.4f}]"
        )
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--base-root", type=Path)
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11883)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--protocol", choices=("311", "5"))
    parser.add_argument("--payload-bytes", type=int)
    parser.add_argument("--count", type=int)
    parser.add_argument("--output", type=Path, default=Path("/tmp/pr204-native-ingress.json"))
    args = parser.parse_args()
    if args.worker:
        if args.protocol is None or args.payload_bytes is None or args.count is None:
            parser.error("worker arguments are required")
    elif args.base_root is None or args.candidate_root is None:
        parser.error("base and candidate roots are required")
    return args


if __name__ == "__main__":
    arguments = _parse_args()
    if arguments.worker:
        print(json.dumps(asyncio.run(_worker(arguments))))
    else:
        _parent(arguments)
