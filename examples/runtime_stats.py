"""Inspect negotiated limits and a side-effect-free runtime snapshot."""

from __future__ import annotations

import argparse
import asyncio
import json
import uuid
from dataclasses import asdict

from mqttium import MQTTProtocolVersion
from mqttium.api import AsyncClient


def _protocol(value: str) -> MQTTProtocolVersion:
    return MQTTProtocolVersion.MQTTv5 if value == "5" else MQTTProtocolVersion.MQTTv311


async def run(
    host: str,
    port: int,
    topic: str,
    protocol: MQTTProtocolVersion,
    timeout: float,
) -> None:
    client = AsyncClient(f"mqttium-stats-{uuid.uuid4().hex}", protocol=protocol)
    try:
        await client.connect(host, port, timeout=timeout)
        receipt = await client.publish(topic, b"observed", qos=1)
        await receipt.wait()

        payload = {
            "effective_client_id": client.effective_client_id,
            "negotiated": asdict(client.negotiated),
            "stats": asdict(client.stats()),
        }
        print(json.dumps(payload, indent=2, default=str))
    finally:
        await client.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--topic", default=f"mqttium/stats/{uuid.uuid4().hex}")
    parser.add_argument("--protocol", choices=("311", "5"), default="5")
    parser.add_argument("--timeout", type=float, default=5.0)
    args = parser.parse_args()
    asyncio.run(run(args.host, args.port, args.topic, _protocol(args.protocol), args.timeout))


if __name__ == "__main__":
    main()
