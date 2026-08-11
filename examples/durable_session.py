"""Configure broker session retention and SQLite-backed inflight state."""

from __future__ import annotations

import argparse
import asyncio
import uuid
from pathlib import Path

from mqttium import MQTTProtocolVersion
from mqttium.api import AsyncClient, Properties, ReconnectPolicy
from mqttium.persistence import SqliteInflightStore


def _protocol(value: str) -> MQTTProtocolVersion:
    return MQTTProtocolVersion.MQTTv5 if value == "5" else MQTTProtocolVersion.MQTTv311


async def run(
    host: str,
    port: int,
    client_id: str,
    topic: str,
    protocol: MQTTProtocolVersion,
    session_expiry: int,
    timeout: float,
    store: SqliteInflightStore,
) -> None:
    connect_properties = (
        Properties({"session_expiry_interval": session_expiry})
        if protocol == MQTTProtocolVersion.MQTTv5
        else None
    )
    client = AsyncClient(
        client_id,
        protocol=protocol,
        clean_start=False,
        connect_properties=connect_properties,
        reconnect=ReconnectPolicy(connect_timeout=timeout),
        store=store,
    )
    try:
        connack = await client.connect(host, port, timeout=timeout)
        receipt = await client.publish(topic, b"durable hello", qos=1)
        await receipt.wait()
        print(
            "connected",
            f"session_present={connack.session_present}",
            f"pending={client.stats().outbound.pending_messages}",
        )
    finally:
        await client.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--client-id", default=f"mqttium-durable-{uuid.uuid4().hex}")
    parser.add_argument("--topic", default=f"mqttium/durable/{uuid.uuid4().hex}")
    parser.add_argument("--protocol", choices=("311", "5"), default="5")
    parser.add_argument("--session-expiry", type=int, default=86_400)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--database", type=Path, default=Path("mqttium-session.sqlite"))
    args = parser.parse_args()

    with SqliteInflightStore(args.database) as store:
        asyncio.run(
            run(
                args.host,
                args.port,
                args.client_id,
                args.topic,
                _protocol(args.protocol),
                args.session_expiry,
                args.timeout,
                store,
            )
        )


if __name__ == "__main__":
    main()
