"""Minimal publish/subscribe example (requires a local MQTT broker)."""

from __future__ import annotations

import argparse
import asyncio

from mqttium.api import AsyncClient


async def main(host: str, port: int, topic: str) -> None:
    client = AsyncClient(client_id="mqttium-example")

    def on_message(msg) -> None:
        print(f"{msg.topic} => {msg.payload!r}")

    client.on_message = on_message
    await client.connect(host, port)
    sub = await client.subscribe(topic, qos=1)
    print(f"subscribed mid={sub.mid} rc={sub.reason_codes}")
    receipt = await client.publish(topic, b"hello from mqttium", qos=1)
    await receipt.wait()
    await asyncio.sleep(0.5)
    await client.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--topic", default="mqttium/demo")
    args = parser.parse_args()
    asyncio.run(main(args.host, args.port, args.topic))
