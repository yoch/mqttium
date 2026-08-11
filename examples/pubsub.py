"""Complete native publish/subscribe round trip against an MQTT broker."""

from __future__ import annotations

import argparse
import asyncio
import uuid

from mqttium import MQTTProtocolVersion
from mqttium.api import AsyncClient, Message


def _protocol(value: str) -> MQTTProtocolVersion:
    return MQTTProtocolVersion.MQTTv5 if value == "5" else MQTTProtocolVersion.MQTTv311


async def run(
    host: str,
    port: int,
    topic: str,
    protocol: MQTTProtocolVersion,
    timeout: float,
) -> None:
    received: asyncio.Future[Message] = asyncio.get_running_loop().create_future()
    client = AsyncClient(f"mqttium-pubsub-{uuid.uuid4().hex}", protocol=protocol)

    async def on_message(message: Message) -> None:
        if message.topic == topic and not received.done():
            received.set_result(message)

    client.on_message = on_message
    try:
        await client.connect(host, port, timeout=timeout)
        result = await client.subscribe(topic, qos=1, timeout=timeout)
        if any(code >= 0x80 for code in result.reason_codes):
            raise RuntimeError(f"subscription rejected: {result.reason_codes}")

        receipt = await client.publish(topic, b"hello from mqttium", qos=1)
        await receipt.wait()
        message = await asyncio.wait_for(received, timeout=timeout)
        print(f"{message.topic} => {message.payload!r}")
    finally:
        await client.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--topic", default=f"mqttium/example/{uuid.uuid4().hex}")
    parser.add_argument("--protocol", choices=("311", "5"), default="311")
    parser.add_argument("--timeout", type=float, default=5.0)
    args = parser.parse_args()
    asyncio.run(run(args.host, args.port, args.topic, _protocol(args.protocol), args.timeout))


if __name__ == "__main__":
    main()
