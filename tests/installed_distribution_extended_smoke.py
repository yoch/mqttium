"""Exercise installed-distribution transports, migration, and clean shutdown."""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
from collections.abc import Awaitable, Callable
from pathlib import Path

from mqttium import ConnectionState, MQTTProtocolVersion, QoS, __version__
from mqttium.api import AsyncClient, Message, PublishReceipt

Connector = Callable[[AsyncClient], Awaitable[object]]


def _verify_version(expected_version: str) -> None:
    assert importlib.metadata.version("mqttium") == expected_version
    assert __version__ == expected_version


async def _native_roundtrip(
    connector: Connector,
    *,
    protocol: MQTTProtocolVersion,
    transport: str,
) -> None:
    topic = f"mqttium/distribution-extended/{transport}/{int(protocol)}"
    payload = f"{transport}-{int(protocol)}".encode()
    received: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()
    subscriber = AsyncClient(
        f"dist-{transport}-sub-{int(protocol)}", protocol=protocol, message_delivery="callback"
    )
    publisher = AsyncClient(f"dist-{transport}-pub-{int(protocol)}", protocol=protocol)

    def on_message(message: Message) -> None:
        if message.topic == topic and not received.done():
            received.set_result(message.payload)

    subscriber.on_message = on_message
    try:
        await connector(subscriber)
        await subscriber.subscribe(topic, qos=QoS.AT_LEAST_ONCE)
        await connector(publisher)
        receipt = await publisher.publish(topic, payload, qos=QoS.AT_LEAST_ONCE)
        assert isinstance(receipt, PublishReceipt)
        await receipt.wait()
        assert await asyncio.wait_for(received, timeout=5) == payload
    finally:
        if publisher.is_connected:
            await publisher.disconnect()
        if subscriber.is_connected:
            await subscriber.disconnect()


async def _websocket_smoke(url: str) -> None:
    async def connect(client: AsyncClient) -> object:
        return await client.connect_ws(url, timeout=5)

    for protocol in (MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5):
        await _native_roundtrip(connect, protocol=protocol, transport="websocket")


async def _unix_smoke(path: Path) -> None:
    assert path.is_socket()

    async def connect(client: AsyncClient) -> object:
        return await client.connect_unix(str(path), timeout=5)

    for protocol in (MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5):
        await _native_roundtrip(connect, protocol=protocol, transport="unix")


async def _cancellation_and_shutdown_smoke(host: str, port: int) -> None:
    client = AsyncClient("dist-clean-shutdown", message_delivery="iterator")
    await client.connect(host, port, timeout=5)
    iterator = client.messages()
    waiting = asyncio.create_task(anext(iterator))
    await asyncio.sleep(0)
    waiting.cancel()
    try:
        await waiting
    except asyncio.CancelledError:
        pass
    else:
        raise AssertionError("pending iterator read did not cancel")
    await iterator.aclose()
    await client.disconnect()

    snapshot = client.stats()
    assert snapshot.state is ConnectionState.DISCONNECTED
    assert not any(
        (
            snapshot.tasks.reader,
            snapshot.tasks.writer,
            snapshot.tasks.keepalive,
            snapshot.tasks.reconnect,
            snapshot.tasks.effect_flush,
            snapshot.tasks.callback_worker,
        )
    )
    assert snapshot.receipts.publish == 0
    assert snapshot.receipts.subscribe == 0
    assert snapshot.receipts.unsubscribe == 0
    assert snapshot.receipts.publish_waiters == 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("websocket", "unix", "shutdown"))
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11886)
    parser.add_argument("--url", default="ws://127.0.0.1:18083/mqtt")
    parser.add_argument("--socket", type=Path, default=Path("/tmp/mqttium-dist.sock"))
    args = parser.parse_args()

    _verify_version(args.expected_version)
    if args.command == "websocket":
        asyncio.run(_websocket_smoke(args.url))
    elif args.command == "unix":
        asyncio.run(_unix_smoke(args.socket))
    else:
        asyncio.run(_cancellation_and_shutdown_smoke(args.host, args.port))


if __name__ == "__main__":
    main()
