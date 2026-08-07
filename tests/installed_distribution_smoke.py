"""Smoke-test an installed MQTTium distribution against a real broker."""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata

from mqttium import (
    ConnectionState,
    FlowControlError,
    MQTTError,
    MQTTProtocolVersion,
    MQTTTimeoutError,
    MalformedPacketError,
    MessageDeliveryError,
    NotConnectedError,
    PacketTooLargeError,
    ProtocolError,
    PublishBatchError,
    QoS,
    SessionDiscardedError,
    __version__,
)
from mqttium.api import (
    AsyncClient,
    AuthPacket,
    ConnAckPacket,
    Message,
    MessageDelivery,
    NegotiatedSettings,
    Properties,
    PublishBackpressure,
    PublishBatchReceipt,
    PublishMessage,
    PublishReceipt,
    ReconnectPolicy,
    SubscribeOptions,
    SubscribeResult,
    UnsubscribeResult,
)
from mqttium.helpers import publish as helper_publish
from mqttium.helpers import subscribe as helper_subscribe

_STABLE_IMPORTS = (
    ConnectionState,
    FlowControlError,
    MQTTError,
    MQTTProtocolVersion,
    MQTTTimeoutError,
    MalformedPacketError,
    MessageDeliveryError,
    NotConnectedError,
    PacketTooLargeError,
    ProtocolError,
    PublishBatchError,
    QoS,
    SessionDiscardedError,
    AsyncClient,
    AuthPacket,
    ConnAckPacket,
    Message,
    MessageDelivery,
    NegotiatedSettings,
    Properties,
    PublishBackpressure,
    PublishBatchReceipt,
    PublishMessage,
    PublishReceipt,
    ReconnectPolicy,
    SubscribeOptions,
    SubscribeResult,
    UnsubscribeResult,
    helper_publish,
    helper_subscribe,
)


async def _roundtrip(host: str, port: int, protocol: MQTTProtocolVersion) -> None:
    suffix = f"{int(protocol)}-{id(asyncio.current_task())}"
    topic = f"mqttium/beta-smoke/{suffix}"
    payload = f"installed-artifact-{int(protocol)}".encode()
    received: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()

    subscriber = AsyncClient(f"mqttium-beta-sub-{suffix}", protocol=protocol)
    publisher = AsyncClient(f"mqttium-beta-pub-{suffix}", protocol=protocol)

    def on_message(message: Message) -> None:
        if message.topic == topic and not received.done():
            received.set_result(message.payload)

    subscriber.on_message = on_message
    try:
        connack = await subscriber.connect(host, port, timeout=5)
        assert isinstance(connack, ConnAckPacket)
        assert subscriber.state is ConnectionState.CONNECTED
        assert subscriber.is_connected
        assert subscriber.negotiated is not None

        subscription = await subscriber.subscribe(topic, qos=QoS.AT_LEAST_ONCE)
        assert isinstance(subscription, SubscribeResult)

        await publisher.connect(host, port, timeout=5)
        receipt = await publisher.publish(topic, payload, qos=QoS.AT_LEAST_ONCE)
        assert isinstance(receipt, PublishReceipt)
        await receipt.wait()

        assert await asyncio.wait_for(received, timeout=5) == payload
        unsubscription = await subscriber.unsubscribe(topic)
        assert isinstance(unsubscription, UnsubscribeResult)

        publisher.stats()
        subscriber.stats()
    finally:
        if publisher.is_connected:
            await publisher.disconnect()
        if subscriber.is_connected:
            await subscriber.disconnect()


async def _main(expected_version: str, host: str, port: int) -> None:
    installed_version = importlib.metadata.version("mqttium")
    assert installed_version == expected_version
    assert __version__ == expected_version
    assert _STABLE_IMPORTS

    for protocol in (MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5):
        await _roundtrip(host, port, protocol)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11883)
    args = parser.parse_args()
    asyncio.run(_main(args.expected_version, args.host, args.port))


if __name__ == "__main__":
    main()
