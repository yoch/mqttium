"""One-shot async publish helpers (Paho publish.single / multiple analogue)."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from mqttium.helpers._common import create_client
from mqttium.enums import MQTTProtocolVersion, QoS


def _normalize_message(
    message: Mapping[str, Any] | Sequence[Any],
) -> tuple[str, bytes | str, int, bool]:
    if isinstance(message, Mapping):
        topic = str(message["topic"])
        payload = message.get("payload", b"")
        qos = int(message.get("qos", 0))
        retain = bool(message.get("retain", False))
        return topic, payload, qos, retain
    if len(message) == 2:
        topic, payload = message
        return str(topic), payload, 0, False
    if len(message) == 3:
        topic, payload, qos = message
        return str(topic), payload, int(qos), False
    topic, payload, qos, retain = message  # type: ignore[misc]
    return str(topic), payload, int(qos), bool(retain)


async def multiple(
    msgs: Iterable[Mapping[str, Any] | Sequence[Any]],
    *,
    hostname: str = "localhost",
    port: int = 1883,
    client_id: str = "",
    keepalive: int = 60,
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    username: str | None = None,
    password: bytes | str | None = None,
    ssl: Any = None,
) -> None:
    """Connect, publish several messages (await QoS completion), disconnect."""
    client = create_client(
        client_id=client_id,
        protocol=protocol,
        keepalive=keepalive,
        username=username,
        password=password,
    )
    await client.connect(hostname, port, ssl=ssl)
    try:
        receipts = []
        for raw in msgs:
            topic, payload, qos, retain = _normalize_message(raw)
            receipts.append(await client.publish(topic, payload, qos=QoS(qos), retain=retain))
        for receipt in receipts:
            await receipt.wait()
    finally:
        await client.disconnect()


async def single(
    topic: str,
    payload: bytes | str = b"",
    *,
    qos: int = 0,
    retain: bool = False,
    hostname: str = "localhost",
    port: int = 1883,
    client_id: str = "",
    keepalive: int = 60,
    protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
    username: str | None = None,
    password: bytes | str | None = None,
    ssl: Any = None,
) -> None:
    """Connect, publish one message, wait for QoS completion, disconnect."""
    await multiple(
        [{"topic": topic, "payload": payload, "qos": qos, "retain": retain}],
        hostname=hostname,
        port=port,
        client_id=client_id,
        keepalive=keepalive,
        protocol=protocol,
        username=username,
        password=password,
        ssl=ssl,
    )
