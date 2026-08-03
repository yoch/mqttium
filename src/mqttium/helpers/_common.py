"""Shared construction helpers for one-shot convenience APIs."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Literal

from mqttium.api.async_client import AsyncClient
from mqttium.enums import MQTTProtocolVersion


def create_client(
    *,
    client_id: str,
    protocol: MQTTProtocolVersion,
    keepalive: int,
    username: str | None,
    password: bytes | str | None,
    message_delivery: Literal["auto", "callback"] = "auto",
) -> AsyncClient:
    return AsyncClient(
        client_id=client_id,
        protocol=protocol,
        keepalive=keepalive,
        username=username,
        password=password,
        message_delivery=message_delivery,
    )


def normalize_topics(topics: str | Iterable[str]) -> tuple[str, ...]:
    return (topics,) if isinstance(topics, str) else tuple(topics)
