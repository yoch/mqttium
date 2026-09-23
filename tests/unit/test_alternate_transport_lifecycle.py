"""AsyncClient lifecycle over Unix sockets and WebSockets."""

from __future__ import annotations

import ssl

import pytest

from mqttium.api import AsyncClient
from mqttium.transport.unix import UnixSocketTransport
from mqttium.transport.websocket import WebSocketTransport
from tests.support import ScriptedBrokerTransport


async def test_connect_unix_uses_the_path_and_resets_tcp_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = ScriptedBrokerTransport()
    paths: list[str] = []

    async def connect(path: str):
        paths.append(path)
        return transport

    monkeypatch.setattr(UnixSocketTransport, "connect", staticmethod(connect))
    client = AsyncClient("unix-client")

    connack = await client.connect_unix("/tmp/mqttium-test.sock", timeout=1.0)
    try:
        assert connack.reason_code == 0
        assert paths == ["/tmp/mqttium-test.sock"]
        assert client.is_connected
    finally:
        await client.disconnect()


async def test_connect_ws_forwards_tls_and_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = ScriptedBrokerTransport()
    calls: list[tuple[str, object, dict[str, str] | None, float | None, int]] = []
    context = ssl.create_default_context()

    async def connect(
        url: str,
        *,
        ssl: object = None,
        extra_headers: dict[str, str] | None = None,
        timeout: float | None,
        max_frame_size: int,
    ):
        # Required keywords: a caller that stops forwarding them fails here.
        calls.append((url, ssl, extra_headers, timeout, max_frame_size))
        return transport

    monkeypatch.setattr(WebSocketTransport, "connect", staticmethod(connect))
    client = AsyncClient("websocket-client")
    headers = {"X-Test": "mqttium"}

    connack = await client.connect_ws(
        "wss://broker.example/mqtt",
        ssl=context,
        extra_headers=headers,
        timeout=1.0,
    )
    try:
        assert connack.reason_code == 0
        # The attempt owns the deadline; frames and messages fit the MQTT limit.
        assert calls == [("wss://broker.example/mqtt", context, headers, None, 16 * 1024 * 1024)]
        assert client.is_connected
    finally:
        await client.disconnect()
