"""Reject unsafe WebSocket configuration before opening a socket."""

import ssl
from unittest.mock import AsyncMock

import pytest

from mqttium.api import AsyncClient
from mqttium.transport.websocket import WebSocketTransport, _parse_websocket_endpoint


@pytest.mark.parametrize("public_api", [False, True])
@pytest.mark.parametrize("invalid_ssl", [False, 0, "", 1, "true", [], {}])
async def test_wss_rejects_invalid_tls_before_connect(monkeypatch, public_api, invalid_ssl):
    opening = AsyncMock(side_effect=AssertionError("must not open a socket"))
    monkeypatch.setattr("asyncio.open_connection", opening)
    client = AsyncClient("invalid-wss", keepalive=0)
    connect = client.connect_ws if public_api else WebSocketTransport.connect
    try:
        with pytest.raises(ValueError, match="ssl|TLS"):
            await connect(
                "wss://broker.example/mqtt",
                ssl=invalid_ssl,
                extra_headers={"Authorization": "test-only-sensitive-header"},
            )
        opening.assert_not_awaited()
        assert client._transport is None
        assert not any(client._running_tasks().values())
    finally:
        await client.disconnect()


@pytest.mark.parametrize("public_api", [False, True])
@pytest.mark.parametrize(
    "url",
    ["ws://", "wss://", "ws:///mqtt", "wss:///mqtt", "ws:/mqtt", "ws://:80/mqtt"],
)
async def test_hostless_websocket_never_opens_a_socket(monkeypatch, public_api, url):
    opening = AsyncMock(side_effect=AssertionError("must not open a socket"))
    monkeypatch.setattr("asyncio.open_connection", opening)
    client = AsyncClient("invalid-ws-host", keepalive=0)
    connect = client.connect_ws if public_api else WebSocketTransport.connect
    try:
        with pytest.raises(ValueError, match="hostname"):
            await connect(url, extra_headers={"Authorization": "test-only-sensitive-header"})
        opening.assert_not_awaited()
        assert client._transport is None
        assert not any(client._running_tasks().values())
    finally:
        await client.disconnect()


@pytest.mark.parametrize("url", ["ws://localhost/mqtt", "wss://broker.example/mqtt"])
@pytest.mark.parametrize("option", [None, True, "context"])
async def test_valid_websocket_tls_configuration_is_preserved(monkeypatch, url, option):
    tls = ssl.create_default_context() if option == "context" else option
    failure = OSError("stop after checking the transport arguments")
    opening = AsyncMock(side_effect=failure)
    monkeypatch.setattr("asyncio.open_connection", opening)
    with pytest.raises(OSError) as caught:
        await WebSocketTransport.connect(url, ssl=tls)
    assert caught.value is failure
    expected = True if tls is None and url.startswith("wss:") else tls
    host = "broker.example" if url.startswith("wss:") else "localhost"
    port = 443 if url.startswith("wss:") else 80
    opening.assert_awaited_once_with(host, port, ssl=expected)


def test_plain_websocket_retains_explicit_false_and_ipv6_endpoint():
    assert _parse_websocket_endpoint("ws://[::1]:1883/mqtt?key=value", False) == (
        "::1",
        1883,
        "/mqtt?key=value",
        False,
    )


@pytest.mark.parametrize("invalid_ssl", [0, 1, "", "true", [], {}])
def test_plain_websocket_rejects_invalid_ssl_types(invalid_ssl):
    with pytest.raises(ValueError, match="ssl"):
        _parse_websocket_endpoint("ws://localhost/mqtt", invalid_ssl)
