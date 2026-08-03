"""TLS transport smoke test (audit P1.10): AsyncClient over a real TLS socket."""

from __future__ import annotations

import asyncio
import shutil
import ssl
import subprocess
from pathlib import Path

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.codec.buffer import IncrementalDecoder
from mqttium.enums import PacketType
from mqttium.packets import encode_frame


def _make_certs(tmp_path: Path) -> tuple[Path, Path]:
    if shutil.which("openssl") is None:
        pytest.skip("openssl not available")
    key = tmp_path / "key.pem"
    cert = tmp_path / "cert.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
    )
    return key, cert


async def test_connect_publish_over_tls(tmp_path: Path) -> None:
    key, cert = _make_certs(tmp_path)
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.load_cert_chain(cert, key)
    published = asyncio.Event()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        decoder = IncrementalDecoder()
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    return
                decoder.feed(data)
                for raw in decoder.drain_packets():
                    if raw.packet_type is PacketType.CONNECT:
                        writer.write(encode_frame(PacketType.CONNACK, 0, b"\x00\x00"))
                    elif raw.packet_type is PacketType.PUBLISH:
                        published.set()
                    elif raw.packet_type is PacketType.DISCONNECT:
                        return
                    await writer.drain()
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0, ssl=server_ctx)
    assert server.sockets, "TLS server has no socket"
    port = server.sockets[0].getsockname()[1]
    async with server:
        client_ctx = ssl.create_default_context()
        client_ctx.load_verify_locations(cert)
        client = AsyncClient(client_id="tls-test")
        connack = await client.connect("127.0.0.1", port, ssl=client_ctx, timeout=5.0)
        assert connack.reason_code == 0
        assert client.is_connected
        await client.publish("tls/t", b"secure", qos=0)
        await asyncio.wait_for(published.wait(), timeout=2.0)
        await client.disconnect()
        assert not client.is_connected


async def test_tls_rejects_untrusted_server(tmp_path: Path) -> None:
    key, cert = _make_certs(tmp_path)
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.load_cert_chain(cert, key)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0, ssl=server_ctx)
    assert server.sockets, "TLS server has no socket"
    port = server.sockets[0].getsockname()[1]
    async with server:
        # Default context trusts public CAs only: the self-signed cert fails.
        client = AsyncClient(client_id="tls-reject")
        with pytest.raises(ssl.SSLError):
            await client.connect("127.0.0.1", port, ssl=ssl.create_default_context(), timeout=5.0)
        assert not client.is_connected


async def test_tls_rejects_hostname_mismatch(tmp_path: Path) -> None:
    if shutil.which("openssl") is None:
        pytest.skip("openssl not available")
    key = tmp_path / "wrong-key.pem"
    cert = tmp_path / "wrong-cert.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-subj",
            "/CN=wrong.example",
            "-addext",
            "subjectAltName=DNS:wrong.example",
        ],
        check=True,
        capture_output=True,
    )
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.load_cert_chain(cert, key)

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0, ssl=server_ctx)
    assert server.sockets
    port = server.sockets[0].getsockname()[1]
    async with server:
        client_ctx = ssl.create_default_context()
        client_ctx.load_verify_locations(cert)
        client = AsyncClient(client_id="tls-hostname")
        with pytest.raises(ssl.SSLCertVerificationError):
            await client.connect(
                "127.0.0.1",
                port,
                ssl=client_ctx,
                timeout=5.0,
            )
        assert not client.is_connected
