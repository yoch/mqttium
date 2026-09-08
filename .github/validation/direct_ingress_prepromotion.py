#!/usr/bin/env python3
"""Production direct-ingress pre-promotion validation.

This script exercises the real product wiring: there is no install hook, client
subclass, sentinel, or alternate decoder.  On CPython's stdlib selector loop,
clear-text TCP must expose DecoderPushTransport; TLS, Unix and WebSocket remain
ordinary pull transports.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
import socket
import ssl
import subprocess
import tempfile
import time
from pathlib import Path

from mqttium.api import AsyncClient
from mqttium.codec.vbi import encode_vbi
from mqttium.enums import MQTTProtocolVersion
from mqttium.protocol.reconnect import ReconnectPolicy
from mqttium.transport._stream import DecoderPushTransport


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_port(port: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.02)
    raise TimeoutError(f"port {port} did not become ready")


def _publish(payload: bytes, topic: bytes = b"t") -> bytes:
    body = len(topic).to_bytes(2, "big") + topic + payload
    return b"\x30" + encode_vbi(len(body)) + body


async def _read_mqtt_packet(reader: asyncio.StreamReader) -> bytes:
    head = await reader.readexactly(1)
    encoded = bytearray()
    remaining = 0
    multiplier = 1
    for _ in range(4):
        byte = (await reader.readexactly(1))[0]
        encoded.append(byte)
        remaining += (byte & 0x7F) * multiplier
        if not byte & 0x80:
            body = await reader.readexactly(remaining)
            return head + bytes(encoded) + body
        multiplier *= 128
    raise RuntimeError("invalid MQTT VBI from client")


async def _mqtt_stream_handler(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    after_connack: bytes | None = None,
) -> None:
    try:
        await _read_mqtt_packet(reader)
        writer.write(b"\x20\x02\x00\x00")
        await writer.drain()
        if after_connack is not None:
            await asyncio.sleep(0.02)
            writer.write(after_connack)
            await writer.drain()
        while True:
            try:
                await _read_mqtt_packet(reader)
            except (asyncio.IncompleteReadError, ConnectionError):
                break
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


def _make_tls_contexts(root: Path) -> tuple[ssl.SSLContext, ssl.SSLContext]:
    key = root / "key.pem"
    cert = root / "cert.pem"
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
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.load_cert_chain(str(cert), str(key))
    client = ssl.create_default_context()
    client.check_hostname = False
    client.verify_mode = ssl.CERT_NONE
    return server, client


async def _websocket_mqtt_server(host: str, port: int):
    import websockets

    async def handler(ws):
        first = await ws.recv()
        assert isinstance(first, bytes)
        await ws.send(b"\x20\x02\x00\x00")
        try:
            async for _ in ws:
                pass
        except Exception:
            pass

    return await websockets.serve(handler, host, port, subprotocols=["mqtt"])


async def lifecycle_matrix() -> dict[str, object]:
    loop = asyncio.get_running_loop()
    if not isinstance(loop, asyncio.SelectorEventLoop):
        raise RuntimeError("production lifecycle validation requires stdlib SelectorEventLoop")

    with tempfile.TemporaryDirectory(prefix="mqttium-ingress-life-") as td:
        root = Path(td)
        tls_server_ctx, tls_client_ctx = _make_tls_contexts(root)
        unix_path = root / "mqtt.sock"
        direct_port = _free_port()
        tls_port = _free_port()
        ws_port = _free_port()
        large_payload = b"L" * 900_000
        large_seen = asyncio.Event()

        direct_server = await asyncio.start_server(
            lambda r, w: _mqtt_stream_handler(r, w, after_connack=_publish(large_payload)),
            "127.0.0.1",
            direct_port,
        )
        tls_server = await asyncio.start_server(
            _mqtt_stream_handler,
            "127.0.0.1",
            tls_port,
            ssl=tls_server_ctx,
        )
        unix_server = await asyncio.start_unix_server(_mqtt_stream_handler, path=str(unix_path))
        ws_server = await _websocket_mqtt_server("127.0.0.1", ws_port)

        client = AsyncClient(
            "production-ingress-lifecycle",
            maximum_packet_size=4 * 1024 * 1024,
            message_delivery="callback",
            max_pending_delivery_bytes=8 * 1024 * 1024,
        )

        def on_message(message) -> None:
            if message.payload == large_payload:
                large_seen.set()

        client.on_message = on_message
        transitions: list[dict[str, object]] = []
        try:
            await client.connect("127.0.0.1", direct_port, timeout=3)
            assert isinstance(client._transport, DecoderPushTransport)
            await asyncio.wait_for(large_seen.wait(), timeout=3)
            decoder = client._decoder
            direct_peak = decoder.capacity_peak
            direct_retained = decoder.capacity
            assert direct_peak >= len(large_payload)
            assert direct_retained >= len(large_payload)
            transitions.append(
                {
                    "transport": "direct-tcp",
                    "push": True,
                    "capacity": direct_retained,
                    "capacity_peak": direct_peak,
                    "max_packet_size": decoder.max_packet_size,
                }
            )
            await client.disconnect()

            await client.connect("127.0.0.1", tls_port, ssl=tls_client_ctx, timeout=3)
            assert not isinstance(client._transport, DecoderPushTransport)
            assert client._decoder is decoder
            assert decoder.capacity <= 64 * 1024
            transitions.append(
                {
                    "transport": "tls",
                    "push": False,
                    "capacity": decoder.capacity,
                    "max_packet_size": decoder.max_packet_size,
                }
            )
            await client.disconnect()

            await client.connect_unix(str(unix_path), timeout=3)
            assert not isinstance(client._transport, DecoderPushTransport)
            assert client._decoder is decoder
            transitions.append(
                {
                    "transport": "unix",
                    "push": False,
                    "capacity": decoder.capacity,
                    "max_packet_size": decoder.max_packet_size,
                }
            )
            await client.disconnect()

            await client.connect_ws(f"ws://127.0.0.1:{ws_port}/mqtt", timeout=3)
            assert not isinstance(client._transport, DecoderPushTransport)
            assert client._decoder is decoder
            transitions.append(
                {
                    "transport": "websocket",
                    "push": False,
                    "capacity": decoder.capacity,
                    "max_packet_size": decoder.max_packet_size,
                }
            )
            await client.disconnect()

            await client.connect("127.0.0.1", direct_port, timeout=3)
            assert isinstance(client._transport, DecoderPushTransport)
            assert client._decoder is decoder
            assert decoder.max_packet_size == 4 * 1024 * 1024
            transitions.append(
                {
                    "transport": "direct-tcp-return",
                    "push": True,
                    "capacity": decoder.capacity,
                    "max_packet_size": decoder.max_packet_size,
                }
            )
            await client.disconnect()
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass
            direct_server.close()
            tls_server.close()
            unix_server.close()
            ws_server.close()
            await direct_server.wait_closed()
            await tls_server.wait_closed()
            await unix_server.wait_closed()
            await ws_server.wait_closed()

        return {
            "transitions": transitions,
            "same_decoder_across_transitions": True,
            "large_direct_peak": direct_peak,
            "capacity_after_tls_reset": transitions[1]["capacity"],
        }


class Broker:
    def __init__(self, root: Path, port: int) -> None:
        self.root = root
        self.port = port
        self.proc: subprocess.Popen[bytes] | None = None
        self.conf = root / "mosquitto.conf"
        self.log = root / "mosquitto.log"
        self.conf.write_text(
            f"listener {port} 127.0.0.1\n"
            "protocol mqtt\n"
            "set_tcp_nodelay true\n"
            "allow_anonymous true\n"
            "persistence false\n"
            "connection_messages false\n"
            "log_type error\n"
            "max_inflight_messages 1000\n"
            "max_queued_messages 10000\n"
            "max_packet_size 16777216\n"
        )

    def start(self) -> None:
        log = self.log.open("ab")
        self.proc = subprocess.Popen(
            ["mosquitto", "-c", str(self.conf)], stdout=log, stderr=subprocess.STDOUT
        )
        _wait_port(self.port)

    def stop(self, *, hard: bool = False) -> None:
        proc = self.proc
        if proc is None:
            return
        if proc.poll() is None:
            proc.kill() if hard else proc.terminate()
            try:
                proc.wait(3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(3)
        self.proc = None


async def _safe_disconnect(client: AsyncClient) -> None:
    try:
        await client.disconnect()
    except (Exception, asyncio.CancelledError):
        pass


async def _wait_until(predicate, timeout: float, label: str) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise TimeoutError(label)


async def qos_matrix(port: int) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for protocol in (MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5):
        received: list[bytes] = []
        ready = asyncio.Event()
        name = "v311" if protocol is MQTTProtocolVersion.MQTTv311 else "v5"
        topic = f"direct-prod/{name}/{os.getpid()}"
        sub = AsyncClient(f"sub-{name}", protocol=protocol, message_delivery="callback")
        pub = AsyncClient(f"pub-{name}", protocol=protocol, max_outbound_bytes=8 * 1024 * 1024)

        def callback(message) -> None:
            assert isinstance(message.payload, bytes)
            received.append(message.payload)
            if len(received) == 3:
                ready.set()

        sub.on_message = callback
        try:
            await sub.connect("127.0.0.1", port, timeout=3)
            assert isinstance(sub._transport, DecoderPushTransport)
            await sub.subscribe(topic, qos=2, timeout=3)
            await pub.connect("127.0.0.1", port, timeout=3)
            expected: list[bytes] = []
            for qos in (0, 1, 2):
                payload = f"{name}-q{qos}".encode()
                expected.append(payload)
                receipt = await pub.publish(topic, payload, qos=qos)
                await asyncio.wait_for(receipt.wait(), timeout=3)
            await asyncio.wait_for(ready.wait(), timeout=3)
            assert set(received) == set(expected)
            stats = sub._transport.receive_stats()  # type: ignore[union-attr]
            rows.append({"protocol": name, "received": len(received), "stats": stats})
        finally:
            await _safe_disconnect(pub)
            await _safe_disconnect(sub)
    return rows


async def reconnect_case(broker: Broker) -> dict[str, object]:
    topic = f"direct-prod/reconnect/{os.getpid()}"
    first = asyncio.Event()
    second = asyncio.Event()
    received: list[bytes] = []
    policy = ReconnectPolicy(
        enabled=True,
        initial_delay=0.05,
        multiplier=1,
        max_delay=0.05,
        max_retries=60,
        stable_after=0.05,
        connect_timeout=1,
    )
    sub = AsyncClient(
        "direct-prod-reconnect",
        reconnect=policy,
        message_delivery="callback",
        maximum_packet_size=4 * 1024 * 1024,
        max_pending_delivery_bytes=16 * 1024 * 1024,
    )

    def callback(message) -> None:
        received.append(message.payload)
        (first if len(received) == 1 else second).set()

    sub.on_message = callback
    pub = AsyncClient("direct-prod-reconnect-pub", max_outbound_bytes=8 * 1024 * 1024)
    try:
        await sub.connect("127.0.0.1", broker.port, timeout=3)
        assert isinstance(sub._transport, DecoderPushTransport)
        decoder = sub._decoder
        await sub.subscribe(topic, qos=1, timeout=3)
        await pub.connect("127.0.0.1", broker.port, timeout=3)
        receipt = await pub.publish(topic, b"R" * 900_000, qos=1)
        await asyncio.wait_for(receipt.wait(), timeout=3)
        await asyncio.wait_for(first.wait(), timeout=3)
        retained_before_loss = decoder.capacity
        assert retained_before_loss >= 900_000

        broker.stop(hard=True)
        await _wait_until(lambda: not sub.is_connected, 3, "connection loss not observed")
        await _safe_disconnect(pub)
        await asyncio.sleep(0.12)
        broker.start()
        await _wait_until(lambda: sub.is_connected, 6, "automatic reconnect failed")
        assert sub._decoder is decoder
        assert isinstance(sub._transport, DecoderPushTransport)
        capacity_after_reconnect = decoder.capacity
        assert capacity_after_reconnect <= 64 * 1024

        await sub.subscribe(topic, qos=1, timeout=3)
        pub = AsyncClient("direct-prod-reconnect-pub-2")
        await pub.connect("127.0.0.1", broker.port, timeout=3)
        receipt = await pub.publish(topic, b"after", qos=1)
        await asyncio.wait_for(receipt.wait(), timeout=3)
        await asyncio.wait_for(second.wait(), timeout=3)
        return {
            "same_decoder": True,
            "retained_before_loss": retained_before_loss,
            "capacity_after_reconnect": capacity_after_reconnect,
            "received_sizes": [len(item) for item in received],
        }
    finally:
        await _safe_disconnect(pub)
        await _safe_disconnect(sub)


async def run_e2e() -> dict[str, object]:
    lifecycle = await lifecycle_matrix()
    with tempfile.TemporaryDirectory(prefix="mqttium-ingress-broker-") as td:
        broker = Broker(Path(td), _free_port())
        broker.start()
        try:
            matrix = await qos_matrix(broker.port)
            reconnect = await reconnect_case(broker)
        finally:
            broker.stop()
    return {"lifecycle": lifecycle, "qos_matrix": matrix, "reconnect": reconnect}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("e2e",))
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    result = asyncio.run(run_e2e())
    Path(args.output).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
