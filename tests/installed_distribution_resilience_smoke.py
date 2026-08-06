"""Exercise TLS and SQLite restart paths from an installed beta artifact."""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import ssl
from pathlib import Path

from mqttium import MQTTProtocolVersion, QoS, __version__
from mqttium.api import AsyncClient, Message, PublishReceipt
from mqttium.enums import OutboundQoSState
from mqttium.persistence import SqliteInflightStore
from mqttium.types import OutboundMessage, Properties

_RECORD = OutboundMessage(
    mid=17,
    topic="mqttium/beta-persistence",
    payload=b"persisted-beta-payload",
    qos=QoS.AT_LEAST_ONCE,
    retain=False,
    state=OutboundQoSState.WAIT_PUBACK,
    dup=True,
    properties=Properties({"user_property": [("campaign", "beta")]}),
    logical_size=4096,
)


def _verify_version(expected_version: str) -> None:
    assert importlib.metadata.version("mqttium") == expected_version
    assert __version__ == expected_version


async def _tls_protocol_roundtrip(
    host: str,
    port: int,
    context: ssl.SSLContext,
    protocol: MQTTProtocolVersion,
) -> None:
    topic = f"mqttium/beta-tls/{int(protocol)}"
    payload = f"tls-{int(protocol)}".encode()
    received: asyncio.Future[bytes] = asyncio.get_running_loop().create_future()
    subscriber = AsyncClient(f"beta-tls-sub-{int(protocol)}", protocol=protocol)
    publisher = AsyncClient(f"beta-tls-pub-{int(protocol)}", protocol=protocol)

    def on_message(message: Message) -> None:
        if message.topic == topic and not received.done():
            received.set_result(message.payload)

    subscriber.on_message = on_message
    try:
        await subscriber.connect(host, port, ssl=context, timeout=5)
        await subscriber.subscribe(topic, qos=QoS.AT_LEAST_ONCE)
        await publisher.connect(host, port, ssl=context, timeout=5)
        receipt = await publisher.publish(topic, payload, qos=QoS.AT_LEAST_ONCE)
        assert isinstance(receipt, PublishReceipt)
        await receipt.wait()
        assert await asyncio.wait_for(received, timeout=5) == payload
    finally:
        if publisher.is_connected:
            await publisher.disconnect()
        if subscriber.is_connected:
            await subscriber.disconnect()


async def _tls_roundtrip(host: str, port: int, ca_file: Path) -> None:
    context = ssl.create_default_context(cafile=str(ca_file))
    for protocol in (MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5):
        await _tls_protocol_roundtrip(host, port, context, protocol)


def _write_sqlite(path: Path) -> None:
    with SqliteInflightStore(path) as store:
        store.put_out(_RECORD)
        assert store.get_out(_RECORD.mid) == _RECORD
    assert path.is_file()


def _read_sqlite(path: Path) -> None:
    with SqliteInflightStore(path) as store:
        assert store.get_out(_RECORD.mid) == _RECORD
        assert tuple(store.out_items()) == (_RECORD,)
        assert store.delete_out(_RECORD.mid)

    with SqliteInflightStore(path) as store:
        assert store.get_out(_RECORD.mid) is None
        assert tuple(store.out_items()) == ()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("tls", "sqlite-write", "sqlite-read"))
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=18884)
    parser.add_argument("--ca-file", type=Path)
    parser.add_argument("--database", type=Path)
    args = parser.parse_args()

    _verify_version(args.expected_version)
    if args.command == "tls":
        if args.ca_file is None:
            parser.error("--ca-file is required for tls")
        asyncio.run(_tls_roundtrip(args.host, args.port, args.ca_file))
    elif args.command == "sqlite-write":
        if args.database is None:
            parser.error("--database is required for sqlite-write")
        _write_sqlite(args.database)
    else:
        if args.database is None:
            parser.error("--database is required for sqlite-read")
        _read_sqlite(args.database)


if __name__ == "__main__":
    main()
