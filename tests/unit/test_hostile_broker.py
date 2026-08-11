"""Liveness under a hostile or broken peer.

A real TCP server that speaks just enough MQTT to get a client connected, then
misbehaves. No external broker is required, so this runs everywhere the unit
suite does.

The contract asserted here is deliberately weak, because most of these inputs
legitimately end the session: the client may fail, disconnect or raise, but it
must not hang, must not wedge ``disconnect()``, and must not leave one of its
own background tasks running afterwards. Anything a misbehaving broker can put
on the wire has to leave the client in a state the application can recover from.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import pytest

from mqttium.api.async_client import AsyncClient
from mqttium.enums import MQTTProtocolVersion

Script = Callable[[asyncio.StreamReader, asyncio.StreamWriter, "HostileBroker"], Awaitable[None]]

SETTLE_TIMEOUT = 15.0
SHUTDOWN_TIMEOUT = 10.0


class HostileBroker:
    """Accepts one connection, answers CONNECT, then runs ``script``."""

    def __init__(self, script: Script, protocol: MQTTProtocolVersion) -> None:
        self._script = script
        self.protocol = protocol
        self._server: asyncio.AbstractServer | None = None
        self.port = 0

    async def start(self) -> int:
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self.port

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        try:
            await asyncio.wait_for(self._server.wait_closed(), timeout=2)
        except (TimeoutError, asyncio.CancelledError):
            pass

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await asyncio.wait_for(reader.read(4096), timeout=5)  # CONNECT
            await self._script(reader, writer, self)
        except (OSError, asyncio.IncompleteReadError, TimeoutError, asyncio.CancelledError):
            pass
        finally:
            try:
                writer.close()
            except OSError:
                pass

    def connack(self, *, session_present: bool = False, reason_code: int = 0) -> bytes:
        body = bytes((1 if session_present else 0, reason_code))
        if self.protocol is MQTTProtocolVersion.MQTTv5:
            body += b"\x00"  # empty property table
        return bytes((0x20, len(body))) + body


# ------------------------------------------------------------------- scripts


async def _close_before_connack(reader, writer, broker) -> None:  # noqa: ANN001
    writer.close()


async def _connack_then_silence(reader, writer, broker) -> None:  # noqa: ANN001
    writer.write(broker.connack())
    await writer.drain()
    await asyncio.sleep(30)


async def _connack_then_abrupt_close(reader, writer, broker) -> None:  # noqa: ANN001
    writer.write(broker.connack())
    await writer.drain()
    await asyncio.sleep(0.3)
    writer.close()


async def _truncated_publish(reader, writer, broker) -> None:  # noqa: ANN001
    """A PUBLISH whose declared Remaining Length never arrives."""
    writer.write(broker.connack())
    await writer.drain()
    await asyncio.sleep(0.2)
    writer.write(bytes((0x30, 200)) + b"\x00\x05topic")
    await writer.drain()
    await asyncio.sleep(0.25)
    writer.close()


async def _garbage_after_connack(reader, writer, broker) -> None:  # noqa: ANN001
    writer.write(broker.connack())
    await writer.drain()
    await asyncio.sleep(0.2)
    writer.write(bytes(range(256)) * 8)
    await writer.drain()
    await asyncio.sleep(0.25)


async def _oversized_remaining_length(reader, writer, broker) -> None:  # noqa: ANN001
    """A VBI announcing a 256 MiB packet must be refused, not buffered."""
    writer.write(broker.connack())
    await writer.drain()
    await asyncio.sleep(0.2)
    writer.write(b"\x30\xff\xff\xff\x7f")
    await writer.drain()
    await asyncio.sleep(0.25)


async def _ack_storm_for_unknown_mids(reader, writer, broker) -> None:  # noqa: ANN001
    """PUBACK/PUBREC/PUBCOMP for identifiers the client never allocated."""
    writer.write(broker.connack())
    await writer.drain()
    await asyncio.sleep(0.2)
    storm = bytearray()
    for mid in range(1, 400):
        high, low = mid >> 8, mid & 0xFF
        storm += bytes((0x40, 2, high, low))
        storm += bytes((0x50, 2, high, low))
        storm += bytes((0x70, 2, high, low))
    writer.write(bytes(storm))
    await writer.drain()
    await asyncio.sleep(0.35)


async def _publish_storm_past_receive_maximum(reader, writer, broker) -> None:  # noqa: ANN001
    writer.write(broker.connack())
    await writer.drain()
    await asyncio.sleep(0.2)
    storm = bytearray()
    for mid in range(1, 60):
        body = b"\x00\x04evil" + bytes((mid >> 8, mid & 0xFF)) + b"\x00" + b"x" * 20
        storm += bytes((0x34, len(body))) + body
    writer.write(bytes(storm))
    await writer.drain()
    await asyncio.sleep(0.35)


async def _reserved_packet_types(reader, writer, broker) -> None:  # noqa: ANN001
    writer.write(broker.connack())
    await writer.drain()
    await asyncio.sleep(0.2)
    writer.write(b"\x00\x00")  # type 0 is forbidden
    writer.write(b"\xf0\x00")  # type 15 is reserved in MQTT 3.1.1
    await writer.drain()
    await asyncio.sleep(0.25)


async def _second_connack(reader, writer, broker) -> None:  # noqa: ANN001
    writer.write(broker.connack())
    await writer.drain()
    await asyncio.sleep(0.2)
    writer.write(broker.connack(session_present=True))
    await writer.drain()
    await asyncio.sleep(0.25)


async def _dribbled_connack(reader, writer, broker) -> None:  # noqa: ANN001
    """One byte per 50 ms: the incremental decoder must reassemble the frame."""
    for byte in broker.connack():
        writer.write(bytes((byte,)))
        await writer.drain()
        await asyncio.sleep(0.02)
    await asyncio.sleep(0.3)


async def _wrong_ack_family(reader, writer, broker) -> None:  # noqa: ANN001
    """Answer a QoS 1 PUBLISH with the QoS 2 acknowledgements."""
    writer.write(broker.connack())
    await writer.drain()
    while True:
        data = await reader.read(4096)
        if not data:
            return
        if data[0] & 0xF0 == 0x30:
            mid = data[-1] if len(data) > 4 else 1
            writer.write(bytes((0x50, 2, 0, mid)))
            writer.write(bytes((0x70, 2, 0, mid)))
            await writer.drain()
            await asyncio.sleep(0.4)
            return


# ------------------------------------------------------------------- actions


async def _idle(client: AsyncClient) -> None:
    await asyncio.sleep(0.4)


async def _publish_a_few(client: AsyncClient) -> None:
    for index in range(3):
        try:
            receipt = await asyncio.wait_for(
                client.publish("evil/t", f"p{index}".encode(), qos=1), timeout=1
            )
            await asyncio.wait_for(receipt.wait(), timeout=1)
        except Exception:  # noqa: BLE001 - any failure is acceptable, a hang is not
            pass


async def _publish_once(client: AsyncClient) -> None:
    try:
        receipt = await asyncio.wait_for(client.publish("evil/t", b"x", qos=1), timeout=1)
        await asyncio.wait_for(receipt.wait(), timeout=1.5)
    except Exception:  # noqa: BLE001
        pass


SCENARIOS: list[tuple[str, Script, Callable[[AsyncClient], Awaitable[None]], dict]] = [
    ("tcp closed before CONNACK", _close_before_connack, _idle, {}),
    (
        "CONNACK then permanent silence",
        _connack_then_silence,
        _idle,
        {"keepalive": 1, "ping_timeout": 0.5},
    ),
    ("abrupt close mid-session", _connack_then_abrupt_close, _publish_a_few, {}),
    ("truncated PUBLISH then close", _truncated_publish, _idle, {}),
    ("random garbage after CONNACK", _garbage_after_connack, _idle, {}),
    ("VBI announcing a 256 MiB packet", _oversized_remaining_length, _idle, {}),
    ("storm of ACKs for unknown packet ids", _ack_storm_for_unknown_mids, _idle, {}),
    (
        "QoS 2 PUBLISH storm past Receive Maximum",
        _publish_storm_past_receive_maximum,
        _idle,
        {"local_receive_maximum": 5},
    ),
    ("reserved packet types 0 and 15", _reserved_packet_types, _idle, {}),
    ("second CONNACK on an established session", _second_connack, _idle, {}),
    ("CONNACK dribbled one byte at a time", _dribbled_connack, _idle, {}),
    ("QoS 1 answered with PUBREC/PUBCOMP", _wrong_ack_family, _publish_once, {}),
]


@pytest.mark.parametrize("protocol", [MQTTProtocolVersion.MQTTv311, MQTTProtocolVersion.MQTTv5])
@pytest.mark.parametrize(
    ("name", "script", "action", "client_kwargs"),
    SCENARIOS,
    ids=[scenario[0] for scenario in SCENARIOS],
)
async def test_client_survives_a_hostile_broker(
    protocol: MQTTProtocolVersion,
    name: str,
    script: Script,
    action: Callable[[AsyncClient], Awaitable[None]],
    client_kwargs: dict,
) -> None:
    broker = HostileBroker(script, protocol)
    port = await broker.start()
    client = AsyncClient("hostile-probe", protocol=protocol, **client_kwargs)

    async def exercise() -> None:
        try:
            await client.connect("127.0.0.1", port, timeout=1.5)
        except Exception:  # noqa: BLE001 - refusing to connect is a valid outcome
            return
        try:
            await action(client)
        except Exception:  # noqa: BLE001
            pass

    try:
        await asyncio.wait_for(exercise(), timeout=SETTLE_TIMEOUT)
    except TimeoutError:
        pytest.fail(f"{name}: the client hung instead of settling within {SETTLE_TIMEOUT}s")

    try:
        await asyncio.wait_for(_shutdown(client, broker), timeout=SHUTDOWN_TIMEOUT)
    except TimeoutError:
        pytest.fail(f"{name}: shutdown hung; disconnect() never returned")

    await asyncio.sleep(0.1)
    leaked = [
        task
        for task in asyncio.all_tasks()
        if not task.done() and "mqttium" in (task.get_name() or "")
    ]
    assert not leaked, (
        f"{name}: background tasks survived shutdown: {[t.get_name() for t in leaked]}"
    )


async def _shutdown(client: AsyncClient, broker: HostileBroker) -> None:
    try:
        await client.disconnect()
    except Exception:  # noqa: BLE001
        pass
    await broker.stop()
