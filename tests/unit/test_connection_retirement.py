"""Connection retirement is one ownership transition with one terminal cause.

Reader teardown publishes the new connection epoch and retires the protocol
engine in one synchronous step, so no producer can observe the new epoch while
the dead connection still looks CONNECTED (#544). The fact that ended a
connection is latched when observed; failures caused by retiring it afterwards
cannot replace it (#543).
"""

from __future__ import annotations

import asyncio

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import BrokerDisconnectError, MQTTError
from mqttium.packets import encode_frame
from tests.support import ScriptedBrokerTransport, transport_factory, wait_until


async def test_publish_is_refused_while_reader_teardown_is_suspended() -> None:
    transport = ScriptedBrokerTransport()
    client = AsyncClient("retire-atomic", keepalive=0)
    client._transport_factory = transport_factory(transport)
    await client.connect("fake")
    old_epoch = client._connection_epoch
    space = client._write_pump.space
    try:
        # Hold the writer's condition so teardown suspends at its first await.
        await space.acquire()
        transport.push_rx(b"")
        await wait_until(lambda: client._connection_epoch == old_epoch + 1)
        assert client._reader_task is not None and not client._reader_task.done()

        # The epoch is already new: the engine must already be retired.
        assert client._engine.state is not ConnectionState.CONNECTED
        writes = len(transport.written)
        with pytest.raises(MQTTError):
            client.publish_nowait("after/eof", b"ghost", qos=QoS.AT_MOST_ONCE)
        assert len(transport.written) == writes
    finally:
        if space.locked():
            space.release()
        await client.disconnect()


class _BlockedWriteBroker(ScriptedBrokerTransport):
    """A PUBLISH write blocks until the transport closes, then fails."""

    def __init__(self) -> None:
        super().__init__(protocol=MQTTProtocolVersion.MQTTv5)
        self.closed = asyncio.Event()
        self.publish_started = asyncio.Event()

    async def write(self, data: bytes) -> None:
        if data and data[0] & 0xF0 == int(PacketType.PUBLISH):
            self.publish_started.set()
            await self.closed.wait()
            raise OSError("writer failed because peer closed")
        await super().write(data)

    async def close(self) -> None:
        self.closed.set()
        await super().close()


@pytest.mark.parametrize("broker_disconnect", [True, False])
async def test_writer_failure_while_closing_keeps_the_broker_reason(
    broker_disconnect: bool,
) -> None:
    transport = _BlockedWriteBroker()
    disconnects: list[BaseException | None] = []
    client = AsyncClient("broker-cause", protocol=MQTTProtocolVersion.MQTTv5, keepalive=0)
    client.on_disconnect = disconnects.append
    client._transport_factory = transport_factory(transport)
    await client.connect("fake")
    try:
        await client.publish("blocked/write", b"x", qos=QoS.AT_MOST_ONCE)
        await asyncio.wait_for(transport.publish_started.wait(), 1)
        if broker_disconnect:
            transport.push_rx(encode_frame(PacketType.DISCONNECT, 0, b"\x8b\x00"))
        else:
            # Negative control: with no broker verdict the writer failure is
            # the connection's cause.
            await transport.close()
        await wait_until(lambda: disconnects != [])
        if broker_disconnect:
            assert isinstance(disconnects[0], BrokerDisconnectError)
            assert disconnects[0].reason_code == 0x8B
        else:
            assert isinstance(disconnects[0], OSError)
    finally:
        await client.disconnect()


def test_cause_precedence_is_first_wins_with_protocol_and_local_above() -> None:
    client = AsyncClient("precedence")
    from mqttium.api import async_client as module

    first = OSError("first")
    assert client._propose_disconnect_cause(first, module._CAUSE_TRANSPORT) is first
    assert client._propose_disconnect_cause(OSError("later"), module._CAUSE_BROKER) is first
    violation = MQTTError("violation")
    assert client._propose_disconnect_cause(violation, module._CAUSE_PROTOCOL) is violation
    local = MQTTError("local")
    assert client._propose_disconnect_cause(local, module._CAUSE_LOCAL) is local
    assert client._propose_disconnect_cause(MQTTError("x"), module._CAUSE_PROTOCOL) is local
