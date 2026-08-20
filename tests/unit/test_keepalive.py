"""Keepalive behaviour via AsyncClient + fake transport."""

from __future__ import annotations

import asyncio

from mqttium.api.async_client import AsyncClient
from tests.support import ScriptedBrokerTransport, transport_factory


async def test_keepalive_sends_pingreq() -> None:
    client = AsyncClient(client_id="ka", keepalive=1, ping_timeout=2.0)
    fake = ScriptedBrokerTransport(auto_pingresp=True)
    client._transport_factory = transport_factory(fake)
    await client.connect("fake", 1883, timeout=2.0)
    # Wait long enough for keepalive to fire (K=1s).
    await asyncio.sleep(1.6)
    assert fake.pingreqs >= 1
    await client.disconnect()


async def test_keepalive_zero_disabled() -> None:
    client = AsyncClient(client_id="ka0", keepalive=0)
    fake = ScriptedBrokerTransport()
    client._transport_factory = transport_factory(fake)
    await client.connect("fake", 1883, timeout=2.0)
    await asyncio.sleep(0.3)
    assert fake.pingreqs == 0
    await client.disconnect()
