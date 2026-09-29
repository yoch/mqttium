"""A host whose first address family is unreachable still connects promptly."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from mqttium.transport.tcp import HAPPY_EYEBALLS_DELAY, TcpTransport


class _Stop(Exception):
    pass


@pytest.mark.parametrize("ssl", [None, True])
async def test_tcp_connect_races_address_families(monkeypatch: pytest.MonkeyPatch, ssl) -> None:  # noqa: ANN001
    seen: dict[str, Any] = {}

    async def create_connection(*args: object, **kwargs: Any) -> None:
        seen.update(kwargs)
        raise _Stop

    monkeypatch.setattr(asyncio.get_running_loop(), "create_connection", create_connection)
    with pytest.raises(_Stop):
        await TcpTransport.connect("broker.example", 1883, ssl=ssl)
    assert seen["happy_eyeballs_delay"] == HAPPY_EYEBALLS_DELAY == 0.25
