"""Provisional concrete TCP transport factory contract."""

from __future__ import annotations

import asyncio
import socket
import sys

from mqttium.transport._push import PushStreamTransport
from mqttium.transport.tcp import TcpTransport, _direct_ingress_supported


class CustomTcpTransport(TcpTransport):
    """Representative external subclass; subclass preservation is not promised."""


async def test_tcp_connect_factory_does_not_promise_subclass_identity() -> None:
    loop = asyncio.get_running_loop()
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.setblocking(False)
    host, port = listener.getsockname()
    accepted: list[socket.socket] = []

    async def serve() -> None:
        conn, _ = await loop.sock_accept(listener)
        accepted.append(conn)

    server = asyncio.create_task(serve())
    transport = await CustomTcpTransport.connect(str(host), int(port))
    try:
        supported = _direct_ingress_supported(None, loop)
        assert supported is (
            sys.implementation.name == "cpython"
            and isinstance(loop, asyncio.SelectorEventLoop)
            and type(loop).__module__.startswith("asyncio.")
        )
        if supported:
            # The Provisional contract is the returned AsyncTransport behavior;
            # an optimized receive implementation may cross the concrete class
            # boundary even when connect() was invoked through a subclass.
            assert isinstance(transport, PushStreamTransport)
            assert not isinstance(transport, CustomTcpTransport)
        else:
            # Fallback implementation details may currently preserve cls, but
            # callers must not depend on this asymmetry as an extension seam.
            assert isinstance(transport, CustomTcpTransport)
    finally:
        await transport.close()
        await server
        for conn in accepted:
            conn.close()
        listener.close()
