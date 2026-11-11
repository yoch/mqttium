"""TCP connections carry transport-level dead-peer detection.

MQTT keepalive is the application-level liveness bound and can be disabled.
TCP keepalive is the net underneath it: a vanished host or a cut path ends the
connection even with ``keepalive=0``.
"""

import asyncio
import socket

import pytest

from mqttium.transport import tcp
from mqttium.transport.tcp import TcpTransport


async def test_tcp_transport_enables_keepalive_and_nodelay():
    async def serve(reader, writer):
        await reader.read()
        writer.close()

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    transport = await TcpTransport.connect("127.0.0.1", port)
    try:
        sock = transport._writer.get_extra_info("socket")
        assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) != 0
        assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY) != 0
        if hasattr(socket, "TCP_KEEPIDLE"):
            assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE) == (
                tcp.TCP_KEEPALIVE_IDLE
            )
        if hasattr(socket, "TCP_KEEPINTVL"):
            assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL) == (
                tcp.TCP_KEEPALIVE_INTERVAL
            )
        if hasattr(socket, "TCP_KEEPCNT"):
            assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT) == (
                tcp.TCP_KEEPALIVE_PROBES
            )
    finally:
        await transport.close()
        server.close()
        await server.wait_closed()


def test_socket_options_are_best_effort():
    class RefusingSocket:
        def __init__(self):
            self.attempts = 0

        def setsockopt(self, *args):
            self.attempts += 1
            raise OSError("unsupported")

    class Writer:
        def __init__(self, sock):
            self.sock = sock

        def get_extra_info(self, name):
            return self.sock

    class Transport:
        def __init__(self, sock):
            self._writer = Writer(sock)

    sock = RefusingSocket()
    tcp._configure_socket(Transport(sock))  # must not raise
    assert sock.attempts >= 2
    tcp._configure_socket(Transport(None))


@pytest.mark.parametrize("name", ["TCP_KEEPALIVE_IDLE", "TCP_KEEPALIVE_INTERVAL"])
def test_keepalive_constants_are_positive(name):
    assert getattr(tcp, name) > 0
