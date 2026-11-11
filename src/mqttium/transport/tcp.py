"""TCP transport for MQTT."""

from __future__ import annotations

import asyncio
import socket
import sys
from contextlib import suppress
from typing import Any

from mqttium.transport._push import DecoderPushProtocol, PushStreamTransport
from mqttium.transport._stream import StreamTransport, StreamTransportBase


# RFC 8305 recommends 250 ms before trying the next address family, so a host
# whose IPv6 address is unreachable still connects promptly over IPv4.
HAPPY_EYEBALLS_DELAY = 0.25

# Transport-level dead-peer detection, independent of MQTT keepalive. An idle
# connection is probed after TCP_KEEPALIVE_IDLE seconds, then every
# TCP_KEEPALIVE_INTERVAL seconds, and dropped after TCP_KEEPALIVE_PROBES
# unanswered probes: about 90 seconds for a vanished host or a cut path. Where
# the platform has it, the same bound applies to written data that is never
# acknowledged. It cannot see a peer whose host still answers TCP while its
# application has stopped: only MQTT keepalive detects that.
TCP_KEEPALIVE_IDLE = 60
TCP_KEEPALIVE_INTERVAL = 10
TCP_KEEPALIVE_PROBES = 3
_TCP_DEAD_PEER_SECONDS = TCP_KEEPALIVE_IDLE + TCP_KEEPALIVE_INTERVAL * TCP_KEEPALIVE_PROBES


class TcpTransport(StreamTransport):
    """asyncio stream transport with TCP_NODELAY and TCP keepalive when available."""

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        *,
        ssl: Any = None,
    ) -> StreamTransportBase:
        loop = asyncio.get_running_loop()
        if ssl is None and _is_stdlib_selector_loop(loop):
            transport: StreamTransportBase = await _connect_push(loop, host, port)
        else:
            # TLS receives through SSLProtocol, which does not expose the raw
            # socket to a BufferedProtocol; Proactor and third-party loops keep
            # the mature stdlib path. Both still use the decoder's feed().
            reader, writer = await asyncio.open_connection(
                host, port, ssl=ssl, happy_eyeballs_delay=HAPPY_EYEBALLS_DELAY
            )
            transport = cls(reader, writer)
        _configure_socket(transport)
        return transport


def _is_stdlib_selector_loop(loop: asyncio.AbstractEventLoop) -> bool:
    """Whether this loop is a stdlib selector loop with the measured recv_into path.

    Deliberately narrower than ``isinstance(loop, SelectorEventLoop)``. A
    third-party subclass may install its own transport, and the allocator
    evidence behind this path is CPython-only, so neither is generalised to.
    """
    return (
        sys.implementation.name == "cpython"
        and isinstance(loop, asyncio.SelectorEventLoop)
        and type(loop).__module__.startswith("asyncio.")
    )


async def _connect_push(
    loop: asyncio.AbstractEventLoop, host: str, port: int
) -> PushStreamTransport:
    reader = asyncio.StreamReader(loop=loop)
    protocol = DecoderPushProtocol(reader, loop=loop)
    transport, _ = await loop.create_connection(
        lambda: protocol, host, port, happy_eyeballs_delay=HAPPY_EYEBALLS_DELAY
    )
    writer = asyncio.StreamWriter(transport, protocol, reader, loop)
    return PushStreamTransport(reader, writer, protocol)


def _configure_socket(transport: StreamTransportBase) -> None:
    sock = transport._writer.get_extra_info("socket")
    if sock is None:
        return
    options = [
        (socket.IPPROTO_TCP, "TCP_NODELAY", 1),
        (socket.SOL_SOCKET, "SO_KEEPALIVE", 1),
        # macOS names the idle time TCP_KEEPALIVE; elsewhere it is TCP_KEEPIDLE.
        (socket.IPPROTO_TCP, "TCP_KEEPIDLE", TCP_KEEPALIVE_IDLE),
        (socket.IPPROTO_TCP, "TCP_KEEPALIVE", TCP_KEEPALIVE_IDLE),
        (socket.IPPROTO_TCP, "TCP_KEEPINTVL", TCP_KEEPALIVE_INTERVAL),
        (socket.IPPROTO_TCP, "TCP_KEEPCNT", TCP_KEEPALIVE_PROBES),
        (socket.IPPROTO_TCP, "TCP_USER_TIMEOUT", _TCP_DEAD_PEER_SECONDS * 1000),
    ]
    for level, name, value in options:
        option = getattr(socket, name, None)
        if option is None:
            continue
        # Each option is best effort: a platform or socket type without it
        # keeps the others.
        with suppress(OSError):
            sock.setsockopt(level, option, value)
