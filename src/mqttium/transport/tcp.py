"""TCP transport for MQTT."""

from __future__ import annotations

import asyncio
import socket
import sys
from contextlib import suppress
from typing import Any

from mqttium.transport._stream import StreamTransport

# CPython's selector transport reads ordinary Protocol sockets with
# ``socket.recv(max_size)``. ``socket.recv`` allocates a bytes object of the
# requested size before the syscall and shrinks it afterwards.  At the stdlib
# default (256 KiB), glibc can service that transient allocation with mmap;
# on layout-sensitive processes this turns every small MQTT read into
# mmap/mremap/munmap plus page faults. 64 KiB remains large enough for the
# stream reader's normal chunk while staying below glibc's initial mmap
# threshold. Keep this workaround narrowly bound to the exact CPython selector
# default so alternate loops and future stdlib implementations fail open.
_CPYTHON_SELECTOR_DEFAULT_READ_SIZE = 256 * 1024
_CPYTHON_SELECTOR_READ_SIZE = 64 * 1024


def _cap_cpython_selector_read_size(transport: object) -> None:
    """Avoid CPython selector's transient 256 KiB allocation on plain TCP."""
    if (
        sys.implementation.name == "cpython"
        and type(transport).__module__ == "asyncio.selector_events"
        and getattr(transport, "max_size", None) == _CPYTHON_SELECTOR_DEFAULT_READ_SIZE
    ):
        setattr(transport, "max_size", _CPYTHON_SELECTOR_READ_SIZE)


class TcpTransport(StreamTransport):
    """asyncio stream transport with TCP_NODELAY enabled when available."""

    @classmethod
    async def connect(
        cls,
        host: str,
        port: int,
        *,
        ssl: Any = None,
    ) -> TcpTransport:
        reader, writer = await asyncio.open_connection(host, port, ssl=ssl)
        # SSL wraps the selector transport and has different buffering rules;
        # the measured allocation pathology is the plaintext selector path.
        if ssl is None:
            _cap_cpython_selector_read_size(writer.transport)
        sock = writer.get_extra_info("socket")
        if sock is not None:
            with suppress(OSError):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return cls(reader, writer)
