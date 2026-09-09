"""The architectural probe must terminate on EOF and stalled peers."""

from __future__ import annotations

import asyncio
import socket
import threading
from contextlib import suppress

import pytest

from tools import recv_arch_rtt_probe as probe


@pytest.mark.parametrize("arm", list(probe.ARMS))
@pytest.mark.parametrize("tail", [b"", b"\x30\x80"])
async def test_probe_drains_a_complete_frame_then_reports_eof(arm: str, tail: bytes) -> None:
    finished = asyncio.Event()
    frame = probe.build_publish(4)

    async def peer(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            writer.write(frame + tail)
            await writer.drain()
        finally:
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()
            finished.set()

    server = await asyncio.start_server(peer, "127.0.0.1", 0)
    sock = socket.socket()
    sock.setblocking(False)
    transport = None
    try:
        await asyncio.get_running_loop().sock_connect(sock, server.sockets[0].getsockname())
        transport, receive = await probe.ARMS[arm](sock)
        assert await asyncio.wait_for(receive(), 1) == b"\x00\x01tpppp"
        with pytest.raises(EOFError, match="complete MQTT frame"):
            await asyncio.wait_for(receive(), 1)
    finally:
        if transport is not None:
            transport.close()
        else:
            sock.close()
        server.close()
        await server.wait_closed()
        await asyncio.wait_for(finished.wait(), 1)
        await asyncio.sleep(0)


async def test_probe_bounds_a_stalled_peer_and_reaps_its_thread(monkeypatch) -> None:
    finished = threading.Event()

    def stalled(sock: socket.socket, stop: threading.Event) -> None:
        try:
            stop.wait(2)
        finally:
            sock.close()
            finished.set()

    monkeypatch.setattr(probe, "echo_server", stalled)
    with pytest.raises(TimeoutError):
        await probe.measure("push", probe.build_publish(4), 1, 0, timeout_s=0.03)
    assert finished.is_set()


@pytest.mark.parametrize("stage", ["connect", "accept"])
async def test_probe_setup_failure_starts_no_thread_and_closes_sockets(monkeypatch, stage) -> None:
    sockets = []
    original_init = socket.socket.__init__
    started = []
    original_start = threading.Thread.start

    def track_socket(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        sockets.append(self)

    def track_start(self):
        started.append(self)
        original_start(self)

    def fail(*args, **kwargs):
        raise OSError(f"injected {stage} failure")

    monkeypatch.setattr(socket.socket, "__init__", track_socket)
    monkeypatch.setattr(threading.Thread, "start", track_start)
    if stage == "connect":
        monkeypatch.setattr(socket, "create_connection", fail)
    else:
        monkeypatch.setattr(socket.socket, "accept", fail)
    with pytest.raises(OSError, match=f"injected {stage} failure"):
        await probe.measure("push", probe.build_publish(4), 1, 0)
    assert not started
    assert sockets and all(sock.fileno() == -1 for sock in sockets)


async def test_probe_arm_setup_failure_reaps_an_already_started_echo_thread(monkeypatch) -> None:
    finished = threading.Event()
    original_echo = probe.echo_server

    def echo(sock, stop):
        try:
            original_echo(sock, stop)
        finally:
            finished.set()

    async def fail(sock):
        raise OSError("injected arm setup failure")

    monkeypatch.setattr(probe, "echo_server", echo)
    monkeypatch.setitem(probe.ARMS, "push", fail)
    with pytest.raises(OSError, match="injected arm setup failure"):
        await probe.measure("push", probe.build_publish(4), 1, 0)
    assert finished.is_set()
