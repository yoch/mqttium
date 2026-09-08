"""Request/response RTT across the four candidate receive architectures.

Throughput probes saturate the socket and hide scheduling latency. This one
measures the opposite regime: one small MQTT PUBLISH out, one echoed back, and
the wall time until the client's receive path has produced an owned payload.

The server is a plain blocking echo thread, so every difference between arms
belongs to the client receive path under test.

    python tools/recv_arch_rtt_probe.py --payload-size 256 --count 20000
"""

from __future__ import annotations

import argparse
import asyncio
import json
import socket
import statistics
import threading
import time
from collections import deque
from pathlib import Path

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.codec.vbi import decode_vbi
from mqttium.errors import MalformedPacketError

_RECV = 128 * 1024


def build_publish(payload_size: int) -> bytes:
    body = b"\x00\x01t" + b"p" * payload_size
    remaining = len(body)
    out = bytearray([0x30])
    while True:
        digit = remaining % 128
        remaining //= 128
        out.append(digit | 0x80 if remaining else digit)
        if not remaining:
            break
    return bytes(out) + body


class Ring:
    """Decoder-owned storage that `recv_into()` writes into directly."""

    __slots__ = ("_buf", "_mv", "_start", "_end", "_cap")

    def __init__(self, capacity: int) -> None:
        self._cap = capacity
        self._buf = bytearray(capacity)
        self._mv = memoryview(self._buf)
        self._start = 0
        self._end = 0

    def writable(self, need: int = 16384) -> memoryview:
        if self._cap - self._end < need:
            live = self._end - self._start
            if self._start:
                self._buf[0:live] = self._mv[self._start : self._end]
                self._start, self._end = 0, live
            if self._cap - self._end < need:
                self._mv.release()
                new_cap = max(self._cap * 2, live + need)
                grown = bytearray(new_cap)
                grown[0:live] = self._buf[0:live]
                self._buf, self._mv, self._cap = grown, memoryview(grown), new_cap
                self._start, self._end = 0, live
        return self._mv[self._end :]

    def commit(self, nbytes: int) -> None:
        self._end += nbytes

    def next_payload(self) -> bytes | None:
        buf, start = self._buf, self._start
        available = self._end - start
        if available < 2:
            return None
        try:
            remaining_length, rl_end = decode_vbi(buf, start + 1)
        except MalformedPacketError:
            return None
        total = (rl_end - start) + remaining_length
        if available < total:
            return None
        body = bytes(self._mv[rl_end : start + total])
        self._start = start + total
        if self._start == self._end:
            self._start = self._end = 0
        return body


def echo_server(sock: socket.socket, stop: threading.Event) -> None:
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    try:
        while not stop.is_set():
            chunk = sock.recv(65536)
            if not chunk:
                break
            sock.sendall(chunk)
    except OSError:
        pass
    finally:
        sock.close()


# --- arms: each returns (writer, awaitable that yields one owned payload) -----


async def arm_rc13(sock: socket.socket):
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(loop=loop)
    transport, _ = await loop.create_connection(
        lambda: asyncio.StreamReaderProtocol(reader, loop=loop), sock=sock
    )
    decoder = IncrementalDecoder()

    async def recv_one() -> bytes:
        while True:
            packet = decoder.next_packet()
            if packet is not None:
                return packet.remaining
            decoder.feed(await reader.read(256 * 1024))

    return transport, recv_one


async def arm_446(sock: socket.socket):
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(loop=loop)

    class Protocol(asyncio.StreamReaderProtocol, asyncio.BufferedProtocol):
        def __init__(self) -> None:
            super().__init__(reader, loop=loop)
            self.receive_buffer = memoryview(bytearray(_RECV))

        def get_buffer(self, sizehint: int) -> memoryview:
            return self.receive_buffer

        def buffer_updated(self, nbytes: int) -> None:
            if nbytes > 0:
                reader.feed_data(self.receive_buffer[:nbytes])

    transport, _ = await loop.create_connection(Protocol, sock=sock)
    decoder = IncrementalDecoder()

    async def recv_one() -> bytes:
        while True:
            packet = decoder.next_packet()
            if packet is not None:
                return packet.remaining
            decoder.feed(await reader.read(256 * 1024))

    return transport, recv_one


async def arm_445(sock: socket.socket):
    loop = asyncio.get_running_loop()

    class Protocol(asyncio.BufferedProtocol):
        def __init__(self) -> None:
            self.buf = bytearray(_RECV)
            self.mv = memoryview(self.buf)
            self.chunks: deque[bytes] = deque()
            self.waiter: asyncio.Future[None] | None = None

        def get_buffer(self, sizehint: int) -> bytearray:
            return self.buf

        def buffer_updated(self, nbytes: int) -> None:
            if nbytes <= 0:
                return
            self.chunks.append(bytes(self.mv[:nbytes]))
            if self.waiter is not None and not self.waiter.done():
                self.waiter.set_result(None)

        async def read(self) -> bytes:
            while not self.chunks:
                self.waiter = loop.create_future()
                await self.waiter
                self.waiter = None
            return self.chunks.popleft()

    transport, protocol = await loop.create_connection(Protocol, sock=sock)
    decoder = IncrementalDecoder()

    async def recv_one() -> bytes:
        while True:
            packet = decoder.next_packet()
            if packet is not None:
                return packet.remaining
            decoder.feed(await protocol.read())

    return transport, recv_one


async def arm_447(sock: socket.socket):
    loop = asyncio.get_running_loop()
    ring = Ring(_RECV)

    class Protocol(asyncio.BufferedProtocol):
        def __init__(self) -> None:
            self.waiter: asyncio.Future[None] | None = None
            self.fed = 0

        def get_buffer(self, sizehint: int) -> memoryview:
            return ring.writable()

        def buffer_updated(self, nbytes: int) -> None:
            if nbytes <= 0:
                return
            ring.commit(nbytes)
            self.fed += 1
            if self.waiter is not None and not self.waiter.done():
                self.waiter.set_result(None)

    transport, protocol = await loop.create_connection(Protocol, sock=sock)

    async def recv_one() -> bytes:
        # Edge-triggered: "bytes arrived" is not "a frame is complete", so
        # waiting on a level condition would spin without ever yielding.
        while True:
            payload = ring.next_payload()
            if payload is not None:
                return payload
            seen = protocol.fed
            while protocol.fed == seen:
                protocol.waiter = loop.create_future()
                await protocol.waiter
                protocol.waiter = None

    return transport, recv_one


ARMS = {
    "rc13": arm_rc13,
    "445": arm_445,
    "446": arm_446,
    "447": arm_447,
}


async def measure(arm: str, packet: bytes, count: int, warmup: int) -> list[float]:
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    stop = threading.Event()
    accepted: list[socket.socket] = []

    def accept() -> None:
        conn, _ = listener.accept()
        accepted.append(conn)
        echo_server(conn, stop)

    thread = threading.Thread(target=accept, daemon=True)
    thread.start()

    client = socket.create_connection(listener.getsockname())
    client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    transport, recv_one = await ARMS[arm](client)

    samples: list[float] = []
    try:
        for index in range(warmup + count):
            start = time.perf_counter()
            transport.write(packet)
            await recv_one()
            elapsed = time.perf_counter() - start
            if index >= warmup:
                samples.append(elapsed * 1e6)
    finally:
        stop.set()
        transport.close()
        listener.close()
        for conn in accepted:
            conn.close()
    return samples


async def main_async(args: argparse.Namespace) -> None:
    packet = build_publish(args.payload_size)
    results: dict[str, dict[str, float]] = {}
    order = list(ARMS)
    per_arm: dict[str, list[float]] = {name: [] for name in order}

    for _ in range(args.repeat):
        for name in order:  # interleave so drift hits every arm equally
            per_arm[name].extend(await measure(name, packet, args.count, args.warmup))

    print(
        f"payload {args.payload_size} B, {args.count} round trips x {args.repeat} "
        f"interleaved reps, TCP loopback, echo server thread\n"
    )
    header = f"{'arm':8}{'p50 us':>9}{'p90 us':>9}{'p99 us':>9}{'mean':>9}{'vs rc13':>9}"
    print(header)
    base = statistics.median(per_arm["rc13"])
    for name in order:
        values = sorted(per_arm[name])
        stats = {
            "p50": statistics.median(values),
            "p90": values[int(len(values) * 0.90)],
            "p99": values[int(len(values) * 0.99)],
            "mean": statistics.fmean(values),
            "samples": len(values),
        }
        results[name] = stats
        print(
            f"{name:8}{stats['p50']:9.2f}{stats['p90']:9.2f}{stats['p99']:9.2f}"
            f"{stats['mean']:9.2f}{base / stats['p50']:8.3f}x"
        )
    if args.output:
        Path(args.output).write_text(json.dumps(results, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--payload-size", type=int, default=256)
    parser.add_argument("--count", type=int, default=5000)
    parser.add_argument("--warmup", type=int, default=500)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--output")
    asyncio.run(main_async(parser.parse_args()))


if __name__ == "__main__":
    main()
