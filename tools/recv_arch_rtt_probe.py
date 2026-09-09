"""Request/response RTT across receive *mechanisms*.

Throughput probes saturate the socket and hide scheduling latency. This one
measures the opposite regime: one small MQTT PUBLISH out, one echoed back, and
the wall time until the receive path has produced an owned payload. The server
is a plain blocking echo thread, so every difference belongs to the client
receive path.

WHAT THIS IS. A controlled ablation of four receive *mechanisms* against one
shared decoder -- this checkout's ``IncrementalDecoder`` -- so the only variable
is how bytes get from the socket into it. The ``push`` arm is the real
production code (``DecoderPushProtocol`` + ``PushStreamTransport``); the other
three are minimal reimplementations of the mechanism each PR uses.

WHAT THIS IS NOT. It is not RC13 vs #445 vs #446 vs #448 at their respective
commits. The three baseline arms do not run those branches' code, and they
share this branch's decoder rather than each PR's own. Numbers from it support
"this mechanism costs less per round trip", not "PR X is faster than PR Y", and
not "the merged client will show this in application RTT" -- see the end-to-end
figures in docs/reports/, which are far smaller.

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
from contextlib import suppress
from pathlib import Path

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.transport._push import DecoderPushProtocol, PushStreamTransport

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


def echo_server(sock: socket.socket, stop: threading.Event) -> None:
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
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
            data = await reader.read(256 * 1024)
            if not data:
                raise EOFError("peer closed before a complete MQTT frame")
            decoder.feed(data)

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
            data = await reader.read(256 * 1024)
            if not data:
                raise EOFError("peer closed before a complete MQTT frame")
            decoder.feed(data)

    return transport, recv_one


class _ChunkQueueProtocol(asyncio.BufferedProtocol):
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self.buf = bytearray(_RECV)
        self.mv = memoryview(self.buf)
        self.chunks: deque[bytes] = deque()
        self.waiter: asyncio.Future[None] | None = None
        self.eof = False
        self.error: Exception | None = None

    def get_buffer(self, sizehint: int) -> bytearray:
        return self.buf

    def buffer_updated(self, nbytes: int) -> None:
        if nbytes <= 0:
            return
        self.chunks.append(bytes(self.mv[:nbytes]))
        if self.waiter is not None and not self.waiter.done():
            self.waiter.set_result(None)

    def eof_received(self) -> None:
        self.eof = True
        if self.waiter is not None and not self.waiter.done():
            self.waiter.set_result(None)

    def connection_lost(self, exc: Exception | None) -> None:
        self.error = exc
        self.eof_received()

    async def read(self) -> bytes:
        while not self.chunks:
            if self.error is not None:
                raise self.error
            if self.eof:
                return b""
            self.waiter = self._loop.create_future()
            try:
                await self.waiter
            finally:
                self.waiter = None
        return self.chunks.popleft()


async def arm_445(sock: socket.socket):
    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_connection(lambda: _ChunkQueueProtocol(loop), sock=sock)
    decoder = IncrementalDecoder()

    async def recv_one() -> bytes:
        while True:
            packet = decoder.next_packet()
            if packet is not None:
                return packet.remaining
            data = await protocol.read()
            if not data:
                raise EOFError("peer closed before a complete MQTT frame")
            decoder.feed(data)

    return transport, recv_one


async def arm_push(sock: socket.socket):
    """The shipped implementation: production protocol, transport and decoder."""
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader(loop=loop)
    protocol = DecoderPushProtocol(reader, loop=loop)
    raw_transport, _ = await loop.create_connection(lambda: protocol, sock=sock)
    writer = asyncio.StreamWriter(raw_transport, protocol, reader, loop)
    transport = PushStreamTransport(reader, writer, protocol)
    decoder = IncrementalDecoder()
    transport.attach_decoder(decoder)

    async def recv_one() -> bytes:
        while True:
            packet = decoder.next_packet()
            if packet is not None:
                return packet.remaining
            if not await transport.receive():
                raise EOFError("peer closed before a complete MQTT frame")

    return raw_transport, recv_one


# Mechanism names, not PR names: these are ablations, not those branches.
ARMS = {
    "streamreader": arm_rc13,
    "chunk-queue": arm_445,
    "buffered-sr": arm_446,
    "push": arm_push,
}


async def measure(
    arm: str, packet: bytes, count: int, warmup: int, timeout_s: float = 30.0
) -> list[float]:
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(timeout_s)
    stop = threading.Event()
    accepted = None
    thread = None
    client = None
    transport = None
    samples: list[float] = []
    try:
        # The listening backlog completes the local connection before accept.
        # Keep setup in the owning thread: a failed connect must not leave a
        # background thread blocked in accept until its socket timeout expires.
        client = socket.create_connection(listener.getsockname(), timeout=timeout_s)
        accepted, _ = listener.accept()
        thread = threading.Thread(target=echo_server, args=(accepted, stop), daemon=True)
        thread.start()
        client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # One timeout per arm, outside individual RTT samples. A stalled peer
        # must terminate the probe without adding per-message timer overhead.
        async with asyncio.timeout(timeout_s):
            transport, recv_one = await ARMS[arm](client)
            for index in range(warmup + count):
                start = time.perf_counter()
                transport.write(packet)
                await recv_one()
                elapsed = time.perf_counter() - start
                if index >= warmup:
                    samples.append(elapsed * 1e6)
    finally:
        stop.set()
        if transport is not None:
            transport.close()
        elif client is not None:
            client.close()
        listener.close()
        if accepted is not None:
            with suppress(OSError):
                accepted.shutdown(socket.SHUT_RDWR)
            accepted.close()
        if thread is not None and thread.ident is not None:
            await asyncio.to_thread(thread.join, 1.0)
            if thread.is_alive():
                raise RuntimeError("echo thread did not terminate after socket shutdown")
    return samples


async def main_async(args: argparse.Namespace) -> None:
    packet = build_publish(args.payload_size)
    results: dict[str, dict[str, float]] = {}
    order = list(ARMS)
    per_arm: dict[str, list[float]] = {name: [] for name in order}

    for _ in range(args.repeat):
        for name in order:  # interleave so drift hits every arm equally
            per_arm[name].extend(
                await measure(name, packet, args.count, args.warmup, args.timeout_s)
            )

    print(
        f"payload {args.payload_size} B, {args.count} round trips x {args.repeat} "
        f"interleaved reps, TCP loopback, echo server thread\n"
        "Mechanism ablation over one shared decoder -- not a comparison of the "
        "PR branches at their commits.\n"
    )
    header = f"{'mechanism':14}{'p50 us':>9}{'p90 us':>9}{'p99 us':>9}{'mean':>9}{'vs base':>9}"
    print(header)
    base = statistics.median(per_arm["streamreader"])
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
            f"{name:14}{stats['p50']:9.2f}{stats['p90']:9.2f}{stats['p99']:9.2f}"
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
    parser.add_argument("--timeout-s", type=float, default=30.0, help="Deadline per arm")
    args = parser.parse_args()
    if args.count <= 0 or args.repeat <= 0 or args.warmup < 0 or args.timeout_s <= 0:
        parser.error("count, repeat and timeout must be positive; warmup must be nonnegative")
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
