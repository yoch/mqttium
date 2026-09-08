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
            await transport.receive()

    return raw_transport, recv_one


# Mechanism names, not PR names: these are ablations, not those branches.
ARMS = {
    "streamreader": arm_rc13,
    "chunk-queue": arm_445,
    "buffered-sr": arm_446,
    "push": arm_push,
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
    asyncio.run(main_async(parser.parse_args()))


if __name__ == "__main__":
    main()
