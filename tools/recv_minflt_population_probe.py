"""Fresh-process minor-fault population for the receive path.

The regime this work targets is layout-dependent: the same code lands in a fast
or a slow allocator mode depending on where the process maps its arenas, so a
single run proves nothing. This spawns many fresh children under normal ASLR
and reports the whole distribution, which is where the bimodality shows.

    python tools/recv_minflt_population_probe.py --samples 40 \
        --base-root /path/to/base --candidate-root /path/to/candidate
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import resource
import socket
import statistics
import subprocess
import sys
import threading
from pathlib import Path


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


async def _receive(total_bytes: int, payload_size: int) -> int:
    from mqttium.codec.buffer import IncrementalDecoder
    from mqttium.transport.tcp import TcpTransport

    frame = build_publish(payload_size)
    block = frame * max(1, (1 << 20) // len(frame))

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    host, port = listener.getsockname()

    def peer() -> None:
        conn, _ = listener.accept()
        sent = 0
        try:
            while sent < total_bytes:
                conn.sendall(block)
                sent += len(block)
        except OSError:
            pass
        finally:
            conn.close()

    thread = threading.Thread(target=peer, daemon=True)
    thread.start()

    transport = await TcpTransport.connect(host, port)
    decoder = IncrementalDecoder()
    attach = getattr(transport, "attach_decoder", None)
    receive = getattr(transport, "receive", None)
    if attach is not None:
        attach(decoder)

    before = resource.getrusage(resource.RUSAGE_SELF).ru_minflt
    delivered = 0
    while delivered < total_bytes:
        if receive is not None:
            if not await receive():
                break
        else:
            data = await transport.read(256 * 1024)
            if not data:
                break
            decoder.feed(data)
        while True:
            packet = decoder.next_packet()
            if packet is None:
                break
            delivered += len(packet.remaining)
    faults = resource.getrusage(resource.RUSAGE_SELF).ru_minflt - before
    await transport.close()
    listener.close()
    return faults


def _child(args: argparse.Namespace) -> None:
    faults = asyncio.run(_receive(args.total_bytes, args.payload_size))
    print(json.dumps({"minflt": faults}))


def _population(root: str, args: argparse.Namespace) -> list[int]:
    env = dict(os.environ, PYTHONPATH=str(Path(root) / "src"), PYTHONHASHSEED="random")
    faults: list[int] = []
    for _ in range(args.samples):
        proc = subprocess.run(
            [
                sys.executable,
                __file__,
                "--child",
                "--total-bytes",
                str(args.total_bytes),
                "--payload-size",
                str(args.payload_size),
            ],
            capture_output=True,
            text=True,
            env=env,
            check=True,
        )
        faults.append(json.loads(proc.stdout.strip().splitlines()[-1])["minflt"])
    return faults


def _summary(faults: list[int]) -> dict[str, float]:
    ordered = sorted(faults)
    median = statistics.median(ordered)
    # A "slow" process is one whose fault count is far off the population floor;
    # that separation is the bimodality itself, not a fixed threshold.
    floor = ordered[0] or 1
    return {
        "n": len(ordered),
        "min": ordered[0],
        "median": median,
        "p95": ordered[int(len(ordered) * 0.95) - 1],
        "max": ordered[-1],
        "max_over_min": ordered[-1] / floor,
        "slow_processes": sum(1 for f in ordered if f > 4 * floor),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", action="store_true")
    parser.add_argument("--samples", type=int, default=40)
    parser.add_argument("--total-bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--payload-size", type=int, default=1024)
    parser.add_argument("--base-root")
    parser.add_argument("--candidate-root")
    parser.add_argument("--output")
    args = parser.parse_args()

    if args.child:
        _child(args)
        return

    results = {}
    for label, root in (("base", args.base_root), ("candidate", args.candidate_root)):
        if root is None:
            continue
        results[label] = _summary(_population(root, args))

    header = (
        f"{'arm':11}{'n':>4}{'min':>9}{'median':>9}{'p95':>9}{'max':>9}{'max/min':>9}{'slow':>6}"
    )
    print(
        f"{args.samples} fresh processes, normal ASLR, "
        f"{args.total_bytes / 2**20:.0f} MiB, {args.payload_size} B payloads\n"
    )
    print(header)
    for label, s in results.items():
        print(
            f"{label:11}{s['n']:>4}{s['min']:>9.0f}{s['median']:>9.0f}{s['p95']:>9.0f}"
            f"{s['max']:>9.0f}{s['max_over_min']:>8.1f}x{s['slow_processes']:>6}"
        )
    if args.output:
        Path(args.output).write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
