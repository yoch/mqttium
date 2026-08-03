"""Tiny microbenchmark baseline for ingress decode + publish encode."""

from __future__ import annotations

import time

from mqttium.codec.buffer import IncrementalDecoder
from mqttium.packets import PublishPacket
from mqttium.enums import QoS


def _run(label: str, fn, iterations: int = 5000) -> float:
    # Warmup
    for _ in range(100):
        fn()
    start = time.perf_counter()
    for _ in range(iterations):
        fn()
    elapsed = time.perf_counter() - start
    ops = iterations / elapsed
    print(f"{label}: {ops:,.0f} ops/s ({elapsed:.4f}s for {iterations})")
    return ops


def main() -> None:
    packet = PublishPacket(
        topic="sensors/temp",
        payload=b'{"t":21.5}',
        qos=QoS.AT_MOST_ONCE,
        retain=False,
        dup=False,
    )
    wire = packet.encode()

    def encode() -> None:
        packet.encode()

    def decode_batch() -> None:
        dec = IncrementalDecoder()
        dec.feed(wire * 10)
        while dec.next_packet() is not None:
            pass

    _run("publish_encode_qos0_small", encode)
    _run("ingress_decode_10x_qos0_small", decode_batch, iterations=2000)


if __name__ == "__main__":
    main()
