"""Diagnostic screen of WritePump byte-quantum candidates.

Varies ``_WRITER_BATCH_MAX_BYTES`` under mixed frame sizes and prints batch
counts. This is not a release gate and must not commit result JSON.

Example::

    PYTHONPATH=src python benchmarks/writer_byte_quantum_screen.py
    PYTHONPATH=src python benchmarks/writer_byte_quantum_screen.py --quantum-kib 32,64,128,256
"""

from __future__ import annotations

import argparse
import asyncio

import mqttium.api._writer as writer
from mqttium.transport.writes import item_size


class _RecordingTransport:
    async def write(self, data: bytes) -> None:
        return None

    async def write_many(self, parts: list[bytes]) -> None:
        return None

    async def read(self, n: int = 65536) -> bytes:
        return b""

    async def close(self) -> None:
        return None

    def is_closing(self) -> bool:
        return False


async def _no_failure(exc: BaseException) -> None:
    raise AssertionError(f"unexpected writer failure: {exc!r}")


def _chunk(size: int) -> bytes:
    return b"x" * size


async def _run_mix(quantum: int, sizes: list[int]) -> tuple[int, int, int]:
    writer._WRITER_BATCH_MAX_BYTES = quantum
    pump = writer.WritePump(
        max_bytes=64 << 20,
        max_messages=100_000,
        on_failure=_no_failure,
    )
    transport = _RecordingTransport()
    items = [_chunk(size) for size in sizes]
    for item in items:
        assert pump.try_enqueue(item)
    pump.start(transport)
    await pump.join()
    await pump.stop()
    total_bytes = sum(item_size(item) for item in items)
    return pump.batches, pump.batched_items, total_bytes


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--quantum-kib",
        default="32,64,128,256",
        help="comma-separated candidate quanta in KiB",
    )
    args = parser.parse_args()
    quanta = [int(part) * 1024 for part in args.quantum_kib.split(",") if part.strip()]
    mixes = {
        "40kib_pair": [40 * 1024, 40 * 1024],
        "10x_10kib": [10 * 1024] * 10,
        "1x_oversize_then_1kib": [96 * 1024, 1024],
        "256x_64b": [64] * 256,
        "mixed_control_tail": [32 * 1024, 64, 32 * 1024, 64, 4 * 1024],
    }
    print("quantum_kib mix batches items bytes")
    for quantum in quanta:
        for name, sizes in mixes.items():
            batches, items, total_bytes = asyncio.run(_run_mix(quantum, sizes))
            print(f"{quantum // 1024} {name} {batches} {items} {total_bytes}")


if __name__ == "__main__":
    main()
