"""Short-lived ARM64 lab for asyncio write-path choices.

This benchmark is intentionally narrow and experimental.  It compares the
transport primitives discussed while tuning MQTTium's writer without changing
production code:

* repeated ``write()`` calls;
* ``b''.join(parts)`` followed by one ``write()``;
* ``writelines(list)``;
* ``writelines(tuple)``;
* large segmented ``(header, payload)`` writes.

A separate sink process should listen on the requested host/port so the measured
process can be CPU-pinned independently.  Results are descriptive, not release
evidence.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import socket
import sys
import time
from pathlib import Path
from typing import Literal

_WRITE_HIGH_WATER = 64 * 1024
Mode = Literal["write_loop", "join_write", "writelines_list", "writelines_tuple"]


def _iterations(total_bytes: int, target_bytes: int, *, minimum: int = 64) -> int:
    if total_bytes <= 0:
        raise ValueError("total_bytes must be positive")
    return max(minimum, min(100_000, target_bytes // total_bytes))


async def _drain_if_needed(writer: asyncio.StreamWriter) -> None:
    transport = writer.transport
    if transport is not None and transport.get_write_buffer_size() > _WRITE_HIGH_WATER:
        await writer.drain()


async def _run_case(
    writer: asyncio.StreamWriter,
    mode: Mode,
    parts_list: list[bytes],
    parts_tuple: tuple[bytes, ...],
    iterations: int,
) -> dict[str, float | int | str]:
    total_bytes = sum(map(len, parts_tuple))

    # Tiny warmup to populate the selector/sendmsg path and translation caches.
    warmup = min(64, iterations)
    for _ in range(warmup):
        if mode == "write_loop":
            for part in parts_tuple:
                writer.write(part)
        elif mode == "join_write":
            writer.write(b"".join(parts_tuple))
        elif mode == "writelines_list":
            writer.writelines(parts_list)
        else:
            writer.writelines(parts_tuple)
        await _drain_if_needed(writer)
    await writer.drain()
    await asyncio.sleep(0)

    cpu_started = time.process_time()
    started = time.perf_counter()
    for _ in range(iterations):
        if mode == "write_loop":
            for part in parts_tuple:
                writer.write(part)
        elif mode == "join_write":
            writer.write(b"".join(parts_tuple))
        elif mode == "writelines_list":
            writer.writelines(parts_list)
        else:
            writer.writelines(parts_tuple)
        await _drain_if_needed(writer)
    await writer.drain()
    elapsed = time.perf_counter() - started
    cpu = time.process_time() - cpu_started
    moved = total_bytes * iterations
    return {
        "mode": mode,
        "iterations": iterations,
        "part_count": len(parts_tuple),
        "part_bytes": len(parts_tuple[0]),
        "bytes_per_group": total_bytes,
        "elapsed_seconds": elapsed,
        "cpu_seconds": cpu,
        "groups_per_second": iterations / elapsed,
        "mib_per_second": moved / elapsed / (1024 * 1024),
        "cpu_us_per_group": cpu * 1_000_000 / iterations,
    }


async def _measure(host: str, port: int) -> dict[str, object]:
    _reader, writer = await asyncio.open_connection(host, port)
    transport_name = type(writer.transport).__name__ if writer.transport is not None else "none"
    tiny: list[dict[str, float | int | str]] = []
    segmented: list[dict[str, float | int | str]] = []
    modes: tuple[Mode, ...] = (
        "write_loop",
        "join_write",
        "writelines_list",
        "writelines_tuple",
    )

    try:
        # Small MQTT-like frames.  4 MiB/case is enough to expose call/setup
        # overhead while keeping the whole matrix comfortably below a minute.
        for part_bytes in (64, 256, 4096):
            part = b"x" * part_bytes
            for part_count in (2, 4, 8, 16, 32):
                parts_tuple = (part,) * part_count
                parts_list = list(parts_tuple)
                iterations = _iterations(part_bytes * part_count, 4 * 1024 * 1024)
                for mode in modes:
                    result = await _run_case(
                        writer, mode, parts_list, parts_tuple, iterations
                    )
                    result["family"] = "tiny_batch"
                    tiny.append(result)
                    await asyncio.sleep(0.005)

        # Large MQTT Publish segmented form: small encoded header plus immutable
        # payload.  Here copying is expected to matter, so use 32 MiB/case.
        header = b"h" * 64
        for payload_bytes in (128 * 1024, 256 * 1024, 1024 * 1024):
            payload = b"p" * payload_bytes
            parts_tuple = (header, payload)
            parts_list = list(parts_tuple)
            iterations = _iterations(
                len(header) + payload_bytes, 32 * 1024 * 1024, minimum=32
            )
            for mode in modes:
                result = await _run_case(
                    writer, mode, parts_list, parts_tuple, iterations
                )
                result["family"] = "segmented"
                result["payload_bytes"] = payload_bytes
                segmented.append(result)
                await asyncio.sleep(0.01)
    finally:
        writer.close()
        await writer.wait_closed()

    return {
        "python": sys.version,
        "platform": platform.platform(),
        "transport": transport_name,
        "socket_sendmsg": hasattr(socket.socket, "sendmsg"),
        "tiny_batch": tiny,
        "segmented": segmented,
    }


def _ratio(results: list[dict[str, object]], numerator: str, denominator: str) -> float:
    by_mode = {str(item["mode"]): float(item["mib_per_second"]) for item in results}
    return by_mode[numerator] / by_mode[denominator]


def _markdown(payload: dict[str, object]) -> str:
    tiny = payload["tiny_batch"]
    segmented = payload["segmented"]
    assert isinstance(tiny, list)
    assert isinstance(segmented, list)
    lines = [
        "# Experimental asyncio write-path lab",
        "",
        f"- Python: `{payload['python']}`",
        f"- Transport: `{payload['transport']}`",
        f"- socket.sendmsg available: `{payload['socket_sendmsg']}`",
        "",
        "## Small batches",
        "",
        "| part bytes | parts | best | list writelines / join | tuple writelines / join |",
        "|---:|---:|---|---:|---:|",
    ]
    for part_bytes in (64, 256, 4096):
        for part_count in (2, 4, 8, 16, 32):
            group = [
                item
                for item in tiny
                if int(item["part_bytes"]) == part_bytes
                and int(item["part_count"]) == part_count
            ]
            best = max(group, key=lambda item: float(item["mib_per_second"]))
            lines.append(
                f"| {part_bytes} | {part_count} | {best['mode']} "
                f"({float(best['mib_per_second']):.1f} MiB/s) | "
                f"{_ratio(group, 'writelines_list', 'join_write'):.3f}× | "
                f"{_ratio(group, 'writelines_tuple', 'join_write'):.3f}× |"
            )

    lines.extend(
        [
            "",
            "## Segmented header + payload",
            "",
            "| payload | best | tuple writelines / write-loop | tuple writelines / join |",
            "|---:|---|---:|---:|",
        ]
    )
    for payload_bytes in (128 * 1024, 256 * 1024, 1024 * 1024):
        group = [
            item for item in segmented if int(item["payload_bytes"]) == payload_bytes
        ]
        best = max(group, key=lambda item: float(item["mib_per_second"]))
        lines.append(
            f"| {payload_bytes // 1024} KiB | {best['mode']} "
            f"({float(best['mib_per_second']):.1f} MiB/s) | "
            f"{_ratio(group, 'writelines_tuple', 'write_loop'):.3f}× | "
            f"{_ratio(group, 'writelines_tuple', 'join_write'):.3f}× |"
        )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=11885)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--summary-output", type=Path, required=True)
    return parser.parse_args()


async def _main(args: argparse.Namespace) -> None:
    payload = await _measure(args.host, args.port)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    args.summary_output.write_text(_markdown(payload), encoding="utf-8")
    print(args.summary_output.read_text(encoding="utf-8"))


if __name__ == "__main__":
    asyncio.run(_main(parse_args()))
