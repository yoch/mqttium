#!/usr/bin/env python3
"""Process-isolated memory-envelope probe for old/new direct decoder storage."""

from __future__ import annotations

import argparse
import gc
import json
import os
import resource
from pathlib import Path


def encode_vbi(value: int) -> bytes:
    out = bytearray()
    while True:
        digit = value % 128
        value //= 128
        if value:
            digit |= 0x80
        out.append(digit)
        if not value:
            return bytes(out)


def frame_with_total_limit(limit: int, *, fill: int = 0x61) -> bytes:
    # At the sizes used here Remaining Length occupies four bytes, so this
    # produces a legal frame whose total wire size is exactly *limit*.
    body_size = limit - 5
    body = bytes((fill,)) * body_size
    wire = b"\x30" + encode_vbi(body_size) + body
    assert len(wire) == limit
    return wire


def frame_of_total(total: int, *, fill: int = 0x62) -> bytes:
    for fixed in range(2, 6):
        body_size = total - fixed
        if body_size < 0:
            continue
        encoded = encode_vbi(body_size)
        if 1 + len(encoded) == fixed:
            return b"\x30" + encoded + bytes((fill,)) * body_size
    raise ValueError(total)


def rss_bytes() -> int:
    with open("/proc/self/statm", encoding="ascii") as handle:
        resident_pages = int(handle.read().split()[1])
    return resident_pages * os.sysconf("SC_PAGE_SIZE")


def peak_rss_bytes() -> int:
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * 1024


class Adapter:
    def __init__(self, kind: str, max_packet_size: int) -> None:
        self.kind = kind
        self._full_windows = 0
        self._window_target = 64 * 1024
        if kind == "old":
            from mqttium._direct_decoder_ingress_prototype import DirectIngressDecoder

            self.decoder = DirectIngressDecoder(max_packet_size)
        else:
            from mqttium.codec.buffer import IncrementalDecoder

            self.decoder = IncrementalDecoder(max_packet_size=max_packet_size)
        self.initial_capacity = int(self.decoder.capacity)

    def _window(self):
        if self.kind == "old":
            return self.decoder.writable_buffer()
        return self.decoder.writable_window(self._window_target)

    def _commit(self, nbytes: int, offered: int) -> None:
        if self.kind == "old":
            self.decoder.commit_written(nbytes)
            return
        self.decoder.commit(nbytes)
        if self._window_target < 256 * 1024:
            if offered >= 64 * 1024 and nbytes * 4 >= offered * 3:
                self._full_windows += 1
                if self._full_windows >= 2:
                    self._window_target = 256 * 1024
                    self._full_windows = 0
            else:
                self._full_windows = 0

    def receive(self, wire: bytes, fragments: tuple[int, ...] = (1 << 30,)) -> None:
        offset = 0
        frag_index = 0
        while offset < len(wire):
            window = self._window()
            try:
                offered = len(window)
                fragment = fragments[frag_index % len(fragments)]
                take = min(offered, fragment, len(wire) - offset)
                window[:take] = wire[offset : offset + take]
            finally:
                window.release()
            self._commit(take, offered)
            offset += take
            frag_index += 1

    def drain_one(self) -> None:
        packet = self.decoder.next_packet()
        assert packet is not None
        assert self.decoder.next_packet() is None

    def clear(self) -> None:
        self.decoder.clear()

    def stats(self) -> dict[str, int]:
        decoder = self.decoder
        return {
            "initial_capacity": self.initial_capacity,
            "capacity": int(decoder.capacity),
            "capacity_peak": int(getattr(decoder, "capacity_peak", decoder.capacity)),
            "logical_high_water": int(decoder.high_water),
            "buffered": int(decoder.buffered),
            "growth_count": int(decoder.growth_count),
            "shrink_count": int(getattr(decoder, "shrink_count", 0)),
            "compaction_count": int(decoder.compaction_count),
            "storage_generation": int(getattr(decoder, "storage_generation", 0)),
        }


def run_single(kind: str, scenario: str) -> dict[str, object]:
    limit = 8 * 1024 * 1024
    before = rss_bytes()
    adapter = Adapter(kind, limit)
    after_construct = rss_bytes()

    if scenario == "tiny":
        wire = frame_of_total(64)
        for _ in range(3000):
            adapter.receive(wire)
            adapter.drain_one()
    elif scenario == "mixed":
        frames = (frame_of_total(64), frame_of_total(64 * 1024), frame_of_total(180 * 1024))
        for _ in range(20):
            for wire in frames:
                adapter.receive(wire, (4096, 32768, 131071))
                adapter.drain_one()
    elif scenario == "large_fragmented":
        wire = frame_of_total(2 * 1024 * 1024)
        adapter.receive(wire, (16381, 65536, 131071, 32768))
        adapter.drain_one()
    elif scenario == "near_limit":
        wire = frame_with_total_limit(limit)
        adapter.receive(wire, (65536, 262144, 32767, 131071))
        adapter.drain_one()
    elif scenario == "fragment_alignments":
        wire = frame_of_total(1024 * 1024)
        for fragments in ((65536,), (65535, 65537), (17, 131071, 8192), (262143, 1, 32768)):
            adapter.receive(wire, fragments)
            adapter.drain_one()
    elif scenario == "large_to_tiny":
        adapter.receive(frame_with_total_limit(limit), (65536, 262144))
        adapter.drain_one()
        tiny = frame_of_total(64)
        for _ in range(64):
            adapter.receive(tiny)
            adapter.drain_one()
    elif scenario == "repeated_large":
        wire = frame_of_total(2 * 1024 * 1024)
        for _ in range(20):
            adapter.receive(wire, (65536, 262144))
            adapter.drain_one()
    elif scenario == "reconnect":
        wire = frame_of_total(2 * 1024 * 1024)
        adapter.receive(wire, (65536, 262144))
        adapter.drain_one()
        retained_before_clear = int(adapter.decoder.capacity)
        adapter.clear()
        retained_after_clear = int(adapter.decoder.capacity)
    else:
        raise ValueError(scenario)

    gc.collect()
    result: dict[str, object] = {
        "kind": kind,
        "scenario": scenario,
        "rss_before": before,
        "rss_after_construct": after_construct,
        "rss_after": rss_bytes(),
        "peak_rss": peak_rss_bytes(),
        **adapter.stats(),
    }
    if scenario == "reconnect":
        result["capacity_before_clear"] = retained_before_clear
        result["capacity_after_clear"] = retained_after_clear
    return result


def run_multi(kind: str) -> dict[str, object]:
    limit = 8 * 1024 * 1024
    before = rss_bytes()
    adapters = [Adapter(kind, limit) for _ in range(32)]
    after_construct = rss_bytes()
    wire = frame_of_total(64)
    for adapter in adapters:
        adapter.receive(wire)
        adapter.drain_one()
    gc.collect()
    return {
        "kind": kind,
        "scenario": "multi_connection",
        "connections": len(adapters),
        "rss_before": before,
        "rss_after_construct": after_construct,
        "rss_after": rss_bytes(),
        "peak_rss": peak_rss_bytes(),
        "capacity_sum": sum(int(adapter.decoder.capacity) for adapter in adapters),
        "capacity_peak_sum": sum(
            int(getattr(adapter.decoder, "capacity_peak", adapter.decoder.capacity))
            for adapter in adapters
        ),
        "initial_capacity_sum": sum(adapter.initial_capacity for adapter in adapters),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--kind", choices=("old", "new"), required=True)
    parser.add_argument(
        "--scenario",
        choices=(
            "tiny",
            "mixed",
            "large_fragmented",
            "near_limit",
            "fragment_alignments",
            "large_to_tiny",
            "repeated_large",
            "reconnect",
            "multi_connection",
        ),
        required=True,
    )
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    result = run_multi(args.kind) if args.scenario == "multi_connection" else run_single(args.kind, args.scenario)
    Path(args.output).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
