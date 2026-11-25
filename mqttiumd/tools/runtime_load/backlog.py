#!/usr/bin/env python3
"""Paced QoS 2 publication, persistent subscription offline and observed event log."""
import argparse
import asyncio
from pathlib import Path
import struct
import time

from wire import append_json, broker, close, connect, expect, mqtt, proc_sample, read_packet, write_json

TOPIC = "load/backlog"


async def run(args, port, proc):
    reader, writer = await connect(port, "backlog-publisher")
    sub, peer = await connect(port, "backlog-subscriber", clean=False)
    peer.write(mqtt.subscribe(1, [(TOPIC, 2)]))
    await expect(sub, 0x90, b"\0\1\2")
    total = args.duration * args.rate
    received = bytearray(total)
    records = {"offered": total, "started": 0, "pubcomp": 0, "delivered": 0, "duplicates": 0,
               "unexpected_pubrel": 0, "sequence_reversals": 0, "max_pacing_lag_ms": 0}
    start = time.monotonic()
    done = asyncio.Event()

    async def subscriber():
        nonlocal sub, peer
        pending = {}
        previous = -1
        offline = False
        while records["delivered"] < total:
            if not offline and time.monotonic() >= start + args.offline_at:
                await close(peer)
                append_json(args.out / "samples.jsonl", {"offline": time.monotonic() - start,
                                                         "pubcomp": records["pubcomp"]})
                await asyncio.sleep(args.offline_seconds)
                sub, peer = await connect(port, "backlog-subscriber", clean=False, present=True)
                append_json(args.out / "samples.jsonl", {"reconnected": time.monotonic() - start,
                                                         "pubcomp": records["pubcomp"]})
                offline = True
            header, body = await read_packet(sub)
            if header >> 4 == mqtt.PUBLISH and (header >> 1) & 3 == 2:
                offset = 2 + int.from_bytes(body[:2], "big")
                ident = int.from_bytes(body[offset:offset + 2], "big")
                if body[2:offset] != TOPIC.encode():
                    raise ValueError("unexpected topic")
                seq, = struct.unpack("!I", body[offset + 2:])
                if seq >= total or (ident in pending and pending[ident] != seq):
                    raise ValueError("unexpected publication")
                pending[ident] = seq
                peer.write(mqtt.pubrec(ident))
            elif header == 0x62 and len(body) == 2:
                ident = int.from_bytes(body, "big")
                seq = pending.pop(ident, None)
                if seq is None:
                    records["unexpected_pubrel"] += 1
                else:
                    records["duplicates"] += received[seq]
                    if not received[seq]:
                        received[seq] = 1
                        records["delivered"] += 1
                        records["sequence_reversals"] += int(seq < previous)
                        previous = seq
                peer.write(mqtt.pubcomp(ident))
            else:
                raise ValueError(f"unexpected subscriber packet {header:#x}")
            await peer.drain()
        done.set()

    async def publisher():
        for seq in range(total):
            delay = start + seq / args.rate - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            lag_ms = (time.monotonic() - start - seq / args.rate) * 1000
            records["max_pacing_lag_ms"] = max(records["max_pacing_lag_ms"], lag_ms)
            ident = seq % 65535 + 1
            pid = ident.to_bytes(2, "big")
            writer.write(mqtt.publish(TOPIC, struct.pack("!I", seq), qos=2, packet_id=ident))
            records["started"] += 1
            await expect(reader, 0x50, pid)
            writer.write(mqtt.pubrel(ident))
            await expect(reader, 0x70, pid)
            records["pubcomp"] += 1

    async def monitor():
        while True:
            append_json(args.out / "samples.jsonl", {"elapsed": time.monotonic() - start,
                        **records, **proc_sample(proc.pid)})
            await asyncio.sleep(min(60, max(1, args.duration // 12)))

    reading = asyncio.create_task(subscriber())
    sampling = asyncio.create_task(monitor())
    try:
        await asyncio.wait_for(publisher(), args.duration + 60)
        records["publishing_seconds"] = time.monotonic() - start
        await asyncio.wait_for(reading, 60)
    finally:
        for task in (reading, sampling):
            task.cancel()
        outcomes = await asyncio.gather(reading, sampling, return_exceptions=True)
        for result in outcomes:
            if isinstance(result, Exception):
                records["subscriber_error"] = repr(result)
        records["elapsed_seconds"] = time.monotonic() - start
        records["missing"] = total - records["delivered"]
        write_json(args.out / "result.json", records)
        writer.close()
        peer.close()
    if not done.is_set() or records["missing"] or records["duplicates"]:
        raise ValueError(f"delivery mismatch: {records}")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--binary", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--duration", type=int, default=3600)
    ap.add_argument("--rate", type=int, default=100)
    ap.add_argument("--offline-at", type=int, default=60)
    ap.add_argument("--offline-seconds", type=int, default=35)
    args = ap.parse_args()
    if not (1 <= args.duration <= 3600 and 1 <= args.rate <= 100 and
            0 < args.offline_at < args.offline_at + args.offline_seconds < args.duration):
        ap.error("duration <= 3600, rate <= 100, and offline window strictly within duration")
    with broker(args) as (port, proc):
        asyncio.run(run(args, port, proc))


if __name__ == "__main__":
    main()
