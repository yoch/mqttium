#!/usr/bin/env python3
"""Record Linux memory at fill, drain and idle across six persistent MQTT sessions."""
import argparse
import asyncio
from pathlib import Path
import struct

from wire import append_json, broker, close, connect, expect, mqtt, proc_sample, read_packet, write_json

TOPIC = "load/memory"


async def run(args, port, proc):
    publisher, peer = await connect(port, "memory-publisher")
    members = [f"memory-{i}" for i in range(6)]
    for member in members:
        read, write = await connect(port, member, clean=False)
        write.write(mqtt.subscribe(1, [(TOPIC, 1)]))
        await expect(read, 0x90, b"\0\1\1")
        await close(write)
    idle = []
    for cycle in range(args.cycles):
        prefix = mqtt.utf8(TOPIC)
        start = proc_sample(proc.pid)
        for seq in range(args.messages):
            ident = seq + 1
            payload = struct.pack("!II", cycle, seq) + bytes(args.payload - 8)
            peer.write(mqtt.publish(TOPIC, payload, qos=1, packet_id=ident))
            await expect(publisher, 0x40, ident.to_bytes(2, "big"))
        full = proc_sample(proc.pid)

        async def drain(member):
            read, write = await connect(port, member, clean=False, present=True)
            seen = bytearray(args.messages)
            for _ in range(args.messages):
                head, body = await read_packet(read)
                if head != 0x32 or not body.startswith(prefix) or len(body) != len(prefix) + 2 + args.payload:
                    raise ValueError("invalid delivery")
                pid = int.from_bytes(body[len(prefix):len(prefix) + 2], "big")
                batch, index = struct.unpack_from("!II", body, len(prefix) + 2)
                if batch != cycle or index >= args.messages or seen[index]:
                    raise ValueError("session received an unexpected or duplicate publication")
                seen[index] = 1
                write.write(mqtt.puback(pid))
            write.write(mqtt.pingreq())
            await expect(read, 0xd0, b"")
            await close(write)

        await asyncio.gather(*(drain(member) for member in members))
        drained = proc_sample(proc.pid)
        await asyncio.sleep(args.idle_seconds)
        idle.append(proc_sample(proc.pid))
        append_json(args.out / "cycles.jsonl", {"cycle": cycle, "empty": start, "full": full,
                                               "drained": drained, "idle": idle[-1]})
    await close(peer)
    return {"sessions": 6, "cycles": args.cycles, "offered_per_cycle": args.messages,
            "payload_bytes": args.payload, "idle_seconds": args.idle_seconds,
            "deliveries_verified": args.messages * 6 * args.cycles, "post_drain_idle": idle}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--binary", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--broker-cpus")
    ap.add_argument("--cycles", type=int, default=5)
    ap.add_argument("--messages", type=int, default=200)
    ap.add_argument("--payload", type=int, default=1024)
    ap.add_argument("--idle-seconds", type=int, default=2)
    args = ap.parse_args()
    if not (1 <= args.cycles <= 20 and 1 <= args.messages <= 1024 and
            8 <= args.payload <= 4096 and 1 <= args.idle_seconds <= 10):
        ap.error("outside the bounded memory profile")
    with broker(args) as (port, proc):
        result = asyncio.run(asyncio.wait_for(run(args, port, proc), timeout=600))
        write_json(args.out / "result.json", result)


if __name__ == "__main__":
    main()
