#!/usr/bin/env python3
"""Report the actual mount and bounded local write/WAL latencies."""
from __future__ import annotations

import argparse
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import time

from common import machine, require, sha, write
from load import distribution
from site_health import cgroup_io


def process_io():
    return {name: int(value) for name, value in
            (line.split(": ") for line in Path("/proc/self/io").read_text().splitlines())}


def identity(path):
    raw = subprocess.check_output(["findmnt", "-T", str(path), "-J", "-o",
                                   "SOURCE,FSTYPE,MAJ:MIN,OPTIONS"], text=True)
    mount = json.loads(raw)["filesystems"][0]
    mount["options"] = [option.split("=")[0] if option.startswith(("lowerdir=", "upperdir=", "workdir="))
                        else option for option in mount["options"].split(",")]
    name = mount["maj:min"]
    device = Path("/sys/dev/block") / name
    found = None
    if device.exists():
        real = device.resolve()
        drive = real.parent if (real / "partition").exists() else real
        def read(relative):
            metric = drive / relative
            return metric.read_text().strip() if metric.is_file() else None
        found = {"device": real.name, "model": read("device/model"), "rotational": read("queue/rotational"),
                 "kernel_path_class": "virtual" if "/virtual/" in str(real) else "device"}
    # The reported device name in an overlay or VM does not establish the SSD
    # behind it. Physical qualification needs independent operator provenance.
    return {"mount": mount, "mapped_device": found,
            "physical_hardware_qualification": "not_established",
            "scope": "logical writes and visible operating-system counters"}


def device_sample(topology):
    return next(({"device": fields[2], "sectors_written": int(fields[9]),
                  "writes_completed": int(fields[7]), "write_ms": int(fields[10])}
                 for fields in (line.split() for line in Path("/proc/diskstats").read_text().splitlines())
                 if f"{fields[0]}:{fields[1]}" == topology), None)


def run(out, count):
    host = machine(out)
    mount = identity(out)
    topology = mount["mount"]["maj:min"]
    after = {}
    start = time.perf_counter_ns()
    before = {"self_io": process_io(), "cgroup_io": cgroup_io(), "device": device_sample(topology)}
    io = {"append": [], "fsync": [], "sqlite_commit": [], "checkpoint": []}
    with tempfile.TemporaryDirectory(prefix="local-storage-", dir=out) as temp:
        base = Path(temp)
        with (base / "writes.bin").open("xb", buffering=0) as file:
            for _ in range(count):
                reference = time.perf_counter_ns()
                require(file.write(b"S" * 4096) == 4096, "storage_short_write")
                written = time.perf_counter_ns()
                os.fsync(file.fileno())
                synced = time.perf_counter_ns()
                io["append"].append(written - reference)
                io["fsync"].append(synced - written)
        path = base / "bench.sqlite"
        with closing(sqlite3.connect(path)) as db:
            require(db.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal", "storage_check_wal")
            db.execute("PRAGMA synchronous=FULL")
            db.execute("PRAGMA wal_autocheckpoint=128")
            db.execute("CREATE TABLE sample (seq INTEGER PRIMARY KEY, payload BLOB)")
            for i in range(count):
                reference = time.perf_counter_ns()
                with db:
                    db.execute("INSERT INTO sample VALUES (?,?)", (i + 1, i.to_bytes(4, "little") * 64))
                io["sqlite_commit"].append(time.perf_counter_ns() - reference)
                if (i + 1) % 64 == 0:
                    reference = time.perf_counter_ns()
                    require(db.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()[0] == 0, "storage_checkpoint_busy")
                    io["checkpoint"].append(time.perf_counter_ns() - reference)
            require(db.execute("PRAGMA quick_check").fetchone()[0] == "ok", "storage_integrity")
            after["sqlite"] = {name: db.execute(f"PRAGMA {name}").fetchone()[0]
                               for name in ("journal_mode", "synchronous", "wal_autocheckpoint", "page_count")}
            after["sqlite"]["image_bytes"] = path.stat().st_size
            after["sqlite"]["wal_bytes"] = path.with_suffix(".sqlite-wal").stat().st_size
    after.update(self_io=process_io(), cgroup_io=cgroup_io(), device=device_sample(topology))
    return {"status": "PASS", "machine": host, **mount, "samples": count,
            "latency_statistics_ms": {name: distribution(data) for name, data in io.items()},
            "before": before, "after": after, "wchar_delta": after["self_io"]["wchar"] - before["self_io"]["wchar"],
            "write_bytes_delta": after["self_io"]["write_bytes"] - before["self_io"]["write_bytes"],
            "elapsed_s": (time.perf_counter_ns() - start) / 1e9, "tool_sha256": sha(Path(__file__))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--samples", type=int, default=256)
    args = parser.parse_args()
    require(64 <= args.samples <= 2000, "storage_sample_budget")
    os.umask(0o077)
    args.output.mkdir(mode=0o700)
    result = run(args.output, args.samples)
    write(args.output / "result.json", result)
    print(json.dumps({"status": result["status"], "hardware": result["physical_hardware_qualification"]}))


if __name__ == "__main__":
    main()
