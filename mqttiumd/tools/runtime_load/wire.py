"""Loopback wire and Linux subprocess measurements for the runtime load probes."""
import asyncio
import contextlib
import hashlib
import json
import os
from pathlib import Path
import platform
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tests/compat"))
import mqttwire as mqtt


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def append_json(path, value):
    with path.open("a") as file:
        print(json.dumps(value), file=file, flush=True)


def sha256(path):
    with Path(path).open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def proc_sample(pid):
    memory = {}
    for name in ("status", "smaps_rollup"):
        for line in Path(f"/proc/{pid}/{name}").read_text().splitlines():
            key, _, rest = line.partition(":")
            if key in ("VmSize", "VmRSS", "VmHWM", "RssAnon", "Pss", "Private_Dirty"):
                memory[key + "_KiB"] = int(rest.split()[0])
    ticks = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    switches = {}
    for thread in Path(f"/proc/{pid}/task").iterdir():
        try:
            lines = (thread / "status").read_text().splitlines()
        except FileNotFoundError:
            continue
        values = dict(line.split(":", 1) for line in lines if ":" in line)
        switches[thread.name] = {
            "name": values["Name"].strip(),
            "voluntary": int(values["voluntary_ctxt_switches"]),
            "involuntary": int(values["nonvoluntary_ctxt_switches"]),
        }
    return {"monotonic": time.monotonic(), "context_switches": switches, "cpu_seconds": (
        int(ticks[11]) + int(ticks[12])) / os.sysconf("SC_CLK_TCK"), **memory}


@contextlib.contextmanager
def broker(args, extra=()):
    args.out.mkdir(parents=True, exist_ok=False)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    events = args.out / "events.jsonl"
    binary = args.binary.resolve()
    cmd = [str(binary), "--listen", f"127.0.0.1:{port}", "--events", str(events),
           "--event-level", "ops", "--storage-mode", "volatile", *extra]
    broker_cpus = getattr(args, "broker_cpus", None)
    if broker_cpus:
        cmd = ["taskset", "-c", broker_cpus, *cmd]
    sources = [Path(__file__), Path(sys.argv[0]), ROOT / "tests/compat/mqttwire.py"]
    if any(str(arg) == "--tls-cert" for arg in extra):
        sources.extend((ROOT / "tools/arm64_bench/run_arm64_bench.py", ROOT / "preview/demo-pki.py"))
    cpu_control = Path("/sys/fs/cgroup/cpu.max")
    manifest = {"command": cmd, "driver_command": sys.argv, "binary_sha256": sha256(binary),
                "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT,
                                                     text=True).strip(),
                "driver_sources": {p.name: sha256(p) for p in sources},
                "cpu_quota": cpu_control.read_text().strip() if cpu_control.exists() else None,
                "python": sys.version,
                "kernel": platform.uname()._asdict(), "affinity": sorted(os.sched_getaffinity(0)),
                "utc_started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    write_json(args.out / "manifest.json", manifest)
    with (args.out / "broker.log").open("wb") as log:
        proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 15
            while proc.poll() is None and time.monotonic() < deadline:
                if events.exists() and b'"ready"' in events.read_bytes():
                    break
                time.sleep(.02)
            else:
                raise RuntimeError("broker did not become ready; see broker.log")
            yield port, proc
        except BaseException as error:
            write_json(args.out / "failure.json", {"error": repr(error)})
            raise
        finally:
            if proc.poll() is None:
                proc.terminate()
            try:
                proc.wait(10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            write_json(args.out / "exit.json", {"returncode": proc.returncode,
                       "utc_stopped": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})


async def read_packet(reader):
    header = await reader.readexactly(1)
    length = 0
    for shift in range(0, 28, 7):
        value = (await reader.readexactly(1))[0]
        length += (value & 127) << shift
        if value < 128:
            if length > 1_114_112:
                raise ValueError(f"packet exceeds profile: {length}")
            return header[0], await reader.readexactly(length)
    raise ValueError("invalid remaining length")


async def expect(reader, header, body):
    actual = await read_packet(reader)
    if actual != (header, body):
        raise ValueError(f"expected {header:#x} {body.hex()}, got {actual!r}")


async def connect(port, name, *, clean=True, present=False, tls=None):
    opts = {"ssl": tls, "server_hostname": "127.0.0.1"} if tls else {}
    reader, writer = await asyncio.open_connection("127.0.0.1", port, **opts)
    writer.write(mqtt.connect(name, clean=clean, keepalive=0))
    try:
        await expect(reader, 0x20, bytes([int(present), 0]))
    except BaseException:
        writer.close()
        raise
    return reader, writer


async def close(writer):
    writer.write(mqtt.disconnect())
    await writer.drain()
    writer.close()
    await writer.wait_closed()
