#!/usr/bin/env python3
"""Adversarial & balanced ARM64 load matrix runner:
   fan-out (1:N), fan-in (N:1), slow-subscriber isolation, session churn under QoS 1,
   and mixed 256B + 64KiB workloads across mqttiumd (volatile/log) and Mosquitto.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time

from run_arm64_bench import children_cpu_seconds, mosquitto_identity, process_cpu_seconds

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def ensure_driver() -> Path:
    src = HERE / "arm64_matrix_bench.c"
    out = Path("/tmp/arm64_matrix_bench")
    if not out.exists() or out.stat().st_mtime < src.stat().st_mtime:
        subprocess.run(
            ["gcc", "-O2", "-Wall", "-Wextra", str(src), "-o", str(out)],
            check=True,
        )
    return out


def wait_port(port: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.02)
    raise RuntimeError(f"broker did not listen on 127.0.0.1:{port}")


def run_case(
    driver: Path,
    label: str,
    kind: str,
    binary: Path,
    extra: list[str],
    case_name: str,
    case_args: list[str],
    work_dir: str,
    broker_cpus: str,
    gen_cpus: str,
) -> dict:
    port = free_port()
    with tempfile.TemporaryDirectory(dir=work_dir, prefix=f"matrix-{label}-") as td:
        td_path = Path(td)
        if kind == "mqttiumd":
            cmd = [
                "taskset", "-c", broker_cpus, str(binary),
                "--listen", f"127.0.0.1:{port}",
                "--events", str(td_path / "events.jsonl"),
                "--event-level", "ops",
                "--max-connections", "1024",
            ]
            if "--no-db" not in extra:
                cmd += ["--db", str(td_path / "store.sqlite")]
            cmd += [a for a in extra if a != "--no-db"]
        else:
            conf = td_path / "mosquitto.conf"
            conf.write_text(
                f"listener {port} 127.0.0.1\n"
                "max_connections -1\n"
                "max_inflight_messages 64\n"
                "max_queued_messages 0\n"
                "allow_anonymous true\n"
                "persistence false\n"
                "log_type error\n"
            )
            cmd = ["taskset", "-c", broker_cpus, str(binary), "-c", str(conf)]

        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        try:
            wait_port(port)
            gen_cmd = [
                "taskset", "-c", gen_cpus, str(driver),
                "--host", "127.0.0.1", "--port", str(port),
                *case_args,
            ]
            broker_before = process_cpu_seconds(proc.pid)
            generator_before = children_cpu_seconds()
            res = subprocess.run(gen_cmd, capture_output=True, text=True, check=True)
            generator_cpu = children_cpu_seconds() - generator_before
            broker_cpu = process_cpu_seconds(proc.pid) - broker_before
            data = json.loads(res.stdout.strip().splitlines()[-1])
            # CPU while the driver ran: the broker's start and stop are not
            # counted.
            data["broker_cpu_s"] = broker_cpu
            data["generator_cpu_s"] = generator_cpu
            data["label"] = label
            data["case"] = case_name
            return data
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


SCENARIOS: list[tuple[str, list[str]]] = [
    ("fanout-1x4-q1",   ["--groups", "4", "--pubs-per-group", "1", "--subs-per-group", "4",  "--qos", "1", "--rate", "40000", "--duration-ms", "2000"]),
    ("fanout-1x16-q1",  ["--groups", "2", "--pubs-per-group", "1", "--subs-per-group", "16", "--qos", "1", "--rate", "15000", "--duration-ms", "2000"]),
    ("fanout-1x16-q0",  ["--groups", "2", "--pubs-per-group", "1", "--subs-per-group", "16", "--qos", "0", "--rate", "30000", "--duration-ms", "2000"]),
    ("fanin-32x1-q1",   ["--groups", "1", "--pubs-per-group", "32", "--subs-per-group", "1", "--qos", "1", "--rate", "80000", "--duration-ms", "2000"]),
    ("slow-sub-q0",     ["--groups", "2", "--pubs-per-group", "1", "--subs-per-group", "4", "--slow-subs", "1", "--qos", "0", "--rate", "40000", "--duration-ms", "2000"]),
    ("slow-sub-q1",     ["--groups", "2", "--pubs-per-group", "1", "--subs-per-group", "4", "--slow-subs", "1", "--qos", "1", "--rate", "20000", "--duration-ms", "2000"]),
    ("churn-q1",        ["--groups", "8", "--pubs-per-group", "1", "--subs-per-group", "1", "--churners", "4", "--qos", "1", "--rate", "30000", "--duration-ms", "2000"]),
    ("mixed-256b-64k",  ["--groups", "16", "--pubs-per-group", "1", "--subs-per-group", "1", "--large-pairs", "2", "--qos", "1", "--rate", "30000", "--large-rate", "400", "--duration-ms", "2000"]),
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", default=str(REPO / "target/dist/mqttiumd"))
    ap.add_argument("--mosquitto", default="/usr/sbin/mosquitto")
    ap.add_argument("--work-dir", default="/tmp")
    ap.add_argument("--broker-cpus", default="0,1,2")
    ap.add_argument("--gen-cpus", default="3")
    ap.add_argument("--only", choices=["all", "volatile", "log", "mosquitto"], default="all")
    ap.add_argument("--delivery-shards", type=int, default=0)
    ap.add_argument("--io-threads", type=int, default=2)
    ap.add_argument("--cases", default="")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    driver = ensure_driver()
    bin_path = Path(args.binary)
    mosq_path = Path(args.mosquitto)
    shard_args = ["--delivery-shards", str(args.delivery_shards)] if args.delivery_shards > 0 else []
    suffix = f"-s{args.delivery_shards}" if args.delivery_shards > 0 else ""
    targets = []
    if args.only in ("all", "volatile"):
        targets.append((f"mqttiumd-vol{suffix}", "mqttiumd", bin_path, ["--storage-mode", "volatile", "--no-db", "--io-threads", str(args.io_threads), *shard_args]))
    if args.only in ("all", "log"):
        targets.append((f"mqttiumd-log{suffix}", "mqttiumd", bin_path, ["--storage-mode", "log", "--io-threads", str(args.io_threads), *shard_args]))
    if args.only in ("all", "mosquitto"):
        reference = mosquitto_identity(mosq_path)
        targets.append((f"mosquitto-{reference['version']}", "mosquitto", mosq_path, []))

    wanted = {c.strip() for c in args.cases.split(",") if c.strip()}
    rows = []
    for case_name, case_args in SCENARIOS:
        if wanted and case_name not in wanted:
            continue
        print(f"\n=== {case_name} ===", flush=True)
        for label, kind, binary, extra in targets:
            r = run_case(
                driver, label, kind, binary, extra, case_name, case_args,
                args.work_dir, args.broker_cpus, args.gen_cpus,
            )
            if kind == "mosquitto":
                r["reference"] = reference
            rows.append(r)
            d = r["deliv_fast_ms"]
            print(
                f"{label:16s} pub={r['pub_mps']:8.0f}/s deliv={r['deliv_fast_mps']:8.0f}/s "
                f"ok={r['acked_ok_fast']:6d} ref={r['acked_refused_fast']:4d}(80:{r['refused_0x80']},97:{r['refused_0x97']}) "
                f"loss={r['loss_fast_pct']:5.2f}% disc={r['disconnects_fast']} churn={r['churn_completed']:3d} "
                f"p50/p99={d[0]:6.2f}/{d[2]:6.2f}ms",
                flush=True,
            )
    if args.out:
        Path(args.out).write_text(json.dumps(rows, indent=2))


if __name__ == "__main__":
    main()
