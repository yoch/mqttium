#!/usr/bin/env python3
"""Alternated campaign over several brokers with the pairs or matrix driver.

Each pass plays every case on every target, in the order of `--target` on
even passes and in the reverse order on odd passes, as the performance gate
of report 0043 alternates its two builds. One JSON line per run is appended
to `--out`; `--summary` prints the median and the range of each figure over
the passes.

    run_ab_campaign.py --driver pairs --target lot8=A/mqttiumd \\
        --target lot9=B/mqttiumd --mode volatile --cases 32:1:100000 \\
        --passes 6 --out runs.jsonl
    run_ab_campaign.py --driver matrix --target head=B/mqttiumd \\
        --target head-k2=B/mqttiumd,--delivery-shards,2 --mode log \\
        --cases fanout-1x16-q1 --passes 6 --out runs.jsonl
    run_ab_campaign.py --summary runs.jsonl

A target is LABEL=EXECUTABLE[,ARG...]: the arguments are added to the
broker's command line. The label `mosquitto` names a Mosquitto executable,
which runs with `persistence false` and is only meaningful against
`--mode volatile`. Modes: volatile (no database), log and durable, on
`--work-dir`.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import statistics
import sys
import tempfile
import time

from run_arm64_bench import ensure_driver as ensure_pairs_driver
from run_arm64_bench import make_pki, mosquitto_identity, run_one
from run_matrix_bench import SCENARIOS, run_case
from run_matrix_bench import ensure_driver as ensure_matrix_driver

MODES = {
    "volatile": ["--storage-mode", "volatile", "--no-db"],
    "log": ["--storage-mode", "log"],
    "durable": ["--storage-mode", "durable"],
}


def parse_target(text: str) -> tuple[str, Path, list[str]]:
    label, _, rest = text.partition("=")
    if not label or not rest:
        raise SystemExit(f"bad target {text!r}: LABEL=EXECUTABLE[,ARG...]")
    executable, *args = rest.split(",")
    return label, Path(executable).resolve(), args


def load_average() -> float:
    return float(Path("/proc/loadavg").read_text().split()[0])


def campaign(args: argparse.Namespace) -> None:
    targets = [parse_target(t) for t in args.target]
    mode = MODES[args.mode] + ["--io-threads", str(args.io_threads)]
    out = Path(args.out)
    if args.driver == "pairs":
        driver = ensure_pairs_driver(Path(args.build_dir))
        cases = [tuple(map(int, c.split(":"))) for c in args.cases.split(",")]
    else:
        driver = ensure_matrix_driver()
        known = dict(SCENARIOS)
        cases = [c.strip() for c in args.cases.split(",") if c.strip()] or list(known)
        unknown = [c for c in cases if c not in known]
        if unknown:
            raise SystemExit(f"unknown matrix cases: {unknown}")
    identities = {
        label: mosquitto_identity(path) for label, path, _ in targets if label == "mosquitto"
    }
    with tempfile.TemporaryDirectory(dir="/dev/shm", prefix="campaign-pki-") as td:
        pki = None
        if args.tls:
            pki = make_pki(Path(td), max(64, max(c[0] for c in cases)))
        for index in range(args.passes):
            order = targets if index % 2 == 0 else list(reversed(targets))
            for case in cases:
                for label, path, extra in order:
                    kind = "mosquitto" if label == "mosquitto" else "mqttiumd"
                    broker_args = [] if kind == "mosquitto" else [*mode, *extra]
                    load = load_average()
                    started = time.time()
                    if args.driver == "pairs":
                        pairs, qos, rate = case
                        row = run_one(
                            driver,
                            kind,
                            path,
                            broker_args,
                            pki,
                            args.tls,
                            pairs,
                            qos,
                            rate,
                            args.duration_ms,
                            args.payload,
                            args.broker_cpus,
                            args.gen_cpus,
                            work_dir=args.work_dir,
                        )
                        name = f"{pairs}:{qos}:{rate}"
                    else:
                        row = run_case(
                            driver,
                            label,
                            kind,
                            path,
                            broker_args,
                            case,
                            dict(SCENARIOS)[case],
                            args.work_dir,
                            args.broker_cpus,
                            args.gen_cpus,
                        )
                        name = case
                    row.update(
                        {
                            "pass": index,
                            "target": label,
                            "case_name": name,
                            "mode": args.mode if kind == "mqttiumd" else "mosquitto",
                            "tls": args.tls,
                            "payload": args.payload,
                            "load_average": load,
                            "started": started,
                            "executable": str(path),
                            "broker_args": broker_args,
                        }
                    )
                    if label in identities:
                        row["reference"] = identities[label]
                    with out.open("a") as stream:
                        stream.write(json.dumps(row) + "\n")
                    print(
                        f"pass {index} {name:16s} {label:12s} {figures(row)}",
                        flush=True,
                    )


def figures(row: dict) -> dict:
    """The comparable figures of one run, from either driver."""
    if "deliv_fast_mps" in row:
        delivered_per_s = row["deliv_fast_mps"]
        latency = row["deliv_fast_ms"]
        refused = row["acked_refused_fast"]
        loss = row["loss_fast_pct"]
        deliveries = row["delivered_fast"] + row["delivered_large"]
    else:
        delivered_per_s = row["delivered_mps"]
        latency = row["deliv_ms"]
        refused = row["acked_refused"]
        loss = 100.0 * (1.0 - row["delivered_after_drain"] / max(1, row["submitted"]))
        deliveries = row["delivered_after_drain"]
    return {
        "deliv_per_s": round(delivered_per_s),
        "p50_ms": latency[0],
        "p99_ms": latency[2],
        "refused": refused,
        "loss_pct": round(loss, 2),
        "broker_cpu_us_per_delivery": round(1e6 * row["broker_cpu_s"] / max(1, deliveries), 2),
        "generator_cpu_s": round(row["generator_cpu_s"], 2),
    }


def summary(path: str) -> None:
    groups: dict[tuple[str, str, str], list[dict]] = {}
    for line in Path(path).read_text().splitlines():
        row = json.loads(line)
        key = (row["case_name"], row["mode"], row["target"])
        groups.setdefault(key, []).append(figures(row))
    for (case, mode, target), runs in sorted(groups.items()):
        cells = []
        for name in runs[0]:
            values = [r[name] for r in runs]
            cells.append(f"{name}={statistics.median(values):g} [{min(values):g}..{max(values):g}]")
        print(f"{case:16s} {mode:9s} {target:12s} n={len(runs)} " + " ".join(cells))


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--summary", metavar="RUNS_JSONL")
    ap.add_argument("--driver", choices=("pairs", "matrix"))
    ap.add_argument("--target", action="append", default=[])
    ap.add_argument("--mode", choices=sorted(MODES), default="volatile")
    ap.add_argument("--cases", default="")
    ap.add_argument("--passes", type=int, default=6)
    ap.add_argument("--tls", action="store_true")
    ap.add_argument("--payload", type=int, default=256)
    ap.add_argument("--duration-ms", type=int, default=2000)
    ap.add_argument("--io-threads", type=int, default=2)
    ap.add_argument("--broker-cpus", default="0,1,2")
    ap.add_argument("--gen-cpus", default="3")
    ap.add_argument("--work-dir", default="/var/tmp")
    ap.add_argument(
        "--build-dir", default=os.path.join(tempfile.gettempdir(), "mqttiumd-arm64-bench")
    )
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    if args.summary:
        summary(args.summary)
        return
    if not args.driver or not args.target or not args.out:
        ap.error("--driver, --target and --out are required for a campaign")
    if args.tls and args.driver == "matrix":
        ap.error("the matrix driver is plain TCP only")
    campaign(args)


if __name__ == "__main__":
    sys.exit(main())
