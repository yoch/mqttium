#!/usr/bin/env python3
"""Run a short rotating four-arm saturated receive matrix."""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

ARMS = ("base80", "stream128", "cap128", "direct")
MATRIX = (
    ("base80", "stream128", "cap128", "direct"),
    ("stream128", "cap128", "direct", "base80"),
    ("cap128", "direct", "base80", "stream128"),
    ("direct", "base80", "stream128", "cap128"),
)


def gm(vals):
    return math.exp(statistics.mean(math.log(v) for v in vals))


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--root", required=True)
    p.add_argument("--output-dir", required=True)
    p.add_argument("--broker-pid", type=int, required=True)
    p.add_argument("--port", type=int, default=11883)
    p.add_argument("--sut-cpu", type=int, default=0)
    p.add_argument("--publisher-cpu", type=int, default=2)
    p.add_argument("--payload-size", type=int, default=65536)
    p.add_argument("--warmup-s", type=float, default=0.30)
    p.add_argument("--measure-s", type=float, default=1.0)
    p.add_argument("--publisher-s", type=float, default=1.6)
    args = p.parse_args()

    root = Path(args.root).resolve()
    out = Path(args.output_dir).resolve()
    out.mkdir(parents=True, exist_ok=True)
    probe = root / "tools" / "receive_architecture_probe.py"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root / "src")
    rows = []
    idx = 0

    for block, order in enumerate(MATRIX):
        for position, arm in enumerate(order):
            topic = f"bench/direct-ingress/{os.getpid()}/{idx}"
            ready = out / f"ready-{idx}"
            subjson = out / f"sub-{idx}.json"
            pubjson = out / f"pub-{idx}.json"
            sublog = open(out / f"sub-{idx}.log", "w")
            publog = open(out / f"pub-{idx}.log", "w")
            subcmd = [
                "taskset", "-c", str(args.sut_cpu), sys.executable, str(probe),
                "subscriber", "--arm", arm, "--host", "127.0.0.1",
                "--port", str(args.port), "--topic", topic,
                "--payload-size", str(args.payload_size), "--warmup-s", str(args.warmup_s),
                "--duration-s", str(args.measure_s), "--broker-pid", str(args.broker_pid),
                "--ready-file", str(ready), "--output", str(subjson),
            ]
            sub = subprocess.Popen(subcmd, cwd=root, env=env, stdout=sublog, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 10
            while not ready.exists() and time.monotonic() < deadline:
                if sub.poll() is not None:
                    break
                time.sleep(0.03)
            if not ready.exists():
                sub.terminate()
                sub.wait(timeout=3)
                sublog.close()
                publog.close()
                raise SystemExit(f"subscriber {arm} failed before ready; see {out / f'sub-{idx}.log'}")

            pubcmd = [
                "taskset", "-c", str(args.publisher_cpu), sys.executable, str(probe),
                "publisher", "--host", "127.0.0.1", "--port", str(args.port),
                "--topic", topic, "--payload-size", str(args.payload_size),
                "--duration-s", str(args.publisher_s), "--output", str(pubjson),
            ]
            pub = subprocess.Popen(pubcmd, cwd=root, env=env, stdout=publog, stderr=subprocess.STDOUT)
            src = sub.wait(timeout=20)
            prc = pub.wait(timeout=20)
            sublog.close()
            publog.close()
            if src or prc:
                raise SystemExit(f"cell {idx} arm={arm} failed sub={src} pub={prc}")

            r = json.loads(subjson.read_text())
            q = json.loads(pubjson.read_text())
            r.update(
                idx=idx,
                block=block,
                position=position,
                arm=arm,
                publisher_mib_s=q["payload_mib_s"],
            )
            r["publisher_margin"] = q["payload_mib_s"] / r["payload_mib_s"] if r["payload_mib_s"] else 0
            mib = r["payload_bytes"] / (1024 * 1024)
            r["cpu_per_mib"] = r["ru_cpu_s"] / mib if mib else None
            r["stime_per_mib"] = r["ru_stime_s"] / mib if mib else None
            r["qualified"] = (
                r["publisher_margin"] >= 1.15
                and r["sut_cpu_pct"] >= 85
                and (r["broker_cpu_pct"] is None or r["broker_cpu_pct"] < 95)
                and r["ru_majflt"] == 0
            )
            rows.append(r)
            idx += 1
            time.sleep(0.05)

    (out / "rows.json").write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n")
    blocks = []
    for b in range(4):
        rs = {r["arm"]: r for r in rows if r["block"] == b}
        base = rs["base80"]
        item = {"block": b, "qualified": all(x["qualified"] for x in rs.values())}
        for arm in ARMS[1:]:
            x = rs[arm]
            item[f"throughput_ratio_{arm}_over_base80"] = x["payload_mib_s"] / base["payload_mib_s"]
            item[f"cpu_per_mib_ratio_{arm}_over_base80"] = x["cpu_per_mib"] / base["cpu_per_mib"]
            item[f"stime_per_mib_ratio_{arm}_over_base80"] = (
                x["stime_per_mib"] / base["stime_per_mib"] if base["stime_per_mib"] else None
            )
        blocks.append(item)

    summary = {
        "all_cells_qualified": all(r["qualified"] for r in rows),
        "blocks": blocks,
        "arms": {},
    }
    for arm in ARMS:
        rs = [r for r in rows if r["arm"] == arm]
        summary["arms"][arm] = {
            k: statistics.median(r[k] for r in rs)
            for k in (
                "payload_mib_s",
                "sut_cpu_pct",
                "ru_stime_s",
                "ru_utime_s",
                "ru_cpu_s",
                "ru_minflt",
                "broker_cpu_pct",
                "publisher_margin",
                "cpu_per_mib",
                "stime_per_mib",
            )
            if all(r[k] is not None for r in rs)
        }
    for arm in ARMS[1:]:
        summary[f"{arm}_vs_base80"] = {
            "throughput_geomean_ratio": gm(
                [b[f"throughput_ratio_{arm}_over_base80"] for b in blocks]
            ),
            "cpu_per_mib_geomean_ratio": gm(
                [b[f"cpu_per_mib_ratio_{arm}_over_base80"] for b in blocks]
            ),
            "stime_per_mib_geomean_ratio": gm(
                [
                    b[f"stime_per_mib_ratio_{arm}_over_base80"]
                    for b in blocks
                    if b[f"stime_per_mib_ratio_{arm}_over_base80"] is not None
                ]
            ),
        }

    (out / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
