#!/usr/bin/env python3
"""59-scenario ARM64 benchmark suite: mqttiumd (volatile/log/durable) vs the selected Mosquitto executable."""
import argparse
import json
from pathlib import Path
import sys
import tempfile

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(HERE))
import run_arm64_bench as bench


def run_group(
    title: str,
    cases: list[tuple[int, int, int]],
    targets: list[tuple[str, str, Path, list[str]]],
    tls: bool = False,
    payload: int = 256,
    duration_ms: int = 2000,
    broker_cpus: str = "0,1,2",
    gen_cpus: str = "3",
    work_dir: str = "/var/tmp",
) -> list[dict]:
    driver = bench.ensure_driver()
    max_pairs = max(p for p, _, _ in cases)
    print(
        f"\n=== {title} (tls={int(tls)}, payload={payload}B, dur={duration_ms}ms, work_dir={work_dir}) ===",
        flush=True,
    )
    results = []
    with tempfile.TemporaryDirectory(dir="/dev/shm", prefix="suite-pki-") as td:
        pki = bench.make_pki(Path(td), max(max_pairs, 256)) if tls else None
        for pairs, qos, rate in cases:
            for label, kind, binary, extra in targets:
                try:
                    r = bench.run_one(
                        driver,
                        kind,
                        binary,
                        extra,
                        pki,
                        tls,
                        pairs,
                        qos,
                        rate,
                        duration_ms,
                        payload,
                        broker_cpus,
                        gen_cpus,
                        work_dir=work_dir,
                    )
                    r["label"] = label
                    r["payload"] = payload
                    r["group"] = title
                    results.append(r)
                    print(
                        f"{label:22s} p={pairs:3d} q={qos} offer={rate:7d}/s -> "
                        f"sub={r['submitted']:6d} ack={r['acked_ok']:6d} ref={r['acked_refused']:5d} "
                        f"deliv={r['delivered_after_drain']:6d} ({r['delivered_mps']:8.0f}/s) "
                        f"ack_nodel={r['ack_no_deliv']} disc={r['disconnects']} inv={r['invalid']} "
                        f"fs={r['work_dir_backing']['fstype']} wal={r['wal_bytes']}B mlog={r['mlog_bytes']}B "
                        f"p50/p99={r['deliv_ms'][0]:.2f}/{r['deliv_ms'][2]:.2f}ms",
                        flush=True,
                    )
                except Exception as exc:
                    print(
                        f"{label:22s} p={pairs:3d} q={qos} offer={rate:7d}/s -> ERROR: {exc}",
                        flush=True,
                    )
                    results.append(
                        {
                            "label": label,
                            "group": title,
                            "pairs": pairs,
                            "qos": qos,
                            "rate": rate,
                            "payload": payload,
                            "error": str(exc),
                        }
                    )
    return results


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", default=str(REPO / "target/dist/mqttiumd"))
    ap.add_argument("--mosquitto", default="/usr/sbin/mosquitto")
    ap.add_argument("--work-dir", default="/var/tmp")
    ap.add_argument("--out", default="/tmp/arm64_suite_results.json")
    args = ap.parse_args()

    bin_path = Path(args.binary)
    mosq_path = Path(args.mosquitto)
    reference = bench.mosquitto_identity(mosq_path)
    all_targets = [
        (
            "mqttiumd-volatile-ram",
            "mqttiumd",
            bin_path,
            ["--storage-mode", "volatile", "--no-db", "--io-threads", "2"],
        ),
        (
            "mqttiumd-volatile-snap",
            "mqttiumd",
            bin_path,
            ["--storage-mode", "volatile", "--snapshot-interval", "5", "--io-threads", "2"],
        ),
        (
            "mqttiumd-log-ext4",
            "mqttiumd",
            bin_path,
            ["--storage-mode", "log", "--io-threads", "2"],
        ),
        (
            "mqttiumd-durable-ext4",
            "mqttiumd",
            bin_path,
            ["--storage-mode", "durable", "--io-threads", "2"],
        ),
        (f"mosquitto-{reference['version']}-ram", "mosquitto", mosq_path, []),
    ]

    all_results = []
    plain_cases = [
        (4, 0, 60000),
        (32, 0, 120000),
        (4, 1, 3000),
        (32, 1, 3000),
        (32, 1, 20000),
        (32, 1, 60000),
    ]
    all_results += run_group(
        "Plain TCP 256B (real ext4 backing)",
        plain_cases,
        all_targets,
        tls=False,
        payload=256,
        work_dir=args.work_dir,
    )

    tls_cases = [
        (32, 0, 60000),
        (32, 1, 20000),
    ]
    all_results += run_group(
        "TLS 1.3 mTLS 256B (real ext4 backing)",
        tls_cases,
        all_targets,
        tls=True,
        payload=256,
        work_dir=args.work_dir,
    )

    scale_cases = [
        (128, 0, 120000),
        (128, 1, 20000),
    ]
    all_results += run_group(
        "High Concurrency 256 conns Plain (real ext4 backing)",
        scale_cases,
        all_targets,
        tls=False,
        payload=256,
        work_dir=args.work_dir,
    )
    all_results += run_group(
        "Large Payload 64 KiB Plain (real ext4 backing)",
        [(4, 1, 2000)],
        all_targets,
        tls=False,
        payload=65536,
        work_dir=args.work_dir,
    )
    all_results += run_group(
        "Durable QoS 1 Control on tmpfs (/dev/shm) for backing contrast",
        [(32, 1, 20000), (32, 1, 60000)],
        [
            (
                "mqttiumd-log-tmpfs",
                "mqttiumd",
                bin_path,
                ["--storage-mode", "log", "--io-threads", "2"],
            ),
            (
                "mqttiumd-durable-tmpfs",
                "mqttiumd",
                bin_path,
                ["--storage-mode", "durable", "--io-threads", "2"],
            ),
        ],
        tls=False,
        payload=256,
        work_dir="/dev/shm",
    )

    for row in all_results:
        if row["label"].startswith("mosquitto-"):
            row["reference"] = reference
    Path(args.out).write_text(json.dumps(all_results, indent=2) + "\n")
    print(f"\nSaved {len(all_results)} rows to {args.out}", flush=True)


if __name__ == "__main__":
    main()
