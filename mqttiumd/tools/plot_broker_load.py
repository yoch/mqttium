#!/usr/bin/env python3
"""Render throughput, latency and generator lag from the archived load matrix."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
from statistics import median


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("matrix", type=Path)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--pairs", type=int, default=4)
    args = parser.parse_args()
    import matplotlib.pyplot as plt

    rows = json.loads(args.matrix.read_text())
    groups = defaultdict(lambda: defaultdict(list))
    for row in rows:
        if row["pairs"] == args.pairs and row["target_mps"]:
            groups[row["qos"], row["broker"], row["cores"]][row["target_mps"]].append(row)

    def latency(row, field):
        value = row[field]
        return None if value is None else value["p99"]

    metrics = [
        ("Unique deliveries inside the send window", "messages/s", lambda row: row["wire_mps"]),
        ("Submitted → subscriber p99", "ms", lambda row: latency(row, "submit_to_subscriber_ms")),
        ("Generator delay p99", "ms", lambda row: latency(row, "submit_schedule_lag_ms")),
        ("Submitted → positive PUBACK p99", "ms", lambda row: latency(row, "submit_to_positive_puback_ms")),
    ]
    kinds = sorted({qos for qos, _, _ in groups})
    figure, axes = plt.subplots(len(kinds), len(metrics), figsize=(18, 4.5 * len(kinds)), squeeze=False)
    for i, qos in enumerate(kinds):
        for axis, (title, units, value) in zip(axes[i], metrics, strict=True):
            for (quality, broker, cores), rates in sorted(groups.items()):
                if quality != qos:
                    continue
                points = []
                for rate, trials in sorted(rates.items()):
                    measured = [v for trial in trials if (v := value(trial)) is not None]
                    if measured:
                        points.append((rate, median(measured), min(measured), max(measured)))
                if points:
                    x, center, low, high = zip(*points, strict=True)
                    axis.errorbar(x, center, yerr=([m-a for m, a in zip(center, low, strict=True)],
                                                   [b-m for m, b in zip(center, high, strict=True)]),
                                  marker="o", markersize=3, capsize=2, label=f"{broker} · {cores} CPUs")
            axis.set_title(f"QoS {qos} · {title}")
            axis.set_xscale("log")
            axis.set_xlabel("scheduled messages/s")
            axis.set_ylabel(units)
            axis.grid(alpha=0.22)
            if axis.has_data():
                axis.legend(fontsize=7)
            else:
                axis.text(0.5, 0.5, "No PUBACK in QoS 0", ha="center", transform=axis.transAxes)
    figure.suptitle(f"Loopback MQTT 5 over mTLS · {args.pairs} disjoint publisher/subscriber pairs", fontsize=14)
    figure.tight_layout()
    figure.savefig(args.out, dpi=180)


if __name__ == "__main__":
    main()
