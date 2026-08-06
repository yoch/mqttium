from __future__ import annotations

import argparse
import importlib.util
import json
import statistics
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from types import ModuleType
from typing import Any


@dataclass(frozen=True, slots=True)
class Case:
    name: str
    mode: str
    payload_size: int
    count: int
    subscription: str


CASES = (
    Case("exact_sync_p0", "callback_sync", 0, 120_000, "bench/native-hotpath/telemetry"),
    Case("exact_sync_p256", "callback_sync", 256, 120_000, "bench/native-hotpath/telemetry"),
    Case("wildcard_sync_p256", "callback_sync", 256, 120_000, "bench/+/telemetry"),
    Case("exact_async_p256", "callback_async", 256, 120_000, "bench/native-hotpath/telemetry"),
    Case("exact_iterator_p256", "iterator", 256, 120_000, "bench/native-hotpath/telemetry"),
    Case("exact_both_p256", "both_sync", 256, 120_000, "bench/native-hotpath/telemetry"),
    Case("exact_sync_p4096", "callback_sync", 4_096, 20_000, "bench/native-hotpath/telemetry"),
    Case("exact_iterator_p4096", "iterator", 4_096, 20_000, "bench/native-hotpath/telemetry"),
    Case("exact_both_p4096", "both_sync", 4_096, 20_000, "bench/native-hotpath/telemetry"),
    Case("exact_sync_p65536", "callback_sync", 65_536, 2_000, "bench/native-hotpath/telemetry"),
)


def load_harness(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location("native_ingress_delivery_matrix", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load harness from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def revision(root: Path) -> str:
    return subprocess.check_output(
        ["git", "-C", str(root), "rev-parse", "HEAD"],
        text=True,
    ).strip()


def median(values: list[float]) -> float:
    return statistics.median(values)


def counter(sample: dict[str, Any], name: str) -> float:
    return float(sample["counters"].get(name, 0.0))


def summarize_case(case: Case, samples: list[dict[str, Any]]) -> dict[str, Any]:
    base = [sample for sample in samples if sample["variant"] == "base"]
    candidate = [sample for sample in samples if sample["variant"] == "candidate"]
    blocks = sorted({int(sample["block"]) for sample in samples})
    block_deltas: list[float] = []
    for block in blocks:
        base_rate = median(
            [sample["rate"] for sample in base if int(sample["block"]) == block]
        )
        candidate_rate = median(
            [sample["rate"] for sample in candidate if int(sample["block"]) == block]
        )
        block_deltas.append((candidate_rate / base_rate - 1.0) * 100.0)

    base_rate = median([sample["rate"] for sample in base])
    candidate_rate = median([sample["rate"] for sample in candidate])
    metrics = {}
    for name in (
        "loop_lag_p50_ms",
        "loop_lag_p95_ms",
        "loop_lag_p99_ms",
        "effect_suspensions",
        "effect_hwm",
        "callback_hwm",
        "iterator_hwm",
        "delivery_pending_hwm_bytes",
        "post_offer_drain_ms",
        "offered_rate",
    ):
        metrics[name] = {
            "base_median": median([counter(sample, name) for sample in base]),
            "candidate_median": median([counter(sample, name) for sample in candidate]),
        }

    return {
        "case": asdict(case),
        "base_rate_median": base_rate,
        "candidate_rate_median": candidate_rate,
        "rate_delta_from_medians_pct": (candidate_rate / base_rate - 1.0) * 100.0,
        "paired_block_delta_median_pct": median(block_deltas),
        "paired_block_deltas_pct": block_deltas,
        "positive_blocks": sum(delta > 0.0 for delta in block_deltas),
        "blocks": len(block_deltas),
        "base_completion_min": min(counter(sample, "completion_ratio") for sample in base),
        "candidate_completion_min": min(
            counter(sample, "completion_ratio") for sample in candidate
        ),
        "metrics": metrics,
    }


def markdown(summaries: list[dict[str, Any]]) -> str:
    lines = [
        "| Case | main | candidate | paired delta | positive blocks | lag p95 main → candidate | completion |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in summaries:
        lag = item["metrics"]["loop_lag_p95_ms"]
        lines.append(
            "| `{name}` | {base:,.0f} | {candidate:,.0f} | {delta:+.2f}% | "
            "{positive}/{blocks} | {base_lag:.3f} → {candidate_lag:.3f} ms | "
            "{completion:.3f} |".format(
                name=item["case"]["name"],
                base=item["base_rate_median"],
                candidate=item["candidate_rate_median"],
                delta=item["paired_block_delta_median_pct"],
                positive=item["positive_blocks"],
                blocks=item["blocks"],
                base_lag=lag["base_median"],
                candidate_lag=lag["candidate_median"],
                completion=min(
                    item["base_completion_min"], item["candidate_completion_min"]
                ),
            )
        )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-root", type=Path, required=True)
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--harness", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--blocks", type=int, default=4)
    args = parser.parse_args()

    base_root = args.base_root.resolve()
    candidate_root = args.candidate_root.resolve()
    harness = load_harness(args.harness.resolve())
    roots = {"base": base_root, "candidate": candidate_root}
    samples: list[dict[str, Any]] = []

    for block in range(args.blocks):
        offset = block % len(CASES)
        case_order = CASES[offset:] + CASES[:offset]
        variant_order = (
            ("base", "candidate", "candidate", "base")
            if block % 2 == 0
            else ("candidate", "base", "base", "candidate")
        )
        for case in case_order:
            for position, variant in enumerate(variant_order):
                sample = harness._sample(
                    root=roots[variant],
                    library="mqttium",
                    mode=case.mode,
                    payload_size=case.payload_size,
                    count=case.count,
                    subscription=case.subscription,
                )
                sample.update(
                    {
                        "case": case.name,
                        "variant": variant,
                        "block": block,
                        "position": position,
                    }
                )
                samples.append(sample)
                print(
                    f"SAMPLE block={block} case={case.name} variant={variant} "
                    f"rate={sample['rate']:.3f}",
                    flush=True,
                )

    summaries = [
        summarize_case(case, [sample for sample in samples if sample["case"] == case.name])
        for case in CASES
    ]
    table = markdown(summaries)
    payload = {
        "base_revision": revision(base_root),
        "candidate_revision": revision(candidate_root),
        "blocks": args.blocks,
        "samples_per_variant_per_case": args.blocks * 2,
        "cases": [asdict(case) for case in CASES],
        "summaries": summaries,
        "samples": samples,
        "table_md": table,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n")
    print(table)


if __name__ == "__main__":
    main()
