from __future__ import annotations

import argparse
import importlib.util
import json
import os
import statistics
import subprocess
import sys
import time
import tracemalloc
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
    subscription: str = "bench/native-hotpath/telemetry"


CASES = (
    Case("exact_sync_p0", "callback_sync", 0, 200_000),
    Case("exact_sync_p256", "callback_sync", 256, 200_000),
    Case("wildcard_sync_p256", "callback_sync", 256, 200_000, "bench/+/telemetry"),
    Case("exact_async_p256", "callback_async", 256, 200_000),
    Case("exact_iterator_p256", "iterator", 256, 200_000),
    Case("exact_both_p256", "both_sync", 256, 200_000),
    Case("exact_sync_p4096", "callback_sync", 4_096, 60_000),
    Case("exact_iterator_p4096", "iterator", 4_096, 60_000),
    Case("exact_both_p4096", "both_sync", 4_096, 60_000),
    Case("exact_sync_p65536", "callback_sync", 65_536, 12_000),
    Case("exact_iterator_p65536", "iterator", 65_536, 12_000),
    Case("exact_both_p65536", "both_sync", 65_536, 12_000),
)
VARIANTS = ("main", "prototype", "refined")


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
        ["git", "-C", str(root), "rev-parse", "HEAD"], text=True
    ).strip()


def median(values: list[float]) -> float:
    return statistics.median(values)


def counter(sample: dict[str, Any], name: str) -> float:
    return float(sample["counters"].get(name, 0.0))


def variant_order(block: int) -> tuple[str, ...]:
    rotations = (
        ("main", "prototype", "refined"),
        ("prototype", "refined", "main"),
        ("refined", "main", "prototype"),
    )
    order = rotations[block % len(rotations)]
    return tuple(reversed(order)) if block % 2 else order


def summarize_case(case: Case, samples: list[dict[str, Any]]) -> dict[str, Any]:
    by_variant = {
        variant: [sample for sample in samples if sample["variant"] == variant]
        for variant in VARIANTS
    }
    rates = {
        variant: median([float(sample["rate"]) for sample in variant_samples])
        for variant, variant_samples in by_variant.items()
    }
    comparisons: dict[str, Any] = {}
    for numerator, denominator in (
        ("prototype", "main"),
        ("refined", "main"),
        ("refined", "prototype"),
    ):
        paired = []
        for block in sorted({int(sample["block"]) for sample in samples}):
            num = next(
                float(sample["rate"])
                for sample in by_variant[numerator]
                if int(sample["block"]) == block
            )
            den = next(
                float(sample["rate"])
                for sample in by_variant[denominator]
                if int(sample["block"]) == block
            )
            paired.append((num / den - 1.0) * 100.0)
        comparisons[f"{numerator}_vs_{denominator}"] = {
            "median_pct": median(paired),
            "block_deltas_pct": paired,
            "positive_blocks": sum(delta > 0 for delta in paired),
            "blocks": len(paired),
        }

    metrics: dict[str, Any] = {}
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
        "completion_ratio",
    ):
        metrics[name] = {
            variant: median([counter(sample, name) for sample in variant_samples])
            for variant, variant_samples in by_variant.items()
        }
    return {
        "case": asdict(case),
        "rate_median": rates,
        "comparisons": comparisons,
        "metrics": metrics,
    }


def message_object_measurement(root: Path, *, iterations: int, retained: int) -> dict[str, Any]:
    code = r'''
import gc
import json
import statistics
import sys
import time
import tracemalloc
from mqttium.types import Message

iterations = int(sys.argv[1])
retained = int(sys.argv[2])
topic = "bench/native-hotpath/telemetry"
payload = b"p" * 256

def run_once():
    start = time.perf_counter_ns()
    for _ in range(iterations):
        Message(topic=topic, payload=payload)
    return (time.perf_counter_ns() - start) / iterations

for _ in range(2):
    run_once()
samples = [run_once() for _ in range(7)]
gc.collect()
tracemalloc.start()
objects = [Message(topic=topic, payload=payload) for _ in range(retained)]
current, peak = tracemalloc.get_traced_memory()
result = {
    "constructor_ns_median": statistics.median(samples),
    "constructor_ns_samples": samples,
    "shallow_size_bytes": sys.getsizeof(objects[0]),
    "retained_current_bytes": current,
    "retained_peak_bytes": peak,
    "retained_bytes_per_message": current / retained,
    "slots": list(getattr(Message, "__slots__", ())),
}
print(json.dumps(result))
'''
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root / "src")
    completed = subprocess.run(
        [sys.executable, "-c", code, str(iterations), str(retained)],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    return json.loads(completed.stdout.splitlines()[-1])


def markdown(summaries: list[dict[str, Any]]) -> str:
    lines = [
        "| Case | main | prototype | refined | prototype/main | refined/main | refined/prototype | refined positive blocks |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in summaries:
        rates = item["rate_median"]
        comparisons = item["comparisons"]
        refined_main = comparisons["refined_vs_main"]
        lines.append(
            "| `{name}` | {main:,.0f} | {prototype:,.0f} | {refined:,.0f} | "
            "{prototype_main:+.2f}% | {refined_main:+.2f}% | {refined_prototype:+.2f}% | "
            "{positive}/{blocks} |".format(
                name=item["case"]["name"],
                main=rates["main"],
                prototype=rates["prototype"],
                refined=rates["refined"],
                prototype_main=comparisons["prototype_vs_main"]["median_pct"],
                refined_main=refined_main["median_pct"],
                refined_prototype=comparisons["refined_vs_prototype"]["median_pct"],
                positive=refined_main["positive_blocks"],
                blocks=refined_main["blocks"],
            )
        )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--main-root", type=Path, required=True)
    parser.add_argument("--prototype-root", type=Path, required=True)
    parser.add_argument("--refined-root", type=Path, required=True)
    parser.add_argument("--harness", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--blocks", type=int, default=6)
    args = parser.parse_args()

    roots = {
        "main": args.main_root.resolve(),
        "prototype": args.prototype_root.resolve(),
        "refined": args.refined_root.resolve(),
    }
    harness = load_harness(args.harness.resolve())
    samples: list[dict[str, Any]] = []
    started = time.time()
    for block in range(args.blocks):
        offset = block % len(CASES)
        cases = CASES[offset:] + CASES[:offset]
        for case in cases:
            order = variant_order(block)
            for position, variant in enumerate(order):
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
                        "order": list(order),
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
    objects = {
        variant: message_object_measurement(root, iterations=500_000, retained=100_000)
        for variant, root in roots.items()
    }
    table = markdown(summaries)
    payload = {
        "revisions": {variant: revision(root) for variant, root in roots.items()},
        "blocks": args.blocks,
        "duration_seconds": time.time() - started,
        "cases": [asdict(case) for case in CASES],
        "summaries": summaries,
        "message_objects": objects,
        "samples": samples,
        "table_md": table,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n")
    print(table)
    print("MESSAGE_OBJECTS " + json.dumps(objects, sort_keys=True))


if __name__ == "__main__":
    main()
