from __future__ import annotations

from pathlib import Path


BENCHMARK = Path("benchmarks/native_rtt_ingress.py")


def replace_once(text: str, old: str, new: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"expected one occurrence, found {count}: {old!r}")
    return text.replace(old, new, 1)


def main() -> None:
    text = BENCHMARK.read_text()
    old_run = '''async def _run(args: argparse.Namespace) -> dict[str, Any]:
    rtt_samples: list[Sample] = []
    ingress_samples: list[Sample] = []
    for run in range(args.runs):
        rtt_samples.append(await _rtt_once(args.rtt_pairs, 32, f"{args.label}-{run}"))
    for run in range(args.runs):
        ingress_samples.append(
            await _ingress_once(args.ingress_messages, f"{args.label}-{run}")
        )

    def aggregate(name: str, samples: list[Sample]) -> dict[str, Any]:
'''
    new_run = '''async def _run(args: argparse.Namespace) -> dict[str, Any]:
    rtt_samples: list[Sample] = []
    ingress_samples: list[Sample] = []
    if args.scenario in ("all", "rtt"):
        for run in range(args.runs):
            rtt_samples.append(await _rtt_once(args.rtt_pairs, 32, f"{args.label}-{run}"))
    if args.scenario in ("all", "ingress"):
        for run in range(args.runs):
            ingress_samples.append(
                await _ingress_once(args.ingress_messages, f"{args.label}-{run}")
            )

    def aggregate(name: str, samples: list[Sample]) -> dict[str, Any]:
'''
    text = replace_once(text, old_run, new_run)
    old_return = '''    return {
        "label": args.label,
        "rtt": aggregate("rtt_qos1_w32", rtt_samples),
        "ingress": aggregate("ingress_qos0_exact_telemetry256", ingress_samples),
    }
'''
    new_return = '''    result: dict[str, Any] = {"label": args.label}
    if rtt_samples:
        result["rtt"] = aggregate("rtt_qos1_w32", rtt_samples)
    if ingress_samples:
        result["ingress"] = aggregate(
            "ingress_qos0_exact_telemetry256", ingress_samples
        )
    return result
'''
    text = replace_once(text, old_return, new_return)
    text = replace_once(
        text,
        '    parser.add_argument("--runs", type=int, default=5)\n',
        '    parser.add_argument("--runs", type=int, default=5)\n'
        '    parser.add_argument(\n'
        '        "--scenario", choices=("all", "rtt", "ingress"), default="all"\n'
        '    )\n',
    )
    BENCHMARK.write_text(text)


if __name__ == "__main__":
    main()
