"""CLI for the runtime soak / quiescence harness.

Examples::

    PYTHONPATH=src python benchmarks/runtime_soak.py --profile ci
    PYTHONPATH=src python benchmarks/runtime_soak.py --profile local --seed 7 --protocol 5
    PYTHONPATH=src python benchmarks/runtime_soak.py --profile ci --backend mosquitto --port 11883
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import asdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT / "src"):
    text = str(path)
    if text not in sys.path:
        sys.path.insert(0, text)

from mqttium.enums import MQTTProtocolVersion  # noqa: E402

from benchmarks.runtime_soak_lib.profiles import PROFILES, SoakProfile  # noqa: E402
from benchmarks.runtime_soak_lib.runner import SoakFailure, run_soak  # noqa: E402


def _protocol(value: str) -> MQTTProtocolVersion:
    if value in {"311", "3.1.1", "MQTTv311"}:
        return MQTTProtocolVersion.MQTTv311
    if value in {"5", "5.0", "MQTTv5"}:
        return MQTTProtocolVersion.MQTTv5
    raise argparse.ArgumentTypeError("protocol must be 311 or 5")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=sorted(PROFILES), default="ci")
    parser.add_argument("--seed", type=int, action="append")
    parser.add_argument("--protocol", type=_protocol, action="append")
    parser.add_argument("--operations", type=int)
    parser.add_argument("--backend", choices=("fake", "mosquitto"), default="fake")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--timeout", type=float)
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace) -> dict[str, object]:
    profile = PROFILES[args.profile]
    if args.timeout is not None:
        profile = SoakProfile(**{**asdict(profile), "timeout": args.timeout, "name": profile.name})
    seeds = tuple(args.seed) if args.seed else profile.seeds
    protocols = tuple(args.protocol) if args.protocol else profile.protocols
    reports = []
    for protocol in protocols:
        for seed in seeds:
            report = await run_soak(
                profile,
                seed=seed,
                protocol=protocol,
                backend=args.backend,
                host=args.host,
                port=args.port,
                operations=args.operations,
            )
            reports.append(asdict(report))
    return {
        "profile": args.profile,
        "backend": args.backend,
        "reports": reports,
    }


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        payload = asyncio.run(_run(args))
    except SoakFailure as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": str(exc),
                    "history": exc.history,
                    "reduced": exc.reduced,
                },
                indent=2,
            )
        )
        raise SystemExit(1) from exc
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    print(rendered, end="")
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")


if __name__ == "__main__":
    main()
