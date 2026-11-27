#!/usr/bin/env python3
"""The performance gate of a change that must not cost performance (report 0043).

Plays two mqttiumd executables, A (the reference) and B (the candidate),
pass after pass, alternating which goes first, and compares the broker CPU
time per delivered message. The broker runs on the cores of `--broker-cpus`
and the clients on those of `--client-cpus`, through `taskset`.

The statistic of a shape is the median, over the passes, of B/A: the
ratio of the two plays of one pass. Four shapes decide; `blast` sheds
QoS 0 messages by design and is only reported. The rule, accepted by the
owner of the repository on 24 November 2026:

- six passes; a deciding shape whose median exceeds `--threshold` (1.05)
  is played again alone over twelve passes, and that median decides;
- the gate resolves about 4 % on QoS 0 bursts with one subscriber, so a
  median between 1.03 and the threshold is followed by an instruction
  count (`tools/count_broker_instructions.sh`), which does not decide.

Load measurements use `dist` builds (`Cargo.toml`).

    python3 tools/run_perf_gate.py A/mqttiumd B/mqttiumd --passes 6

Standard library only. Exit status 0 when every deciding shape passes, 1
otherwise.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
QOS0 = HERE / "measure_qos0_delivery.py"
QOS1 = HERE / "measure_qos1_pairs.py"

DECIDING = {
    "qos0 bursts x1": (QOS0, ["--shape", "bursts", "--messages", "300000", "--subscribers", "1"]),
    "qos0 bursts x8": (QOS0, ["--shape", "bursts", "--messages", "150000", "--subscribers", "8"]),
    "qos1 pairs x1": (QOS1, ["--pairs", "1", "--messages", "300000"]),
    "qos1 pairs x8": (QOS1, ["--pairs", "8", "--messages", "60000"]),
}
REPORTED = {
    "qos0 blast x1": (QOS0, ["--shape", "blast", "--messages", "300000", "--subscribers", "1"]),
    "qos0 blast x8": (QOS0, ["--shape", "blast", "--messages", "150000", "--subscribers", "8"]),
}


def median_of_ratios(a: list, b: list) -> float | None:
    """The median of B/A over the passes where both plays gave a figure."""
    ratios = [y / x for x, y in zip(a, b) if x and y]
    return statistics.median(ratios) if ratios else None


def verdict(median: float | None, threshold: float, watch: float = 1.03) -> str:
    """What the rule says of one median of a deciding shape."""
    if median is None:
        return "no figure"
    if median > threshold:
        return "above"
    if median > watch:
        return "count instructions"
    return "pass"


def pinned(binary: str, cpus: str, directory: Path) -> str:
    """A wrapper that execs the broker under taskset, keeping its pid."""
    path = directory / f"pinned-{abs(hash(binary))}.sh"
    path.write_text(f'#!/usr/bin/env bash\nexec taskset -c {cpus} {binary} "$@"\n')
    path.chmod(0o755)
    return str(path)


def play(tool: Path, args: list, binary: str, client_cpus: str) -> dict:
    out = subprocess.run(["taskset", "-c", client_cpus, sys.executable, "-I", str(tool), "--binary", binary, *args],
                         capture_output=True, text=True, timeout=900)
    lines = [line for line in out.stdout.splitlines() if line.startswith("{")]
    return json.loads(lines[-1]) if lines else {}


def campaign(forms: dict, a: str, b: str, passes: int, client_cpus: str) -> dict:
    cpu = {form: {"A": [], "B": []} for form in forms}
    for number in range(passes):
        for form, (tool, args) in forms.items():
            for side in (("A", "B") if number % 2 == 0 else ("B", "A")):
                record = play(tool, args, a if side == "A" else b, client_cpus)
                cpu[form][side].append(record.get("broker_cpu_us_per_delivery"))
                print(json.dumps({"pass": number, "form": form, "side": side, **record}), flush=True)
    return {form: median_of_ratios(c["A"], c["B"]) for form, c in cpu.items()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("reference", help="mqttiumd A")
    parser.add_argument("candidate", help="mqttiumd B")
    parser.add_argument("--passes", type=int, default=6)
    parser.add_argument("--confirm-passes", type=int, default=12)
    parser.add_argument("--threshold", type=float, default=1.05)
    parser.add_argument("--only", action="append", help="a shape to play; repeatable")
    parser.add_argument("--broker-cpus", default="0,1")
    parser.add_argument("--client-cpus", default="2,3")
    args = parser.parse_args()
    forms = {**DECIDING, **REPORTED}
    if args.only:
        unknown = set(args.only) - set(forms)
        if unknown:
            parser.error(f"unknown shapes: {sorted(unknown)}")
        forms = {k: v for k, v in forms.items() if k in args.only}
    with tempfile.TemporaryDirectory(prefix="perf-gate-") as temporary:
        directory = Path(temporary)
        a = pinned(os.path.abspath(args.reference), args.broker_cpus, directory)
        b = pinned(os.path.abspath(args.candidate), args.broker_cpus, directory)
        medians = campaign(forms, a, b, args.passes, args.client_cpus)
        confirmed = {}
        for form, median in medians.items():
            if form in DECIDING and verdict(median, args.threshold) == "above":
                confirmed[form] = campaign({form: forms[form]}, a, b, args.confirm_passes, args.client_cpus)[form]
    failed = False
    print("SUMMARY: median over passes of B/A, broker CPU per delivery (above 1 = B costs more)")
    for form, median in medians.items():
        deciding = form in DECIDING
        final = confirmed.get(form, median)
        result = verdict(final, args.threshold) if deciding else "reported"
        failed |= deciding and result in ("above", "no figure")
        again = f", {args.confirm_passes} passes {final:.3f}" if form in confirmed else ""
        shown = "none" if median is None else f"{median:.3f}"
        print(f"{form}: {args.passes} passes {shown}{again} -> {result}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
