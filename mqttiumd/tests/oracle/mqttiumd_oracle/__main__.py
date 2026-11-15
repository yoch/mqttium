"""Command line of the oracle.

Run from the ``mqttiumd`` directory::

    python -m mqttiumd_oracle judge --scenario <file> --trace <file>
    python -m mqttiumd_oracle recompute --result <file>
    python -m mqttiumd_oracle produce --scenario <file> --trace <file> \
        --manifest <file> --out <file> --scope <text>

Paths are relative to ``--root``, which defaults to the current directory.
``judge`` prints the status of every assertion with its reasons and exits
with the code of the verdict they determine: 0 PASS, 1 FAIL, 2 INCONCLUSIVE,
3 OUT_OF_SCOPE (DEM-023). ``recompute`` exits 0 when the result stands and 2
when it is refused: a refused result concludes nothing. ``produce`` writes
the run-result of the run and exits with the code of the verdict it
writes. It writes a qualifying result only when every condition of a
qualifying run holds as far as the given files establish it (``results.unmet``),
and otherwise a provisional one, which never carries PASS, with one reason
per unmet condition. No option makes it claim qualifying.

A defect of the oracle itself, or contracts it cannot read, exits 70: the
run was not judged, and no verdict code may say otherwise.
"""

from __future__ import annotations

import argparse
import json
import sys
import traceback

from . import judge, results
from .documents import Contracts, ContractsUnreadable, contained
from .semantic import reached as reached_by
from .semantic import verdict_of
from .strict import Refused


# The exit status of a defect of the oracle itself. It is none of the four
# verdict codes: a run that was never judged is not recorded as a verdict.
ORACLE_DEFECT = 70


def main(arguments=None) -> int:
    try:
        return _main(arguments)
    except ContractsUnreadable as error:
        print(f"oracle defect: the contracts cannot be read: {error}", file=sys.stderr)
    except Exception:  # every defect, whatever raised it, has the one code
        traceback.print_exc()
        print("oracle defect: the run was not judged", file=sys.stderr)
    return ORACLE_DEFECT


def _main(arguments) -> int:
    parser = argparse.ArgumentParser(prog="mqttiumd_oracle", description=__doc__.splitlines()[0])
    parser.add_argument("--root", default=".")
    parser.add_argument("--contracts", default="contracts/v1")
    commands = parser.add_subparsers(dest="command", required=True)
    judging = commands.add_parser("judge")
    judging.add_argument("--scenario", required=True)
    judging.add_argument("--trace", required=True)
    recomputing = commands.add_parser("recompute")
    recomputing.add_argument("--result", required=True)
    producing = commands.add_parser("produce")
    for name in ("--scenario", "--trace", "--manifest", "--out", "--scope"):
        producing.add_argument(name, required=True)
    options = parser.parse_args(arguments)

    contracts = Contracts.open(options.root, options.contracts)
    if options.command == "recompute":
        refusals = results.recompute(contracts, options.result)
        for refusal in refusals:
            print(f"refused {refusal}", file=sys.stderr)
        print("stands" if not refusals else f"refused: {len(refusals)} reasons")
        return 0 if not refusals else results.EXIT_CODES["INCONCLUSIVE"]
    if options.command == "produce":
        return _produce(contracts, options)
    try:
        run = judge.Run.load(contracts, options.scenario, options.trace)
    except (Refused, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return results.EXIT_CODES["INCONCLUSIVE"]
    statuses, faults = results._observed(run)
    reached = all(reached_by(fault, fault["hits"]) for fault in faults)
    verdict = verdict_of([status.status for status in statuses.values()], reached)
    report = {
        "contracts": f"sha256:{contracts.digest}",
        "verdict": verdict,
        "assertions": {
            name: {"status": status.status, "reasons": status.reasons}
            for name, status in statuses.items()
        },
        "faults": faults,
    }
    print(json.dumps(report, indent=2))
    return results.EXIT_CODES[verdict]


def _produce(contracts, options) -> int:
    try:
        result = results.produce(
            contracts, options.scenario, options.trace, options.manifest, options.scope
        )
    except (Refused, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return results.EXIT_CODES["INCONCLUSIVE"]
    conditions = results.unmet(contracts, options.scenario, options.manifest, options.trace)
    if conditions:
        result = results.provisional(result, conditions)
    if not contained(options.out):
        print(f"error: {options.out!r} is not a path below the root", file=sys.stderr)
        return results.EXIT_CODES["INCONCLUSIVE"]
    target = contracts.root / options.out
    target.write_text(json.dumps(result, indent=2) + "\n")
    if result["qualification"] == "qualifying":
        refusals = results.recompute(contracts, options.out)
        if refusals:
            raise AssertionError(f"the result written does not stand: {refusals}")
    print(f"{result['qualification']} {result['verdict']}")
    return result["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
