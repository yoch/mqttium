#!/usr/bin/env bash
# Bounded K2a exploration: 64 calibration trials, then exactly one fixed
# campaign of 1000 real process kills. No oracle qualification is claimed.
# Run from mqttiumd: tools/run_k2a_sweep.sh <empty output directory> [seed]
set -euo pipefail

[[ $# -ge 1 && $# -le 2 ]] || { echo "usage: $0 <empty output directory> [seed]" >&2; exit 2; }
out=$1
seed=${2:-1}
mkdir -p "$out"
out=$(cd "$out" && pwd)
[[ -z $(ls -A "$out") ]] || { echo "output directory is not empty" >&2; exit 2; }

{
    git rev-parse HEAD
    git status --short
    rustc --version --verbose
    uname -a
    df -T "$out"
} >"$out/machine.txt"

# Build once; compilation belongs to neither calibration nor campaign.
cargo build -q --release --locked -p mqttiumd-store-sqlite --example kill_sweep
target=$(cargo metadata --format-version 1 --no-deps --locked |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')
binary="$target/release/examples/kill_sweep"
sha256sum "$binary" >"$out/binary.sha256"
"$binary" calibration "$out/calibration" 64 "$seed" 1600 >"$out/calibration.log"

# Predeclared selection rule: the first grid bucket with >= 4/8 found
# outcomes is the crossover; freeze max = 2*crossover, with a floor of
# 50us and a ceiling of 3200us. If none crosses, use 3200us. No tuning or
# rerun of the 1000-trial campaign is performed afterwards.
python3 - "$out" "$seed" <<'PY'
import json
import pathlib
import sys

out = pathlib.Path(sys.argv[1])
rows = [json.loads(line) for line in (out / "calibration/trials.jsonl").read_text().splitlines()]
buckets = {}
for row in rows:
    delay = int(row["requested_delay_ns"]) // 1000
    bucket = buckets.setdefault(delay, {"found": 0, "absent": 0, "errors": 0})
    if row.get("state") in ("found", "absent"):
        bucket[row["state"]] += 1
    else:
        bucket["errors"] += 1
crossovers = [delay for delay, counts in sorted(buckets.items()) if counts["found"] >= 4]
maximum = min(3200, max(50, 2 * crossovers[0])) if crossovers else 3200
frozen = {
    "seed": sys.argv[2],
    "trials": 1000,
    "max_delay_us": maximum,
    "calibration_buckets": buckets,
    "selection_rule": "first calibration bucket with >=4/8 found; max=2*crossover clamped to [50,3200]us; no crossover gives3200us",
    "distribution": "uniform integer nanoseconds in [0,max_delay_us*1000]",
    "repeat_budget": 0,
}
(out / "frozen-campaign.json").write_text(json.dumps(frozen, indent=2) + "\n")
print(maximum)
PY
maximum=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["max_delay_us"])' "$out/frozen-campaign.json")
"$binary" campaign "$out/campaign" 1000 "$seed" "$maximum" >"$out/campaign.log"
cat "$out/calibration/summary.json"
cat "$out/frozen-campaign.json"
cat "$out/campaign/summary.json"
echo "K2a exploration data: $out"
