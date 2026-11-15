#!/usr/bin/env bash
# Runs one campaign whose result can qualify, and checks that it does
# (record 0017). Run from the mqttiumd directory, on a clean checkout:
#
#   tools/run_qualifying_campaign.sh <scenario> <empty campaign directory> [seed] [sim-store|sqlite] [in-process|child]
#
# 1. `run --rebuild` builds the lab binary from an archive of the recorded
#    content, kept in the campaign as source.tar, and plays the scenario
#    with that binary. The directory must be outside the work tree.
# 2. The oracle produces the run result from the campaign. Its exit code
#    is the verdict's: 0 PASS, 1 FAIL, 2 INCONCLUSIVE, 3 OUT_OF_SCOPE.
# 3. The result must be qualifying, must be accepted with its manifest by
#    `mqttiumd-lab validate`, and must stand at `recompute`.
# 4. tools/check_campaign_binary.sh rebuilds the binary from source.tar
#    and compares it with the one the manifest names: the oracle builds
#    nothing, so this link is checked here.
#
# Exit 0 only when the result is a qualifying PASS and every check holds;
# otherwise the code of the verdict, or 1 when a check fails, 2 when no
# campaign was written.
set -euo pipefail

if [[ $# -lt 2 || $# -gt 5 ]]; then
    echo "usage: tools/run_qualifying_campaign.sh <scenario> <empty campaign directory> [seed] [sim-store|sqlite] [in-process|child]" >&2
    exit 2
fi
scenario=$1
out=$2
seed=${3:-1}
backend=${4:-sim-store}
process=${5:-in-process}
result=qualifying-run-result.json

if ! cargo run -q -p mqttiumd-lab --locked -- run \
    --contracts contracts/v1 --scenario "$scenario" --seed "$seed" --backend "$backend" \
    --process "$process" --rebuild --out "$out"; then
    echo "no campaign was written" >&2
    exit 2
fi
oracle() {
    PYTHONPATH=tests/oracle python -m mqttiumd_oracle --root "$out" --contracts contracts/v1 "$@"
}
status=0
oracle produce --scenario "$scenario" --trace trace.jsonl --manifest run-manifest.json \
    --out "$result" --scope full || status=$?
python3 - "$out/$result" <<'PY'
import json, sys
result = json.load(open(sys.argv[1]))
print(f"{result['qualification']} {result['verdict']}")
for reason in result.get("reasons", []):
    print(f"  {reason}")
sys.exit(0 if result["qualification"] == "qualifying" else 1)
PY
cargo run -q -p mqttiumd-lab --locked -- validate --contracts contracts/v1 --root "$out" \
    --file run-manifest.json --file "$result"
oracle recompute --result "$result"
tools/check_campaign_binary.sh "$out"
exit "$status"
