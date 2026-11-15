#!/usr/bin/env bash
# Runs one campaign with the runner of the bench, then has the oracle
# judge the directory it wrote (lot L4). Run from the mqttiumd directory:
#
#   tools/run_campaign.sh <scenario> <empty campaign directory> [seed] [sim-store|sqlite]
#
# The runner computes no verdict. The exit status is the oracle's:
# 0 PASS, 1 FAIL, 2 INCONCLUSIVE, 3 OUT_OF_SCOPE; 70 when the oracle
# could not judge, and 2 when no campaign could be written. The verdict
# is also written to <campaign>/verdict.json, outside the inventory of
# the campaign, which the runner wrote before any judgement.
set -euo pipefail

if [[ $# -lt 2 || $# -gt 4 ]]; then
    echo "usage: tools/run_campaign.sh <scenario> <empty campaign directory> [seed] [sim-store|sqlite]" >&2
    exit 2
fi
scenario=$1
out=$2
seed=${3:-1}
backend=${4:-sim-store}

if ! cargo run -q -p mqttiumd-lab --locked -- run \
    --contracts contracts/v1 --scenario "$scenario" --seed "$seed" --backend "$backend" \
    --out "$out"; then
    echo "no campaign was written" >&2
    exit 2
fi
status=0
PYTHONPATH=tests/oracle python -m mqttiumd_oracle --root "$out" --contracts contracts/v1 \
    judge --scenario "$scenario" --trace trace.jsonl > "$out/verdict.json" || status=$?
cat "$out/verdict.json"
exit "$status"
