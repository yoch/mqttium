#!/usr/bin/env bash
# Reception of finding R1 of the second audit on real campaigns: a
# qualifying result is refused by `recompute` once a condition of a
# qualifying run no longer holds. Run from the mqttiumd directory, on a
# clean checkout:
#
#   tools/run_recompute_reception.sh <empty directory> [sim-store|sqlite]
#
# 1. A qualifying campaign of P1-02 is produced with the oracle of this
#    checkout (tools/run_qualifying_campaign.sh).
# 2. A second one is produced from a modified tree: a temporary worktree
#    of HEAD with one tracked file changed and one untracked file, played
#    with `run --rebuild`, then `produce`. It must qualify too, with the
#    modified content kept in the campaign.
# 3. tools/reproduce_recompute.py degrades independent copies of both,
#    one defect each, and requires `recompute` to refuse each for its own
#    reason and to let the intact copy stand.
#
# The campaigns are produced here, with this oracle: a campaign judged by
# another oracle would be refused for the identity of its oracle alone,
# which would show nothing about the defect injected.
#
# Exit 0 when every case behaves as expected, 1 otherwise.
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "usage: tools/run_recompute_reception.sh <empty directory> [sim-store|sqlite]" >&2
    exit 2
fi
out=$1
backend=${2:-sim-store}
scenario=tests/scenarios/p1/P1-02.json
here=$(pwd)
repository=$(git rev-parse --show-toplevel)
mkdir -p "$out"
out=$(cd "$out" && pwd)
[[ -z $(ls -A "$out") ]] || { echo "$out is not empty" >&2; exit 2; }
work=$(mktemp -d "${TMPDIR:-/tmp}/mqttiumd-recompute.XXXXXX")
tree="$work/tree"
cleanup() {
    git -C "$repository" worktree remove --force "$tree" >/dev/null 2>&1 || true
    rm -rf "${work:?}"
}
trap cleanup EXIT
fail() {
    echo "recompute reception: $*"
    exit 1
}

tools/run_qualifying_campaign.sh "$scenario" "$out/clean" 1 "$backend" ||
    fail "the campaign of the clean tree does not qualify"

git -C "$repository" worktree add -q --detach "$tree" HEAD
echo "A line added by the reception of finding R1." >> "$tree/mqttiumd/tests/scenarios/audit/README.md"
echo "untracked content of a modified tree" > "$tree/mqttiumd/tools/recompute-probe.txt"
(cd "$tree/mqttiumd" && env -u CARGO_ENCODED_RUSTFLAGS CARGO_TARGET_DIR="$work/target" \
    cargo run -q -p mqttiumd-lab --locked -- run --contracts contracts/v1 \
    --scenario "$scenario" --seed 1 --backend "$backend" --rebuild --out "$out/modified" >/dev/null) ||
    fail "no campaign of the modified tree was written"
git -C "$repository" worktree remove --force "$tree"
[[ -s $out/modified/modified/tracked.patch ]] || fail "the patch of the tracked file is not kept"
[[ -f $out/modified/modified/untracked/tools/recompute-probe.txt ]] ||
    fail "the untracked file is not kept"
# The oracle of this checkout is the one the modified tree had: the
# modification touched no file of the oracle.
status=0
PYTHONPATH=tests/oracle python3 -m mqttiumd_oracle --root "$out/modified" --contracts contracts/v1 \
    produce --scenario "$scenario" --trace trace.jsonl --manifest run-manifest.json \
    --out qualifying-run-result.json --scope full || status=$?
[[ $status -eq 0 ]] || fail "the campaign of the modified tree does not qualify (exit $status)"

python3 tools/reproduce_recompute.py "$out/clean" "$out/cases" --modified "$out/modified" ||
    fail "a degraded campaign was not refused for its own reason"
echo "recompute refuses each degraded campaign for its own reason, and the intact one stands"
