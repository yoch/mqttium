#!/usr/bin/env bash
# A campaign run on a modified tree can be rebuilt and judged from its own
# directory, once the tree is gone (lot L4). Run from the mqttiumd
# directory:
#
#   tools/check_campaign_replay.sh [scenario]
#
# 1. A temporary worktree of HEAD is modified: a change that changes the
#    binary (a line of the usage of the lab) and an untracked file.
# 2. `run --rebuild` plays the scenario there (P1-02 by default), with a
#    build directory of its own.
# 3. The worktree is removed.
# 4. From the campaign directory alone: the modified content is there
#    (patch, untracked file, source.tar), tools/check_campaign_binary.sh
#    rebuilds the binary from source.tar and finds the digest the run
#    manifest names, and the oracle judges the campaign PASS.
#
# Exit 0 when every check holds, 1 otherwise.
set -euo pipefail

scenario=${1:-tests/scenarios/p1/P1-02.json}
here=$(pwd)
repository=$(git rev-parse --show-toplevel)
work=$(mktemp -d "${TMPDIR:-/tmp}/mqttiumd-replay.XXXXXX")
tree="$work/tree"
campaign="$work/campaign"
cleanup() {
    git -C "$repository" worktree remove --force "$tree" >/dev/null 2>&1 || true
    rm -rf "${work:?}"
}
trap cleanup EXIT

fail() {
    echo "replay: $*"
    exit 1
}

git -C "$repository" worktree add -q --detach "$tree" HEAD
mark="replayed from its campaign directory"
sed -i "s/^Paths are relative to --root, which defaults to the current directory\\./& ($mark)/" \
    "$tree/mqttiumd/crates/mqttiumd-lab/src/main.rs"
grep -q "$mark" "$tree/mqttiumd/crates/mqttiumd-lab/src/main.rs" || fail "the modification was not made"
echo "untracked content of a modified tree" > "$tree/mqttiumd/tools/replay-probe.txt"
(cd "$tree/mqttiumd" && CARGO_TARGET_DIR="$work/target" cargo run -q -p mqttiumd-lab --locked -- \
    run --contracts contracts/v1 --scenario "$scenario" --out "$campaign" --rebuild >/dev/null) ||
    fail "no campaign was written"
git -C "$repository" worktree remove --force "$tree"
[[ ! -e $tree ]] || fail "the worktree is still there"

[[ -s $campaign/modified/tracked.patch ]] || fail "the patch of the tracked files is not kept"
[[ -f $campaign/modified/untracked/tools/replay-probe.txt ]] || fail "the untracked file is not kept"
tar -xOf "$campaign/source.tar" ./crates/mqttiumd-lab/src/main.rs | grep -q "$mark" ||
    fail "source.tar does not hold the modified source"
tar -tf "$campaign/source.tar" | grep -qx './tools/replay-probe.txt' ||
    fail "source.tar does not hold the untracked file"
"$here/tools/check_campaign_binary.sh" "$campaign" || fail "the binary is not the one its source builds"
status=0
PYTHONPATH="$here/tests/oracle" python3 -m mqttiumd_oracle --root "$campaign" --contracts contracts/v1 \
    judge --scenario "$scenario" --trace trace.jsonl >"$work/verdict.json" || status=$?
[[ $status -eq 0 ]] || fail "the oracle did not judge the campaign PASS (exit $status)"
echo "a campaign of a modified tree is rebuilt and judged PASS from its own directory"
