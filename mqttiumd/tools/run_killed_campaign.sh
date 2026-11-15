#!/usr/bin/env bash
# Plays a scenario on the SQLite store with the process under test in a
# child process, killed by a real SIGKILL where the scenario crashes the
# broker and started again on the same database file. Run from the
# mqttiumd directory, on a clean checkout:
#
#   tools/run_killed_campaign.sh <scenario> <empty directory> [seed]
#
# Two campaigns are written under the directory:
#
# qualifying/  the run rebuilt from its source (`run --rebuild --process
#              child`), whose result the oracle produces: it must be a
#              qualifying PASS, accepted by `validate`, standing at
#              `recompute`, with a binary a second build gives again
#              (tools/run_qualifying_campaign.sh). Its campaign.json
#              records the kill: the bench's word.
# witnessed/   the same run, not rebuilt, played under
#              `strace -f -e trace=kill,wait4`: the log, kept next to the
#              campaign as kill-witness.strace, is the kernel's account of
#              the kill and of the wait status. The oracle must judge it
#              PASS and its trace must be the bytes of the qualifying one.
#              Tracing the rebuild itself would trace a compiler; the
#              equal traces tie the two runs.
#
# tools/check_campaign_kill.py checks each campaign, the second against
# its witness. Without strace on PATH the second campaign is still played
# and checked, and the script says that the kill has no outside witness.
#
# What this shows: the loss of the process and of its volatile state
# after a commit, outside any transaction, and the recovery on the same
# file. Nothing about a transaction cut by a kill, fsync, a power loss or
# torn writes: the kernel and its page cache survive.
#
# Exit 0 when every check holds, 1 otherwise, 2 on a usage error.
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
    echo "usage: tools/run_killed_campaign.sh <scenario> <empty directory> [seed]" >&2
    exit 2
fi
scenario=$1
out=$2
seed=${3:-1}
mkdir -p "$out"
out=$(cd "$out" && pwd)
[[ -z $(ls -A "$out") ]] || { echo "$out is not empty" >&2; exit 2; }
fail() {
    echo "killed campaign: $*"
    exit 1
}

tools/run_qualifying_campaign.sh "$scenario" "$out/qualifying" "$seed" sqlite child ||
    fail "the rebuilt run with a killed child does not qualify"
python3 tools/check_campaign_kill.py "$out/qualifying" ||
    fail "the qualifying campaign does not record the kill of its child"

cargo build -q -p mqttiumd-lab --locked
lab=$(cargo metadata --format-version 1 --no-deps --locked |
    python3 -c "import json,sys; print(json.load(sys.stdin)['target_directory'])")/debug/mqttiumd-lab
arguments=(run --contracts contracts/v1 --scenario "$scenario" --seed "$seed"
    --backend sqlite --process child --out "$out/witnessed")
witness=()
if command -v strace >/dev/null; then
    witness=(strace -f -qq -e trace=kill,wait4 -e signal=none -o "$out/kill-witness.strace")
fi
"${witness[@]}" "$lab" "${arguments[@]}" >/dev/null || fail "no witnessed campaign was written"
status=0
PYTHONPATH=tests/oracle python3 -m mqttiumd_oracle --root "$out/witnessed" --contracts contracts/v1 \
    judge --scenario "$scenario" --trace trace.jsonl >"$out/witnessed/verdict.json" || status=$?
[[ $status -eq 0 ]] || fail "the oracle did not judge the witnessed campaign PASS (exit $status)"
cmp -s "$out/qualifying/trace.jsonl" "$out/witnessed/trace.jsonl" ||
    fail "the witnessed run wrote another trace than the qualifying one"
if [[ ${#witness[@]} -gt 0 ]]; then
    python3 tools/check_campaign_kill.py "$out/witnessed" --strace "$out/kill-witness.strace" ||
        fail "the witness does not report the kill the campaign records"
    echo "the child was killed by SIGKILL, as the kernel reports it, and the run recovered on the same file"
else
    python3 tools/check_campaign_kill.py "$out/witnessed" ||
        fail "the witnessed campaign does not record the kill of its child"
    echo "the child was killed by SIGKILL, as the bench records it (no strace: no outside witness), and the run recovered on the same file"
fi
