#!/usr/bin/env bash
# Plays the mutations of the real loop (lot L4) and checks that the oracle
# rejects each for its own reason. Run from the mqttiumd directory:
#
#   tools/run_loop_mutants.sh [--backend sim-store|sqlite]
#
# The runner plays every mutation on the backend given (sim-store when
# none is); an entry that names `backends` is played only on those.
# Each mutation of tests/mutants/loop/ is a unified diff named *.mutant
# (the root .gitignore ignores *.patch and *.diff as review artefacts). It
# is applied to a temporary worktree of the repository, never to the
# current tree, and built with a build directory of its own. The runner of the mutated tree plays the scenario
# of the entry; the oracle of the current, unmodified tree judges it.
#
# The exit status is not zero when a patch no longer applies, when a
# campaign cannot be written, when the oracle cannot judge, or when a
# mutation expected to be rejected is not rejected with its verdict, its
# assertion, its reason and, when the entry names one, the stop of the run. A known survivor (expect: survives-known) is
# reported, with the finding it waits for, and does not change the status.
set -euo pipefail

backend=sim-store
if [[ ${1:-} == --backend ]]; then
    backend=${2:?--backend needs a name}
    shift 2
fi
[[ $# -eq 0 ]] || { echo "usage: $0 [--backend sim-store|sqlite]" >&2; exit 64; }
here=$(pwd)
expected=tests/mutants/loop/expected.json
repository=$(git rev-parse --show-toplevel)
work=$(mktemp -d "${TMPDIR:-/tmp}/mqttiumd-loop-mutants.XXXXXX")
cleanup() {
    for tree in "${work:?}"/tree-*; do
        [[ -d $tree ]] && git -C "$repository" worktree remove --force "$tree" >/dev/null 2>&1 || true
    done
    rm -rf "${work:?}"
}
trap cleanup EXIT

failures=0
ids=$(python3 -c "import json,sys; print(' '.join(m['id'] for m in json.load(open(sys.argv[1]))['mutants']))" "$expected")
for id in $ids; do
    field() { python3 -c "import json,sys; m=[m for m in json.load(open(sys.argv[1]))['mutants'] if m['id']==sys.argv[2]][0]; print(m.get(sys.argv[3],''))" "$expected" "$id" "$1"; }
    if ! python3 -c "import json,sys; m=[m for m in json.load(open(sys.argv[1]))['mutants'] if m['id']==sys.argv[2]][0]; sys.exit(0 if sys.argv[3] in m.get('backends', [sys.argv[3]]) else 1)" "$expected" "$id" "$backend"; then
        echo "$id: not played on $backend: $(field not_on)"
        continue
    fi
    patch="$here/tests/mutants/loop/$(field patch)"
    scenario=$(field scenario)
    tree="$work/tree-$id"
    git -C "$repository" worktree add -q --detach "$tree" HEAD
    if ! git -C "$tree" apply "$patch"; then
        echo "$id: the patch no longer applies"
        failures=$((failures + 1))
        continue
    fi
    campaign="$work/campaign-$id"
    if ! (cd "$tree/mqttiumd" && CARGO_TARGET_DIR="$work/target" cargo run -q -p mqttiumd-lab --locked -- \
        run --contracts contracts/v1 --scenario "$scenario" --out "$campaign" --backend "$backend" >/dev/null); then
        echo "$id: no campaign was written"
        failures=$((failures + 1))
        continue
    fi
    status=0
    PYTHONPATH=tests/oracle python3 -m mqttiumd_oracle --root "$campaign" --contracts contracts/v1 \
        judge --scenario "$scenario" --trace trace.jsonl > "$campaign/verdict.json" || status=$?
    if [[ $status -eq 70 ]]; then
        echo "$id: the oracle could not judge the campaign"
        failures=$((failures + 1))
        continue
    fi
    if ! python3 - "$expected" "$id" "$campaign/verdict.json" "$campaign/campaign.json" <<'PY'
import json, sys
mutant = [m for m in json.load(open(sys.argv[1]))["mutants"] if m["id"] == sys.argv[2]][0]
verdict = json.load(open(sys.argv[3]))
stopped = json.load(open(sys.argv[4])).get("stopped") or ""
failing = {name: a for name, a in verdict["assertions"].items() if a["status"] != "PASS"}
summary = "; ".join(f"{name} {a['status']}: {' | '.join(a['reasons'])}" for name, a in failing.items())
if mutant["expect"] == "survives-known":
    if verdict["verdict"] == "PASS":
        print(f"{mutant['id']}: survives, known ({mutant['finding']}): {mutant['note']}")
    else:
        print(f"{mutant['id']}: now {verdict['verdict']} ({summary}); {mutant['finding']} may be closed: make the entry a rejected one")
    sys.exit(0)
assertion = failing.get(mutant["assertion"])
reasons = " | ".join(assertion["reasons"]) if assertion else ""
status = mutant.get("assertion_status", "FAIL")
stop_ok = "stopped" not in mutant or mutant["stopped"] in stopped
if verdict["verdict"] == mutant["verdict"] and assertion and assertion["status"] == status and mutant["reason"] in reasons and stop_ok:
    tail = f"; the run stopped: {stopped}" if stopped else ""
    print(f"{mutant['id']}: rejected, {verdict['verdict']}, {mutant['assertion']} {status}: {reasons}{tail}")
    sys.exit(0)
print(f"{mutant['id']}: NOT REJECTED AS EXPECTED: verdict {verdict['verdict']}, expected {mutant['verdict']} on {mutant['assertion']} {status} with \"{mutant['reason']}\"" + (f" and a stop with \"{mutant['stopped']}\"" if "stopped" in mutant else "") + f"; found: {summary or 'every assertion passes'}; stopped: {stopped or 'no'}")
sys.exit(1)
PY
    then
        failures=$((failures + 1))
    fi
done
if [[ $failures -ne 0 ]]; then
    echo "$failures mutation(s) not rejected as expected"
    exit 1
fi
echo "every mutation expected to be rejected was rejected for its reason on $backend"
