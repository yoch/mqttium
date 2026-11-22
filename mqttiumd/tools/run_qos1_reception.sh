#!/usr/bin/env bash
# Actual product QoS 1 reception. These are finite system tests, not oracle qualification.
set -euo pipefail

if [[ $# != 1 ]]; then
    echo "usage: $0 NEW_OUTPUT_DIRECTORY" >&2
    exit 2
fi
q1_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
q1_repo=$(git -C "$q1_root" rev-parse --show-toplevel)
q1_python=${PYTHON:-python3}
q1_output=$("$q1_python" -c 'from pathlib import Path; import sys; print(Path(sys.argv[1]).resolve())' "$1")
case "$q1_output/" in "$q1_repo/"*) echo "output must be outside the worktree" >&2; exit 2;; esac
mkdir -p -- "$q1_output"
if [[ -n $(ls -A -- "$q1_output") ]]; then echo "output must be empty" >&2; exit 2; fi
if [[ -n $(git -C "$q1_repo" status --porcelain --untracked-files=normal) ]]; then
    echo "the system receipt requires a clean source tree" >&2
    exit 2
fi
q1_revision=$(git -C "$q1_repo" rev-parse HEAD)
q1_targets=${MQTTIUMD_QOS1_TARGET_ROOT:-${TMPDIR:-/tmp}/mqttiumd-qos1-$q1_revision}
q1_scratch=$(mktemp -d "${TMPDIR:-/tmp}/mqttiumd-qos1-mutants.XXXXXX")
q1_worktree=
q1_cleanup() {
    if [[ -n "$q1_worktree" ]]; then
        git -C "$q1_repo" worktree remove --force -- "$q1_worktree" >/dev/null 2>&1 || true
    fi
    rmdir -- "$q1_scratch" >/dev/null 2>&1 || true
}
trap q1_cleanup EXIT
mkdir -p -- "$q1_targets" "$q1_output/binaries"
cd -- "$q1_root"
q1_record() {
    local q1_kind=$1
    shift
    "$q1_python" - "$q1_output/commands.jsonl" "$q1_kind" "$PWD" "$@" <<'PY'
from pathlib import Path
import json, sys
with Path(sys.argv[1]).open('a') as target:
    target.write(json.dumps({'kind': sys.argv[2], 'cwd': sys.argv[3], 'command': sys.argv[4:]}) + '\n')
PY
}
"$q1_python" - <<'PY'
from importlib.metadata import version
if version('paho-mqtt') != '2.1.0':
    raise SystemExit('install tests/system/requirements.txt with the selected interpreter')
PY
"$q1_python" -m unittest discover -s tests/system -p 'test_*.py' -v >"$q1_output/pilot-tests.log" 2>&1
rustc -Vv >"$q1_output/rustc.txt"
cargo -V >"$q1_output/cargo.txt"
"$q1_python" -VV >"$q1_output/python.txt"
git rev-parse HEAD >"$q1_output/source-head.txt"
git diff --binary HEAD >"$q1_output/source.modification"
q1_record source-archive git archive --format=tar "$q1_revision"
git archive --format=tar "$q1_revision" >"$q1_output/source.tar"
for q1_mode in normal hooks; do
    q1_features=()
    if [[ "$q1_mode" == hooks ]]; then q1_features=(--features test-hooks); fi
    cargo tree -p mqttiumd --no-default-features "${q1_features[@]}" --locked --format '{p} features={f}' >"$q1_output/$q1_mode-features.txt"
    q1_record "$q1_mode-build" cargo build -p mqttiumd --no-default-features "${q1_features[@]}" --locked --target-dir "$q1_targets/$q1_mode"
    cargo build -p mqttiumd --no-default-features "${q1_features[@]}" --locked --target-dir "$q1_targets/$q1_mode" >"$q1_output/$q1_mode-build.log" 2>&1
    cp -- "$q1_targets/$q1_mode/debug/mqttiumd" "$q1_output/binaries/mqttiumd-$q1_mode"
done
"$q1_python" - "$q1_output/normal-features.txt" <<'PY'
from pathlib import Path
import sys
tree = Path(sys.argv[1]).read_text()
if any(name in tree for name in ('mqttiumd-lab', 'test-hooks', 'kill-points')):
    raise SystemExit('normal runtime unexpectedly contains instrumentation or lab')
PY
q1_trace=()
if command -v strace >/dev/null 2>&1; then
    q1_trace=(strace -f -qq -s 256 -e trace=process,signal,network,flock -o)
fi
q1_run() {
    local q1_name=$1 q1_binary=$2
    shift 2
    local q1_command=("$q1_python" "$q1_root/tests/system/qos1_reception.py" --binary "$q1_binary" --out "$q1_output/$q1_name" "$@")
    q1_record "$q1_name-reception" "${q1_command[@]}"
    if [[ ${#q1_trace[@]} != 0 ]]; then
        "${q1_trace[@]}" "$q1_output/$q1_name.strace" "${q1_command[@]}"
    else
        "${q1_command[@]}"
    fi
}
q1_run normal "$q1_output/binaries/mqttiumd-normal" --mode normal
q1_run product "$q1_output/binaries/mqttiumd-hooks" --mode complete

for q1_mutation in early-puback stale-receipt; do
    q1_worktree="$q1_scratch/$q1_mutation"
    git -C "$q1_repo" worktree add --detach -- "$q1_worktree" "$q1_revision" >"$q1_output/$q1_mutation-worktree.log" 2>&1
    "$q1_python" - "$q1_worktree/mqttiumd/crates/mqttiumd/src/runtime.rs" "$q1_mutation" <<'PY'
from pathlib import Path
import sys
source = Path(sys.argv[1])
name = {'early-puback': 'MUTANT_EARLY_PUBACK', 'stale-receipt': 'MUTANT_STALE_RECEIPT'}[sys.argv[2]]
old = f'const {name}: bool = false;'
text = source.read_text()
if text.count(old) != 1:
    raise SystemExit(f'exactly one mutation site required: {name}')
source.write_text(text.replace(old, f'const {name}: bool = true;'))
PY
    git -C "$q1_worktree" diff --binary >"$q1_output/$q1_mutation.mutant"
    q1_record "$q1_mutation-build" cargo build --manifest-path "$q1_worktree/mqttiumd/Cargo.toml" -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$q1_targets/$q1_mutation"
    cargo build --manifest-path "$q1_worktree/mqttiumd/Cargo.toml" -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$q1_targets/$q1_mutation" >"$q1_output/$q1_mutation-build.log" 2>&1
    cp -- "$q1_targets/$q1_mutation/debug/mqttiumd" "$q1_output/binaries/mqttiumd-$q1_mutation"
    if [[ "$q1_mutation" == early-puback ]]; then
        q1_case=B
        q1_expected=premature_puback
    else
        q1_case=C-late-receipt
        q1_expected=stale_receipt_released
    fi
    q1_status=0
    q1_run "$q1_mutation" "$q1_output/binaries/mqttiumd-$q1_mutation" --case "$q1_case" || q1_status=$?
    "$q1_python" - "$q1_output/$q1_mutation/receipt.json" "$q1_expected" "$q1_status" <<'PY'
from pathlib import Path
import json, sys
result = json.loads(Path(sys.argv[1]).read_text())
if int(sys.argv[3]) != 1 or result['status'] != 'FAIL' or result.get('failure', {}).get('code') != sys.argv[2]:
    raise SystemExit(f"mutation not caught for exact reason: {result.get('failure')}")
PY
    git -C "$q1_repo" worktree remove --force -- "$q1_worktree"
    q1_worktree=
done
"$q1_python" - "$q1_output" "$q1_revision" "${#q1_trace[@]}" <<'PY'
from pathlib import Path
import hashlib, json, sys
out = Path(sys.argv[1])
receipts = {name: json.loads((out / name / 'receipt.json').read_text())
            for name in ('normal', 'product', 'early-puback', 'stale-receipt')}
for receipt in receipts.values():
    binary = receipt['binary']
    if hashlib.sha256(Path(binary['path']).read_bytes()).hexdigest() != binary['sha256']:
        raise SystemExit('executed and retained binary differ')
    binary['retained_path'] = str(Path(binary['path']).relative_to(out))
index = {'format': 'mqttiumd.qos1-system-reception-index', 'schema_version': 1,
         'status': 'PASS', 'qualification': 'system-test-only', 'source_head': sys.argv[2],
         'source_archive': {'path': 'source.tar', 'sha256': hashlib.sha256((out / 'source.tar').read_bytes()).hexdigest()},
         'commands': 'commands.jsonl',
         'system_witnesses': 'same-run-strace' if int(sys.argv[3]) else 'process-wait-status-only',
         'cases': {name: receipt['cases'] for name, receipt in receipts.items()},
         'receipts': {name: {'path': f'{name}/receipt.json', 'status': receipt['status'],
                             'binary': receipt['binary'], 'failure': receipt.get('failure')}
                      for name, receipt in receipts.items()},
         'limitations': ['Finite system tests do not automatically qualify QoS 1 in the fixture oracle.',
                         'QoS 1 replay after a lost PUBACK may create another application admission.',
                         'No outgoing delivery, general garbage collection or full MQTT conformance claim.',
                         'Process kills do not establish power-loss or storage guarantees.']}
(out / 'index.json').write_text(json.dumps(index, indent=2, sort_keys=True) + '\n')
files = sorted(path for path in out.rglob('*') if path.is_file())
(out / 'SHA256SUMS').write_text(''.join(f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(out)}\n'
                                     for path in files))
print(json.dumps({'status': 'PASS', 'receipt': str(out / 'index.json'), 'source_head': sys.argv[2]}))
PY
tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner \
    -czf "$q1_output.tar.gz" -C "$(dirname -- "$q1_output")" "$(basename -- "$q1_output")"
sha256sum "$q1_output.tar.gz" >"$q1_output.tar.gz.sha256"
