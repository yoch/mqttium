#!/usr/bin/env bash
# Finite live-receipt system reception; this does not extend fixture qualification.
set -euo pipefail

if [[ $# != 1 ]]; then
    echo "usage: $0 NEW_OUTPUT_DIRECTORY" >&2
    exit 2
fi
receipt_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
receipt_repo=$(git -C "$receipt_root" rev-parse --show-toplevel)
receipt_python=${PYTHON:-python3}
receipt_out=$("$receipt_python" -c 'from pathlib import Path; import sys; print(Path(sys.argv[1]).resolve())' "$1")
case "$receipt_out/" in "$receipt_repo/"*) echo "output must be outside the worktree" >&2; exit 2;; esac
mkdir -p -- "$receipt_out"
[[ -z $(ls -A -- "$receipt_out") ]] || { echo "output must be empty" >&2; exit 2; }
if [[ -n $(git -C "$receipt_repo" status --porcelain --untracked-files=normal) ]]; then
    echo "the system reception requires a clean source tree" >&2
    exit 2
fi
receipt_sha=$(git -C "$receipt_repo" rev-parse HEAD)
receipt_targets=${MQTTIUMD_RECEIPT_TARGET_ROOT:-${TMPDIR:-/tmp}/mqttiumd-receipt-$receipt_sha}
receipt_scratch=$(mktemp -d "${TMPDIR:-/tmp}/mqttiumd-receipt-mutants.XXXXXX")
receipt_worktree=
receipt_cleanup() {
    if [[ -n "$receipt_worktree" ]]; then
        git -C "$receipt_repo" worktree remove --force -- "$receipt_worktree" >/dev/null 2>&1 || true
    fi
    rmdir -- "$receipt_scratch" >/dev/null 2>&1 || true
}
trap receipt_cleanup EXIT
mkdir -p -- "$receipt_targets" "$receipt_out/binaries"
cd -- "$receipt_root"
receipt_record() {
    local receipt_kind=$1
    shift
    "$receipt_python" - "$receipt_out/commands.jsonl" "$receipt_kind" "$PWD" "$@" <<'PY'
from pathlib import Path
import json, sys
with Path(sys.argv[1]).open('a') as target:
    target.write(json.dumps({'kind': sys.argv[2], 'cwd': sys.argv[3], 'command': sys.argv[4:]}) + '\n')
PY
}
"$receipt_python" - <<'PY'
from importlib.metadata import version
if version('paho-mqtt') != '2.1.0':
    raise SystemExit('install tests/system/requirements.txt with the selected interpreter')
PY
rustc -Vv >"$receipt_out/rustc.txt"
cargo -V >"$receipt_out/cargo.txt"
"$receipt_python" -VV >"$receipt_out/python.txt"
git rev-parse HEAD >"$receipt_out/source-head.txt"
git diff --binary HEAD >"$receipt_out/source.modification"
receipt_record source-archive git -C "$receipt_repo" archive --format=tar "$receipt_sha"
git -C "$receipt_repo" archive --format=tar "$receipt_sha" >"$receipt_out/source.tar"
receipt_record pilot-tests "$receipt_python" -m unittest discover -s tests/system -p 'test_*.py' -v
"$receipt_python" -m unittest discover -s tests/system -p 'test_*.py' -v >"$receipt_out/pilot-tests.log" 2>&1

for receipt_mode in normal hooks; do
    receipt_features=()
    if [[ "$receipt_mode" == hooks ]]; then receipt_features=(--features test-hooks); fi
    cargo tree -p mqttiumd --no-default-features "${receipt_features[@]}" --locked --format '{p} features={f}' >"$receipt_out/$receipt_mode-features.txt"
    receipt_record "$receipt_mode-build" cargo build -p mqttiumd --no-default-features "${receipt_features[@]}" --locked --target-dir "$receipt_targets/$receipt_mode"
    cargo build -p mqttiumd --no-default-features "${receipt_features[@]}" --locked --target-dir "$receipt_targets/$receipt_mode" >"$receipt_out/$receipt_mode-build.log" 2>&1
    cp -- "$receipt_targets/$receipt_mode/debug/mqttiumd" "$receipt_out/binaries/mqttiumd-$receipt_mode"
done
"$receipt_python" - "$receipt_out/normal-features.txt" <<'PY'
from pathlib import Path
import sys
tree = Path(sys.argv[1]).read_text()
if any(name in tree for name in ('mqttiumd-lab', 'test-hooks', 'kill-points')):
    raise SystemExit('normal product unexpectedly contains instrumentation or lab')
PY
receipt_trace=()
if command -v strace >/dev/null 2>&1; then
    receipt_trace=(strace -f -qq -s 256 -e trace=process,signal,network,flock -o)
fi
receipt_run() {
    local receipt_name=$1 receipt_binary=$2
    shift 2
    local receipt_command=("$receipt_python" "$receipt_root/tests/system/receipt_reception.py" --binary "$receipt_binary" --out "$receipt_out/$receipt_name" --binary-source-head "$receipt_sha" "$@")
    receipt_record "$receipt_name-reception" "${receipt_command[@]}"
    if [[ ${#receipt_trace[@]} != 0 ]]; then
        "${receipt_trace[@]}" "$receipt_out/$receipt_name.strace" "${receipt_command[@]}" >"$receipt_out/$receipt_name.log" 2>&1
    else
        "${receipt_command[@]}" >"$receipt_out/$receipt_name.log" 2>&1
    fi
}
receipt_run normal-ceiling "$receipt_out/binaries/mqttiumd-normal" --case A-ceiling
receipt_run normal-live "$receipt_out/binaries/mqttiumd-normal" --case B-live
for receipt_case in C-pressure E-replay F-before F-after F-credit F-results; do
    receipt_run "hooks-$receipt_case" "$receipt_out/binaries/mqttiumd-hooks" --build-mode test-hooks --case "$receipt_case"
done

# The two identity mutations are separate builds in the same risk group. Each
# changes one existing boolean site and must fail for the intended invariant.
for receipt_mutation in unknown-hole no-closure old-publication old-payload double-credit; do
    receipt_worktree="$receipt_scratch/$receipt_mutation"
    git -C "$receipt_repo" worktree add --detach -- "$receipt_worktree" "$receipt_sha" >"$receipt_out/$receipt_mutation-worktree.log" 2>&1
    "$receipt_python" - "$receipt_worktree/mqttiumd/crates" "$receipt_mutation" <<'PY'
from pathlib import Path
import sys
name = {'unknown-hole': 'MUTANT_CONFIRM_UNKNOWN',
        'no-closure': 'MUTANT_DELETE_WITHOUT_CLOSURE',
        'old-publication': 'MUTANT_ALLOW_OLD_PUBLICATION',
        'old-payload': 'MUTANT_ALLOW_OLD_PAYLOAD',
        'double-credit': 'MUTANT_DOUBLE_RECEIPT_CREDIT'}[sys.argv[2]]
old = f'const {name}: bool = false;'
sites = [(path, path.read_text()) for path in Path(sys.argv[1]).rglob('*.rs') if old in path.read_text()]
if len(sites) != 1 or sites[0][1].count(old) != 1:
    raise SystemExit(f'exactly one mutation site required: {name}')
path, text = sites[0]
path.write_text(text.replace(old, f'const {name}: bool = true;'))
PY
    git -C "$receipt_worktree" diff --binary >"$receipt_out/$receipt_mutation.mutant"
    receipt_record "$receipt_mutation-build" cargo build --manifest-path "$receipt_worktree/mqttiumd/Cargo.toml" -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$receipt_targets/$receipt_mutation"
    cargo build --manifest-path "$receipt_worktree/mqttiumd/Cargo.toml" -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$receipt_targets/$receipt_mutation" >"$receipt_out/$receipt_mutation-build.log" 2>&1
    cp -- "$receipt_targets/$receipt_mutation/debug/mqttiumd" "$receipt_out/binaries/mqttiumd-$receipt_mutation"
    case "$receipt_mutation" in
        unknown-hole) receipt_case=C-pressure; receipt_expected=closure_crossed_unknown;;
        no-closure) receipt_case=F-after; receipt_expected=receipt_deleted_without_closure;;
        old-publication) receipt_case=E-replay; receipt_expected=retired_publication_reused;;
        old-payload) receipt_case=E-replay; receipt_expected=retired_payload_reused;;
        double-credit) receipt_case=F-credit; receipt_expected=receipt_credit_double_returned;;
    esac
    receipt_status=0
    receipt_run "$receipt_mutation" "$receipt_out/binaries/mqttiumd-$receipt_mutation" --build-mode test-hooks --case "$receipt_case" || receipt_status=$?
    "$receipt_python" - "$receipt_out/$receipt_mutation/receipt.json" "$receipt_expected" "$receipt_status" <<'PY'
from pathlib import Path
import json, sys
result = json.loads(Path(sys.argv[1]).read_text())
if int(sys.argv[3]) != 1 or result['status'] != 'FAIL' or result.get('failure', {}).get('code') != sys.argv[2]:
    raise SystemExit(f"mutation not caught for its precise reason: {result.get('failure')}")
PY
    git -C "$receipt_repo" worktree remove --force -- "$receipt_worktree"
    receipt_worktree=
done

# Historical evidence has its own source and executable. Never label an old
# executable with this checkout's identity. Supplying it is optional because
# the review artifact is not required for a fresh fixed-build reception.
if [[ -n ${MQTTIUMD_RECEIPT_BASELINE_BINARY:-} ]]; then
    [[ -n ${MQTTIUMD_RECEIPT_BASELINE_SHA256:-} && -n ${MQTTIUMD_RECEIPT_BASELINE_SOURCE:-} ]] || {
        echo "baseline source and binary SHA-256 are required" >&2; exit 2;
    }
    cp -- "$MQTTIUMD_RECEIPT_BASELINE_BINARY" "$receipt_out/binaries/mqttiumd-historical"
    receipt_baseline_command=("$receipt_python" "$receipt_root/tests/system/receipt_reception.py" --binary "$receipt_out/binaries/mqttiumd-historical" --out "$receipt_out/historical-ceiling" --binary-source-head "$MQTTIUMD_RECEIPT_BASELINE_SOURCE" --expected-binary-sha256 "$MQTTIUMD_RECEIPT_BASELINE_SHA256" --case A-ceiling)
    receipt_record historical-ceiling "${receipt_baseline_command[@]}"
    receipt_baseline_status=0
    "${receipt_baseline_command[@]}" >"$receipt_out/historical-ceiling.log" 2>&1 || receipt_baseline_status=$?
    "$receipt_python" - "$receipt_out/historical-ceiling/receipt.json" "$receipt_baseline_status" <<'PY'
from pathlib import Path
import json, sys
result = json.loads(Path(sys.argv[1]).read_text())
if int(sys.argv[2]) != 1 or result.get('failure', {}).get('code') != 'receipt_capacity_ceiling':
    raise SystemExit('historical ceiling was not reproduced for its precise reason')
PY
fi
"$receipt_python" - "$receipt_out" "$receipt_sha" "${#receipt_trace[@]}" <<'PY'
from pathlib import Path
import hashlib, json, sys
out = Path(sys.argv[1])
names = ('normal-ceiling', 'normal-live', 'hooks-C-pressure', 'hooks-E-replay',
         'hooks-F-before', 'hooks-F-after', 'hooks-F-credit', 'hooks-F-results',
         'unknown-hole', 'no-closure', 'old-publication', 'old-payload', 'double-credit')
receipts = {name: json.loads((out / name / 'receipt.json').read_text()) for name in names}
expected_failures = {'unknown-hole': 'closure_crossed_unknown',
                     'no-closure': 'receipt_deleted_without_closure',
                     'old-publication': 'retired_publication_reused',
                     'old-payload': 'retired_payload_reused',
                     'double-credit': 'receipt_credit_double_returned'}
for name, result in receipts.items():
    if result['status'] != ('FAIL' if name in expected_failures else 'PASS') or result['source_head'] != sys.argv[2]:
        raise SystemExit('receipt or source did not match the tested source')
    if name in expected_failures and result.get('failure', {}).get('code') != expected_failures[name]:
        raise SystemExit('mutation reason changed')
    binary = result['binary']
    if hashlib.sha256(Path(binary['path']).read_bytes()).hexdigest() != binary['sha256']:
        raise SystemExit('executed and retained binary differ')
index = {'format': 'mqttiumd.receipt-system-reception-index', 'schema_version': 1,
         'status': 'PASS', 'qualification': 'system-test-only', 'source_head': sys.argv[2],
         'source_archive': {'path': 'source.tar', 'sha256': hashlib.sha256((out / 'source.tar').read_bytes()).hexdigest()},
         'commands': 'commands.jsonl',
         'system_witnesses': 'same-run-strace' if int(sys.argv[3]) else 'process-wait-status-only',
         'receipts': {name: {'path': f'{name}/receipt.json', 'status': result['status'],
                             'binary': result['binary'], 'failure': result.get('failure')}
                      for name, result in receipts.items()},
         'historical_ceiling': 'historical-ceiling/receipt.json' if (out / 'historical-ceiling/receipt.json').exists() else 'not rerun: old review evidence remains separate',
         'limitations': ['Finite system reception does not establish MQTT certification or unlimited service.',
                         'A collected receipt is retired; its business result cannot be inferred as rejection.',
                         'Process SIGKILL is not power-loss or storage-failure reception.',
                         'Outgoing QoS 1 Packet Identifiers and finite session/epoch bounds remain.',
                         'Database free-list pages can be reusable while the physical file remains large.',
                         'Normal-build WAL observations use the ordinary SQLite checkpoint policy.']}
(out / 'index.json').write_text(json.dumps(index, indent=2, sort_keys=True) + '\n')
files = sorted(path for path in out.rglob('*') if path.is_file())
(out / 'SHA256SUMS').write_text(''.join(f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(out)}\n' for path in files))
print(json.dumps({'status': 'PASS', 'receipt': str(out / 'index.json'), 'source_head': sys.argv[2]}))
PY
tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner \
    -czf "$receipt_out.tar.gz" -C "$(dirname -- "$receipt_out")" "$(basename -- "$receipt_out")"
sha256sum "$receipt_out.tar.gz" >"$receipt_out.tar.gz.sha256"
