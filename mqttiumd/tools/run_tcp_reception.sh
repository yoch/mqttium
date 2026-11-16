#!/usr/bin/env bash
# Build the product, receive its TCP composition, and catch two precise mutants.
# This is a finite system test, not a fixture-based qualifying oracle campaign.
set -euo pipefail

if [[ $# != 1 ]]; then
    echo "usage: $0 NEW_OUTPUT_DIRECTORY" >&2
    exit 2
fi
tcp_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
tcp_repo=$(git -C "$tcp_root" rev-parse --show-toplevel)
tcp_python=${PYTHON:-python3}
tcp_output=$($tcp_python -c 'import pathlib,sys; print(pathlib.Path(sys.argv[1]).resolve())' "$1")
case "$tcp_output/" in "$tcp_repo/"*) echo "output must be outside the worktree" >&2; exit 2;; esac
mkdir -p -- "$tcp_output"
if [[ -n $(ls -A -- "$tcp_output") ]]; then
    echo "output must be empty" >&2
    exit 2
fi
if [[ -n $(git -C "$tcp_repo" status --porcelain --untracked-files=normal) ]]; then
    echo "the system receipt requires a clean source tree" >&2
    exit 2
fi
tcp_revision=$(git -C "$tcp_repo" rev-parse HEAD)
tcp_targets=${MQTTIUMD_TCP_TARGET_ROOT:-${TMPDIR:-/tmp}/mqttiumd-tcp-reception-$tcp_revision}
mkdir -p -- "$tcp_targets"
tcp_scratch=$(mktemp -d "${TMPDIR:-/tmp}/mqttiumd-tcp-mutants.XXXXXX")
tcp_mutant_worktree=
tcp_cleanup() {
    if [[ -n "$tcp_mutant_worktree" ]]; then
        git -C "$tcp_repo" worktree remove --force -- "$tcp_mutant_worktree" >/dev/null 2>&1 || true
    fi
    rmdir -- "$tcp_scratch" >/dev/null 2>&1 || true
}
trap tcp_cleanup EXIT
cd -- "$tcp_root"

"$tcp_python" - <<'PY'
from importlib.metadata import version
if version('paho-mqtt') != '2.1.0':
    raise SystemExit('install the independent client: python -m pip install -r tests/system/requirements.txt')
PY
"$tcp_python" -m unittest discover -s tests/system -p 'test_*.py' -v >"$tcp_output/pilot-tests.log" 2>&1
rustc -Vv >"$tcp_output/rustc.txt"
cargo -V >"$tcp_output/cargo.txt"
"$tcp_python" -VV >"$tcp_output/python.txt"
git rev-parse HEAD >"$tcp_output/source-head.txt"
git diff --binary HEAD >"$tcp_output/source.modification"
cargo tree -p mqttiumd --no-default-features --locked --format '{p} features={f}' >"$tcp_output/normal-feature-tree.txt"
"$tcp_python" - "$tcp_output/normal-feature-tree.txt" <<'PY'
from pathlib import Path
import sys
tree = Path(sys.argv[1]).read_text()
if 'kill-points' in tree or 'test-hooks' in tree or 'mqttiumd-lab' in tree:
    raise SystemExit('normal product dependency tree contains instrumentation or lab')
PY
cargo build -p mqttiumd --no-default-features --locked --target-dir "$tcp_targets/normal" >"$tcp_output/normal-build.log" 2>&1
cargo tree -p mqttiumd --no-default-features --features test-hooks --locked --format '{p} features={f}' >"$tcp_output/test-hooks-feature-tree.txt"
cargo build -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$tcp_targets/hooks" >"$tcp_output/hooks-build.log" 2>&1

# Trace these actual test runs after compilation. Each witness belongs to the
# corresponding receipt, binary hash, broker PID and wait status in that run.
tcp_trace=()
if command -v strace >/dev/null 2>&1; then
    tcp_trace=(strace -f -qq -s 256 -e trace=process,signal,network,flock -o)
fi
tcp_run() {
    local tcp_name=$1 tcp_binary=$2 tcp_mode=$3
    local tcp_command=("$tcp_python" "$tcp_root/tests/system/tcp_reception.py" --binary "$tcp_binary" --out "$tcp_output/$tcp_name" --mode "$tcp_mode")
    if [[ ${#tcp_trace[@]} != 0 ]]; then
        "${tcp_trace[@]}" "$tcp_output/$tcp_name.strace" "${tcp_command[@]}"
    else
        "${tcp_command[@]}"
    fi
}
tcp_run normal "$tcp_targets/normal/debug/mqttiumd" basic
tcp_run product "$tcp_targets/hooks/debug/mqttiumd" complete

for tcp_mutation in early-ack stale-generation; do
    tcp_mutant_worktree="$tcp_scratch/$tcp_mutation"
    git -C "$tcp_repo" worktree add --detach -- "$tcp_mutant_worktree" "$tcp_revision" >"$tcp_output/$tcp_mutation-worktree.log" 2>&1
    "$tcp_python" - "$tcp_mutant_worktree/mqttiumd/crates/mqttiumd/src/runtime.rs" "$tcp_mutation" <<'PY'
from pathlib import Path
import sys
source = Path(sys.argv[1])
name = {'early-ack': 'MUTANT_EARLY_ACK', 'stale-generation': 'MUTANT_ALLOW_STALE'}[sys.argv[2]]
old = f'const {name}: bool = false;'
new = f'const {name}: bool = true;'
text = source.read_text()
if text.count(old) != 1:
    raise SystemExit(f'expected exactly one mutation site: {name}')
source.write_text(text.replace(old, new))
PY
    git -C "$tcp_mutant_worktree" diff --binary >"$tcp_output/$tcp_mutation.mutant"
    cargo build --manifest-path "$tcp_mutant_worktree/mqttiumd/Cargo.toml" -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$tcp_targets/$tcp_mutation" >"$tcp_output/$tcp_mutation-build.log" 2>&1
    if [[ "$tcp_mutation" == early-ack ]]; then
        tcp_case=C
        tcp_expected=premature_pubrec
    else
        tcp_case=G-generation
        tcp_expected=stale_generation_forwarded
    fi
    tcp_status=0
    tcp_command=("$tcp_python" "$tcp_root/tests/system/tcp_reception.py" --binary "$tcp_targets/$tcp_mutation/debug/mqttiumd" --out "$tcp_output/$tcp_mutation" --case "$tcp_case")
    if [[ ${#tcp_trace[@]} != 0 ]]; then
        "${tcp_trace[@]}" "$tcp_output/$tcp_mutation.strace" "${tcp_command[@]}" || tcp_status=$?
    else
        "${tcp_command[@]}" || tcp_status=$?
    fi
    "$tcp_python" - "$tcp_output/$tcp_mutation/receipt.json" "$tcp_expected" "$tcp_status" <<'PY'
from pathlib import Path
import json, sys
result = json.loads(Path(sys.argv[1]).read_text())
if int(sys.argv[3]) != 1 or result['status'] != 'FAIL' or result.get('failure', {}).get('code') != sys.argv[2]:
    raise SystemExit(f"mutation was not caught for its precise reason: {result.get('failure')}")
PY
    git -C "$tcp_repo" worktree remove --force -- "$tcp_mutant_worktree"
    tcp_mutant_worktree=
done
"$tcp_python" - "$tcp_output" "$tcp_revision" "${#tcp_trace[@]}" <<'PY'
from pathlib import Path
from importlib.metadata import version
import hashlib, json, sys
out = Path(sys.argv[1])
receipts = {name: json.loads((out / name / 'receipt.json').read_text())
            for name in ('normal', 'product', 'early-ack', 'stale-generation')}
summary = {'format': 'mqttiumd.tcp-system-reception-index', 'schema_version': 1,
           'status': 'PASS', 'qualification': 'system-test-only', 'source_head': sys.argv[2],
           'independent_client': {'name': 'paho-mqtt', 'version': version('paho-mqtt')},
           'system_witnesses': 'same-run-strace' if int(sys.argv[3]) else 'process-wait-status-only',
           'receipts': {name: {'path': f'{name}/receipt.json', 'status': receipt['status'],
                              'binary': receipt['binary'], 'failure': receipt.get('failure')}
                        for name, receipt in receipts.items()},
           'cases': receipts['product']['cases'],
           'limitations': ['No subscriber delivery, QoS 1 or complete MQTT conformance claim.',
                           'Process kills do not establish storage or power-loss guarantees.',
                           'Finite system tests are distinct from qualifying fixture-based oracle campaigns.']}
(out / 'index.json').write_text(json.dumps(summary, indent=2, sort_keys=True) + '\n')
files = sorted(path for path in out.rglob('*') if path.is_file())
(out / 'SHA256SUMS').write_text(''.join(f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(out)}\n' for path in files))
print(json.dumps({'status': 'PASS', 'source_head': sys.argv[2], 'receipt': str(out / 'index.json')}))
PY
