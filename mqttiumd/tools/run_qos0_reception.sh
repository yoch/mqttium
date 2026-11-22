#!/usr/bin/env bash
# Bounded product QoS 0 system receipt, distinct from fixture qualification.
set -euo pipefail

if [[ $# != 1 ]]; then
    echo "usage: $0 NEW_OUTPUT_DIRECTORY" >&2
    exit 2
fi
q0_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
q0_repo=$(git -C "$q0_root" rev-parse --show-toplevel)
q0_python=${PYTHON:-python3}
q0_out=$("$q0_python" -c 'from pathlib import Path; import sys; print(Path(sys.argv[1]).resolve())' "$1")
case "$q0_out/" in "$q0_repo/"*) echo "output must be outside the worktree" >&2; exit 2;; esac
mkdir -p -- "$q0_out"
[[ -z $(ls -A -- "$q0_out") ]] || { echo "output must be empty" >&2; exit 2; }
if [[ -n $(git -C "$q0_repo" status --porcelain --untracked-files=normal) ]]; then
    echo "the system receipt requires a clean source tree" >&2
    exit 2
fi
q0_sha=$(git -C "$q0_repo" rev-parse HEAD)
q0_targets=${MQTTIUMD_QOS0_TARGET_ROOT:-${TMPDIR:-/tmp}/mqttiumd-qos0-$q0_sha}
q0_scratch=$(mktemp -d "${TMPDIR:-/tmp}/mqttiumd-qos0-mutants.XXXXXX")
q0_worktree=
q0_cleanup() {
    if [[ -n "$q0_worktree" ]]; then
        git -C "$q0_repo" worktree remove --force -- "$q0_worktree" >/dev/null 2>&1 || true
    fi
    rmdir -- "$q0_scratch" >/dev/null 2>&1 || true
}
trap q0_cleanup EXIT
mkdir -p -- "$q0_targets" "$q0_out/binaries"
cd -- "$q0_root"
q0_record() {
    local q0_kind=$1
    shift
    "$q0_python" - "$q0_out/commands.jsonl" "$q0_kind" "$PWD" "$@" <<'PY'
from pathlib import Path
import json, sys
with Path(sys.argv[1]).open('a') as target:
    target.write(json.dumps({'kind': sys.argv[2], 'cwd': sys.argv[3], 'command': sys.argv[4:]}) + '\n')
PY
}
"$q0_python" - <<'PY'
from importlib.metadata import version
if version('paho-mqtt') != '2.1.0':
    raise SystemExit('install tests/system/requirements.txt with the selected interpreter')
PY
"$q0_python" -m unittest discover -s tests/system -p 'test_*.py' -v >"$q0_out/pilot-tests.log" 2>&1
rustc -Vv >"$q0_out/rustc.txt"
cargo -V >"$q0_out/cargo.txt"
"$q0_python" -VV >"$q0_out/python.txt"
git rev-parse HEAD >"$q0_out/source-head.txt"
git diff --binary HEAD >"$q0_out/source.modification"
# Preserve the client checkout as well as the broker: the public mqttium
# driver imports src/mqttium from this exact repository revision.
q0_record source-archive git -C "$q0_repo" archive --format=tar "$q0_sha"
git -C "$q0_repo" archive --format=tar "$q0_sha" >"$q0_out/source.tar"
for q0_mode in normal hooks; do
    q0_features=()
    if [[ "$q0_mode" == hooks ]]; then q0_features=(--features test-hooks); fi
    cargo tree -p mqttiumd --no-default-features "${q0_features[@]}" --locked --format '{p} features={f}' >"$q0_out/$q0_mode-features.txt"
    q0_record "$q0_mode-build" cargo build -p mqttiumd --no-default-features "${q0_features[@]}" --locked --target-dir "$q0_targets/$q0_mode"
    cargo build -p mqttiumd --no-default-features "${q0_features[@]}" --locked --target-dir "$q0_targets/$q0_mode" >"$q0_out/$q0_mode-build.log" 2>&1
    cp -- "$q0_targets/$q0_mode/debug/mqttiumd" "$q0_out/binaries/mqttiumd-$q0_mode"
done
"$q0_python" - "$q0_out/normal-features.txt" <<'PY'
from pathlib import Path
import sys
tree = Path(sys.argv[1]).read_text()
if any(name in tree for name in ('mqttiumd-lab', 'test-hooks', 'kill-points')):
    raise SystemExit('normal product unexpectedly contains instrumentation or lab')
PY
q0_trace=()
if command -v strace >/dev/null 2>&1; then
    q0_trace=(strace -f -qq -s 256 -e trace=process,signal,network,flock -o)
fi
q0_run() {
    local q0_name=$1 q0_binary=$2
    shift 2
    local q0_command=("$q0_python" "$q0_root/tests/system/qos0_reception.py" --binary "$q0_binary" --out "$q0_out/$q0_name" "$@")
    q0_record "$q0_name-reception" "${q0_command[@]}"
    if [[ ${#q0_trace[@]} != 0 ]]; then
        "${q0_trace[@]}" "$q0_out/$q0_name.strace" "${q0_command[@]}"
    else
        "${q0_command[@]}"
    fi
}
q0_ready=("$q0_python" "$q0_root/tests/system/ready_backlog.py" --binary "$q0_out/binaries/mqttiumd-normal" --out "$q0_out/ready-backlog" --binary-source-head "$q0_sha")
q0_record ready-backlog-witness "${q0_ready[@]}"
if [[ ${#q0_trace[@]} != 0 ]]; then
    "${q0_trace[@]}" "$q0_out/ready-backlog.strace" "${q0_ready[@]}" >"$q0_out/ready-backlog.log"
else
    "${q0_ready[@]}" >"$q0_out/ready-backlog.log"
fi
cp -- "$q0_out/ready-backlog/result.json" "$q0_out/ready-backlog/receipt.json"
q0_run normal "$q0_out/binaries/mqttiumd-normal" --mode normal
q0_run product "$q0_out/binaries/mqttiumd-hooks" --mode complete

for q0_mutation in early-qos0 replay-qos0 qos0-accounting; do
    q0_worktree="$q0_scratch/$q0_mutation"
    git -C "$q0_repo" worktree add --detach -- "$q0_worktree" "$q0_sha" >"$q0_out/$q0_mutation-worktree.log" 2>&1
    "$q0_python" - "$q0_worktree/mqttiumd/crates/mqttiumd/src/runtime.rs" "$q0_mutation" <<'PY'
from pathlib import Path
import sys
source = Path(sys.argv[1])
name = {'early-qos0': 'MUTANT_EARLY_QOS0', 'replay-qos0': 'MUTANT_REPLAY_QOS0',
        'qos0-accounting': 'MUTANT_QOS0_ACCOUNTING'}[sys.argv[2]]
old = f'const {name}: bool = false;'
text = source.read_text()
if text.count(old) != 1:
    raise SystemExit(f'exactly one mutation site required: {name}')
source.write_text(text.replace(old, f'const {name}: bool = true;'))
PY
    git -C "$q0_worktree" diff --binary >"$q0_out/$q0_mutation.mutant"
    q0_record "$q0_mutation-build" cargo build --manifest-path "$q0_worktree/mqttiumd/Cargo.toml" -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$q0_targets/$q0_mutation"
    cargo build --manifest-path "$q0_worktree/mqttiumd/Cargo.toml" -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$q0_targets/$q0_mutation" >"$q0_out/$q0_mutation-build.log" 2>&1
    cp -- "$q0_targets/$q0_mutation/debug/mqttiumd" "$q0_out/binaries/mqttiumd-$q0_mutation"
    case "$q0_mutation" in
        early-qos0) q0_case=D-consume; q0_expected=premature_qos0_publish;;
        replay-qos0) q0_case=E-after; q0_expected=consumed_qos0_reemitted;;
        qos0-accounting) q0_case=B; q0_expected=qos0_entered_qos1_accounting;;
    esac
    q0_status=0
    q0_run "$q0_mutation" "$q0_out/binaries/mqttiumd-$q0_mutation" --case "$q0_case" || q0_status=$?
    "$q0_python" - "$q0_out/$q0_mutation/receipt.json" "$q0_expected" "$q0_status" <<'PY'
from pathlib import Path
import json, sys
result = json.loads(Path(sys.argv[1]).read_text())
if int(sys.argv[3]) != 1 or result['status'] != 'FAIL' or result.get('failure', {}).get('code') != sys.argv[2]:
    raise SystemExit(f"mutation not caught for its precise reason: {result.get('failure')}")
PY
    git -C "$q0_repo" worktree remove --force -- "$q0_worktree"
    q0_worktree=
done
"$q0_python" - "$q0_out" "$q0_sha" "${#q0_trace[@]}" <<'PY'
from pathlib import Path
import hashlib, json, sys
out = Path(sys.argv[1])
names = ('normal', 'product', 'early-qos0', 'replay-qos0', 'qos0-accounting')
receipts = {name: json.loads((out / name / 'receipt.json').read_text()) for name in names}
ready = json.loads((out / 'ready-backlog/receipt.json').read_text())
if (ready['status'] != 'PASS' or ready['source_head'] != sys.argv[2]
        or ready['binary']['sha256'] != receipts['normal']['binary']['sha256']):
    raise SystemExit('ready-backlog witness must use the same normal binary and source')
for receipt in receipts.values():
    binary = receipt['binary']
    if hashlib.sha256(Path(binary['path']).read_bytes()).hexdigest() != binary['sha256']:
        raise SystemExit('executed and retained binary differ')
    binary['retained_path'] = str(Path(binary['path']).relative_to(out))
index = {'format': 'mqttiumd.qos0-system-reception-index', 'schema_version': 1,
         'status': 'PASS', 'qualification': 'system-test-only', 'source_head': sys.argv[2],
         'source_archive': {'path': 'source.tar', 'sha256': hashlib.sha256((out / 'source.tar').read_bytes()).hexdigest()},
         'commands': 'commands.jsonl',
         'ready_backlog': {'path': 'ready-backlog/receipt.json', 'status': ready['status'],
                           'messages': ready['messages'], 'receive_maximum': ready['receive_maximum'],
                           'drain_since_connack_ms': ready['drain_since_connack_ms'],
                           'scope': ready['scope']},
         'system_witnesses': 'same-run-strace' if int(sys.argv[3]) else 'process-wait-status-only',
         'cases': {name: receipt['cases'] for name, receipt in receipts.items()},
         'receipts': {name: {'path': f'{name}/receipt.json', 'status': receipt['status'],
                             'binary': receipt['binary'], 'failure': receipt.get('failure')}
                      for name, receipt in receipts.items()},
         'limitations': ['Finite system reception does not qualify all P1/P2 or MQTT conformance.',
                         'QoS 1 incoming retransmission can create another admission; outgoing replay preserves its delivery.',
                         'Process SIGKILL is not power-loss or storage-failure reception.',
                         'No outgoing QoS 2, wildcard/retained/shared subscriptions, Will/authentication or finite expiry.',
                         'Initially QoS 0 is volatile and not retained offline; durable-origin QoS 0 is consumed before emission and may be lost.',
                         'A public QoS 0 write receipt alone proves neither broker reception nor subscriber reception.']}
(out / 'index.json').write_text(json.dumps(index, indent=2, sort_keys=True) + '\n')
files = sorted(path for path in out.rglob('*') if path.is_file())
(out / 'SHA256SUMS').write_text(''.join(f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(out)}\n'
                                     for path in files))
print(json.dumps({'status': 'PASS', 'receipt': str(out / 'index.json'), 'source_head': sys.argv[2]}))
PY
tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner \
    -czf "$q0_out.tar.gz" -C "$(dirname -- "$q0_out")" "$(basename -- "$q0_out")"
sha256sum "$q0_out.tar.gz" >"$q0_out.tar.gz.sha256"
