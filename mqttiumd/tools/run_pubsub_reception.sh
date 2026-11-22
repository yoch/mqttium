#!/usr/bin/env bash
# Bounded product pub/sub system receipt, distinct from fixture qualification.
set -euo pipefail

if [[ $# != 1 ]]; then
    echo "usage: $0 NEW_OUTPUT_DIRECTORY" >&2
    exit 2
fi
ps_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
ps_repo=$(git -C "$ps_root" rev-parse --show-toplevel)
ps_python=${PYTHON:-python3}
ps_out=$("$ps_python" -c 'from pathlib import Path; import sys; print(Path(sys.argv[1]).resolve())' "$1")
case "$ps_out/" in "$ps_repo/"*) echo "output must be outside the worktree" >&2; exit 2;; esac
mkdir -p -- "$ps_out"
[[ -z $(ls -A -- "$ps_out") ]] || { echo "output must be empty" >&2; exit 2; }
if [[ -n $(git -C "$ps_repo" status --porcelain --untracked-files=normal) ]]; then
    echo "the system receipt requires a clean source tree" >&2
    exit 2
fi
ps_sha=$(git -C "$ps_repo" rev-parse HEAD)
ps_targets=${MQTTIUMD_PUBSUB_TARGET_ROOT:-${TMPDIR:-/tmp}/mqttiumd-pubsub-$ps_sha}
ps_scratch=$(mktemp -d "${TMPDIR:-/tmp}/mqttiumd-pubsub-mutants.XXXXXX")
ps_worktree=
ps_cleanup() {
    if [[ -n "$ps_worktree" ]]; then
        git -C "$ps_repo" worktree remove --force -- "$ps_worktree" >/dev/null 2>&1 || true
    fi
    rmdir -- "$ps_scratch" >/dev/null 2>&1 || true
}
trap ps_cleanup EXIT
mkdir -p -- "$ps_targets" "$ps_out/binaries"
cd -- "$ps_root"
ps_record() {
    local ps_kind=$1
    shift
    "$ps_python" - "$ps_out/commands.jsonl" "$ps_kind" "$PWD" "$@" <<'PY'
from pathlib import Path
import json, sys
with Path(sys.argv[1]).open('a') as target:
    target.write(json.dumps({'kind': sys.argv[2], 'cwd': sys.argv[3], 'command': sys.argv[4:]}) + '\n')
PY
}
"$ps_python" - <<'PY'
from importlib.metadata import version
if version('paho-mqtt') != '2.1.0':
    raise SystemExit('install tests/system/requirements.txt with the selected interpreter')
PY
"$ps_python" -m unittest discover -s tests/system -p 'test_*.py' -v >"$ps_out/pilot-tests.log" 2>&1
rustc -Vv >"$ps_out/rustc.txt"
cargo -V >"$ps_out/cargo.txt"
"$ps_python" -VV >"$ps_out/python.txt"
git rev-parse HEAD >"$ps_out/source-head.txt"
git diff --binary HEAD >"$ps_out/source.modification"
# Preserve the client checkout as well as the broker: the public mqttium
# driver imports src/mqttium from this exact repository revision.
ps_record source-archive git -C "$ps_repo" archive --format=tar "$ps_sha"
git -C "$ps_repo" archive --format=tar "$ps_sha" >"$ps_out/source.tar"
for ps_mode in normal hooks; do
    ps_features=()
    if [[ "$ps_mode" == hooks ]]; then ps_features=(--features test-hooks); fi
    cargo tree -p mqttiumd --no-default-features "${ps_features[@]}" --locked --format '{p} features={f}' >"$ps_out/$ps_mode-features.txt"
    ps_record "$ps_mode-build" cargo build -p mqttiumd --no-default-features "${ps_features[@]}" --locked --target-dir "$ps_targets/$ps_mode"
    cargo build -p mqttiumd --no-default-features "${ps_features[@]}" --locked --target-dir "$ps_targets/$ps_mode" >"$ps_out/$ps_mode-build.log" 2>&1
    cp -- "$ps_targets/$ps_mode/debug/mqttiumd" "$ps_out/binaries/mqttiumd-$ps_mode"
done
"$ps_python" - "$ps_out/normal-features.txt" <<'PY'
from pathlib import Path
import sys
tree = Path(sys.argv[1]).read_text()
if any(name in tree for name in ('mqttiumd-lab', 'test-hooks', 'kill-points')):
    raise SystemExit('normal product unexpectedly contains instrumentation or lab')
PY
ps_trace=()
if command -v strace >/dev/null 2>&1; then
    ps_trace=(strace -f -qq -s 256 -e trace=process,signal,network,flock -o)
fi
ps_run() {
    local ps_name=$1 ps_binary=$2
    shift 2
    local ps_command=("$ps_python" "$ps_root/tests/system/pubsub_reception.py" --binary "$ps_binary" --out "$ps_out/$ps_name" "$@")
    ps_record "$ps_name-reception" "${ps_command[@]}"
    if [[ ${#ps_trace[@]} != 0 ]]; then
        "${ps_trace[@]}" "$ps_out/$ps_name.strace" "${ps_command[@]}"
    else
        "${ps_command[@]}"
    fi
}
ps_run normal "$ps_out/binaries/mqttiumd-normal" --mode normal
ps_run product "$ps_out/binaries/mqttiumd-hooks" --mode complete

for ps_mutation in early-outgoing early-finish notifications-only; do
    ps_worktree="$ps_scratch/$ps_mutation"
    git -C "$ps_repo" worktree add --detach -- "$ps_worktree" "$ps_sha" >"$ps_out/$ps_mutation-worktree.log" 2>&1
    "$ps_python" - "$ps_worktree/mqttiumd/crates/mqttiumd/src/runtime.rs" "$ps_mutation" <<'PY'
from pathlib import Path
import sys
source = Path(sys.argv[1])
name = {'early-outgoing': 'MUTANT_EARLY_OUTGOING', 'early-finish': 'MUTANT_EARLY_FINISH',
        'notifications-only': 'MUTANT_NOTIFICATIONS_ONLY'}[sys.argv[2]]
old = f'const {name}: bool = false;'
text = source.read_text()
if text.count(old) != 1:
    raise SystemExit(f'exactly one mutation site required: {name}')
source.write_text(text.replace(old, f'const {name}: bool = true;'))
PY
    git -C "$ps_worktree" diff --binary >"$ps_out/$ps_mutation.mutant"
    ps_record "$ps_mutation-build" cargo build --manifest-path "$ps_worktree/mqttiumd/Cargo.toml" -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$ps_targets/$ps_mutation"
    cargo build --manifest-path "$ps_worktree/mqttiumd/Cargo.toml" -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$ps_targets/$ps_mutation" >"$ps_out/$ps_mutation-build.log" 2>&1
    cp -- "$ps_targets/$ps_mutation/debug/mqttiumd" "$ps_out/binaries/mqttiumd-$ps_mutation"
    case "$ps_mutation" in
        early-outgoing) ps_case=D-claim; ps_expected=premature_outgoing_publish;;
        early-finish) ps_case=D-finish; ps_expected=premature_delivery_release;;
        notifications-only) ps_case=H-discovery; ps_expected=durable_work_lost_without_notification;;
    esac
    ps_status=0
    ps_run "$ps_mutation" "$ps_out/binaries/mqttiumd-$ps_mutation" --case "$ps_case" || ps_status=$?
    "$ps_python" - "$ps_out/$ps_mutation/receipt.json" "$ps_expected" "$ps_status" <<'PY'
from pathlib import Path
import json, sys
result = json.loads(Path(sys.argv[1]).read_text())
if int(sys.argv[3]) != 1 or result['status'] != 'FAIL' or result.get('failure', {}).get('code') != sys.argv[2]:
    raise SystemExit(f"mutation not caught for its precise reason: {result.get('failure')}")
PY
    git -C "$ps_repo" worktree remove --force -- "$ps_worktree"
    ps_worktree=
done
"$ps_python" - "$ps_out" "$ps_sha" "${#ps_trace[@]}" <<'PY'
from pathlib import Path
import hashlib, json, sys
out = Path(sys.argv[1])
names = ('normal', 'product', 'early-outgoing', 'early-finish', 'notifications-only')
receipts = {name: json.loads((out / name / 'receipt.json').read_text()) for name in names}
for receipt in receipts.values():
    binary = receipt['binary']
    if hashlib.sha256(Path(binary['path']).read_bytes()).hexdigest() != binary['sha256']:
        raise SystemExit('executed and retained binary differ')
    binary['retained_path'] = str(Path(binary['path']).relative_to(out))
index = {'format': 'mqttiumd.pubsub-system-reception-index', 'schema_version': 1,
         'status': 'PASS', 'qualification': 'system-test-only', 'source_head': sys.argv[2],
         'source_archive': {'path': 'source.tar', 'sha256': hashlib.sha256((out / 'source.tar').read_bytes()).hexdigest()},
         'commands': 'commands.jsonl',
         'system_witnesses': 'same-run-strace' if int(sys.argv[3]) else 'process-wait-status-only',
         'cases': {name: receipt['cases'] for name, receipt in receipts.items()},
         'receipts': {name: {'path': f'{name}/receipt.json', 'status': receipt['status'],
                             'binary': receipt['binary'], 'failure': receipt.get('failure')}
                      for name, receipt in receipts.items()},
         'limitations': ['Finite system reception does not qualify all P1/P2 or MQTT conformance.',
                         'QoS 1 incoming retransmission can create another admission; outgoing replay preserves its delivery.',
                         'Process SIGKILL is not power-loss or storage-failure reception.',
                         'No QoS 0/outgoing QoS 2, wildcard/retained/shared subscriptions, Will/authentication or finite expiry.']}
(out / 'index.json').write_text(json.dumps(index, indent=2, sort_keys=True) + '\n')
files = sorted(path for path in out.rglob('*') if path.is_file())
(out / 'SHA256SUMS').write_text(''.join(f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(out)}\n'
                                     for path in files))
print(json.dumps({'status': 'PASS', 'receipt': str(out / 'index.json'), 'source_head': sys.argv[2]}))
PY
tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner \
    -czf "$ps_out.tar.gz" -C "$(dirname -- "$ps_out")" "$(basename -- "$ps_out")"
sha256sum "$ps_out.tar.gz" >"$ps_out.tar.gz.sha256"
