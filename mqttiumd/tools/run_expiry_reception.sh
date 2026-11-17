#!/usr/bin/env bash
# Source-pinned finite session expiry system reception; no fixture qualification.
set -euo pipefail
if [[ $# != 1 ]]; then echo "usage: $0 NEW_OUTPUT_DIRECTORY" >&2; exit 2; fi
expiry_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
expiry_repo=$(git -C "$expiry_root" rev-parse --show-toplevel)
expiry_python=${PYTHON:-python3}
expiry_out=$("$expiry_python" -c 'from pathlib import Path; import sys; print(Path(sys.argv[1]).resolve())' "$1")
case "$expiry_out/" in "$expiry_repo/"*) echo "output must be outside the checkout" >&2; exit 2;; esac
if [[ -z ${MQTTIUMD_EXPIRY_PIN_SOURCE:-} ]]; then
    [[ -z $(git -C "$expiry_repo" status --porcelain --untracked-files=normal) ]] || {
        echo "reception requires a clean source tree" >&2; exit 2;
    }
    expiry_pin_sha=$(git -C "$expiry_repo" rev-parse HEAD)
    expiry_pin_parent=$(mktemp -d "${TMPDIR:-/tmp}/mqttiumd-expiry-source.XXXXXX")
    expiry_pin_cleanup() {
        git -C "$expiry_repo" worktree remove --force -- "$expiry_pin_parent/source" >/dev/null 2>&1 || true
        rm -f -- "$expiry_pin_parent/source-pin.log"
        rmdir -- "$expiry_pin_parent" >/dev/null 2>&1 || true
    }
    trap expiry_pin_cleanup EXIT
    git -C "$expiry_repo" worktree add --detach -- "$expiry_pin_parent/source" "$expiry_pin_sha" >"$expiry_pin_parent/source-pin.log" 2>&1
    expiry_pin_source=$(git -C "$expiry_pin_parent/source" rev-parse --show-toplevel)
    expiry_pin_status=0
    MQTTIUMD_EXPIRY_PIN_SOURCE="$expiry_pin_source" MQTTIUMD_EXPIRY_PIN_SHA="$expiry_pin_sha" \
        MQTTIUMD_EXPIRY_CALLER_SOURCE="$expiry_repo" \
        "$expiry_pin_source/mqttiumd/tools/run_expiry_reception.sh" "$expiry_out" || expiry_pin_status=$?
    exit "$expiry_pin_status"
fi
[[ "$expiry_repo" == "$MQTTIUMD_EXPIRY_PIN_SOURCE" && $(git -C "$expiry_repo" rev-parse HEAD) == "$MQTTIUMD_EXPIRY_PIN_SHA" ]] || {
    echo "invalid expiry source pin" >&2; exit 2;
}
mkdir -p -- "$expiry_out"
[[ -z $(ls -A -- "$expiry_out") ]] || { echo "output must be empty" >&2; exit 2; }
[[ -z $(git -C "$expiry_repo" status --porcelain --untracked-files=normal) ]] || {
    echo "reception requires a clean pinned source" >&2; exit 2;
}
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-1}
expiry_sha=$(git -C "$expiry_repo" rev-parse HEAD)
expiry_targets=${MQTTIUMD_EXPIRY_TARGET_ROOT:-${TMPDIR:-/tmp}/mqttiumd-expiry-$expiry_sha}
mkdir -p -- "$expiry_targets" "$expiry_out/binaries"
cd -- "$expiry_root"
expiry_record() {
    local expiry_kind=$1
    shift
    "$expiry_python" - "$expiry_out/commands.jsonl" "$expiry_kind" "$PWD" "$@" <<'PY'
from pathlib import Path
import json, sys
with Path(sys.argv[1]).open('a') as stream:
    stream.write(json.dumps({'kind': sys.argv[2], 'cwd': sys.argv[3], 'command': sys.argv[4:]}) + '\n')
PY
}
expiry_record source-pin git -C "$MQTTIUMD_EXPIRY_CALLER_SOURCE" worktree add --detach "$expiry_repo" "$expiry_sha"
"$expiry_python" - "$expiry_out/source-pin.json" "$MQTTIUMD_EXPIRY_CALLER_SOURCE" "$expiry_repo" "$expiry_sha" <<'PY'
from pathlib import Path
import json, sys
Path(sys.argv[1]).write_text(json.dumps({'caller_source': sys.argv[2], 'captured_caller_head': sys.argv[4],
    'pinned_source': sys.argv[3], 'pinned_head': sys.argv[4],
    'assumption': 'this invocation exclusively owns its private detached source tree'}, indent=2) + '\n')
PY
"$expiry_python" - "$expiry_out" <<'PY'
from importlib.metadata import version
from pathlib import Path
import json, os, shutil, sys
if version('paho-mqtt') != '2.1.0': raise SystemExit('paho-mqtt2.1.0 required')
out=Path(sys.argv[1]); quota=Path('/sys/fs/cgroup/cpu.max')
(out/'environment.json').write_text(json.dumps({'CARGO_BUILD_JOBS':os.environ['CARGO_BUILD_JOBS'],
    'cpu_max':quota.read_text().strip() if quota.exists() else None,
    'free_bytes_at_start':shutil.disk_usage(out).free,
    'session_quota':128,'receipt_slots':8192,'rearms':64,'capacity_ttl_seconds':30},indent=2)+'\n')
PY
rustc -Vv >"$expiry_out/rustc.txt"
cargo -V >"$expiry_out/cargo.txt"
"$expiry_python" -VV >"$expiry_out/python.txt"
git rev-parse HEAD >"$expiry_out/source-head.txt"
git diff --binary HEAD >"$expiry_out/source.modification"
expiry_record source-archive git -C "$expiry_repo" archive --format=tar "$expiry_sha"
git -C "$expiry_repo" archive --format=tar "$expiry_sha" >"$expiry_out/source.tar"
expiry_record pilot-tests "$expiry_python" -m unittest discover -s tests/system -p 'test_expiry_reception.py' -v
"$expiry_python" -m unittest discover -s tests/system -p 'test_expiry_reception.py' -v >"$expiry_out/pilot-tests.log" 2>&1
cargo tree -p mqttiumd --no-default-features --locked --format '{p} features={f}' >"$expiry_out/normal-features.txt"
expiry_record normal-build cargo build -p mqttiumd --no-default-features --locked --target-dir "$expiry_targets/normal"
cargo build -p mqttiumd --no-default-features --locked --target-dir "$expiry_targets/normal" >"$expiry_out/normal-build.log" 2>&1
cp -- "$expiry_targets/normal/debug/mqttiumd" "$expiry_out/binaries/mqttiumd-normal"
cargo tree -p mqttiumd --no-default-features --features test-hooks --locked --format '{p} features={f}' >"$expiry_out/hooks-features.txt"
expiry_record hooks-build cargo build -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$expiry_targets/hooks"
cargo build -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$expiry_targets/hooks" >"$expiry_out/hooks-build.log" 2>&1
cp -- "$expiry_targets/hooks/debug/mqttiumd" "$expiry_out/binaries/mqttiumd-hooks"
"$expiry_python" - "$expiry_out/normal-features.txt" <<'PY'
from pathlib import Path
import sys
features=Path(sys.argv[1]).read_text()
if any(name in features for name in ('mqttiumd-lab','kill-points','test-hooks')):
    raise SystemExit('normal product contains lab or instrumentation')
PY
expiry_trace=()
if command -v strace >/dev/null 2>&1; then
    expiry_trace=(strace -f -qq -s 256 -e trace=process,signal,network,flock -o)
fi
expiry_run() {
    local expiry_name=$1 expiry_case=$2 expiry_binary=$3
    local expiry_command=("$expiry_python" "$expiry_root/tests/system/expiry_reception.py" --binary "$expiry_binary" --out "$expiry_out/$expiry_name" --binary-source-head "$expiry_sha" --case "$expiry_case")
    expiry_record "$expiry_name-reception" "${expiry_command[@]}"
    if [[ ${#expiry_trace[@]} != 0 ]]; then
        "${expiry_trace[@]}" "$expiry_out/$expiry_name.strace" "${expiry_command[@]}" >"$expiry_out/$expiry_name.log" 2>&1
    else
        "${expiry_command[@]}" >"$expiry_out/$expiry_name.log" 2>&1
    fi
}
expiry_run normal normal "$expiry_out/binaries/mqttiumd-normal"
expiry_run faults faults "$expiry_out/binaries/mqttiumd-hooks"
"$expiry_python" - "$expiry_out" "$expiry_sha" "${#expiry_trace[@]}" <<'PY'
from pathlib import Path
import hashlib,json,sys
out=Path(sys.argv[1]); normal=json.loads((out/'normal/receipt.json').read_text())
faults=json.loads((out/'faults/receipt.json').read_text())
for name,receipt in [('normal',normal),('faults',faults)]:
    if receipt['status']!='PASS' or receipt['source_head']!=sys.argv[2] or receipt['harness_source_head']!=sys.argv[2]:
        raise SystemExit(f'{name} expiry source/verdict differs')
    if hashlib.sha256(Path(receipt['binary']['path']).read_bytes()).hexdigest()!=receipt['binary']['sha256']:
        raise SystemExit(f'{name} retained and executed binary differs')
criteria={row['criterion']:row for row in normal['cases']}
if set(criteria)!={'B','E','F'}: raise SystemExit('public/responsibility/capacity criteria missing')
if criteria['F']['quota']!=128 or criteria['F']['rearms']!=64:
    raise SystemExit('capacity quota or finite rearm budget differs')
controlled={row['criterion']:row for row in faults['cases']}
if set(controlled)!={'D-unknown','D-crashes'}: raise SystemExit('uncertainty/crash criteria missing')
crashes=controlled['D-crashes']['cases']
expected={('detach','prepared'),('detach','committed'),('expire','prepared'),('expire','committed'),
          ('attached','process-crash')}
if ({(row['action'],row['phase']) for row in crashes}!=expected
        or not all(row['wait_status']==-9 and row['stop_status']==0 for row in crashes)):
    raise SystemExit('each lifecycle boundary needs its own real SIGKILL and clean restart')
index={'format':'mqttiumd.finite-expiry-system-reception-index','schema_version':1,
       'status':'PASS','qualification':'system-test-only','source_head':sys.argv[2],
       'scope':['B-public-finite-paths','D-authentic-unknown-and-five-product-cuts',
                'E-shared-responsibilities','F-full-capacity-and-bounded-rearms'],
       'source_pin':json.loads((out/'source-pin.json').read_text()),
       'source_archive':{'path':'source.tar','sha256':hashlib.sha256((out/'source.tar').read_bytes()).hexdigest()},
       'system_witnesses':'same-run-strace' if int(sys.argv[3]) else 'process-status-only',
       'normal':{'receipt':'normal/receipt.json','binary':normal['binary']},
       'faults':{'receipt':'faults/receipt.json','binary':faults['binary']},
       'limitations':['Finite system evidence, not independent-oracle qualification.',
                      'Deterministic clock/domain/mutation/model results are separate receipts.',
                      'Local DISCONNECT completion alone is not a broker detachment fact.',
                      'Unrecognized clock domains conservatively omit unknown downtime.']}
(out/'index.json').write_text(json.dumps(index,indent=2,sort_keys=True)+'\n')
files=sorted(path for path in out.rglob('*') if path.is_file())
(out/'SHA256SUMS').write_text(''.join(f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(out)}\n' for path in files))
print(json.dumps({'status':'PASS','scope':'finite-session-expiry-system','source_head':sys.argv[2],'index':str(out/'index.json')}))
PY
tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner \
    -czf "$expiry_out.tar.gz" -C "$(dirname -- "$expiry_out")" "$(basename -- "$expiry_out")"
sha256sum "$expiry_out.tar.gz" >"$expiry_out.tar.gz.sha256"
