#!/usr/bin/env bash
# Finite identified session-lifecycle system reception. No fixture qualification.
set -euo pipefail
if [[ $# != 1 ]]; then echo "usage: $0 NEW_OUTPUT_DIRECTORY" >&2; exit 2; fi
session_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
session_repo=$(git -C "$session_root" rev-parse --show-toplevel)
session_python=${PYTHON:-python3}
session_out=$("$session_python" -c 'from pathlib import Path; import sys; print(Path(sys.argv[1]).resolve())' "$1")
case "$session_out/" in "$session_repo/"*) echo "output must be outside the checkout" >&2; exit 2;; esac
if [[ -z ${MQTTIUMD_SESSION_PIN_SOURCE:-} ]]; then
    [[ -z $(git -C "$session_repo" status --porcelain --untracked-files=normal) ]] || {
        echo "reception requires a clean source tree" >&2; exit 2;
    }
    session_pin_sha=$(git -C "$session_repo" rev-parse HEAD)
    session_pin_parent=$(mktemp -d "${TMPDIR:-/tmp}/mqttiumd-session-source.XXXXXX")
    session_pin_cleanup() {
        git -C "$session_repo" worktree remove --force -- "$session_pin_parent/source" >/dev/null 2>&1 || true
        rm -f -- "$session_pin_parent/source-pin.log"
        rmdir -- "$session_pin_parent" >/dev/null 2>&1 || true
    }
    trap session_pin_cleanup EXIT
    git -C "$session_repo" worktree add --detach -- "$session_pin_parent/source" "$session_pin_sha" >"$session_pin_parent/source-pin.log" 2>&1
    session_pin_source=$(git -C "$session_pin_parent/source" rev-parse --show-toplevel)
    session_pin_status=0
    MQTTIUMD_SESSION_PIN_SOURCE="$session_pin_source" MQTTIUMD_SESSION_PIN_SHA="$session_pin_sha" \
        MQTTIUMD_SESSION_CALLER_SOURCE="$session_repo" \
        "$session_pin_source/mqttiumd/tools/run_session_reception.sh" "$session_out" || session_pin_status=$?
    exit "$session_pin_status"
fi
[[ "$session_repo" == "$MQTTIUMD_SESSION_PIN_SOURCE" && $(git -C "$session_repo" rev-parse HEAD) == "$MQTTIUMD_SESSION_PIN_SHA" ]] || {
    echo "invalid session source pin" >&2; exit 2;
}
mkdir -p -- "$session_out"
[[ -z $(ls -A -- "$session_out") ]] || { echo "output must be empty" >&2; exit 2; }
[[ -z $(git -C "$session_repo" status --porcelain --untracked-files=normal) ]] || {
    echo "reception requires a clean pinned source" >&2; exit 2;
}
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-1}
session_sha=$(git -C "$session_repo" rev-parse HEAD)
session_targets=${MQTTIUMD_SESSION_TARGET_ROOT:-${TMPDIR:-/tmp}/mqttiumd-session-$session_sha}
mkdir -p -- "$session_targets" "$session_out/binaries"
cd -- "$session_root"
session_record() {
    local session_kind=$1
    shift
    "$session_python" - "$session_out/commands.jsonl" "$session_kind" "$PWD" "$@" <<'PY'
from pathlib import Path
import json, sys
with Path(sys.argv[1]).open('a') as stream:
    stream.write(json.dumps({'kind': sys.argv[2], 'cwd': sys.argv[3], 'command': sys.argv[4:]}) + '\n')
PY
}
session_record source-pin git -C "$MQTTIUMD_SESSION_CALLER_SOURCE" worktree add --detach "$session_repo" "$session_sha"
"$session_python" - "$session_out/source-pin.json" "$MQTTIUMD_SESSION_CALLER_SOURCE" "$session_repo" "$session_sha" <<'PY'
from pathlib import Path
import json, sys
Path(sys.argv[1]).write_text(json.dumps({'caller_source': sys.argv[2], 'captured_caller_head': sys.argv[4],
    'pinned_source': sys.argv[3], 'pinned_head': sys.argv[4],
    'assumption': 'this invocation exclusively owns its private detached source tree'}, indent=2) + '\n')
PY
"$session_python" - "$session_out" <<'PY'
from importlib.metadata import version
from pathlib import Path
import json, os, shutil, sys
if version('paho-mqtt') != '2.1.0': raise SystemExit('paho-mqtt2.1.0 required')
out=Path(sys.argv[1]); quota=Path('/sys/fs/cgroup/cpu.max')
(out/'environment.json').write_text(json.dumps({'CARGO_BUILD_JOBS':os.environ['CARGO_BUILD_JOBS'],
    'cpu_max':quota.read_text().strip() if quota.exists() else None,
    'free_bytes_at_start':shutil.disk_usage(out).free,
    'ephemeral_cycles':512,'session_quota':128,'receipt_slots':8192},indent=2)+'\n')
PY
rustc -Vv >"$session_out/rustc.txt"
cargo -V >"$session_out/cargo.txt"
"$session_python" -VV >"$session_out/python.txt"
git rev-parse HEAD >"$session_out/source-head.txt"
git diff --binary HEAD >"$session_out/source.modification"
session_record source-archive git -C "$session_repo" archive --format=tar "$session_sha"
git -C "$session_repo" archive --format=tar "$session_sha" >"$session_out/source.tar"
session_record pilot-tests "$session_python" -m unittest discover -s tests/system -p 'test_*.py' -v
"$session_python" -m unittest discover -s tests/system -p 'test_*.py' -v >"$session_out/pilot-tests.log" 2>&1
cargo tree -p mqttiumd --no-default-features --locked --format '{p} features={f}' >"$session_out/normal-features.txt"
session_record normal-build cargo build -p mqttiumd --no-default-features --locked --target-dir "$session_targets/normal"
cargo build -p mqttiumd --no-default-features --locked --target-dir "$session_targets/normal" >"$session_out/normal-build.log" 2>&1
cp -- "$session_targets/normal/debug/mqttiumd" "$session_out/binaries/mqttiumd-normal"
cargo tree -p mqttiumd --no-default-features --features test-hooks --locked --format '{p} features={f}' >"$session_out/hooks-features.txt"
session_record hooks-build cargo build -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$session_targets/hooks"
cargo build -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$session_targets/hooks" >"$session_out/hooks-build.log" 2>&1
cp -- "$session_targets/hooks/debug/mqttiumd" "$session_out/binaries/mqttiumd-hooks"
"$session_python" - "$session_out/normal-features.txt" <<'PY'
from pathlib import Path
import sys
features=Path(sys.argv[1]).read_text()
if any(name in features for name in ('mqttiumd-lab','kill-points','test-hooks')):
    raise SystemExit('normal product contains lab or instrumentation')
PY
session_trace=()
if command -v strace >/dev/null 2>&1; then
    session_trace=(strace -f -qq -s 256 -e trace=process,signal,network,flock -o)
fi
session_run() {
    local session_name=$1 session_case=$2 session_binary=${3:-$session_out/binaries/mqttiumd-normal}
    local session_command=("$session_python" "$session_root/tests/system/session_reception.py" --binary "$session_binary" --out "$session_out/$session_name" --binary-source-head "$session_sha" --case "$session_case")
    session_record "$session_name-reception" "${session_command[@]}"
    if [[ ${#session_trace[@]} != 0 ]]; then
        "${session_trace[@]}" "$session_out/$session_name.strace" "${session_command[@]}" >"$session_out/$session_name.log" 2>&1
    else
        "${session_command[@]}" >"$session_out/$session_name.log" 2>&1
    fi
}
session_run normal nominal
"$session_python" - "$session_out" "$session_sha" "${#session_trace[@]}" <<'PY'
from pathlib import Path
import hashlib,json,sys
out=Path(sys.argv[1]);result=json.loads((out/'normal/receipt.json').read_text())
if result['status']!='PASS' or result['source_head']!=sys.argv[2] or result['harness_source_head']!=sys.argv[2]:
    raise SystemExit('normal session reception source/verdict differs')
by_criterion={item['criterion']:item for item in result['cases']}
if set(by_criterion)!=set('ABCD'):
    raise SystemExit('normal session criteria missing')
cycles=by_criterion['B']
if cycles['completed_cycles']<512 or not cycles['long_requirement_met'] or cycles['session_quota']!=128:
    raise SystemExit('512 complete sessions with unchanged quota required')
binary=result['binary']
if hashlib.sha256(Path(binary['path']).read_bytes()).hexdigest()!=binary['sha256']:
    raise SystemExit('retained and executed binary differs')
index={'format':'mqttiumd.session-system-reception-index','schema_version':1,
       'status':'PASS','qualification':'system-test-only','source_head':sys.argv[2],
       'scope':['A-public-matrix','B-512-cycles','C-128-capacity','D-targeted-cancellation'],
       'complete_mission_scope':False,
       'source_pin':json.loads((out/'source-pin.json').read_text()),
       'source_archive':{'path':'source.tar','sha256':hashlib.sha256((out/'source.tar').read_bytes()).hexdigest()},
       'system_witnesses':'same-run-strace' if int(sys.argv[3]) else 'process-status-only',
       'normal':{'receipt':'normal/receipt.json','binary':binary,'cycles':cycles['completed_cycles'],
                 'same_main_pid':cycles['same_main_pid'],'same_sqlite_inode':cycles['same_sqlite_inode']},
       'limitations':['Nominal A-D system evidence only; E/F/G must be received separately before full lifecycle acceptance.',
                      'Local DISCONNECT completion alone is not a broker cleanup fact.']}
(out/'index.json').write_text(json.dumps(index,indent=2,sort_keys=True)+'\n')
files=sorted(path for path in out.rglob('*') if path.is_file())
(out/'SHA256SUMS').write_text(''.join(f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(out)}\n' for path in files))
print(json.dumps({'status':'PASS','scope':'nominal-A-D-only','source_head':sys.argv[2],'index':str(out/'index.json')}))
PY
tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner \
    -czf "$session_out.tar.gz" -C "$(dirname -- "$session_out")" "$(basename -- "$session_out")"
sha256sum "$session_out.tar.gz" >"$session_out.tar.gz.sha256"
