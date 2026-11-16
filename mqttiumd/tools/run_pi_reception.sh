#!/usr/bin/env bash
# Finite outgoing PI system tests, distinct from fixture qualification.
set -euo pipefail
if [[ $# != 1 ]]; then echo "usage: $0 NEW_OUTPUT_DIRECTORY" >&2; exit 2; fi
pi_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
pi_repo=$(git -C "$pi_root" rev-parse --show-toplevel)
pi_python=${PYTHON:-python3}
pi_out=$("$pi_python" -c 'from pathlib import Path; import sys; print(Path(sys.argv[1]).resolve())' "$1")
case "$pi_out/" in "$pi_repo/"*) echo "output must be outside the checkout" >&2; exit 2;; esac
# Freeze builder, harness and public client imports together. The caller may
# advance afterwards; this invocation owns its private detached source tree.
if [[ -z ${MQTTIUMD_PI_PIN_SOURCE:-} ]]; then
    [[ -z $(git -C "$pi_repo" status --porcelain --untracked-files=normal) ]] || {
        echo "reception requires a clean source tree" >&2; exit 2;
    }
    pi_pin_sha=$(git -C "$pi_repo" rev-parse HEAD)
    pi_pin_parent=$(mktemp -d "${TMPDIR:-/tmp}/mqttiumd-pi-source.XXXXXX")
    pi_pin_cleanup() {
        git -C "$pi_repo" worktree remove --force -- "$pi_pin_parent/source" >/dev/null 2>&1 || true
        rm -f -- "$pi_pin_parent/source-pin.log"
        rmdir -- "$pi_pin_parent" >/dev/null 2>&1 || true
    }
    trap pi_pin_cleanup EXIT
    git -C "$pi_repo" worktree add --detach -- "$pi_pin_parent/source" "$pi_pin_sha" >"$pi_pin_parent/source-pin.log" 2>&1
    pi_pin_status=0
    MQTTIUMD_PI_PIN_SOURCE="$pi_pin_parent/source" MQTTIUMD_PI_PIN_SHA="$pi_pin_sha" \
        MQTTIUMD_PI_CALLER_SOURCE="$pi_repo" \
        "$pi_pin_parent/source/mqttiumd/tools/run_pi_reception.sh" "$pi_out" || pi_pin_status=$?
    exit "$pi_pin_status"
fi
[[ "$pi_repo" == "$MQTTIUMD_PI_PIN_SOURCE" && $(git -C "$pi_repo" rev-parse HEAD) == "$MQTTIUMD_PI_PIN_SHA" ]] || {
    echo "invalid PI source pin" >&2; exit 2;
}
mkdir -p -- "$pi_out"
[[ -z $(ls -A -- "$pi_out") ]] || { echo "output must be empty" >&2; exit 2; }
[[ -z $(git -C "$pi_repo" status --porcelain --untracked-files=normal) ]] || {
    echo "reception requires a clean source tree" >&2; exit 2;
}
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-1}
pi_sha=$(git -C "$pi_repo" rev-parse HEAD)
pi_targets=${MQTTIUMD_PI_TARGET_ROOT:-${TMPDIR:-/tmp}/mqttiumd-pi-$pi_sha}
mkdir -p -- "$pi_targets" "$pi_out/binaries"
cd -- "$pi_root"
pi_record() {
    local pi_kind=$1
    shift
    "$pi_python" - "$pi_out/commands.jsonl" "$pi_kind" "$PWD" "$@" <<'PY'
from pathlib import Path
import json, sys
with Path(sys.argv[1]).open('a') as stream:
    stream.write(json.dumps({'kind': sys.argv[2], 'cwd': sys.argv[3], 'command': sys.argv[4:]}) + '\n')
PY
}
pi_record source-pin git -C "$MQTTIUMD_PI_CALLER_SOURCE" worktree add --detach "$pi_repo" "$MQTTIUMD_PI_PIN_SHA"
"$pi_python" - "$pi_out/source-pin.json" "$MQTTIUMD_PI_CALLER_SOURCE" "$pi_repo" "$pi_sha" <<'PY'
from pathlib import Path
import json, sys
Path(sys.argv[1]).write_text(json.dumps({'caller_source': sys.argv[2], 'captured_caller_head': sys.argv[4],
    'pinned_source': sys.argv[3], 'pinned_head': sys.argv[4],
    'assumption': 'this invocation exclusively owns its private detached source tree'}, indent=2) + '\n')
PY
"$pi_python" - "$pi_out" <<'PY'
from pathlib import Path
from importlib.metadata import version
import json, os, shutil, sys
if version('paho-mqtt') != '2.1.0': raise SystemExit('paho-mqtt2.1.0 required')
out = Path(sys.argv[1]); quota = Path('/sys/fs/cgroup/cpu.max')
(out / 'environment.json').write_text(json.dumps({'CARGO_BUILD_JOBS': os.environ['CARGO_BUILD_JOBS'],
    'cpu_max': quota.read_text().strip() if quota.exists() else None,
    'free_bytes_at_start': shutil.disk_usage(out).free,
    'long_case_deadline_s': 3600, 'long_case_deliveries': 65537}, indent=2) + '\n')
PY
rustc -Vv >"$pi_out/rustc.txt"
cargo -V >"$pi_out/cargo.txt"
"$pi_python" -VV >"$pi_out/python.txt"
git rev-parse HEAD >"$pi_out/source-head.txt"
git diff --binary HEAD >"$pi_out/source.modification"
pi_record source-archive git -C "$pi_repo" archive --format=tar "$pi_sha"
git -C "$pi_repo" archive --format=tar "$pi_sha" >"$pi_out/source.tar"
pi_record pilot-tests "$pi_python" -m unittest discover -s tests/system -p 'test_*.py' -v
"$pi_python" -m unittest discover -s tests/system -p 'test_*.py' -v >"$pi_out/pilot-tests.log" 2>&1
cargo tree -p mqttiumd --no-default-features --locked --format '{p} features={f}' >"$pi_out/normal-features.txt"
pi_record normal-build cargo build -p mqttiumd --no-default-features --locked --target-dir "$pi_targets/normal"
cargo build -p mqttiumd --no-default-features --locked --target-dir "$pi_targets/normal" >"$pi_out/normal-build.log" 2>&1
cp -- "$pi_targets/normal/debug/mqttiumd" "$pi_out/binaries/mqttiumd-normal"
"$pi_python" - "$pi_out/normal-features.txt" <<'PY'
from pathlib import Path
import sys
text = Path(sys.argv[1]).read_text()
if any(name in text for name in ('mqttiumd-lab', 'test-hooks', 'kill-points')):
    raise SystemExit('normal product contains lab or instrumentation')
PY
pi_trace=()
if command -v strace >/dev/null 2>&1; then
    pi_trace=(strace -f -qq -s 256 -e trace=process,signal,network,flock -o)
fi
pi_run() {
    local pi_name=$1 pi_case=$2
    local pi_command=("$pi_python" "$pi_root/tests/system/pi_reception.py" --binary "$pi_out/binaries/mqttiumd-normal" --out "$pi_out/$pi_name" --binary-source-head "$pi_sha" --case "$pi_case")
    pi_record "$pi_name-reception" "${pi_command[@]}"
    if [[ ${#pi_trace[@]} != 0 ]]; then
        "${pi_trace[@]}" "$pi_out/$pi_name.strace" "${pi_command[@]}" >"$pi_out/$pi_name.log" 2>&1
    else
        "${pi_command[@]}" >"$pi_out/$pi_name.log" 2>&1
    fi
}
pi_run normal-long B
pi_run normal-reverse reverse
"$pi_python" - "$pi_out" "$pi_sha" "${#pi_trace[@]}" <<'PY'
from pathlib import Path
import hashlib, json, sys
out = Path(sys.argv[1]); names = ('normal-long', 'normal-reverse')
receipts = {name: json.loads((out / name / 'receipt.json').read_text()) for name in names}
for name, result in receipts.items():
    if result['status'] != 'PASS' or result['source_head'] != sys.argv[2]:
        raise SystemExit('source or verdict does not match')
    binary = result['binary']
    if hashlib.sha256(Path(binary['path']).read_bytes()).hexdigest() != binary['sha256']:
        raise SystemExit('retained and executed binaries differ')
long = receipts['normal-long']['cases'][0]; evidence = long['deliveries']
if evidence['finished_deliveries'] < 65537 or not long['long_requirement_met']:
    raise SystemExit('actual numbered and durably finished delivery quota not reached')
for ledger in (evidence['ledger'], evidence['wire']['ledger']):
    if hashlib.sha256(Path(ledger['path']).read_bytes()).hexdigest() != ledger['sha256']:
        raise SystemExit('streamed ledger identity differs')
index = {'format': 'mqttiumd.pi-system-reception-index', 'schema_version': 1,
         'status': 'PASS', 'qualification': 'system-test-only', 'source_head': sys.argv[2],
         'reception_scope': ['B-normal-long', 'B-normal-reverse'],
         'source_archive': {'path': 'source.tar', 'sha256': hashlib.sha256((out / 'source.tar').read_bytes()).hexdigest()},
         'commands': 'commands.jsonl', 'system_witnesses': 'same-run-strace' if int(sys.argv[3]) else 'process-status-only',
         'source_pin': json.loads((out / 'source-pin.json').read_text()),
         'normal_summary': {'finished_deliveries': evidence['finished_deliveries'],
             'wire_identifier_reuses': evidence['wire']['identifier_reuses'],
             'unique_wire_identifiers': evidence['wire']['unique_identifiers'],
             'elapsed_ms': evidence['elapsed_ms'], 'publisher_connections': evidence['publisher_connections'],
             'subscriber_connections': evidence['subscriber_connections'], 'proxy_connections': evidence['wire']['connections']},
         'receipt_collection': long['receipt_collection'],
         'receipts': {name: {'path': f'{name}/receipt.json', 'binary': result['binary'], 'status': result['status']}
                      for name, result in receipts.items()},
         'limitations': ['Finite system evidence does not qualify all MQTT behavior or indefinite service.',
             'A Packet Identifier is a recyclable wire resource, not a causal identity.',
             'No claim is made about arbitrary identical ACK bytes upstream of the authoritative observation boundary.',
             'Session, receipt, byte and operation identity bounds remain finite.']}
(out / 'index.json').write_text(json.dumps(index, indent=2, sort_keys=True) + '\n')
files = sorted(path for path in out.rglob('*') if path.is_file())
(out / 'SHA256SUMS').write_text(''.join(f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(out)}\n' for path in files))
print(json.dumps({'status': 'PASS', 'receipt': str(out / 'index.json'), 'source_head': sys.argv[2]}))
PY
tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner \
    -czf "$pi_out.tar.gz" -C "$(dirname -- "$pi_out")" "$(basename -- "$pi_out")"
sha256sum "$pi_out.tar.gz" >"$pi_out.tar.gz.sha256"
