#!/usr/bin/env bash
# Source-pinned retained-value/replay system reception; no fixture qualification.
set -euo pipefail
if [[ $# != 1 ]]; then echo "usage: $0 NEW_OUTPUT_DIRECTORY" >&2; exit 2; fi
retained_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
retained_repo=$(git -C "$retained_root" rev-parse --show-toplevel)
retained_python=${PYTHON:-python3}
retained_out=$("$retained_python" -c 'from pathlib import Path; import sys; print(Path(sys.argv[1]).resolve())' "$1")
case "$retained_out/" in "$retained_repo/"*) echo "output must be outside the checkout" >&2; exit 2;; esac
if [[ -z ${MQTTIUMD_RETAINED_PIN_SOURCE:-} ]]; then
    [[ -z $(git -C "$retained_repo" status --porcelain --untracked-files=normal) ]] || {
        echo "reception requires a clean source tree" >&2; exit 2;
    }
    retained_pin_sha=$(git -C "$retained_repo" rev-parse HEAD)
    retained_pin_parent=$(mktemp -d "${TMPDIR:-/tmp}/mqttiumd-retained-source.XXXXXX")
    retained_pin_cleanup() {
        git -C "$retained_repo" worktree remove --force -- "$retained_pin_parent/source" >/dev/null 2>&1 || true
        rm -f -- "$retained_pin_parent/source-pin.log"
        rmdir -- "$retained_pin_parent" >/dev/null 2>&1 || true
    }
    trap retained_pin_cleanup EXIT
    git -C "$retained_repo" worktree add --detach -- "$retained_pin_parent/source" "$retained_pin_sha" >"$retained_pin_parent/source-pin.log" 2>&1
    retained_pin_source=$(git -C "$retained_pin_parent/source" rev-parse --show-toplevel)
    retained_pin_status=0
    MQTTIUMD_RETAINED_PIN_SOURCE="$retained_pin_source" MQTTIUMD_RETAINED_PIN_SHA="$retained_pin_sha" \
        MQTTIUMD_RETAINED_CALLER_SOURCE="$retained_repo" \
        "$retained_pin_source/mqttiumd/tools/run_retained_reception.sh" "$retained_out" || retained_pin_status=$?
    exit "$retained_pin_status"
fi
[[ "$retained_repo" == "$MQTTIUMD_RETAINED_PIN_SOURCE" && $(git -C "$retained_repo" rev-parse HEAD) == "$MQTTIUMD_RETAINED_PIN_SHA" ]] || {
    echo "invalid retained source pin" >&2; exit 2;
}
mkdir -p -- "$retained_out"
[[ -z $(ls -A -- "$retained_out") ]] || { echo "output must be empty" >&2; exit 2; }
[[ -z $(git -C "$retained_repo" status --porcelain --untracked-files=normal) ]] || {
    echo "reception requires a clean pinned source" >&2; exit 2;
}
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-1} CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0
retained_sha=$(git -C "$retained_repo" rev-parse HEAD)
retained_targets=${MQTTIUMD_RETAINED_TARGET_ROOT:-${RETAINED_TARGET_ROOT:-${TMPDIR:-/tmp}/mqttiumd-retained-$retained_sha}}
mkdir -p -- "$retained_targets" "$retained_out/binaries"
cd -- "$retained_root"
retained_record() {
    local retained_kind=$1
    shift
    "$retained_python" - "$retained_out/commands.jsonl" "$retained_kind" "$PWD" "$@" <<'PY'
from pathlib import Path
import json, sys
with Path(sys.argv[1]).open('a') as stream:
    stream.write(json.dumps({'kind': sys.argv[2], 'cwd': sys.argv[3], 'command': sys.argv[4:]}) + '\n')
PY
}
retained_record source-pin git -C "$MQTTIUMD_RETAINED_CALLER_SOURCE" worktree add --detach "$retained_repo" "$retained_sha"
"$retained_python" - "$retained_out/source-pin.json" "$MQTTIUMD_RETAINED_CALLER_SOURCE" "$retained_repo" "$retained_sha" <<'PY'
from pathlib import Path
import json, sys
Path(sys.argv[1]).write_text(json.dumps({'caller_source': sys.argv[2], 'captured_caller_head': sys.argv[4],
    'pinned_source': sys.argv[3], 'pinned_head': sys.argv[4],
    'assumption': 'this invocation exclusively owns its private detached source tree'}, indent=2) + '\n')
PY
"$retained_python" - "$retained_out" <<'PY'
from importlib.metadata import version
from pathlib import Path
import json, os, shutil, sys
if version('paho-mqtt') != '2.1.0': raise SystemExit('paho-mqtt2.1.0 required')
out=Path(sys.argv[1]); quota=Path('/sys/fs/cgroup/cpu.max')
(out/'environment.json').write_text(json.dumps({'CARGO_BUILD_JOBS':os.environ['CARGO_BUILD_JOBS'],
    'MQTTIUMD_MOSQUITTO_BIN_DIR':os.environ.get('MQTTIUMD_MOSQUITTO_BIN_DIR'),
    'LD_LIBRARY_PATH':os.environ.get('LD_LIBRARY_PATH'),
    'cpu_max':quota.read_text().strip() if quota.exists() else None,
    'free_bytes_at_start':shutil.disk_usage(out).free,
    'session_quota':128,'receipt_slots':8192,'retained_topics':32,'retained_bytes':1048576,'max_targets':16},indent=2)+'\n')
PY
rustc -Vv >"$retained_out/rustc.txt"
cargo -V >"$retained_out/cargo.txt"
"$retained_python" -VV >"$retained_out/python.txt"
git rev-parse HEAD >"$retained_out/source-head.txt"
git diff --binary HEAD >"$retained_out/source.modification"
retained_record source-archive git -C "$retained_repo" archive --format=tar "$retained_sha"
git -C "$retained_repo" archive --format=tar "$retained_sha" >"$retained_out/source.tar"
retained_record pilot-tests "$retained_python" -m unittest discover -s tests/system -p 'test_retained_reception.py' -v
"$retained_python" -m unittest discover -s tests/system -p 'test_retained_reception.py' -v >"$retained_out/pilot-tests.log" 2>&1
retained_record product-event-schema-pilot "$retained_python" -m unittest discover -s tests/system -p 'test_product_events.py' -v
"$retained_python" -m unittest discover -s tests/system -p 'test_product_events.py' -v >"$retained_out/product-event-schema-pilot.log" 2>&1
PYTHONPATH="$retained_root/tests/system${PYTHONPATH:+:$PYTHONPATH}" "$retained_python" - "$retained_out/clients.json" <<'PY'
import json,sys
from pathlib import Path
from external_mqttium import client_identity
from external_retained import mosquitto_identity
Path(sys.argv[1]).write_text(json.dumps({'mqttium':client_identity(),'mosquitto':mosquitto_identity()},indent=2)+'\n')
PY
# The package builder is the sole normal release build. Copy its actual bytes
# for every normal system run; the archive smoke executes those same bytes.
retained_record preview-build "$retained_python" tools/build_preview.py "$retained_out/preview" --target-dir "$retained_targets/normal"
"$retained_python" tools/build_preview.py "$retained_out/preview" --target-dir "$retained_targets/normal" >"$retained_out/preview-build.log" 2>&1
retained_archives=("$retained_out"/preview/mqttiumd-preview-*.tar.gz)
[[ ${#retained_archives[@]} == 1 && -f "${retained_archives[0]}" ]] || { echo 'one preview archive required' >&2; exit 2; }
retained_package=${retained_archives[0]%.tar.gz}
cp -- "$retained_package/mqttiumd" "$retained_out/binaries/mqttiumd-normal"
cp -- "$retained_package/NORMAL-FEATURES.txt" "$retained_out/normal-features.txt"
cargo tree -p mqttiumd --no-default-features --features test-hooks --locked --format '{p} features={f}' >"$retained_out/hooks-features.txt"
retained_record hooks-build cargo build -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$retained_targets/hooks"
cargo build -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$retained_targets/hooks" >"$retained_out/hooks-build.log" 2>&1
cp -- "$retained_targets/hooks/debug/mqttiumd" "$retained_out/binaries/mqttiumd-hooks"
retained_trace=()
if command -v strace >/dev/null 2>&1; then
    retained_trace=(strace -f -qq -s 256 -e trace=process,signal,network,flock -o)
fi
retained_run() {
    local retained_name=$1
    shift
    retained_record "$retained_name" "$@"
    if [[ ${#retained_trace[@]} != 0 ]]; then
        "${retained_trace[@]}" "$retained_out/$retained_name.strace" "$@" >"$retained_out/$retained_name.log" 2>&1
    else
        "$@" >"$retained_out/$retained_name.log" 2>&1
    fi
}
retained_run normal "$retained_python" tests/system/retained_reception.py --binary "$retained_out/binaries/mqttiumd-normal" --out "$retained_out/normal" --binary-source-head "$retained_sha"
retained_run boundaries "$retained_python" tests/system/retained_boundaries.py --binary "$retained_out/binaries/mqttiumd-hooks" --out "$retained_out/boundaries" --binary-source-head "$retained_sha"
retained_run preview-smoke "$retained_python" tools/check_preview.py "${retained_archives[0]}" "$retained_out/preview-smoke"
# Execute the public guide from a fresh, hash-verified extraction, using the
# exact normal bytes just exercised above. No source path or Cargo is needed.
retained_demo_package=("$retained_out"/preview-smoke/extracted/mqttiumd-preview-*)
[[ ${#retained_demo_package[@]} == 1 && -x "${retained_demo_package[0]}/demo.sh" ]] || { echo 'one packaged retained demo required' >&2; exit 2; }
retained_demo_port=$("$retained_python" - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(('127.0.0.1',0));print(sock.getsockname()[1])
PY
)
retained_run package-demo env MQTTIUMD_DEMO_PORT="$retained_demo_port" \
    PATH="${MQTTIUMD_MOSQUITTO_BIN_DIR:+$MQTTIUMD_MOSQUITTO_BIN_DIR:}$PATH" \
    "${retained_demo_package[0]}/demo.sh" "$retained_out/package-demo"
retained_record product-event-schema-check "$retained_python" tools/check_product_event_stream.py "$retained_out/normal" "$retained_out/boundaries" "$retained_out/preview-smoke" "$retained_out/package-demo" --out "$retained_out/product-event-streams.json"
"$retained_python" tools/check_product_event_stream.py "$retained_out/normal" "$retained_out/boundaries" "$retained_out/preview-smoke" "$retained_out/package-demo" --out "$retained_out/product-event-streams.json" >"$retained_out/product-event-streams.log" 2>&1
retained_record compiled-negative-controls "$retained_python" tests/system/retained_mutants.py "$retained_out/mutants"
MQTTIUMD_RETAINED_MUTANT_TARGET_ROOT="${MQTTIUMD_RETAINED_MUTANT_TARGET_ROOT:-$retained_targets/mutants}" \
    "$retained_python" tests/system/retained_mutants.py "$retained_out/mutants" >"$retained_out/mutants.log" 2>&1
"$retained_python" - "$retained_out" "$retained_sha" "${#retained_trace[@]}" <<'PY'
from pathlib import Path
import hashlib,json,sys
out=Path(sys.argv[1]);source=sys.argv[2]
normal=json.loads((out/'normal/receipt.json').read_text())
boundaries=json.loads((out/'boundaries/receipt.json').read_text())
mutants=json.loads((out/'mutants/result.json').read_text())
smoke=json.loads((out/'preview-smoke/receipt.json').read_text())
streams=json.loads((out/'product-event-streams.json').read_text())
for name,receipt in [('normal',normal),('boundaries',boundaries)]:
    if receipt['status']!='PASS' or receipt['source_head']!=source or receipt['harness_source_head']!=source:
        raise SystemExit(f'{name} source/verdict differs')
    if hashlib.sha256(Path(receipt['binary']['path']).read_bytes()).hexdigest()!=receipt['binary']['sha256']:
        raise SystemExit(f'{name} executed binary differs')
cases={row['criterion']:row for row in normal['cases']}
if set(cases)!={'minimal','demo','matrix','options','replacement','capacity'}:
    raise SystemExit('normal retained criteria missing')
if len(cases['matrix']['cells'])!=24 or cases['capacity']['current_topics']!=32 or cases['capacity']['current_payload_bytes_at_full']!=1048576:
    raise SystemExit('retained matrix or unchanged inventory quotas incomplete')
if len(boundaries['cases'])!=9 or sum(row.get('kill_status')==-9 for row in boundaries['cases'])!=6:
    raise SystemExit('six actual retained/replay process kills and three lost outcomes required')
if not all(row.get('same_operation') for row in boundaries['cases'] if row['criterion'].startswith('lost-')):
    raise SystemExit('unknown outcomes must resolve the original operation')
if (smoke['status']!='PASS' or smoke['source_sha']!=source
        or smoke['binary_sha256']!=normal['binary']['sha256']):
    raise SystemExit('archive smoke and retained reception must execute identical normal bytes')
if streams['status']!='PASS' or streams['product_events']<1 or streams['skipped_records']!=0:
    raise SystemExit('actual product streams must pass strict/schema validation')
if (mutants['status']!='PASS' or mutants['source_head']!=source or not mutants['restored']
        or not mutants['controls'] or not all(row['detected'] and row['positive_status']==0
            and row['mutant_status']==101 and row['observed_reason']==row['expected_reason']
            for row in mutants['controls'])):
    raise SystemExit('compiled controls must detect their stated properties and restore source')
demo=out/'package-demo'
if (demo/'first.txt').read_text()!=(demo/'expected-first.txt').read_text() or (demo/'last.txt').read_text()!=(demo/'expected-last.txt').read_text():
    raise SystemExit('public packaged retained guide failed its exact observations')
index={'format':'mqttiumd.retained-system-reception-index','schema_version':1,
       'status':'PASS','qualification':'system-test-only','source_head':source,
       'scope':['retained-after-producer-termination-and-restart','three-public-client-implementations',
                'six-QoS-cells-both-RAP-values-two-directions','ordered-RH-and-filter-occurrences',
                'NoLocal-by-producer-ClientID','replacement-clear-and-old-owed-payload',
                '32-topic-1MiB-inventory-and-atomic-replay-capacity','six-own-SIGKILL-runs',
                'three-authentic-unknown-outcomes','compiled-controls','standalone-packaged-public-demo'],
       'source_pin':json.loads((out/'source-pin.json').read_text()),
       'source_archive':{'path':'source.tar','sha256':hashlib.sha256((out/'source.tar').read_bytes()).hexdigest()},
       'normal':{'receipt':'normal/receipt.json','binary':normal['binary']},
       'boundaries':{'receipt':'boundaries/receipt.json','binary':boundaries['binary']},
       'product_event_schema':{'result':'product-event-streams.json','product_events':streams['product_events'],
                               'stream_files':len(streams['files']),'scope':'strict-json-and-schema-only'},
       'negative_controls':{'result':'mutants/result.json','reasons':[row['expected_reason'] for row in mutants['controls']],'restored':True},
       'preview':{'receipt':'preview-smoke/receipt.json','archive_sha256':smoke['archive_sha256'],
                  'same_normal_binary':True,'public_retained_demo':'package-demo'},
       'system_witnesses':'same-run-strace' if int(sys.argv[3]) else 'process-status-only',
       'limitations':['Finite system evidence, not independent-oracle qualification or complete MQTT conformance.',
                     'QoS0 publisher completion proves only a local write; actual recipient/SQLite observations establish the tested facts.',
                     'Replay duplicates from separate filter occurrences are intentional; live overlap remains one delivery/session.']}
(out/'index.json').write_text(json.dumps(index,indent=2,sort_keys=True)+'\n')
files=sorted(path for path in out.rglob('*') if path.is_file())
(out/'SHA256SUMS').write_text(''.join(f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(out)}\n' for path in files))
print(json.dumps({'status':'PASS','source_head':source,'index':str(out/'index.json')}))
PY
tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner \
    -czf "$retained_out.tar.gz" -C "$(dirname -- "$retained_out")" "$(basename -- "$retained_out")"
sha256sum "$retained_out.tar.gz" >"$retained_out.tar.gz.sha256"
