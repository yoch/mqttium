#!/usr/bin/env bash
# Source-pinned hierarchical wildcard/No Local system reception; no fixture qualification.
set -euo pipefail
if [[ $# != 1 ]]; then echo "usage: $0 NEW_OUTPUT_DIRECTORY" >&2; exit 2; fi
wildcard_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
wildcard_repo=$(git -C "$wildcard_root" rev-parse --show-toplevel)
wildcard_python=${PYTHON:-python3}
wildcard_out=$("$wildcard_python" -c 'from pathlib import Path; import sys; print(Path(sys.argv[1]).resolve())' "$1")
case "$wildcard_out/" in "$wildcard_repo/"*) echo "output must be outside the checkout" >&2; exit 2;; esac
if [[ -z ${MQTTIUMD_WILDCARD_PIN_SOURCE:-} ]]; then
    [[ -z $(git -C "$wildcard_repo" status --porcelain --untracked-files=normal) ]] || {
        echo "reception requires a clean source tree" >&2; exit 2;
    }
    wildcard_pin_sha=$(git -C "$wildcard_repo" rev-parse HEAD)
    wildcard_pin_parent=$(mktemp -d "${TMPDIR:-/tmp}/mqttiumd-wildcard-source.XXXXXX")
    wildcard_pin_cleanup() {
        git -C "$wildcard_repo" worktree remove --force -- "$wildcard_pin_parent/source" >/dev/null 2>&1 || true
        rm -f -- "$wildcard_pin_parent/source-pin.log"
        rmdir -- "$wildcard_pin_parent" >/dev/null 2>&1 || true
    }
    trap wildcard_pin_cleanup EXIT
    git -C "$wildcard_repo" worktree add --detach -- "$wildcard_pin_parent/source" "$wildcard_pin_sha" >"$wildcard_pin_parent/source-pin.log" 2>&1
    wildcard_pin_source=$(git -C "$wildcard_pin_parent/source" rev-parse --show-toplevel)
    wildcard_pin_status=0
    MQTTIUMD_WILDCARD_PIN_SOURCE="$wildcard_pin_source" MQTTIUMD_WILDCARD_PIN_SHA="$wildcard_pin_sha" \
        MQTTIUMD_WILDCARD_CALLER_SOURCE="$wildcard_repo" \
        "$wildcard_pin_source/mqttiumd/tools/run_wildcard_reception.sh" "$wildcard_out" || wildcard_pin_status=$?
    exit "$wildcard_pin_status"
fi
[[ "$wildcard_repo" == "$MQTTIUMD_WILDCARD_PIN_SOURCE" && $(git -C "$wildcard_repo" rev-parse HEAD) == "$MQTTIUMD_WILDCARD_PIN_SHA" ]] || {
    echo "invalid wildcard source pin" >&2; exit 2;
}
mkdir -p -- "$wildcard_out"
[[ -z $(ls -A -- "$wildcard_out") ]] || { echo "output must be empty" >&2; exit 2; }
[[ -z $(git -C "$wildcard_repo" status --porcelain --untracked-files=normal) ]] || {
    echo "reception requires a clean pinned source" >&2; exit 2;
}
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-1} CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0
wildcard_sha=$(git -C "$wildcard_repo" rev-parse HEAD)
wildcard_targets=${MQTTIUMD_WILDCARD_TARGET_ROOT:-${WILDCARD_TARGET_ROOT:-${TMPDIR:-/tmp}/mqttiumd-wildcard-$wildcard_sha}}
mkdir -p -- "$wildcard_targets" "$wildcard_out/binaries"
cd -- "$wildcard_root"
wildcard_record() {
    local wildcard_kind=$1
    shift
    "$wildcard_python" - "$wildcard_out/commands.jsonl" "$wildcard_kind" "$PWD" "$@" <<'PY'
from pathlib import Path
import json, sys
with Path(sys.argv[1]).open('a') as stream:
    stream.write(json.dumps({'kind': sys.argv[2], 'cwd': sys.argv[3], 'command': sys.argv[4:]}) + '\n')
PY
}
wildcard_record source-pin git -C "$MQTTIUMD_WILDCARD_CALLER_SOURCE" worktree add --detach "$wildcard_repo" "$wildcard_sha"
"$wildcard_python" - "$wildcard_out/source-pin.json" "$MQTTIUMD_WILDCARD_CALLER_SOURCE" "$wildcard_repo" "$wildcard_sha" <<'PY'
from pathlib import Path
import json, sys
Path(sys.argv[1]).write_text(json.dumps({'caller_source': sys.argv[2], 'captured_caller_head': sys.argv[4],
    'pinned_source': sys.argv[3], 'pinned_head': sys.argv[4],
    'assumption': 'this invocation exclusively owns its private detached source tree'}, indent=2) + '\n')
PY
"$wildcard_python" - "$wildcard_out" <<'PY'
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
    'session_quota':128,'receipt_slots':8192,'route_sizes':[1,16,64,128],
    'samples_per_size':16,'modes':['exact','wildcard'],'max_targets':16},indent=2)+'\n')
PY
rustc -Vv >"$wildcard_out/rustc.txt"
cargo -V >"$wildcard_out/cargo.txt"
"$wildcard_python" -VV >"$wildcard_out/python.txt"
git rev-parse HEAD >"$wildcard_out/source-head.txt"
git diff --binary HEAD >"$wildcard_out/source.modification"
wildcard_record source-archive git -C "$wildcard_repo" archive --format=tar "$wildcard_sha"
git -C "$wildcard_repo" archive --format=tar "$wildcard_sha" >"$wildcard_out/source.tar"
wildcard_record pilot-tests "$wildcard_python" -m unittest discover -s tests/system -p 'test_wildcard_reception.py' -v
"$wildcard_python" -m unittest discover -s tests/system -p 'test_wildcard_reception.py' -v >"$wildcard_out/pilot-tests.log" 2>&1
wildcard_record product-event-schema-pilot "$wildcard_python" -m unittest discover -s tests/system -p 'test_product_events.py' -v
"$wildcard_python" -m unittest discover -s tests/system -p 'test_product_events.py' -v >"$wildcard_out/product-event-schema-pilot.log" 2>&1
PYTHONPATH="$wildcard_root/tests/system${PYTHONPATH:+:$PYTHONPATH}" "$wildcard_python" - "$wildcard_out/clients.json" <<'PY'
import json,sys
from pathlib import Path
from external_mqttium import client_identity
from external_wildcard import mosquitto_identity
Path(sys.argv[1]).write_text(json.dumps({'mqttium':client_identity(),'mosquitto':mosquitto_identity()},indent=2)+'\n')
PY
# The package builder is the sole normal release build. Copy its actual bytes
# for every normal system run; the archive smoke executes those same bytes.
wildcard_record preview-build "$wildcard_python" tools/build_preview.py "$wildcard_out/preview" --target-dir "$wildcard_targets/normal"
"$wildcard_python" tools/build_preview.py "$wildcard_out/preview" --target-dir "$wildcard_targets/normal" >"$wildcard_out/preview-build.log" 2>&1
wildcard_archives=("$wildcard_out"/preview/mqttiumd-preview-*.tar.gz)
[[ ${#wildcard_archives[@]} == 1 && -f "${wildcard_archives[0]}" ]] || { echo 'one preview archive required' >&2; exit 2; }
wildcard_package=${wildcard_archives[0]%.tar.gz}
cp -- "$wildcard_package/mqttiumd" "$wildcard_out/binaries/mqttiumd-normal"
cp -- "$wildcard_package/NORMAL-FEATURES.txt" "$wildcard_out/normal-features.txt"
cargo tree -p mqttiumd --no-default-features --features test-hooks --locked --format '{p} features={f}' >"$wildcard_out/hooks-features.txt"
wildcard_record hooks-build cargo build -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$wildcard_targets/hooks"
cargo build -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$wildcard_targets/hooks" >"$wildcard_out/hooks-build.log" 2>&1
cp -- "$wildcard_targets/hooks/debug/mqttiumd" "$wildcard_out/binaries/mqttiumd-hooks"
wildcard_trace=()
if command -v strace >/dev/null 2>&1; then
    wildcard_trace=(strace -f -qq -s 256 -e trace=process,signal,network,flock -o)
fi
wildcard_run() {
    local wildcard_name=$1
    shift
    wildcard_record "$wildcard_name" "$@"
    if [[ ${#wildcard_trace[@]} != 0 ]]; then
        "${wildcard_trace[@]}" "$wildcard_out/$wildcard_name.strace" "$@" >"$wildcard_out/$wildcard_name.log" 2>&1
    else
        "$@" >"$wildcard_out/$wildcard_name.log" 2>&1
    fi
}
wildcard_run normal "$wildcard_python" tests/system/wildcard_reception.py --binary "$wildcard_out/binaries/mqttiumd-normal" --out "$wildcard_out/normal" --binary-source-head "$wildcard_sha" --case normal
wildcard_run boundaries "$wildcard_python" tests/system/wildcard_boundaries.py --binary "$wildcard_out/binaries/mqttiumd-hooks" --out "$wildcard_out/boundaries" --binary-source-head "$wildcard_sha"
wildcard_run preview-smoke "$wildcard_python" tools/check_preview.py "${wildcard_archives[0]}" "$wildcard_out/preview-smoke"
wildcard_record product-event-schema-check "$wildcard_python" tools/check_product_event_stream.py "$wildcard_out/normal" "$wildcard_out/boundaries" "$wildcard_out/preview-smoke" --out "$wildcard_out/product-event-streams.json"
"$wildcard_python" tools/check_product_event_stream.py "$wildcard_out/normal" "$wildcard_out/boundaries" "$wildcard_out/preview-smoke" --out "$wildcard_out/product-event-streams.json" >"$wildcard_out/product-event-streams.log" 2>&1
wildcard_record compiled-negative-controls "$wildcard_python" tools/run_wildcard_mutants.py "$wildcard_out/mutants"
MQTTIUMD_WILDCARD_MUTANT_TARGET_ROOT="${MQTTIUMD_WILDCARD_MUTANT_TARGET_ROOT:-$wildcard_targets/mutants}" \
    "$wildcard_python" tools/run_wildcard_mutants.py "$wildcard_out/mutants" >"$wildcard_out/mutants.log" 2>&1
"$wildcard_python" - "$wildcard_out" "$wildcard_sha" "${#wildcard_trace[@]}" <<'PY'
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
if set(cases)!={'demo','matrix','no-local','filter-edges','offline-expiry','fanout','routing-measure','pressure'}:
    raise SystemExit('normal wildcard criteria missing')
if len(cases['matrix']['cells'])!=12 or cases['fanout']['no_local_eligible_targets']!=16:
    raise SystemExit('QoS matrix or post-NoLocal destination budget missing')
measurement=cases['routing-measure']
if (measurement['modes']!=['exact','wildcard'] or measurement['sizes']!=[1,16,64,128]
        or len(measurement['measurements'])!=8
        or not all(row['messages_received']==row['durably_finished']==16 and row['distinct_targets']==1
                   for row in measurement['measurements'])):
    raise SystemExit('bounded exact/wildcard measurements are incomplete')
if cases['offline-expiry']['kill_status']!=-9:
    raise SystemExit('offline wildcard restart needs its own true SIGKILL')
expected={'wildcard_routes_not_aggregated','wildcard_maximum_qos_lost',
          'wildcard_fanout_truncated','no_local_applied_after_aggregation'}
if (mutants['status']!='PASS' or mutants['source_head']!=source or not mutants['restored']
        or len(mutants['mutants'])!=4 or {row['reason'] for row in mutants['mutants']}!=expected
        or not all(row['detected'] for row in mutants['mutants'])):
    raise SystemExit('four compiled controls must detect their exact properties and restore source')
if (smoke['status']!='PASS' or smoke['source_sha']!=source
        or smoke['binary_sha256']!=normal['binary']['sha256']):
    raise SystemExit('archive smoke and normal system reception must execute identical packaged bytes')
if streams['status']!='PASS' or streams['product_events']<1 or streams['skipped_records']!=0:
    raise SystemExit('actual product streams must pass strict/schema validation')
index={'format':'mqttiumd.wildcard-system-reception-index','schema_version':1,
       'status':'PASS','qualification':'system-test-only','source_head':source,
       'scope':['public-three-client-demo','six-QoS-cells-two-directions','NoLocal-per-contribution',
                'hierarchical-grammar-and-system-topics','literal-filter-mutations','durable-offline-and-finite-expiry',
                'sixteen-seventeen-complete-fanout','wildcard-output-pressure-and-independent-progress',
                'real-subscription-update-boundaries',
                'bounded-exact-and-wildcard-measurements','four-compiled-controls','Linux-preview-archive-smoke'],
       'source_pin':json.loads((out/'source-pin.json').read_text()),
       'source_archive':{'path':'source.tar','sha256':hashlib.sha256((out/'source.tar').read_bytes()).hexdigest()},
       'normal':{'receipt':'normal/receipt.json','binary':normal['binary']},
       'boundaries':{'receipt':'boundaries/receipt.json','binary':boundaries['binary']},
       'product_event_schema':{'result':'product-event-streams.json','product_events':streams['product_events'],
                               'stream_files':len(streams['files']),'scope':'strict-json-and-schema-only'},
       'negative_controls':{'result':'mutants/result.json','reasons':sorted(expected),'restored':True},
       'preview':{'receipt':'preview-smoke/receipt.json','archive_sha256':smoke['archive_sha256'],
                  'same_normal_binary':True},
       'system_witnesses':'same-run-strace' if int(sys.argv[3]) else 'process-status-only',
       'limitations':['Finite system evidence, not independent-oracle qualification or complete MQTT conformance.',
                     'Measurements include TCP, SQLite and scheduling; they do not isolate matcher CPU cost.',
                     'Mosquitto 2.0 CLI exercises wildcard interoperability; No Local uses public mqttium/Paho APIs.']}
(out/'index.json').write_text(json.dumps(index,indent=2,sort_keys=True)+'\n')
files=sorted(path for path in out.rglob('*') if path.is_file())
(out/'SHA256SUMS').write_text(''.join(f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(out)}\n' for path in files))
print(json.dumps({'status':'PASS','source_head':source,'index':str(out/'index.json')}))
PY
tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner \
    -czf "$wildcard_out.tar.gz" -C "$(dirname -- "$wildcard_out")" "$(basename -- "$wildcard_out")"
sha256sum "$wildcard_out.tar.gz" >"$wildcard_out.tar.gz.sha256"
