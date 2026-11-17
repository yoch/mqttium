#!/usr/bin/env bash
# Exact-source, finite Will system reception. This is not oracle qualification.
set -euo pipefail
if [[ $# != 1 ]]; then echo "usage: $0 NEW_OUTPUT_DIRECTORY" >&2; exit 2; fi
will_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
will_repo=$(git -C "$will_root" rev-parse --show-toplevel)
will_python=${PYTHON:-python3}
will_out=$("$will_python" -c 'from pathlib import Path; import sys; print(Path(sys.argv[1]).resolve())' "$1")
case "$will_out/" in "$will_repo/"*) echo "output must be outside the checkout" >&2; exit 2;; esac
if [[ -z ${MQTTIUMD_WILL_PIN_SOURCE:-} ]]; then
    [[ -z $(git -C "$will_repo" status --porcelain --untracked-files=normal) ]] || {
        echo "reception requires a clean source tree" >&2; exit 2;
    }
    will_pin_sha=$(git -C "$will_repo" rev-parse HEAD)
    will_pin_parent=$(mktemp -d "${TMPDIR:-/tmp}/mqttiumd-will-source.XXXXXX")
    will_pin_cleanup() {
        git -C "$will_repo" worktree remove --force -- "$will_pin_parent/source" >/dev/null 2>&1 || true
        rm -f -- "$will_pin_parent/source-pin.log"
        rmdir -- "$will_pin_parent" >/dev/null 2>&1 || true
    }
    trap will_pin_cleanup EXIT
    git -C "$will_repo" worktree add --detach -- "$will_pin_parent/source" "$will_pin_sha" >"$will_pin_parent/source-pin.log" 2>&1
    will_pin_source=$(git -C "$will_pin_parent/source" rev-parse --show-toplevel)
    will_pin_status=0
    MQTTIUMD_WILL_PIN_SOURCE="$will_pin_source" MQTTIUMD_WILL_PIN_SHA="$will_pin_sha" \
        MQTTIUMD_WILL_CALLER_SOURCE="$will_repo" \
        "$will_pin_source/mqttiumd/tools/run_will_reception.sh" "$will_out" || will_pin_status=$?
    exit "$will_pin_status"
fi
[[ "$will_repo" == "$MQTTIUMD_WILL_PIN_SOURCE" && $(git -C "$will_repo" rev-parse HEAD) == "$MQTTIUMD_WILL_PIN_SHA" ]] || {
    echo "invalid Will source pin" >&2; exit 2;
}
mkdir -p -- "$will_out"
[[ -z $(ls -A -- "$will_out") ]] || { echo "output must be empty" >&2; exit 2; }
[[ -z $(git -C "$will_repo" status --porcelain --untracked-files=normal) ]] || {
    echo "reception requires a clean pinned source" >&2; exit 2;
}
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-1} CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
will_sha=$(git -C "$will_repo" rev-parse HEAD)
will_targets=${MQTTIUMD_WILL_TARGET_ROOT:-${TMPDIR:-/tmp}/mqttiumd-will-$will_sha}
mkdir -p -- "$will_targets" "$will_out/binaries"
cd -- "$will_root"
will_record() {
    local will_kind=$1
    shift
    "$will_python" - "$will_out/commands.jsonl" "$will_kind" "$PWD" "$@" <<'PY'
from pathlib import Path
import json, sys
with Path(sys.argv[1]).open('a') as stream:
    stream.write(json.dumps({'kind':sys.argv[2], 'cwd':sys.argv[3], 'command':sys.argv[4:]})+'\n')
PY
}
will_record source-pin git -C "$MQTTIUMD_WILL_CALLER_SOURCE" worktree add --detach "$will_repo" "$will_sha"
"$will_python" - "$will_out/source-pin.json" "$MQTTIUMD_WILL_CALLER_SOURCE" "$will_repo" "$will_sha" <<'PY'
from pathlib import Path
import json, sys
Path(sys.argv[1]).write_text(json.dumps({'caller_source':sys.argv[2], 'captured_caller_head':sys.argv[4],
    'pinned_source':sys.argv[3], 'pinned_head':sys.argv[4],
    'assumption':'this invocation exclusively owns its private detached source tree'}, indent=2)+'\n')
PY
"$will_python" - "$will_out" <<'PY'
from importlib.metadata import version
from pathlib import Path
import json, os, platform, shutil, sys
if version('paho-mqtt') != '2.1.0': raise SystemExit('paho-mqtt2.1.0 required')
out=Path(sys.argv[1]);quota=Path('/sys/fs/cgroup/cpu.max')
(out/'environment.json').write_text(json.dumps({'CARGO_BUILD_JOBS':os.environ['CARGO_BUILD_JOBS'],
    'execution_platform':platform.platform(), 'execution_machine':platform.machine(),
    'MQTTIUMD_MOSQUITTO_BIN_DIR':os.environ.get('MQTTIUMD_MOSQUITTO_BIN_DIR'),
    'LD_LIBRARY_PATH':os.environ.get('LD_LIBRARY_PATH'),
    'cpu_max':quota.read_text().strip() if quota.exists() else None,
    'free_bytes_at_start':shutil.disk_usage(out).free, 'sessions':128,'receipt_slots':8192,
    'retained_topics':32,'retained_bytes':1048576,'max_targets':16,
    'will_intents':256,'will_payload_bytes':8388608},indent=2)+'\n')
PY
rustc -Vv >"$will_out/rustc.txt"
cargo -V >"$will_out/cargo.txt"
"$will_python" -VV >"$will_out/python.txt"
git rev-parse HEAD >"$will_out/source-head.txt"
git diff --binary HEAD >"$will_out/source.modification"
will_record source-archive git -C "$will_repo" archive --format=tar "$will_sha"
git -C "$will_repo" archive --format=tar "$will_sha" >"$will_out/source.tar"
will_record pilot-tests "$will_python" -m unittest discover -s tests/system -p 'test_will*.py' -v
"$will_python" -m unittest discover -s tests/system -p 'test_will*.py' -v >"$will_out/pilot-tests.log" 2>&1
PYTHONPATH="$will_root/tests/system${PYTHONPATH:+:$PYTHONPATH}" "$will_python" - "$will_out/clients.json" <<'PY'
from pathlib import Path
import json, sys
from external_mqttium import client_identity
from external_wildcard import mosquitto_identity
Path(sys.argv[1]).write_text(json.dumps({'mqttium':client_identity(),'mosquitto':mosquitto_identity()},indent=2)+'\n')
PY
will_record preview-build "$will_python" tools/build_preview.py "$will_out/preview" --target-dir "$will_targets/normal"
"$will_python" tools/build_preview.py "$will_out/preview" --target-dir "$will_targets/normal" >"$will_out/preview-build.log" 2>&1
will_archives=("$will_out"/preview/mqttiumd-preview-*.tar.gz)
[[ ${#will_archives[@]} == 1 && -f "${will_archives[0]}" ]] || { echo 'one preview archive required' >&2; exit 2; }
will_package=${will_archives[0]%.tar.gz}
cp -- "$will_package/mqttiumd" "$will_out/binaries/mqttiumd-normal"
cp -- "$will_package/NORMAL-FEATURES.txt" "$will_out/normal-features.txt"
cargo tree -p mqttiumd --no-default-features --features test-hooks --locked --format '{p} features={f}' >"$will_out/hooks-features.txt"
will_record hooks-build cargo build -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$will_targets/hooks"
cargo build -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$will_targets/hooks" >"$will_out/hooks-build.log" 2>&1
cp -- "$will_targets/hooks/debug/mqttiumd" "$will_out/binaries/mqttiumd-hooks"
will_trace=()
if command -v strace >/dev/null 2>&1; then
    will_trace=(strace -f -qq -s 256 -e trace=process,signal,network,flock -o)
fi
will_run() {
    local will_name=$1
    shift
    will_record "$will_name" "$@"
    if [[ ${#will_trace[@]} != 0 ]]; then
        "${will_trace[@]}" "$will_out/$will_name.strace" "$@" >"$will_out/$will_name.log" 2>&1
    else
        "$@" >"$will_out/$will_name.log" 2>&1
    fi
}
will_run normal "$will_python" tests/system/will_reception.py --binary "$will_out/binaries/mqttiumd-normal" --source "$will_sha" --output "$will_out/normal"
will_run boundaries "$will_python" tests/system/will_boundaries.py --binary "$will_out/binaries/mqttiumd-hooks" --binary-source-head "$will_sha" --out "$will_out/boundaries"
will_run preview-smoke "$will_python" tools/check_preview.py "${will_archives[0]}" "$will_out/preview-smoke"
will_demo_package=("$will_out"/preview-smoke/extracted/mqttiumd-preview-*)
[[ ${#will_demo_package[@]} == 1 && -x "${will_demo_package[0]}/will-demo.sh" ]] || { echo 'one packaged Will demo required' >&2; exit 2; }
will_demo_port=$("$will_python" - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(('127.0.0.1',0));print(sock.getsockname()[1])
PY
)
will_run package-demo env MQTTIUMD_DEMO_PORT="$will_demo_port" \
    PATH="${MQTTIUMD_MOSQUITTO_BIN_DIR:+$MQTTIUMD_MOSQUITTO_BIN_DIR:}$PATH" \
    "${will_demo_package[0]}/will-demo.sh" "$will_out/package-demo"
will_record product-event-schema-check "$will_python" tools/check_product_event_stream.py "$will_out/normal" "$will_out/boundaries" "$will_out/preview-smoke" "$will_out/package-demo" --out "$will_out/product-event-streams.json"
"$will_python" tools/check_product_event_stream.py "$will_out/normal" "$will_out/boundaries" "$will_out/preview-smoke" "$will_out/package-demo" --out "$will_out/product-event-streams.json" >"$will_out/product-event-streams.log" 2>&1
will_record core-negative-controls "$will_python" tools/check_will_core_mutants.py "$will_out/core-mutants" --target-dir "$will_targets/core-mutants"
"$will_python" tools/check_will_core_mutants.py "$will_out/core-mutants" --target-dir "$will_targets/core-mutants" >"$will_out/core-mutants.log" 2>&1
will_record store-negative-controls "$will_python" tools/run_will_store_mutants.py "$will_out/store-mutants" --target-dir "$will_targets/store-mutants"
"$will_python" tools/run_will_store_mutants.py "$will_out/store-mutants" --target-dir "$will_targets/store-mutants" >"$will_out/store-mutants.log" 2>&1
"$will_python" - "$will_out" "$will_sha" "${#will_trace[@]}" <<'PY'
from pathlib import Path
import hashlib, json, sys
sys.path.insert(0,str(Path.cwd()/'tests/system'))
from message_expiry_probe import successful_sigkill_targets
out=Path(sys.argv[1]);source=sys.argv[2]
normal=json.loads((out/'normal/result.json').read_text())
boundaries=json.loads((out/'boundaries/receipt.json').read_text())
core=json.loads((out/'core-mutants/result.json').read_text())
store=json.loads((out/'store-mutants/result.json').read_text())
smoke=json.loads((out/'preview-smoke/receipt.json').read_text())
demo=json.loads((out/'package-demo/will-demo.json').read_text())
streams=json.loads((out/'product-event-streams.json').read_text())
if (normal['status']!='PASS' or normal['source_revision']!=source or normal['client_source']['source_revision']!=source
        or normal['features']!=[] or boundaries['status']!='PASS' or boundaries['source_head']!=source
        or boundaries['harness_source_head']!=source or boundaries['binary']['features']!=['test-hooks']):
    raise SystemExit('source, features or reception verdict differ')
normal_hash=hashlib.sha256((out/'binaries/mqttiumd-normal').read_bytes()).hexdigest()
hooks_hash=hashlib.sha256((out/'binaries/mqttiumd-hooks').read_bytes()).hexdigest()
if normal_hash!=normal['binary_sha256'] or hooks_hash!=boundaries['binary']['sha256']:
    raise SystemExit('executed binary differs')
expected={'A-public-sensor','B-triggers','D-shared-responsibilities','E-takeover','E-fanout-pressure','E-no-local'}
if {row['group'] for row in normal['cases']}!=expected or len(normal['cases'])!=7:
    raise SystemExit('seven normal public/trigger/responsibility/pressure/NoLocal cases required')
public=[row for row in normal['cases'] if row['group']=='A-public-sensor']
if {(row['sensor_client'],row['dashboard_client']) for row in public}!={('mqttium','paho'),('paho','mqttium')}:
    raise SystemExit('public client roles were not inverted')
if any(row['sigkill']['wait_status']!=-9 or row['normal_stop']['wait_status']!=0 for row in public):
    raise SystemExit('actual sensor kill and normal shutdown required')
cuts={'open-prepared','open-committed','admission-prepared','admission-committed','publish-observed','finish-committed'}
if {row['criterion'] for row in boundaries['cases']}!=cuts|{'lost-end','held-end','lost-admission','held-admission'}:
    raise SystemExit('six crash boundaries and four genuine lost/held results required')
kills=[row for row in boundaries['cases'] if row.get('kill_status')==-9]
if len(kills)!=6 or len({row['killed_pid'] for row in kills})!=6 or any(not row['same_file'] for row in kills):
    raise SystemExit('six distinct own-process SIGKILL cuts on same files required')
shared=next(row for row in normal['cases'] if row['group']=='D-shared-responsibilities')
finite=next(row for row in normal['cases'] if row['group']=='E-takeover')['finite_source_expiry']
if shared['original_broker_status']!=-9 or finite['kill_status']!=-9 or not finite['same_file']:
    raise SystemExit('shared and finite-source responsibility cases require their own broker SIGKILL')
normal_kills=[{'criterion':'sensor-'+row['sensor_client'], 'pid':row['sigkill']['pid']} for row in public]
normal_kills += [{'criterion':'shared-responsibilities','pid':shared['original_broker_pid']},
                 {'criterion':'finite-source-expiry','pid':finite['killed_pid']}]
if len({row['pid'] for row in normal_kills}|{row['killed_pid'] for row in kills})!=10:
    raise SystemExit('ten distinct normal and boundary process kills required')
kernel_kills=[];normal_kernel_kills=[]
if int(sys.argv[3]):
    observed=successful_sigkill_targets((out/'boundaries.strace').read_text())
    for row in kills:
        if row['killed_pid'] not in observed: raise SystemExit('boundary SIGKILL missing in own kernel witness')
        kernel_kills.append({'criterion':row['criterion'],'pid':row['killed_pid']})
    observed=successful_sigkill_targets((out/'normal.strace').read_text())
    for row in normal_kills:
        if row['pid'] not in observed:
            raise SystemExit(f"normal SIGKILL for {row['criterion']} missing in its own kernel witness")
        normal_kernel_kills.append(row)
if (not core['passed'] or not core['restored'] or not core['cache_restored'] or core['source_revision']!=source
        or len(core['mutants'])!=4 or not all(row['detected'] and row['restored'] for row in core['mutants'])
        or store['status']!='PASS' or store['source_sha']!=source or len(store['results'])!=2
        or not all(row['detected'] and row['control_exit']==0 and row['mutated_exit']==101 for row in store['results'])):
    raise SystemExit('six compiled precise negative controls and source/cache restoration required')
if (smoke['status']!='PASS' or smoke['source_sha']!=source or smoke['binary_sha256']!=normal_hash
        or demo['status']!='PASS' or demo['source_sha']!=source or demo['binary_sha256']!=normal_hash):
    raise SystemExit('extracted preview and packaged demo must execute the same normal bytes')
if streams['status']!='PASS' or streams['product_events']<1 or streams['skipped_records']!=0:
    raise SystemExit('strict/schema validation must cover actual product streams')
index={'format':'mqttiumd.will-system-reception-index','schema_version':1,'status':'PASS',
    'qualification':'system-test-only','source_head':source,
    'source_pin':json.loads((out/'source-pin.json').read_text()),
    'source_archive':{'path':'source.tar','sha256':hashlib.sha256((out/'source.tar').read_bytes()).hexdigest()},
    'normal':{'receipt':'normal/result.json','binary_sha256':normal_hash,'features':[]},
    'boundaries':{'receipt':'boundaries/receipt.json','binary_sha256':hooks_hash,'features':['test-hooks']},
    'own_sigkill':{'boundary_count':6,'normal_sensor_count':2,'normal_broker_count':2,'total_count':10,
                   'same_run_kernel_boundaries':kernel_kills,'same_run_kernel_normal':normal_kernel_kills},
    'negative_controls':{'core':'core-mutants/result.json','store':'store-mutants/result.json','count':6},
    'preview':{'receipt':'preview-smoke/receipt.json','archive_sha256':smoke['archive_sha256'],
               'public_demo':'package-demo/will-demo.json','same_normal_binary':True},
    'product_event_schema':{'result':'product-event-streams.json','product_events':streams['product_events']},
    'system_witnesses':'same-run-strace' if int(sys.argv[3]) else 'process-status-only',
    'limitations':['Finite implementation/system evidence, not independent-oracle qualification or complete MQTT conformance.',
                  'Process SIGKILL does not establish power-loss or storage fsync durability.',
                  'A local write receipt never establishes remote MQTT receipt; actual recipient and wire observations are separate.',
                  'The supported Will is non-retained QoS1, delay0, no other Will properties.']}
(out/'index.json').write_text(json.dumps(index,indent=2,sort_keys=True)+'\n')
files=sorted(path for path in out.rglob('*') if path.is_file())
(out/'SHA256SUMS').write_text(''.join(f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(out)}\n' for path in files))
print(json.dumps({'status':'PASS','source_head':source,'index':str(out/'index.json')}))
PY
tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner \
    -czf "$will_out.tar.gz" -C "$(dirname -- "$will_out")" "$(basename -- "$will_out")"
sha256sum "$will_out.tar.gz" >"$will_out.tar.gz.sha256"
