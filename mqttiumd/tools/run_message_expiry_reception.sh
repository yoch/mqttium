#!/usr/bin/env bash
# Source-pinned Message Expiry system reception; no oracle qualification.
set -euo pipefail
if [[ $# != 1 ]]; then echo "usage: $0 NEW_OUTPUT_DIRECTORY" >&2; exit 2; fi
msgexp_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
msgexp_repo=$(git -C "$msgexp_root" rev-parse --show-toplevel)
msgexp_python=${PYTHON:-python3}
msgexp_out=$("$msgexp_python" -c 'from pathlib import Path; import sys; print(Path(sys.argv[1]).resolve())' "$1")
case "$msgexp_out/" in "$msgexp_repo/"*) echo "output must be outside the checkout" >&2; exit 2;; esac
if [[ -z ${MQTTIUMD_MESSAGE_EXPIRY_PIN_SOURCE:-} ]]; then
    [[ -z $(git -C "$msgexp_repo" status --porcelain --untracked-files=normal) ]] || {
        echo "reception requires a clean source tree" >&2; exit 2;
    }
    msgexp_pin_sha=$(git -C "$msgexp_repo" rev-parse HEAD)
    msgexp_pin_parent=$(mktemp -d "${TMPDIR:-/tmp}/mqttiumd-message-expiry-source.XXXXXX")
    msgexp_pin_cleanup() {
        git -C "$msgexp_repo" worktree remove --force -- "$msgexp_pin_parent/source" >/dev/null 2>&1 || true
        rm -f -- "$msgexp_pin_parent/source-pin.log"
        rmdir -- "$msgexp_pin_parent" >/dev/null 2>&1 || true
    }
    trap msgexp_pin_cleanup EXIT
    git -C "$msgexp_repo" worktree add --detach -- "$msgexp_pin_parent/source" "$msgexp_pin_sha" >"$msgexp_pin_parent/source-pin.log" 2>&1
    msgexp_pin_source=$(git -C "$msgexp_pin_parent/source" rev-parse --show-toplevel)
    msgexp_pin_status=0
    MQTTIUMD_MESSAGE_EXPIRY_PIN_SOURCE="$msgexp_pin_source" MQTTIUMD_MESSAGE_EXPIRY_PIN_SHA="$msgexp_pin_sha" \
        MQTTIUMD_MESSAGE_EXPIRY_CALLER_SOURCE="$msgexp_repo" \
        "$msgexp_pin_source/mqttiumd/tools/run_message_expiry_reception.sh" "$msgexp_out" || msgexp_pin_status=$?
    exit "$msgexp_pin_status"
fi
[[ "$msgexp_repo" == "$MQTTIUMD_MESSAGE_EXPIRY_PIN_SOURCE" && $(git -C "$msgexp_repo" rev-parse HEAD) == "$MQTTIUMD_MESSAGE_EXPIRY_PIN_SHA" ]] || {
    echo "invalid Message Expiry source pin" >&2; exit 2;
}
mkdir -p -- "$msgexp_out"
[[ -z $(ls -A -- "$msgexp_out") ]] || { echo "output must be empty" >&2; exit 2; }
[[ -z $(git -C "$msgexp_repo" status --porcelain --untracked-files=normal) ]] || {
    echo "reception requires a clean pinned source" >&2; exit 2;
}
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-1} CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0
msgexp_sha=$(git -C "$msgexp_repo" rev-parse HEAD)
msgexp_targets=${MQTTIUMD_MESSAGE_EXPIRY_TARGET_ROOT:-${TMPDIR:-/tmp}/mqttiumd-message-expiry-$msgexp_sha}
mkdir -p -- "$msgexp_targets" "$msgexp_out/binaries"
cd -- "$msgexp_root"
msgexp_record() {
    local msgexp_kind=$1
    shift
    "$msgexp_python" - "$msgexp_out/commands.jsonl" "$msgexp_kind" "$PWD" "$@" <<'PY'
from pathlib import Path
import json, sys
with Path(sys.argv[1]).open('a') as stream:
    stream.write(json.dumps({'kind': sys.argv[2], 'cwd': sys.argv[3], 'command': sys.argv[4:]}) + '\n')
PY
}
msgexp_record source-pin git -C "$MQTTIUMD_MESSAGE_EXPIRY_CALLER_SOURCE" worktree add --detach "$msgexp_repo" "$msgexp_sha"
"$msgexp_python" - "$msgexp_out/source-pin.json" "$MQTTIUMD_MESSAGE_EXPIRY_CALLER_SOURCE" "$msgexp_repo" "$msgexp_sha" <<'PY'
from pathlib import Path
import json, sys
Path(sys.argv[1]).write_text(json.dumps({'caller_source': sys.argv[2], 'captured_caller_head': sys.argv[4],
    'pinned_source': sys.argv[3], 'pinned_head': sys.argv[4],
    'assumption': 'this invocation exclusively owns its private detached source tree'}, indent=2) + '\n')
PY
"$msgexp_python" - "$msgexp_out" <<'PY'
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
rustc -Vv >"$msgexp_out/rustc.txt"
cargo -V >"$msgexp_out/cargo.txt"
"$msgexp_python" -VV >"$msgexp_out/python.txt"
git rev-parse HEAD >"$msgexp_out/source-head.txt"
git diff --binary HEAD >"$msgexp_out/source.modification"
msgexp_record source-archive git -C "$msgexp_repo" archive --format=tar "$msgexp_sha"
git -C "$msgexp_repo" archive --format=tar "$msgexp_sha" >"$msgexp_out/source.tar"
msgexp_record pilot-tests "$msgexp_python" -m unittest discover -s tests/system -p 'test_message_expiry*.py' -v
"$msgexp_python" -m unittest discover -s tests/system -p 'test_message_expiry*.py' -v >"$msgexp_out/pilot-tests.log" 2>&1
PYTHONPATH="$msgexp_root/tests/system${PYTHONPATH:+:$PYTHONPATH}" "$msgexp_python" - "$msgexp_out/clients.json" <<'PY'
import json,sys
from pathlib import Path
from external_mqttium import client_identity
from external_wildcard import mosquitto_identity
Path(sys.argv[1]).write_text(json.dumps({'mqttium':client_identity(),'mosquitto':mosquitto_identity()},indent=2)+'\n')
PY
# The normal bytes come from the sole package build, then run unchanged below.
msgexp_record preview-build "$msgexp_python" tools/build_preview.py "$msgexp_out/preview" --target-dir "$msgexp_targets/normal"
"$msgexp_python" tools/build_preview.py "$msgexp_out/preview" --target-dir "$msgexp_targets/normal" >"$msgexp_out/preview-build.log" 2>&1
msgexp_archives=("$msgexp_out"/preview/mqttiumd-preview-*.tar.gz)
[[ ${#msgexp_archives[@]} == 1 && -f "${msgexp_archives[0]}" ]] || { echo 'one preview archive required' >&2; exit 2; }
msgexp_package=${msgexp_archives[0]%.tar.gz}
cp -- "$msgexp_package/mqttiumd" "$msgexp_out/binaries/mqttiumd-normal"
cp -- "$msgexp_package/NORMAL-FEATURES.txt" "$msgexp_out/normal-features.txt"
cargo tree -p mqttiumd --no-default-features --features test-hooks --locked --format '{p} features={f}' >"$msgexp_out/hooks-features.txt"
msgexp_record hooks-build cargo build -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$msgexp_targets/hooks"
cargo build -p mqttiumd --no-default-features --features test-hooks --locked --target-dir "$msgexp_targets/hooks" >"$msgexp_out/hooks-build.log" 2>&1
cp -- "$msgexp_targets/hooks/debug/mqttiumd" "$msgexp_out/binaries/mqttiumd-hooks"
msgexp_trace=()
if command -v strace >/dev/null 2>&1; then
    msgexp_trace=(strace -f -qq -s 256 -e trace=process,signal,network,flock -o)
fi
msgexp_run() {
    local msgexp_name=$1
    shift
    msgexp_record "$msgexp_name" "$@"
    if [[ ${#msgexp_trace[@]} != 0 ]]; then
        "${msgexp_trace[@]}" "$msgexp_out/$msgexp_name.strace" "$@" >"$msgexp_out/$msgexp_name.log" 2>&1
    else
        "$@" >"$msgexp_out/$msgexp_name.log" 2>&1
    fi
}
msgexp_run normal "$msgexp_python" tests/system/message_expiry_reception.py --binary "$msgexp_out/binaries/mqttiumd-normal" --out "$msgexp_out/normal" --binary-source-head "$msgexp_sha"
msgexp_run normal-restart "$msgexp_python" tests/system/message_expiry_boundaries.py --case restart --binary "$msgexp_out/binaries/mqttiumd-normal" --out "$msgexp_out/normal-restart" --binary-source-head "$msgexp_sha"
msgexp_run boundaries "$msgexp_python" tests/system/message_expiry_boundaries.py --binary "$msgexp_out/binaries/mqttiumd-hooks" --out "$msgexp_out/boundaries" --binary-source-head "$msgexp_sha"
msgexp_run preview-smoke "$msgexp_python" tools/check_preview.py "${msgexp_archives[0]}" "$msgexp_out/preview-smoke"
msgexp_demo_package=("$msgexp_out"/preview-smoke/extracted/mqttiumd-preview-*)
[[ ${#msgexp_demo_package[@]} == 1 && -x "${msgexp_demo_package[0]}/demo.sh" ]] || { echo 'one packaged public demo required' >&2; exit 2; }
msgexp_demo_port=$("$msgexp_python" - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(('127.0.0.1',0));print(sock.getsockname()[1])
PY
)
msgexp_run package-demo env MQTTIUMD_DEMO_PORT="$msgexp_demo_port" \
    PATH="${MQTTIUMD_MOSQUITTO_BIN_DIR:+$MQTTIUMD_MOSQUITTO_BIN_DIR:}$PATH" \
    "${msgexp_demo_package[0]}/demo.sh" "$msgexp_out/package-demo"
msgexp_record product-event-schema-check "$msgexp_python" tools/check_product_event_stream.py "$msgexp_out/normal" "$msgexp_out/normal-restart" "$msgexp_out/boundaries" "$msgexp_out/preview-smoke" "$msgexp_out/package-demo" --out "$msgexp_out/product-event-streams.json"
"$msgexp_python" tools/check_product_event_stream.py "$msgexp_out/normal" "$msgexp_out/normal-restart" "$msgexp_out/boundaries" "$msgexp_out/preview-smoke" "$msgexp_out/package-demo" --out "$msgexp_out/product-event-streams.json" >"$msgexp_out/product-event-streams.log" 2>&1
msgexp_record compiled-negative-controls "$msgexp_python" tests/system/message_expiry_mutants.py "$msgexp_out/mutants"
MQTTIUMD_MESSAGE_EXPIRY_MUTANT_TARGET_ROOT="${MQTTIUMD_MESSAGE_EXPIRY_MUTANT_TARGET_ROOT:-$msgexp_targets/mutants}" \
    "$msgexp_python" tests/system/message_expiry_mutants.py "$msgexp_out/mutants" >"$msgexp_out/mutants.log" 2>&1
"$msgexp_python" - "$msgexp_out" "$msgexp_sha" "${#msgexp_trace[@]}" <<'PY'
from pathlib import Path
import hashlib,json,sys
sys.path.insert(0,str(Path.cwd()/'tests/system'))
from message_expiry_probe import successful_sigkill_targets
out=Path(sys.argv[1]);source=sys.argv[2]
normal=json.loads((out/'normal/receipt.json').read_text())
boundaries=json.loads((out/'boundaries/receipt.json').read_text())
restart=json.loads((out/'normal-restart/receipt.json').read_text())
mutants=json.loads((out/'mutants/result.json').read_text())
smoke=json.loads((out/'preview-smoke/receipt.json').read_text())
streams=json.loads((out/'product-event-streams.json').read_text())
for name,receipt in [('normal',normal),('normal-restart',restart),('boundaries',boundaries)]:
    if receipt['status']!='PASS' or receipt['source_head']!=source or receipt['harness_source_head']!=source:
        raise SystemExit(f'{name} source/verdict differs')
    if hashlib.sha256(Path(receipt['binary']['path']).read_bytes()).hexdigest()!=receipt['binary']['sha256']:
        raise SystemExit(f'{name} executed binary differs')
cases={row['criterion']:row for row in normal['cases']}
if set(cases)!={'minimal','matrix','offline','original-ttl','replay32','options','late-puback','malformed','capacity'}:
    raise SystemExit('normal Message Expiry criteria missing')
if len(cases['matrix']['cells'])!=120 or len(cases['offline']['cells'])!=24 or cases['replay32']['count']!=32:
    raise SystemExit('Message Expiry matrix, offline cells or valid replay incomplete')
if len(cases['late-puback']['cases'])!=2 or len(cases['malformed']['cases'])!=24:
    raise SystemExit('real late PUBACK or malformed-property coverage incomplete')
if (cases['capacity']['full_retained_topics']!=32 or cases['capacity']['overflow']['exception']!='PublishRejectedError'
        or cases['capacity']['overflow']['reason_code']!=0x97 or not cases['capacity']['refusal_business_unchanged']):
    raise SystemExit('full retained inventory, typed quota refusal and physical capacity recovery required')
required={'transfer-claim','transfer-prepared','transfer-committed','sweep-prepared','sweep-committed',
          'normal-restart','held-admission','certain-unsent',*(f'recipe-{kind}-{phase}' for kind in ('qos1','qos0','replay')
                             for phase in ('prepared','committed'))}
if {row['criterion'] for row in boundaries['cases']}!=required:
    raise SystemExit('timed admission/replay, claim/start, physical sweep and restart boundaries required')
kills=[row for row in boundaries['cases'] if row.get('kill_status')==-9]
if len(kills)!=11 or len({row['killed_pid'] for row in kills})!=11:
    raise SystemExit('eleven distinct own SIGKILL boundary runs required')
kernel_kills=[]
if int(sys.argv[3]):
    trace=(out/'boundaries.strace').read_text()
    successful_kills=successful_sigkill_targets(trace)
    for row in kills:
        pid=row['killed_pid']
        if pid not in successful_kills:
            raise SystemExit(f'own SIGKILL for {pid} not present in the same-run kernel witness')
        kernel_kills.append({'criterion':row['criterion'],'pid':pid,'signal':'SIGKILL'})
if (restart['cases'][0]['criterion']!='normal-restart' or restart['binary']['sha256']!=normal['binary']['sha256']):
    raise SystemExit('normal restart must execute the exact normal package bytes')
expected_controls={'replay-refreshes-ttl','stale-retained-emission','expiry-frees-started-payload',
                   'offline-expiry-ignored','outgoing-expiry-property-corrupt','unknown-continuity-fresh'}
if (mutants['status']!='PASS' or mutants['source_head']!=source or not mutants['restored']
        or len(mutants['controls'])!=6 or {row['name'] for row in mutants['controls']}!=expected_controls
        or not all(row['detected'] and row['restored'] and row['positive_executed'] and row['mutant_executed']
            and row['positive_binary']['sha256']!=row['mutant_binary']['sha256'] and row['positive_status']==0
            and row['mutant_status']==101 and row['observed_reason']==row['expected_reason']
            for row in mutants['controls'])):
    raise SystemExit('six compiled controls must detect their stated properties and restore source')
if (smoke['status']!='PASS' or smoke['source_sha']!=source
        or smoke['binary_sha256']!=normal['binary']['sha256']):
    raise SystemExit('archive smoke and Message Expiry reception must execute identical normal bytes')
if streams['status']!='PASS' or streams['product_events']<1 or streams['skipped_records']!=0:
    raise SystemExit('actual product streams must pass strict/schema validation')
index={'format':'mqttiumd.message-expiry-system-reception-index','schema_version':1,
       'status':'PASS','qualification':'system-test-only','source_head':source,
       'scope':['public-Q0-Q1-Q2-input-and-Q0-Q1-output','absent-zero-one-two-u32max',
                'retained-and-nonretained','physical-offline-and-retained-reclamation',
                'original-deadline-and-decremented-wire-property','RH-RAP-NoLocal',
                '32-valid-retained-values-replayed','real-started-Q1-PUBACK-after-TTL',
                '32-finite-retained-values-expire-and-return-capacity',
                'duplicate-and-malformed-property-rejection','own-SIGKILL-boundaries',
                'certain-zero-byte-refusal-and-tag16-physical-reclamation',
                'normal-preview-archive-and-packaged-public-demo','six-compiled-negative-controls'],
       'source_pin':json.loads((out/'source-pin.json').read_text()),
       'source_archive':{'path':'source.tar','sha256':hashlib.sha256((out/'source.tar').read_bytes()).hexdigest()},
       'normal':{'receipt':'normal/receipt.json','binary':normal['binary']},
       'normal_restart':{'receipt':'normal-restart/receipt.json','same_normal_binary':True},
       'boundaries':{'receipt':'boundaries/receipt.json','binary':boundaries['binary']},
       'own_sigkill':{'count':len(kills),'same_run_kernel_kills':kernel_kills},
       'negative_controls':{'result':'mutants/result.json','restored':True,
                            'reasons':[row['expected_reason'] for row in mutants['controls']]},
       'product_event_schema':{'result':'product-event-streams.json','product_events':streams['product_events'],
                               'stream_files':len(streams['files']),'scope':'strict-json-and-schema-only'},
       'preview':{'receipt':'preview-smoke/receipt.json','archive_sha256':smoke['archive_sha256'],
                  'same_normal_binary':True,'public_demo':'package-demo'},
       'system_witnesses':'same-run-strace' if int(sys.argv[3]) else 'process-status-only',
       'limitations':['Finite system evidence, not independent-oracle qualification or complete MQTT conformance.',
                     'QoS0 publisher completion proves only a local write; recipient, committed-store or explicit-drop facts establish processing.',
                     'A missing message alone never establishes physical reference reclamation.',
                     'Actual process kill evidence does not establish power-loss or fsync durability.']}
(out/'index.json').write_text(json.dumps(index,indent=2,sort_keys=True)+'\n')
files=sorted(path for path in out.rglob('*') if path.is_file())
(out/'SHA256SUMS').write_text(''.join(f'{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(out)}\n' for path in files))
print(json.dumps({'status':'PASS','source_head':source,'index':str(out/'index.json')}))
PY
tar --sort=name --mtime='UTC 1970-01-01' --owner=0 --group=0 --numeric-owner \
    -czf "$msgexp_out.tar.gz" -C "$(dirname -- "$msgexp_out")" "$(basename -- "$msgexp_out")"
sha256sum "$msgexp_out.tar.gz" >"$msgexp_out.tar.gz.sha256"
