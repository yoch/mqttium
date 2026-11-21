#!/usr/bin/env bash
# Build the normal artifact once and exercise those bytes on loopback.
set -euo pipefail
[[ $# == 1 ]] || { echo "usage: $0 NEW_OUTPUT_DIRECTORY" >&2; exit 2; }
secure_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
secure_repo=$(git -C "$secure_root" rev-parse --show-toplevel)
secure_out=$(realpath -m -- "$1")
case "$secure_out/" in "$secure_repo/"*) echo 'Use a directory outside the checkout' >&2; exit 2;; esac
[[ ! -e "$secure_out" ]] || { echo 'Output must be new' >&2; exit 2; }
[[ -z $(git -C "$secure_repo" status --porcelain) ]] || { echo 'Source must be clean' >&2; exit 2; }
mkdir -p -- "$secure_out"
secure_source=$(git -C "$secure_repo" rev-parse HEAD)
secure_targets=${MQTTIUMD_SECURE_TARGET_ROOT:-"$secure_out/../mqttiumd-secure-targets-$secure_source"}
secure_pinned=$(mktemp -d "${TMPDIR:-/tmp}/mqttiumd-secure-source.XXXXXXXX")
git -C "$secure_repo" worktree add --detach "$secure_pinned/source" "$secure_source" >/dev/null
trap 'git -C "$secure_repo" worktree remove --force "$secure_pinned/source"; rmdir "$secure_pinned"' EXIT
cd "$secure_pinned/source/mqttiumd"
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export PATH="${MQTTIUMD_MOSQUITTO_BIN_DIR:+$MQTTIUMD_MOSQUITTO_BIN_DIR:}$PATH"
git archive --format=tar HEAD > "$secure_out/source.tar"
printf '%s\n' "$secure_source" > "$secure_out/source-head.txt"
python tools/build_preview.py "$secure_out/preview" --target-dir "$secure_targets/normal"
secure_archive=("$secure_out"/preview/mqttiumd-preview-*.tar.gz)
[[ ${#secure_archive[@]} == 1 && -f "${secure_archive[0]}" ]]
secure_package=${secure_archive[0]%.tar.gz}
secure_wire=()
if command -v strace >/dev/null; then
    secure_wire=(strace -qq -f -e trace=process,signal -o)
fi
secure_run() {
    local stream=$1
    shift
    if [[ ${#secure_wire[@]} != 0 ]]; then
        "${secure_wire[@]}" "$secure_out/$stream.strace" "$@" > "$secure_out/$stream.log" 2>&1
    else
        "$@" > "$secure_out/$stream.log" 2>&1
    fi
}
secure_run normal python tests/system/secure_reception.py --binary "$secure_package/mqttiumd" --out "$secure_out/normal"
python tests/system/secure_reception.py --mode measurement --binary "$secure_package/mqttiumd" --out "$secure_out/measurement"
cargo build -p mqttiumd --locked --features test-hooks --target-dir "$secure_targets/hooks" > "$secure_out/hooks-build.log" 2>&1
cp "$secure_targets/hooks/debug/mqttiumd" "$secure_out/mqttiumd-hooks"
secure_run hooks python tests/system/secure_reception.py --mode hooks --binary "$secure_out/mqttiumd-hooks" --out "$secure_out/hooks"
python tools/run_secure_mutants.py "$secure_out/mutants" --target "$secure_targets/mutants"
python tools/check_preview.py "${secure_archive[0]}" "$secure_out/extracted"
secure_demo_package=("$secure_out"/extracted/extracted/mqttiumd-preview-*)
[[ ${#secure_demo_package[@]} == 1 ]]
secure_port=$(python - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(('127.0.0.1',0))
    print(sock.getsockname()[1])
PY
)
secure_run sensor-demo env MQTTIUMD_DEMO_PORT="$secure_port" "${secure_demo_package[0]}/secure-demo.sh" "$secure_out/sensor-demo"
python tools/check_product_event_stream.py "$secure_out/normal" "$secure_out/hooks" "$secure_out/sensor-demo" "$secure_out/extracted" --out "$secure_out/events-check.json" > "$secure_out/events-check.log"
python - "$secure_out" "$secure_source" "${#secure_wire[@]}" <<'PY'
from pathlib import Path
import hashlib,json,sys
sys.path.insert(0, str(Path.cwd()/'tests/system'))
from message_expiry_probe import successful_sigkill_targets

out, source = Path(sys.argv[1]), sys.argv[2]
read = lambda path: json.loads((out/path).read_text())
results = {name:read(name+'/receipt.json') for name in ('normal','hooks','measurement','mutants','extracted')}
demo = read('sensor-demo/will-demo.json')
sha = results['extracted']['binary_sha256']
assert results['extracted']['source_sha'] == demo['source_sha'] == source
assert all(result['status']=='PASS' for result in (*results.values(), demo))
assert sha == demo['binary_sha256'] == results['normal']['binary_sha256'] == results['measurement']['binary_sha256']
assert demo['transport']=='mtls-tls13'
script_kills = [e['pid'] for e in demo['observations'] if e.get('wait_status')==-9]
broker_kill = results['normal']['groups']['authority']['pid']
assert len(script_kills)==2 and results['normal']['groups']['authority']['signal']==-9
system_witness = int(sys.argv[3]) != 0
if system_witness:
    assert set(script_kills) <= successful_sigkill_targets((out/'sensor-demo.strace').read_text())
    assert broker_kill in successful_sigkill_targets((out/'normal.strace').read_text())
files = sorted(p for p in out.rglob('*') if p.is_file())
assert not any(p.suffix in ('.key','.p12','.pfx') for p in files)
for item in files:
    if item.suffix in ('.jsonl','.log','.json','.stderr','.stdout','.pem','.crt'):
        data=item.read_bytes()
        assert not any(label in data for label in (b'-----BEGIN PRIVATE KEY-----', b'-----BEGIN EC PRIVATE KEY-----',
                                                   b'-----BEGIN RSA PRIVATE KEY-----')), item
index={'status':'PASS','source_sha':source,'normal_binary_sha256':sha,
       'hooks_binary_sha256':results['hooks']['binary_sha256'],
       'normal_archive_sha256':results['extracted']['archive_sha256'],
       'sources_sha256':hashlib.sha256((out/'source.tar').read_bytes()).hexdigest(),
       'qualification':'bounded product system reception',
       'process_witness':'same-run-strace' if system_witness else 'observed-process-wait-status',
       'sensor_kill_pids':script_kills,'broker_kill_pid':broker_kill,
       'receipts':{name:name+'/receipt.json' for name in results},
       'sensor_demo':'sensor-demo/will-demo.json','events':'events-check.json'}
(out/'index.json').write_text(json.dumps(index,indent=2,sort_keys=True)+'\n')
files.append(out/'index.json')
(out/'SHA256SUMS').write_text(''.join(f'{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.relative_to(out)}\n' for p in files))
print(json.dumps(index))
PY
