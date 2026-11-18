#!/usr/bin/env bash
# Run the packaged normal broker, fault-enabled broker and isolated mutants.
set -euo pipefail
[[ $# == 1 || $# == 2 ]] || { echo "usage: $0 NEW_OUT [SECURE_RECEPTION]" >&2; exit 2; }
cd "$(dirname -- "${BASH_SOURCE[0]}")/.."
repo=$(git rev-parse --show-toplevel)
sha=$(git rev-parse HEAD)
out=$(realpath -m -- "$1")
case "$out/" in "$repo/"*) echo 'Use an output directory outside the checkout' >&2; exit 2;; esac
[[ -z $(git status --porcelain) && ! -e "$out" ]]
targets=${MQTTIUMD_RETAINED_WILL_TARGET_ROOT:-"$out/../mqttiumd-retained-will-targets-$sha"}
umask 077
mkdir -p -- "$out"
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export PATH="${MQTTIUMD_MOSQUITTO_BIN_DIR:+$MQTTIUMD_MOSQUITTO_BIN_DIR:}$PATH"
git archive --format=tar HEAD > "$out/source.tar"
if [[ $# == 2 ]]; then
    secure=$(realpath -- "$2")
    archive=("$secure"/preview/mqttiumd-preview-*.tar.gz)
    hooks="$secure/mqttiumd-hooks"
    python - "$secure" "$sha" <<'PY'
import hashlib, json, sys
from pathlib import Path
path = Path(sys.argv[1])
index = json.loads((path / 'index.json').read_text())
assert index['status'] == 'PASS' and index['source_sha'] == sys.argv[2]
assert index['hooks_binary_sha256'] == hashlib.sha256((path / 'mqttiumd-hooks').read_bytes()).hexdigest()
PY
else
    python tools/build_preview.py "$out/preview" --target-dir "$targets/normal"
    archive=("$out"/preview/mqttiumd-preview-*.tar.gz)
    cargo build -p mqttiumd --locked --features test-hooks --target-dir "$targets/hooks" > "$out/hooks-build.log" 2>&1
    hooks="$targets/hooks/debug/mqttiumd"
fi
[[ ${#archive[@]} == 1 && -f "${archive[0]}" ]]
python tools/check_preview.py "${archive[0]}" "$out/extracted"
package=("$out"/extracted/extracted/mqttiumd-preview-*)
[[ ${#package[@]} == 1 && -x "${package[0]}/mqttiumd" ]]
cp -- "$hooks" "$out/mqttiumd-hooks"
python "${package[0]}/retained-will-demo.py" "$out/demo"
python tests/system/retained_will_reception.py --binary "${package[0]}/mqttiumd" --source "$sha" --out "$out/normal"
trace=()
if command -v strace >/dev/null; then
    trace=(strace -f -qq -e trace=process,signal -o "$out/crashes.strace")
fi
"${trace[@]}" python tests/system/will_boundaries.py --binary "$out/mqttiumd-hooks" \
    --binary-source-head "$sha" --retained --out "$out/crashes"
python tools/run_retained_will_mutants.py "$out/mutants" --target "$targets/mutants"
python tools/check_product_event_stream.py "$out/demo" "$out/normal" "$out/crashes" --out "$out/events-check.json"
python - "$out" "$sha" "${#trace[@]}" <<'PY'
import hashlib, json, sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'tests/system'))
from message_expiry_probe import successful_sigkill_targets

out, source = Path(sys.argv[1]), sys.argv[2]
read = lambda path: json.loads((out / path).read_text())
demo, normal, crashes, mutants, extracted = [read(name) for name in (
    'demo/retained-will-demo.json', 'normal/receipt.json', 'crashes/receipt.json',
    'mutants/receipt.json', 'extracted/receipt.json')]
assert all(r['status'] == 'PASS' for r in (demo, normal, crashes, mutants, extracted))
assert source == demo['build']['source_sha'] == normal['source_revision'] == crashes['source_head'] == mutants['source_revision'] == extracted['source_sha']
assert extracted['binary_sha256'] == demo['binary_sha256'] == normal['binary_sha256']
cuts = [case for case in crashes['cases'] if 'killed_pid' in case]
assert len(cuts) == 6 and all(case['kill_status'] == -9 and case['same_file'] for case in cuts)
traced = int(sys.argv[3]) > 0
if traced:
    assert {case['killed_pid'] for case in cuts} <= successful_sigkill_targets((out / 'crashes.strace').read_text())
files = sorted(p for p in out.rglob('*') if p.is_file())
assert not any(p.suffix in ('.key', '.pfx', '.p12', '.crt') for p in files)
index = {'status': 'PASS', 'source_sha': source, 'normal_binary_sha256': extracted['binary_sha256'],
         'archive_sha256': extracted['archive_sha256'], 'hooks_binary_sha256': crashes['binary']['sha256'],
         'source_sha256': hashlib.sha256((out / 'source.tar').read_bytes()).hexdigest(),
         'process_witness': 'same-run-strace' if traced else 'child-wait-status',
         'crash_cut_pids': [case['killed_pid'] for case in cuts]}
(out / 'index.json').write_text(json.dumps(index, indent=2) + '\n')
files.append(out / 'index.json')
(out / 'SHA256SUMS').write_text(''.join(f'{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.relative_to(out)}\n' for p in files))
print(json.dumps(index))
PY
