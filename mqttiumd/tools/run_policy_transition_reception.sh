#!/usr/bin/env bash
# Receive the normal package and the two offline transaction cut points.
set -euo pipefail
[[ $# == 1 || $# == 2 ]] || { echo "usage: $0 NEW_OUT [SECURE_RECEPTION]" >&2; exit 2; }
policy_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$policy_root"
policy_repo=$(git rev-parse --show-toplevel)
policy_sha=$(git rev-parse HEAD)
policy_out=$(realpath -m -- "$1")
case "$policy_out/" in "$policy_repo/"*) echo 'Use an output directory outside the checkout' >&2; exit 2;; esac
[[ -z $(git status --porcelain) && ! -e "$policy_out" ]]
policy_targets=${MQTTIUMD_POLICY_TARGET_ROOT:-"$policy_out/../mqttiumd-policy-targets-$policy_sha"}
umask 077
mkdir -p -- "$policy_out"
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2}
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
export PATH="${MQTTIUMD_MOSQUITTO_BIN_DIR:+$MQTTIUMD_MOSQUITTO_BIN_DIR:}$PATH"
git archive --format=tar HEAD > "$policy_out/source.tar"
if [[ $# == 2 ]]; then
    policy_secure=$(realpath -- "$2")
    policy_archive=("$policy_secure"/preview/mqttiumd-preview-*.tar.gz)
    policy_hooks="$policy_secure/mqttiumd-hooks"
    python - "$policy_secure" "$policy_sha" <<'PY'
import hashlib, json, sys
from pathlib import Path
root = Path(sys.argv[1])
index = json.loads((root / 'index.json').read_text())
assert index['status'] == 'PASS' and index['source_sha'] == sys.argv[2]
assert hashlib.sha256((root / 'mqttiumd-hooks').read_bytes()).hexdigest() == index['hooks_binary_sha256']
PY
else
    python tools/build_preview.py "$policy_out/preview" --target-dir "$policy_targets/normal"
    policy_archive=("$policy_out"/preview/mqttiumd-preview-*.tar.gz)
    cargo build -p mqttiumd --locked --features test-hooks --target-dir "$policy_targets/hooks" > "$policy_out/hooks-build.log" 2>&1
    policy_hooks="$policy_targets/hooks/debug/mqttiumd"
fi
[[ ${#policy_archive[@]} == 1 && -f "${policy_archive[0]}" ]]
python tools/check_preview.py "${policy_archive[0]}" "$policy_out/extracted"
policy_package=("$policy_out"/extracted/extracted/mqttiumd-preview-*)
[[ ${#policy_package[@]} == 1 && -x "${policy_package[0]}/mqttiumd" ]]
policy_binary="${policy_package[0]}/mqttiumd"
cp -- "$policy_hooks" "$policy_out/mqttiumd-hooks"
python "${policy_package[0]}/policy-demo.py" "$policy_out/demo"
python tests/system/policy_transition_reception.py --binary "$policy_binary" --out "$policy_out/normal"
policy_trace=()
if command -v strace >/dev/null; then
    policy_trace=(strace -f -qq -e trace=process,signal -o "$policy_out/crashes.strace")
fi
"${policy_trace[@]}" python tests/system/policy_transition_reception.py --binary "$policy_binary" \
    --hooks "$policy_out/mqttiumd-hooks" --mode crash --out "$policy_out/crashes"
python tools/run_policy_mutants.py "$policy_out/mutants" --fixture-binary "$policy_binary" --target "$policy_targets/mutants"
python tools/check_product_event_stream.py "$policy_out/demo" "$policy_out/normal" "$policy_out/crashes" --out "$policy_out/events-check.json"
python - "$policy_out" "$policy_sha" "${#policy_trace[@]}" <<'PY'
import hashlib, json, sys
from pathlib import Path
sys.path.insert(0, str(Path.cwd() / 'tests/system'))
from message_expiry_probe import successful_sigkill_targets

out, source = Path(sys.argv[1]), sys.argv[2]
read = lambda path: json.loads((out / path).read_text())
receipts = {name: read(path) for name, path in {
    'demo': 'demo/policy-demo.json', 'normal': 'normal/receipt.json', 'crashes': 'crashes/receipt.json',
    'mutants': 'mutants/receipt.json', 'extracted': 'extracted/receipt.json',
}.items()}
assert all(x['status'] == 'PASS' for x in receipts.values())
identity = receipts['extracted']
assert identity['source_sha'] == receipts['demo']['source_sha'] == receipts['mutants']['source_sha'] == source
assert len({identity['binary_sha256'], *(receipts[m]['binary_sha256'] for m in ('demo','normal','crashes'))}) == 1
cuts = receipts['crashes']['crashes']
assert len(cuts) == 2 and all(c['signal'] == -9 for c in cuts)
traced = int(sys.argv[3]) > 0
if traced:
    assert {c['pid'] for c in cuts} <= successful_sigkill_targets((out / 'crashes.strace').read_text())
files = sorted(p for p in out.rglob('*') if p.is_file())
assert not any(p.suffix in ('.key', '.pfx', '.p12', '.crt') for p in files)
index = {
    'status': 'PASS', 'source_sha': source, 'normal_binary_sha256': identity['binary_sha256'],
    'archive_sha256': identity['archive_sha256'],
    'source_sha256': hashlib.sha256((out / 'source.tar').read_bytes()).hexdigest(),
    'hooks_binary_sha256': hashlib.sha256((out / 'mqttiumd-hooks').read_bytes()).hexdigest(),
    'process_witness': 'same-run-strace' if traced else 'child-wait-status',
    'apply_kill_pids': [c['pid'] for c in cuts],
}
(out / 'index.json').write_text(json.dumps(index, indent=2) + '\n')
files.append(out / 'index.json')
(out / 'SHA256SUMS').write_text(''.join(f'{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.relative_to(out)}\n' for p in files))
print(json.dumps(index))
PY
