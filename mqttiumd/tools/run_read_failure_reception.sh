#!/usr/bin/env bash
# Receive typed durable reads and process-control failures. A successful test
# receipt is not a qualifying PASS of a business exchange with missing state.
# Run from mqttiumd/: tools/run_read_failure_reception.sh <empty directory>
set -euo pipefail
if [[ $# -ne 1 ]]; then
    echo "usage: $0 <empty directory>" >&2
    exit 2
fi
mkdir -p "$1"
out=$(cd "$1" && pwd)
[[ -z $(ls -A "$out") ]] || { echo "$out is not empty" >&2; exit 2; }
repository=$(git rev-parse --show-toplevel)
case "$out/" in
    "$repository/"*) echo "reception output must be outside the worktree" >&2; exit 2 ;;
esac

git rev-parse HEAD >"$out/revision.txt"
git diff --binary HEAD -- . >"$out/tracked-changes.patch"
rustc --version --verbose >"$out/rustc.txt"

cargo test --locked -p mqttiumd-store-sqlite --test reader_errors -- --nocapture \
    >"$out/store-readers.log" 2>&1
cargo test --locked -p mqttiumd-lab --lib review_control_probe -- --nocapture \
    >"$out/process-control.log" 2>&1
python3 -m unittest discover -t tests/oracle -s tests/oracle/tests \
    -p test_first_recovery.py >"$out/first-recovery.log" 2>&1
MQTTIUMD_READ_RECEPTION_OUT="$out/cases" \
    cargo test --locked -p mqttiumd-lab --test bench_read_failures \
    -- --exact read_failure_reception --nocapture >"$out/vertical-readers.log" 2>&1

python3 - "$out" <<'PY'
import hashlib
import json
from pathlib import Path
import sys

root = Path(sys.argv[1])
cases = root / "cases"
if not cases.is_dir() or not any(cases.iterdir()):
    sys.exit("the vertical test exported no receipt; no successful reception")
receipts = [json.loads(path.read_text()) for path in sorted(cases.glob("*/campaign.json"))]
if len(receipts) != 8:
    sys.exit(f"expected eight real child/inspector receipts, found {len(receipts)}")
for receipt in receipts:
    if (receipt["reception"] != "PASS" or not receipt["children_reaped"]
            or not receipt["writer_released"]):
        sys.exit(f"incomplete failure reception: {receipt['case']}")
    expected = "INCONCLUSIVE" if receipt["expected_code"] else "PASS"
    if receipt["business_verdict"] != expected:
        sys.exit(f"wrong business verdict: {receipt['case']}")
    cause = "/".join(filter(None, [receipt["expected_code"], receipt["expected_phase"]])) or "healthy"
    print(f"{receipt['case']}: {cause}; quick_check={receipt['quick_check']}; "
          f"pages={receipt['valid_pages']}; business={receipt['business_verdict']}; cleaned")
files = sorted(path for path in root.rglob("*") if path.is_file())
(root / "SHA256SUMS").write_text("".join(
    f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(root)}\n"
    for path in files
))
print(f"typed-read failure reception completed; evidence: {root}")
print("interrupted business exchanges remain INCONCLUSIVE or FAIL as the oracle records")
PY
