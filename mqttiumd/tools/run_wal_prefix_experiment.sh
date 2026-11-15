#!/usr/bin/env bash
# Artificial storage-image exploration, separate from real process kills.
# Usage from mqttiumd/: tools/run_wal_prefix_experiment.sh /tmp/wal-prefixes
set -euo pipefail
out=${1:?usage: run_wal_prefix_experiment.sh /absolute/new-output-directory}
if [[ "$out" != /* || -e "$out" ]]; then
    echo 'output must be an absolute path that does not exist' >&2
    exit 2
fi
MQTTIUMD_WAL_PREFIX_OUT="$out" cargo test --locked -p mqttiumd-store-sqlite \
    --test wal_cuts -- --exact every_cut_of_the_wal_of_a_commit_recovers_before_or_after_it --nocapture
python3 - "$out" <<'PY'
import hashlib
import json
from pathlib import Path
import subprocess
import sys
root = Path(sys.argv[1])
metadata = {
    "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
    "tracked_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD", "--"])).hexdigest(),
    "command": ["tools/run_wal_prefix_experiment.sh", str(root)],
    "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
    "cargo": subprocess.check_output(["cargo", "--version"], text=True).strip(),
    "scope": "artificial storage images; no oracle qualifying result is claimed",
}
(root / "provenance.json").write_text(json.dumps(metadata, indent=2) + "\n")
files = sorted(path for path in root.rglob("*") if path.is_file())
(root / "SHA256SUMS").write_text("".join(
    f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(root)}\n"
    for path in files
))
report = json.loads((root / "report.json").read_text())
print(f"WAL images: {len(report['trials'])} trials; {report['before_count']} before, {report['after_count']} after, {report['errors']} errors")
PY
