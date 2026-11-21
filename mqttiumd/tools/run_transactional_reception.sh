#!/usr/bin/env bash
# Fresh qualifying K1/K2b/K3 campaigns, separate system witnesses, and one
# semantic negative control per point. Generated evidence stays outside git.
# Usage from mqttiumd/: tools/run_transactional_reception.sh <empty directory> [seed]
set -euo pipefail
if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "usage: $0 <empty directory> [seed]" >&2
    exit 2
fi
mkdir -p "$1"
out=$(cd "$1" && pwd)
seed=${2:-1}
[[ -z $(ls -A "$out") ]] || { echo "$out is not empty" >&2; exit 2; }
for point in K1 K2b K3; do
    tools/run_killed_campaign.sh "tests/scenarios/crash/$point.json" "$out/$point" "$seed"
    python3 tools/check_transactional_negative.py "$out/$point/qualifying" "$out/$point/negative"
done
python3 - "$out" "$seed" <<'PY'
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1])
summary = {"seed": sys.argv[2] if len(sys.argv) > 2 else "1", "points": {}}
for point in ("K1", "K2b", "K3"):
    directory = root / point
    positive = json.loads((directory / "qualifying/qualifying-run-result.json").read_text())
    negative = json.loads((directory / "negative/negative-control.json").read_text())
    summary["points"][point] = {
        "qualification": positive["qualification"], "verdict": positive["verdict"],
        "negative_detected": negative["detected_for_expected_reason"],
        "system_witness_scope": f"{point}/witnessed only",
        "system_witness_present": (directory / "kill-witness.strace").exists(),
        "source_sha256": hashlib.sha256((directory / "qualifying/source.tar").read_bytes()).hexdigest(),
    }
(root / "reception.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
PY
