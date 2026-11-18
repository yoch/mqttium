#!/bin/sh
set -eu
if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
    echo 'usage: run_prepilot_reception.sh NEW_OUTPUT EXTRACTED_PACKAGE [SECONDS]' >&2
    exit 2
fi
out=$1
package=$(cd "$2" && pwd)
seconds=${3:-1800}
python=${PYTHON:-python3}
umask 077
mkdir "$out"
out=$(cd "$out" && pwd)
private=$(mktemp -d)
trap 'rm -rf "$private"' EXIT HUP INT TERM
"$python" - "$package" "$private/pki" <<'PY'
from pathlib import Path
import sys
sys.path.insert(0,str(Path(sys.argv[1])/"pilot"))
from common import provision
provision(Path(sys.argv[2]), Path(sys.argv[1]))
PY
"$python" "$package/pilot/load.py" "$out/normal" --seconds "$seconds" --pki "$private/pki"
"$python" "$package/pilot/load.py" "$out/one-kib" --seconds 120 --size 1024
"$python" "$package/pilot/perturb.py" "$out/perturbations"
"$python" "$package/pilot/restore.py" "$out/quiet-restore" --mode quiet --db "$out/normal/broker.sqlite" --pki "$private/pki"
"$python" "$package/pilot/restore.py" "$out/charged-restore" --mode charged
