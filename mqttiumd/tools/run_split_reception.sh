#!/bin/sh
set -eu
if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
    echo 'usage: run_split_reception.sh NEW_OUTPUT EXTRACTED_PACKAGE [short|full]' >&2
    exit 2
fi
out=$1
package=$(cd "$2" && pwd)
mode=${3:-short}
python=${PYTHON:-python3}
case "$mode" in
    short|full) ;;
    *) echo 'reception mode must be short or full' >&2; exit 2 ;;
esac
umask 077
mkdir "$out"
out=$(cd "$out" && pwd)
application_source=$("$python" - "$package" <<'PYTHON'
import json
from pathlib import Path
import sys

package = Path(sys.argv[1])
assembly = package / "PACKAGE.json"
if assembly.is_file():
    print(json.loads(assembly.read_text())["package_source_sha"])
else:
    print(json.loads((package / "BUILD.json").read_text())["source_sha"])
PYTHON
)
"$python" "$package/pilot/split_reception.py" "$out/short" --short --catchup-limit 60 --application-source "$application_source"
"$python" "$package/pilot/segment_reception.py" "$out/segments"
"$python" "$package/pilot/split_incidents.py" "$out/incidents"
"$python" "$package/pilot/split_controls.py" "$out/controls"
if [ "$mode" = full ]; then
    "$python" "$package/pilot/segment_reception.py" "$out/old-ledger" --old-ledger
    "$python" "$package/pilot/split_reception.py" "$out/decisive" --application-source "$application_source"
fi
