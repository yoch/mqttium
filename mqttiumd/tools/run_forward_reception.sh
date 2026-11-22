#!/bin/sh
set -eu
if [ "$#" -lt 2 ] || [ "$#" -gt 3 ]; then
    echo 'usage: run_forward_reception.sh NEW_OUTPUT EXTRACTED_PACKAGE [short|full]' >&2
    exit 2
fi
out=$1
package=$(cd "$2" && pwd)
mode=${3:-short}
case "$mode" in
    short|full) ;;
    *) echo 'reception mode must be short or full' >&2; exit 2 ;;
esac
python=${PYTHON:-python3}
umask 077
mkdir "$out"
out=$(cd "$out" && pwd)
"$python" "$package/pilot/pressure.py" "$out/direct-pressure"
"$python" "$package/pilot/forward_checks.py"
"$python" "$package/pilot/forward_controls.py" "$out/controls"
"$python" "$package/pilot/forward_reception.py" "$out/short" --short --catchup-limit 60
"$python" "$package/pilot/forward_faults.py" "$out/faults"
if [ "$mode" = full ]; then
    "$python" "$package/pilot/forward_reception.py" "$out/decisive"
fi
