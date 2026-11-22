#!/bin/sh
set -eu

out=$1
package=$2
mode=${3:-short}
python=${PYTHON:-python3}
umask 077
mkdir "$out"
out=$(cd "$out" && pwd)
package=$(cd "$package" && pwd)
source_sha=$("$python" -c 'import json,sys; print(json.load(open(sys.argv[1]))["package_source_sha"])' "$package/PACKAGE.json")
pilot="$package/pilot"

case "$mode" in
short)
    "$python" "$pilot/rc_reception.py" "$out/site" --duration 16 --outage 10 --catchup-limit 60 --source "$source_sha"
    "$python" "$pilot/site_guard_reception.py" "$out/guards"
    "$python" "$pilot/site_pressure_reception.py" "$out/pressure"
    "$python" "$pilot/storage_check.py" "$out/storage" --samples 64
    "$python" "$pilot/archive_scale.py" "$out/archive" --events 4096 --ceiling 24
    ;;
full)
    MQTTIUMD_DEMO_PKI_DAYS=7 "$python" "$pilot/rc_reception.py" "$out/site" --source "$source_sha"
    "$python" "$pilot/archive_scale.py" "$out/archive-dense" --events 2097152 --ceiling 4104
    "$python" "$pilot/archive_scale.py" "$out/archive-catalog" --events 4096 --ceiling 32768
    "$python" "$pilot/storage_check.py" "$out/storage"
    ;;
*)
    echo 'usage: run-site-reception.sh NEW_OUTPUT EXTRACTED_PACKAGE [short|full]' >&2
    exit 2
    ;;
esac
