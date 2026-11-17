#!/bin/sh
# Execute the retained guide against the packaged normal binary; never reset data.
set -eu
preview_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
preview_data=${1:-"$PWD/retained-demo-data"}
preview_port=${MQTTIUMD_DEMO_PORT:-18884}
preview_pid=
mkdir -p -- "$preview_data"
preview_data=$(CDPATH= cd -- "$preview_data" && pwd)
cleanup() {
    if [ -n "$preview_pid" ]; then kill -INT "$preview_pid" 2>/dev/null || true; wait "$preview_pid" || true; fi
}
trap cleanup EXIT HUP INT TERM
command -v mosquitto_pub >/dev/null
command -v mosquitto_sub >/dev/null
start() {
    MQTTIUMD_LISTEN="127.0.0.1:$preview_port" "$preview_dir/start.sh" "$preview_data" >>"$preview_data/demo-starts.log" 2>&1 &
    preview_pid=$!
    # Readiness is the product event, never a fixed sleep as a ready signal.
    python3 - "$preview_data" "$preview_pid" <<'PY'
import json, os, pathlib, sys, time
root=pathlib.Path(sys.argv[1]); pid=int(sys.argv[2]); deadline=time.monotonic()+10
while time.monotonic()<deadline:
    os.kill(pid,0)
    for file in root.glob('logs/run.*/events.jsonl'):
        for line in file.read_text().splitlines():
            try: event=json.loads(line)
            except json.JSONDecodeError: continue
            if event.get('kind')=='ready' and str(event.get('pid',''))==str(pid):
                sys.exit(0)
    time.sleep(.01)
raise SystemExit('broker did not report readiness')
PY
}
stop() { kill -INT "$preview_pid"; wait "$preview_pid"; preview_pid=; }
publish() {
    mosquitto_pub -V mqttv5 -h 127.0.0.1 -p "$preview_port" -i preview-state-source \
        -q 1 -r -t "$1" -m "$2"
}
read_states() {
    mosquitto_sub -V mqttv5 -h 127.0.0.1 -p "$preview_port" -i "$1" \
        -q 1 -t 'atelier/+/temperature' -C "$2" -W 5 -F '%t|%p|%q|%r'
}
mosquitto_pub --help >"$preview_data/mosquitto-pub-version.txt"
mosquitto_sub --help >"$preview_data/mosquitto-sub-version.txt"
start
publish atelier/salle1/temperature temperature:1
publish atelier/salle2/temperature temperature:2
stop
start
read_states preview-dashboard-first 2 | sort >"$preview_data/first.txt"
printf '%s\n' 'atelier/salle1/temperature|temperature:1|1|1' 'atelier/salle2/temperature|temperature:2|1|1' >"$preview_data/expected-first.txt"
cmp "$preview_data/first.txt" "$preview_data/expected-first.txt"
publish atelier/salle1/temperature temperature:3
publish atelier/salle2/temperature ''
read_states preview-dashboard-new 1 >"$preview_data/last.txt"
printf '%s\n' 'atelier/salle1/temperature|temperature:3|1|1' >"$preview_data/expected-last.txt"
cmp "$preview_data/last.txt" "$preview_data/expected-last.txt"
stop
printf '%s\n' 'PASS: retained values survived restart; replacement and empty-payload clearing were replayed to new dashboards.'
