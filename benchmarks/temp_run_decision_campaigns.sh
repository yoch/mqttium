#!/usr/bin/env bash
set -u -o pipefail
ROOT="$RUNNER_TEMP/scheduler-decision"
mkdir -p "$ROOT"/{pr284,pr285,pr286}
STATUS="$ROOT/status.tsv"; : > "$STATUS"

note(){ printf '\n===== %s =====\n' "$*"; }
run(){ local name="$1"; shift; note "$name"; "$@"; local rc=$?; printf '%s\t%s\n' "$name" "$rc" >> "$STATUS"; return 0; }
probe_cold(){
  local out="$1"; sleep 60
  for attempt in 1 2 3; do
    if python base/benchmarks/runner_probe.py --output "$out" --require-temperature --enforce; then return 0; fi
    [ "$attempt" = 3 ] && return 1
    sleep 120
  done
}
start_broker(){
  local dir="$1" port="$2"
  cat > "$dir/mosquitto.conf" <<MOSQ
persistence false
allow_anonymous true
max_inflight_messages 1000
max_queued_messages 100000
max_queued_bytes 0
connection_messages false
log_type error
listener $port 127.0.0.1
MOSQ
  mosquitto -c "$dir/mosquitto.conf" > "$dir/mosquitto.log" 2>&1 &
  BROKER_PID=$!
  for _ in $(seq 1 50); do (echo > /dev/tcp/127.0.0.1/$port) >/dev/null 2>&1 && return 0; sleep .2; done
  return 1
}
stop_broker(){ kill "${BROKER_PID:-0}" 2>/dev/null || true; wait "${BROKER_PID:-0}" 2>/dev/null || true; unset BROKER_PID; }
trap stop_broker EXIT

note "immutable refs"
test "$(git -C base rev-parse HEAD)" = "$BASE_SHA" || exit 10
test "$(git -C pr284 rev-parse HEAD)" = "$PR284_SHA" || exit 11
test "$(git -C pr285 rev-parse HEAD)" = "$PR285_SHA" || exit 12
test "$(git -C pr286 rev-parse HEAD)" = "$PR286_SHA" || exit 13
test "$(uname -m)" = aarch64 || exit 14
if pgrep -a mosquitto; then echo pre-existing-mosquitto >&2; exit 15; fi

# ---- PR284 ----
note "PR284 cold preflight"
probe_cold "$ROOT/pr284/preflight.json" || printf 'PR284-preflight\t2\n' >> "$STATUS"
start_broker "$ROOT/pr284" 11884 || exit 20
run PR284-tests env PYTHONPATH=pr284/src python -m pytest -q pr284/tests/unit/test_write_pump_targeted_wake.py
run PR284-writer-AA python base/benchmarks/paired_writer_capacity.py --base-root base --candidate-root base --host 127.0.0.1 --port 11884 --protocol 311 --payload-bytes 64 --inflight 20 --outstanding 64 --max-queued 200 --warmup-count 3000 --count-qos0 6000 --count-qos1 6000 --repeat 8 --cpu 1 --policy strict --preflight-report "$ROOT/pr284/preflight.json" --max-baseline-cv .05 --max-aa-ratio-deviation .02 --output "$ROOT/pr284/writer-aa.json" --summary-output "$ROOT/pr284/writer-aa.md"
run PR284-writer-AB python base/benchmarks/paired_writer_capacity.py --base-root base --candidate-root pr284 --host 127.0.0.1 --port 11884 --protocol 311 --payload-bytes 64 --inflight 20 --outstanding 64 --max-queued 200 --warmup-count 3000 --count-qos0 6000 --count-qos1 6000 --repeat 8 --cpu 1 --policy strict --preflight-report "$ROOT/pr284/preflight.json" --max-baseline-cv .05 --min-completed-ratio .97 --output "$ROOT/pr284/writer-ab.json" --summary-output "$ROOT/pr284/writer-ab.md"
run PR284-hetero-AA python harness/benchmarks/temp_scheduler_supplement.py writer-hetero-parent --base-root base --candidate-root base --producer-values 8,12,16,20,32 --repeat 16 --count 20000 --max-messages 8 --max-bytes 16384 --small-bytes 64 --large-bytes 4096 --cpu 1 --output "$ROOT/pr284/hetero-focus-aa.json"
run PR284-hetero-AB python harness/benchmarks/temp_scheduler_supplement.py writer-hetero-parent --base-root base --candidate-root pr284 --producer-values 8,12,16,20,32 --repeat 16 --count 20000 --max-messages 8 --max-bytes 16384 --small-bytes 64 --large-bytes 4096 --cpu 1 --output "$ROOT/pr284/hetero-focus-ab.json"
run PR284-openloop python base/benchmarks/paired_open_loop.py --base-root base --candidate-root pr284 --host 127.0.0.1 --port 11884 --protocols 311 --payloads 64 --completions callback --windows 20 --target-rates 5000,7500 --count-small 5000 --repeat 8 --cpu 1 --policy strict --preflight-report "$ROOT/pr284/preflight.json" --max-baseline-cv .05 --min-completed-ratio .97 --max-loop-lag-ratio 1.05 --output "$ROOT/pr284/open-loop.json" --summary-output "$ROOT/pr284/open-loop.md"
run PR284-network python base/benchmarks/paired_network.py --base-root base --candidate-root pr284 --host 127.0.0.1 --port 11884 --protocols 311 --completions receipt --payloads 64,4096 --windows 20,64 --repeat 8 --count-small 1500 --count-large 750 --cpu 1 --policy strict --preflight-report "$ROOT/pr284/preflight.json" --max-baseline-cv .05 --min-ack-ratio .97 --output "$ROOT/pr284/network.json" --summary-output "$ROOT/pr284/network.md"
stop_broker

# ---- PR285 ----
note "PR285 cold preflight"
probe_cold "$ROOT/pr285/preflight.json" || printf 'PR285-preflight\t2\n' >> "$STATUS"
start_broker "$ROOT/pr285" 11885 || exit 30
run PR285-tests env PYTHONPATH=pr285/src python -m pytest -q pr285/tests/unit/test_publish_targeted_wake.py pr285/tests/unit/test_async_publish_admission.py
run PR285-writer-AA python base/benchmarks/paired_writer_capacity.py --base-root base --candidate-root base --host 127.0.0.1 --port 11885 --protocol 311 --payload-bytes 64 --inflight 20 --outstanding 64 --max-queued 200 --warmup-count 3000 --count-qos0 6000 --count-qos1 6000 --repeat 8 --cpu 1 --policy strict --preflight-report "$ROOT/pr285/preflight.json" --max-baseline-cv .05 --max-aa-ratio-deviation .02 --output "$ROOT/pr285/writer-aa.json" --summary-output "$ROOT/pr285/writer-aa.md"
run PR285-writer-AB python base/benchmarks/paired_writer_capacity.py --base-root base --candidate-root pr285 --host 127.0.0.1 --port 11885 --protocol 311 --payload-bytes 64 --inflight 20 --outstanding 64 --max-queued 200 --warmup-count 3000 --count-qos0 6000 --count-qos1 6000 --repeat 8 --cpu 1 --policy strict --preflight-report "$ROOT/pr285/preflight.json" --max-baseline-cv .05 --min-completed-ratio .97 --output "$ROOT/pr285/writer-ab.json" --summary-output "$ROOT/pr285/writer-ab.md"
run PR285-true-AA python harness/benchmarks/temp_scheduler_supplement.py publish-parent --base-root base --candidate-root base --host 127.0.0.1 --port 11885 --publisher-values 1,4,16,64,256 --inflight-values 1,4,20 --count 4000 --repeat 8 --cpu 1 --timeout 120 --output "$ROOT/pr285/true-aa.json"
run PR285-true-AB python harness/benchmarks/temp_scheduler_supplement.py publish-parent --base-root base --candidate-root pr285 --host 127.0.0.1 --port 11885 --publisher-values 1,4,16,64,256 --inflight-values 1,4,20 --count 4000 --repeat 8 --cpu 1 --timeout 120 --output "$ROOT/pr285/true-ab.json"
run PR285-mixed-AA python harness/benchmarks/temp_decision_supplement.py mixed-parent --base-root base --candidate-root base --host 127.0.0.1 --port 11885 --producer-values 4,16,64 --inflight 4 --batch-size 8 --ops 200 --repeat 8 --cpu 1 --timeout 120 --output "$ROOT/pr285/mixed-aa.json"
run PR285-mixed-AB python harness/benchmarks/temp_decision_supplement.py mixed-parent --base-root base --candidate-root pr285 --host 127.0.0.1 --port 11885 --producer-values 4,16,64 --inflight 4 --batch-size 8 --ops 200 --repeat 8 --cpu 1 --timeout 120 --output "$ROOT/pr285/mixed-ab.json"
run PR285-openloop python base/benchmarks/paired_open_loop.py --base-root base --candidate-root pr285 --host 127.0.0.1 --port 11885 --protocols 311 --payloads 64 --completions callback --windows 20 --target-rates 5000,7500 --count-small 5000 --repeat 8 --cpu 1 --policy strict --preflight-report "$ROOT/pr285/preflight.json" --max-baseline-cv .05 --min-completed-ratio .97 --max-loop-lag-ratio 1.05 --output "$ROOT/pr285/open-loop.json" --summary-output "$ROOT/pr285/open-loop.md"
run PR285-network python base/benchmarks/paired_network.py --base-root base --candidate-root pr285 --host 127.0.0.1 --port 11885 --protocols 311 --completions receipt --payloads 64,4096 --windows 20,64 --repeat 8 --count-small 1500 --count-large 750 --cpu 1 --policy strict --preflight-report "$ROOT/pr285/preflight.json" --max-baseline-cv .05 --min-ack-ratio .97 --output "$ROOT/pr285/network.json" --summary-output "$ROOT/pr285/network.md"
stop_broker

# ---- PR286 ----
note "PR286 cold preflight"
probe_cold "$ROOT/pr286/preflight.json" || printf 'PR286-preflight\t2\n' >> "$STATUS"
start_broker "$ROOT/pr286" 11886 || exit 40
run PR286-tests env PYTHONPATH=pr286/src python -m pytest -q pr286/tests/unit/test_write_pump_byte_quantum.py
run PR286-writer-AA python base/benchmarks/paired_writer_capacity.py --base-root base --candidate-root base --host 127.0.0.1 --port 11886 --protocol 311 --payload-bytes 64 --inflight 20 --outstanding 64 --max-queued 200 --warmup-count 3000 --count-qos0 6000 --count-qos1 6000 --repeat 8 --cpu 1 --policy strict --preflight-report "$ROOT/pr286/preflight.json" --max-baseline-cv .05 --max-aa-ratio-deviation .02 --output "$ROOT/pr286/writer-aa.json" --summary-output "$ROOT/pr286/writer-aa.md"
run PR286-writer-AB python base/benchmarks/paired_writer_capacity.py --base-root base --candidate-root pr286 --host 127.0.0.1 --port 11886 --protocol 311 --payload-bytes 64 --inflight 20 --outstanding 64 --max-queued 200 --warmup-count 3000 --count-qos0 6000 --count-qos1 6000 --repeat 8 --cpu 1 --policy strict --preflight-report "$ROOT/pr286/preflight.json" --max-baseline-cv .05 --min-completed-ratio .97 --output "$ROOT/pr286/writer-ab.json" --summary-output "$ROOT/pr286/writer-ab.md"
run PR286-openloop python base/benchmarks/paired_open_loop.py --base-root base --candidate-root pr286 --host 127.0.0.1 --port 11886 --protocols 311 --payloads 64 --completions callback --windows 20 --target-rates 5000,7500 --count-small 5000 --repeat 8 --cpu 1 --policy strict --preflight-report "$ROOT/pr286/preflight.json" --max-baseline-cv .05 --min-completed-ratio .97 --max-loop-lag-ratio 1.05 --output "$ROOT/pr286/open-loop.json" --summary-output "$ROOT/pr286/open-loop.md"
run PR286-tail-AA python harness/benchmarks/temp_decision_supplement.py quantum-parent --base-root base --candidate-root base --host 127.0.0.1 --port 11886 --quantum-values 0 --flood-count 1000 --flood-bytes 32768 --probes 100 --repeat 8 --cpu 1 --timeout 120 --output "$ROOT/pr286/tail-aa.json"
run PR286-tail-frontier python harness/benchmarks/temp_decision_supplement.py quantum-parent --base-root base --candidate-root pr286 --host 127.0.0.1 --port 11886 --quantum-values 32768,65536,131072,262144 --flood-count 1000 --flood-bytes 32768 --probes 100 --repeat 8 --cpu 1 --timeout 120 --output "$ROOT/pr286/quantum-frontier.json"
run PR286-release-gate python base/benchmarks/network_release_gate.py --base-root base --candidate-root pr286 --host 127.0.0.1 --port 11886 --protocols 311 --completions receipt --payloads 64,4096 --windows 20,64 --control-blocks 1 --control-cycle-seeds 0,1,2 --ab-blocks 2 --cycle-seeds 0,1,2,3,4,5 --count-small 1500 --count-large 750 --target-sample-seconds 1.0 --max-count 20000 --timeout 60 --cpu 1 --runner-probe base/benchmarks/runner_probe.py --inter-phase-quiet-seconds 10 --policy strict --min-throughput .97 --max-ack-p50 1.05 --output "$ROOT/pr286/network-release.json"
stop_broker

note "done"
cat "$STATUS"
exit 0
