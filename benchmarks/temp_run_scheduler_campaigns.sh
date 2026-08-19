#!/usr/bin/env bash
set -euo pipefail

ROOT="$RUNNER_TEMP/scheduler-campaigns"
mkdir -p "$ROOT"/{pr284,pr285,pr286}

log() { printf '\n===== %s =====\n' "$*"; }
probe() { python base/benchmarks/runner_probe.py --output "$1" --require-temperature --enforce; }

log "verify immutable refs and runner"
test "$(git -C base rev-parse HEAD)" = "$BASE_SHA"
test "$(git -C pr284 rev-parse HEAD)" = "$PR284_SHA"
test "$(git -C pr285 rev-parse HEAD)" = "$PR285_SHA"
test "$(git -C pr286 rev-parse HEAD)" = "$PR286_SHA"
test "$(uname -m)" = aarch64
python3 - <<'PY'
import sys
if sys.version_info[:2] != (3, 13):
    raise SystemExit(f"expected Python 3.13, got {sys.version}")
PY
command -v mosquitto
if pgrep -a mosquitto; then
  echo "pre-existing Mosquitto contaminates evidence" >&2
  exit 1
fi

log "eligible runner preflight"
probe "$ROOT/preflight-initial.json"

log "start isolated broker"
cat > "$ROOT/mosquitto.conf" <<'MOSQ'
persistence false
allow_anonymous true
max_inflight_messages 1000
max_queued_messages 100000
max_queued_bytes 0
connection_messages false
log_type error
listener 11883 127.0.0.1
MOSQ
mosquitto -c "$ROOT/mosquitto.conf" > "$ROOT/mosquitto.log" 2>&1 &
MOSQ_PID=$!
echo "$MOSQ_PID" > "$ROOT/mosquitto.pid"
cleanup() { kill "$MOSQ_PID" 2>/dev/null || true; wait "$MOSQ_PID" 2>/dev/null || true; }
trap cleanup EXIT
for _ in $(seq 1 50); do
  if (echo > /dev/tcp/127.0.0.1/11883) >/dev/null 2>&1; then break; fi
  sleep .2
done
kill -0 "$MOSQ_PID"

# PR284
log "PR284 correctness"
PYTHONPATH=pr284/src python -m pytest -q pr284/tests/unit/test_write_pump_targeted_wake.py
probe "$ROOT/pr284/preflight.json"

log "PR284 target A/A"
python pr284/benchmarks/paired_writer_waiter_contention.py \
  --base-root base --candidate-root base --producer-values 1,4,16,64,256 \
  --max-messages 8 --payload-bytes 64 --warmup-count 2000 --count 20000 \
  --repeat 8 --cpu 1 --policy strict --preflight-report "$ROOT/pr284/preflight.json" \
  --max-baseline-cv .05 --max-aa-ratio-deviation .02 \
  --output "$ROOT/pr284/target-aa.json" --summary-output "$ROOT/pr284/target-aa.md"

log "PR284 target A/B"
python pr284/benchmarks/paired_writer_waiter_contention.py \
  --base-root base --candidate-root pr284 --producer-values 1,4,16,64,256 \
  --max-messages 8 --payload-bytes 64 --warmup-count 2000 --count 20000 \
  --repeat 8 --cpu 1 --policy strict --preflight-report "$ROOT/pr284/preflight.json" \
  --max-baseline-cv .05 --min-completed-ratio .97 \
  --output "$ROOT/pr284/target-ab.json" --summary-output "$ROOT/pr284/target-ab.md"

log "PR284 heterogeneous A/A"
python harness/benchmarks/temp_scheduler_supplement.py writer-hetero-parent \
  --base-root base --candidate-root base --producer-values 4,16,64,256 --repeat 8 \
  --count 20000 --max-messages 8 --max-bytes 16384 --small-bytes 64 --large-bytes 4096 \
  --cpu 1 --output "$ROOT/pr284/hetero-aa.json"

log "PR284 heterogeneous A/B"
python harness/benchmarks/temp_scheduler_supplement.py writer-hetero-parent \
  --base-root base --candidate-root pr284 --producer-values 4,16,64,256 --repeat 8 \
  --count 20000 --max-messages 8 --max-bytes 16384 --small-bytes 64 --large-bytes 4096 \
  --cpu 1 --output "$ROOT/pr284/hetero-ab.json"

log "PR284 writer-capacity guard"
python base/benchmarks/paired_writer_capacity.py --base-root base --candidate-root pr284 \
  --protocols 311 --payloads 64 --windows 20 --count-small 6000 --repeat 8 --cpu 1 \
  --policy strict --preflight-report "$ROOT/pr284/preflight.json" \
  --min-qos0-ratio .97 --min-qos1-ratio .97 --output "$ROOT/pr284/writer-capacity.json" \
  --summary-output "$ROOT/pr284/writer-capacity.md"

log "PR284 open-loop guard"
python base/benchmarks/paired_open_loop.py --base-root base --candidate-root pr284 \
  --protocols 311 --payloads 64 --completions callback --windows 20 --target-rates 5000,7500 \
  --count-small 5000 --repeat 8 --cpu 1 --policy strict --preflight-report "$ROOT/pr284/preflight.json" \
  --max-baseline-cv .05 --min-completed-ratio .97 --max-loop-lag-ratio 1.05 \
  --output "$ROOT/pr284/open-loop.json" --summary-output "$ROOT/pr284/open-loop.md"

log "PR284 network guard"
python base/benchmarks/paired_network.py --base-root base --candidate-root pr284 --host 127.0.0.1 --port 11883 \
  --protocols 311 --completions receipt --payloads 64,4096 --windows 20,64 --repeat 8 \
  --count-small 1500 --count-large 750 --cpu 1 --policy strict --preflight-report "$ROOT/pr284/preflight.json" \
  --max-baseline-cv .05 --min-ack-ratio .97 --output "$ROOT/pr284/network.json" --summary-output "$ROOT/pr284/network.md"

# PR285
log "PR285 correctness"
PYTHONPATH=pr285/src python -m pytest -q pr285/tests/unit/test_publish_targeted_wake.py pr285/tests/unit/test_async_publish_admission.py
probe "$ROOT/pr285/preflight.json"

log "PR285 original harness A/A (diagnostic if statistically invalid)"
python pr285/benchmarks/paired_publish_admission_contention.py --base-root base --candidate-root base \
  --host 127.0.0.1 --port 11883 --publisher-values 1,4,16,64,256 --inflight-values 1,4,20 \
  --count 4000 --warmup-count 200 --repeat 8 --cpu 1 --policy strict --preflight-report "$ROOT/pr285/preflight.json" \
  --max-baseline-cv .05 --max-aa-ratio-deviation .02 --output "$ROOT/pr285/original-aa.json" --summary-output "$ROOT/pr285/original-aa.md" || true

log "PR285 original harness A/B (admission-only metric)"
python pr285/benchmarks/paired_publish_admission_contention.py --base-root base --candidate-root pr285 \
  --host 127.0.0.1 --port 11883 --publisher-values 1,4,16,64,256 --inflight-values 1,4,20 \
  --count 4000 --warmup-count 200 --repeat 8 --cpu 1 --policy strict --preflight-report "$ROOT/pr285/preflight.json" \
  --max-baseline-cv .05 --min-completed-ratio .97 --output "$ROOT/pr285/original-ab.json" --summary-output "$ROOT/pr285/original-ab.md" || true

log "PR285 true-completion A/A"
python harness/benchmarks/temp_scheduler_supplement.py publish-parent --base-root base --candidate-root base \
  --host 127.0.0.1 --port 11883 --publisher-values 1,4,16,64,256 --inflight-values 1,4,20 \
  --count 4000 --repeat 8 --cpu 1 --timeout 120 --output "$ROOT/pr285/true-aa.json"

log "PR285 true-completion A/B"
python harness/benchmarks/temp_scheduler_supplement.py publish-parent --base-root base --candidate-root pr285 \
  --host 127.0.0.1 --port 11883 --publisher-values 1,4,16,64,256 --inflight-values 1,4,20 \
  --count 4000 --repeat 8 --cpu 1 --timeout 120 --output "$ROOT/pr285/true-ab.json"

log "PR285 writer-capacity guard"
python base/benchmarks/paired_writer_capacity.py --base-root base --candidate-root pr285 \
  --protocols 311 --payloads 64 --windows 20 --count-small 6000 --repeat 8 --cpu 1 \
  --policy strict --preflight-report "$ROOT/pr285/preflight.json" \
  --min-qos0-ratio .97 --min-qos1-ratio .97 --output "$ROOT/pr285/writer-capacity.json" --summary-output "$ROOT/pr285/writer-capacity.md"

log "PR285 open-loop guard"
python base/benchmarks/paired_open_loop.py --base-root base --candidate-root pr285 \
  --protocols 311 --payloads 64 --completions callback --windows 20 --target-rates 5000,7500 \
  --count-small 5000 --repeat 8 --cpu 1 --policy strict --preflight-report "$ROOT/pr285/preflight.json" \
  --max-baseline-cv .05 --min-completed-ratio .97 --max-loop-lag-ratio 1.05 \
  --output "$ROOT/pr285/open-loop.json" --summary-output "$ROOT/pr285/open-loop.md"

log "PR285 network guard"
python base/benchmarks/paired_network.py --base-root base --candidate-root pr285 --host 127.0.0.1 --port 11883 \
  --protocols 311 --completions receipt --payloads 64,4096 --windows 20,64 --repeat 8 \
  --count-small 1500 --count-large 750 --cpu 1 --policy strict --preflight-report "$ROOT/pr285/preflight.json" \
  --max-baseline-cv .05 --min-ack-ratio .97 --output "$ROOT/pr285/network.json" --summary-output "$ROOT/pr285/network.md"

# PR286
log "PR286 correctness"
PYTHONPATH=pr286/src python -m pytest -q pr286/tests/unit/test_write_pump_byte_quantum.py
probe "$ROOT/pr286/preflight.json"

log "PR286 writer-capacity A/A"
python base/benchmarks/paired_writer_capacity.py --base-root base --candidate-root base \
  --protocols 311 --payloads 64 --windows 20 --count-small 6000 --repeat 8 --cpu 1 \
  --policy strict --preflight-report "$ROOT/pr286/preflight.json" \
  --min-qos0-ratio .97 --min-qos1-ratio .97 --output "$ROOT/pr286/writer-aa.json" --summary-output "$ROOT/pr286/writer-aa.md"

log "PR286 writer-capacity A/B"
python base/benchmarks/paired_writer_capacity.py --base-root base --candidate-root pr286 \
  --protocols 311 --payloads 64 --windows 20 --count-small 6000 --repeat 8 --cpu 1 \
  --policy strict --preflight-report "$ROOT/pr286/preflight.json" \
  --min-qos0-ratio .97 --min-qos1-ratio .97 --output "$ROOT/pr286/writer-ab.json" --summary-output "$ROOT/pr286/writer-ab.md"

log "PR286 open-loop guard"
python base/benchmarks/paired_open_loop.py --base-root base --candidate-root pr286 \
  --protocols 311 --payloads 64 --completions callback --windows 20 --target-rates 5000,7500 \
  --count-small 5000 --repeat 8 --cpu 1 --policy strict --preflight-report "$ROOT/pr286/preflight.json" \
  --max-baseline-cv .05 --min-completed-ratio .97 --max-loop-lag-ratio 1.05 \
  --output "$ROOT/pr286/open-loop-small.json" --summary-output "$ROOT/pr286/open-loop-small.md"

log "PR286 mixed-tail harness A/A"
python harness/benchmarks/temp_scheduler_supplement.py quantum-tail-parent --base-root base --candidate-root base \
  --host 127.0.0.1 --port 11883 --quantum-values 0 --flood-count 1000 --flood-bytes 32768 --probes 100 \
  --repeat 8 --cpu 1 --timeout 120 --output "$ROOT/pr286/tail-aa.json"

log "PR286 32/64/128/256 KiB mixed-tail screening"
python harness/benchmarks/temp_scheduler_supplement.py quantum-tail-parent --base-root base --candidate-root pr286 \
  --host 127.0.0.1 --port 11883 --quantum-values 32768,65536,131072,262144 \
  --flood-count 1000 --flood-bytes 32768 --probes 100 --repeat 8 --cpu 1 --timeout 120 \
  --output "$ROOT/pr286/quantum-tail.json"

log "PR286 release-grade fresh preflight"
probe "$ROOT/pr286/preflight-release.json"

log "PR286 authoritative network release gate"
python base/benchmarks/network_release_gate.py --base-root base --candidate-root pr286 \
  --host 127.0.0.1 --port 11883 --protocols 311 --completions receipt --payloads 64,4096 --windows 20,64 \
  --control-blocks 1 --control-cycle-seeds 0,1,2 --ab-blocks 2 --cycle-seeds 0,1,2,3,4,5 \
  --count-small 1500 --count-large 750 --target-sample-seconds 1.0 --max-count 20000 --timeout 60 --cpu 1 \
  --runner-probe base/benchmarks/runner_probe.py --inter-phase-quiet-seconds 10 \
  --policy strict --min-throughput .97 --max-ack-p50 1.05 --output "$ROOT/pr286/network-release.json"

log "campaign complete"
