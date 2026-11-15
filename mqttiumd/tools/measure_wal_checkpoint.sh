#!/usr/bin/env bash
# The experiment of report 0002 (lot L7): the tail of the writer's
# admissions against the engine's checkpoint of the WAL. One point
# (1 KiB, fan-out 1), the same machine and revision, five runs with
# `wal_autocheckpoint` at 1000 pages (the default) and five at 10 000.
# Exploratory: it qualifies nothing. Its output is never committed.
#
#   tools/measure_wal_checkpoint.sh <output directory> [<directory on the storage measured>]
set -euo pipefail

out=${1:?an output directory}
dir=${2:-$out/db}
ops=${OPS:-2000}
warmup=${WARMUP:-200}
runs=${RUNS:-5}
cd "$(dirname "$0")/.."
mkdir -p "$out" "$dir"
{
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "revision: $(git rev-parse HEAD)"
  echo "tree: $(git status --porcelain | wc -l) modified paths"
  echo "measured directory: $dir"
  df -T "$dir" | sed 's/^/  /'
} > "$out/machine.txt"
cargo build --release --locked -q -p mqttiumd-store-sqlite --example measure_writer
bin=target/release/examples/measure_writer
for run in $(seq 1 "$runs"); do
  for pages in 1000 10000; do
    "$bin" --dir "$dir" --payload 1024 --fanout 1 --ops "$ops" --warmup "$warmup" \
      --wal-autocheckpoint "$pages" > "$out/run-$run-$pages.json"
  done
  echo "run $run: done"
done
echo "reports in $out"
