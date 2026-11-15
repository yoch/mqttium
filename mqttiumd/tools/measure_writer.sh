#!/usr/bin/env bash
# An early, exploratory measurement of the single SQLite writer (record
# 0008). It qualifies nothing. It counts durable store transactions, not
# MQTT messages. Its output is generated data: never commit it.
#
#   tools/measure_writer.sh <output directory> [<directory on the storage measured>]
#
# The output directory receives machine.txt (hardware, storage, revision)
# and one JSON report per point; the store files are created in the
# measured directory and removed after each point.
set -euo pipefail

out=${1:?an output directory}
dir=${2:-$out/db}
ops=${OPS:-2000}
warmup=${WARMUP:-200}
cd "$(dirname "$0")/.."
mkdir -p "$out" "$dir"

{
  echo "date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "revision: $(git rev-parse HEAD)"
  echo "tree: $(git status --porcelain | wc -l) modified paths"
  echo "rustc: $(rustc --version)"
  echo "kernel: $(uname -srm)"
  echo "cpu: $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ *//')"
  echo "cpus online: $(nproc)"
  echo "memory: $(grep MemTotal /proc/meminfo | awk '{print $2 " kB"}')"
  echo "clock ticks per second: $(getconf CLK_TCK)"
  echo "measured directory: $dir"
  echo "filesystem:"; df -T "$dir" | sed 's/^/  /'
  echo "mount:"; (findmnt -T "$dir" -o TARGET,SOURCE,FSTYPE,OPTIONS 2>/dev/null || echo "  findmnt unavailable") | sed 's/^/  /'
  echo "block devices:"; (lsblk -d -o NAME,ROTA,SIZE,MODEL 2>/dev/null || echo "  lsblk unavailable") | sed 's/^/  /'
} > "$out/machine.txt"

cargo build --release --locked -q -p mqttiumd-store-sqlite --example measure_writer
bin=target/release/examples/measure_writer

# payload bytes, fan-out
points="64:1 1024:1 1024:8 16384:1"
for point in $points; do
  payload=${point%%:*}
  fanout=${point##*:}
  "$bin" --dir "$dir" --payload "$payload" --fanout "$fanout" --ops "$ops" --warmup "$warmup" \
    > "$out/sqlite-$payload-$fanout.json"
  "$bin" --memory --payload "$payload" --fanout "$fanout" --ops "$ops" --warmup "$warmup" \
    > "$out/memory-$payload-$fanout.json"
  echo "point $payload bytes, fan-out $fanout: done"
done
echo "reports in $out"
