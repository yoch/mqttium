#!/usr/bin/env bash
# Counts the instructions an mqttiumd executable retires under callgrind
# while it serves one QoS 0 shape of tools/measure_qos0_delivery.py
# (report 0043). The count does not depend on the clock; it includes the
# start and the end of the broker. Used when the performance gate reads a
# median between 1.03 and its threshold.
#
#   tools/count_broker_instructions.sh <mqttiumd> [plays] [messages]
#
# Prints one total per play. Needs valgrind.
set -euo pipefail

if [[ $# -lt 1 || $# -gt 3 ]]; then
    echo "usage: tools/count_broker_instructions.sh <mqttiumd> [plays] [messages]" >&2
    exit 2
fi
binary=$(realpath "$1")
plays=${2:-3}
messages=${3:-20000}
here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

cat > "$work/broker.sh" <<EOF
#!/usr/bin/env bash
exec valgrind --tool=callgrind --log-file=$work/valgrind.%p.log --callgrind-out-file=$work/callgrind.%p.out "$binary" "\$@"
EOF
chmod +x "$work/broker.sh"

for _ in $(seq "$plays"); do
    python3 -I "$here/measure_qos0_delivery.py" --binary "$work/broker.sh" --shape bursts \
        --messages "$messages" --subscribers 1 --timeout 120 > /dev/null
done
for out in "$work"/callgrind.*.out; do
    sed -n 's/^summary: //p; s/^totals: //p' "$out" | head -n 1
done
