#!/usr/bin/env bash
# Counts the instructions an mqttiumd executable retires under callgrind
# while it serves one shape of the performance gate (report 0043). The count
# does not depend on the clock; it includes the start and the end of the
# broker. Used when the gate reads a median between 1.03 and its threshold,
# and as a baseline that the noise of a shared machine does not move.
#
#   tools/count_broker_instructions.sh [--shape S] [--clients N] <mqttiumd> [plays] [messages]
#
# --shape qos0-bursts (default): tools/measure_qos0_delivery.py, bursts of
#   QoS 0 to N subscribers, `messages` publications.
# --shape qos1-pairs: tools/measure_qos1_pairs.py, N QoS 1 pairs on the
#   volatile store, `messages` publications per pair.
# --clients N: subscribers or pairs, 1 by default.
#
# The gate's four shapes, with the sizes of reports 0047 and 0048:
#   --shape qos0-bursts --clients 1 <mqttiumd> 3 20000
#   --shape qos0-bursts --clients 8 <mqttiumd> 2 10000
#   --shape qos1-pairs  --clients 1 <mqttiumd> 3 20000
#   --shape qos1-pairs  --clients 8 <mqttiumd> 2 4000
#
# Prints one total per play. Needs valgrind.
set -euo pipefail

usage="usage: tools/count_broker_instructions.sh [--shape qos0-bursts|qos1-pairs] [--clients N] <mqttiumd> [plays] [messages]"
shape=qos0-bursts
clients=1
while [[ $# -gt 0 && $1 == --* ]]; do
    case $1 in
        --shape) shape=${2:?$usage}; shift 2 ;;
        --clients) clients=${2:?$usage}; shift 2 ;;
        *) echo "$usage" >&2; exit 2 ;;
    esac
done
if [[ $# -lt 1 || $# -gt 3 ]] || ! [[ $clients =~ ^[1-9][0-9]*$ ]]; then
    echo "$usage" >&2
    exit 2
fi
binary=$(realpath "$1")
plays=${2:-3}
messages=${3:-20000}
here=$(cd "$(dirname "$0")" && pwd)
case $shape in
    qos0-bursts)
        driver=("$here/measure_qos0_delivery.py" --shape bursts --subscribers "$clients")
        ;;
    qos1-pairs)
        driver=("$here/measure_qos1_pairs.py" --pairs "$clients" --storage volatile)
        ;;
    *) echo "$usage" >&2; exit 2 ;;
esac
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

cat > "$work/broker.sh" <<EOF
#!/usr/bin/env bash
exec valgrind --tool=callgrind --log-file=$work/valgrind.%p.log --callgrind-out-file=$work/callgrind.%p.out "$binary" "\$@"
EOF
chmod +x "$work/broker.sh"

for _ in $(seq "$plays"); do
    python3 -I "${driver[@]}" --binary "$work/broker.sh" --messages "$messages" \
        --timeout 600 > /dev/null
done
for out in "$work"/callgrind.*.out; do
    sed -n 's/^summary: //p; s/^totals: //p' "$out" | head -n 1
done
