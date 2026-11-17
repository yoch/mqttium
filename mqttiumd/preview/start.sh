#!/bin/sh
# Preserve the same database between starts; create a new log directory each time.
set -eu
preview_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
preview_data=${1:-"$PWD/mqttiumd-data"}
preview_listen=${MQTTIUMD_LISTEN:-127.0.0.1:1883}
mkdir -p -- "$preview_data/logs"
preview_run=$(mktemp -d "$preview_data/logs/run.XXXXXXXX")
printf '%s\n' "mqttiumd local MQTT 5 preview" "listen=$preview_listen" "database=$preview_data/broker.sqlite" "events=$preview_run/events.jsonl" "quotas: sessions=128 subscriptions=128 filters/request=8 destinations/publication=16 receipts=8192 retained_topics=32 retained_bytes=1048576" "profile: MQTT5 input QoS0/1/2, output QoS0/1, +/#, retained, Message Expiry, No Local; no authentication/TLS" "clock recognition, expiry quarantine and readiness: see session_clock_anchor, message_expiry_clock and ready events"
exec "$preview_dir/mqttiumd" --listen "$preview_listen" --db "$preview_data/broker.sqlite" --events "$preview_run/events.jsonl"
