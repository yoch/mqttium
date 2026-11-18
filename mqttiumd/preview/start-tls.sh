#!/bin/sh
# Keep the existing SQLite file and create a new event stream for every start.
set -eu
umask 077
package=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
data=${1:-"$PWD/secure-data"}
pki=${2:-"$PWD/demo-pki"}
listen=${MQTTIUMD_LISTEN:-127.0.0.1:8883}
mkdir -p -- "$data/logs"
run=$(mktemp -d "$data/logs/run.XXXXXXXX")
printf '%s\n' 'mqttiumd MQTT 5 private-network preview (TLS 1.3, client certificate required)' \
    "listen=$listen" "database=$data/broker.sqlite" "events=$run/events.jsonl"
exec "$package/mqttiumd" --listen "$listen" --db "$data/broker.sqlite" --events "$run/events.jsonl" \
    --tls-cert "$pki/server.crt" --tls-key "$pki/server.key" --client-ca "$pki/ca.crt" --identities "$pki/identities.json"
