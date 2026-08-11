#!/usr/bin/env bash
# Start the local Mosquitto broker MQTTium's integration tests expect.
#
# The integration suite connects to 127.0.0.1:11883 and skips silently when no
# broker is present, so this must run on every boot. Idempotent: it no-ops when
# the broker is already listening.
set -euo pipefail

conf=/tmp/mqttium-mosquitto.conf
printf 'listener 11883 127.0.0.1\nallow_anonymous true\npersistence false\n' >"$conf"

if (echo >/dev/tcp/127.0.0.1/11883) >/dev/null 2>&1; then
    echo "mosquitto already listening on 127.0.0.1:11883"
    exit 0
fi

mosquitto="$(command -v mosquitto || echo /usr/sbin/mosquitto)"
"$mosquitto" -c "$conf" -d

for _ in $(seq 1 50); do
    if (echo >/dev/tcp/127.0.0.1/11883) >/dev/null 2>&1; then
        echo "mosquitto is up on 127.0.0.1:11883"
        exit 0
    fi
    sleep 0.2
done

echo "mosquitto failed to start on 127.0.0.1:11883" >&2
exit 1
