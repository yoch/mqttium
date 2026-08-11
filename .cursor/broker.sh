#!/usr/bin/env bash
# Ensure the MQTT broker the integration tests expect is listening on
# 127.0.0.1:11883.
#
# Primary path is a Docker container (eclipse-mosquitto) as requested; if Docker
# is unavailable for any reason the script falls back to the natively installed
# mosquitto so the integration suite never silently skips. Idempotent: it
# no-ops when the port is already open.
set -euo pipefail

port=11883
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
conf="$HOME/.mqttium-mosquitto.conf"

port_open() { (echo >"/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; }

wait_for_port() {
    for _ in $(seq 1 60); do
        port_open && return 0
        sleep 0.25
    done
    return 1
}

write_conf() {
    printf 'listener %s 127.0.0.1\nallow_anonymous true\npersistence false\n' "$port" >"$conf"
    chmod 0644 "$conf"
}

start_via_docker() {
    bash "$here/docker.sh" || return 1
    docker rm -f mqttium-broker >/dev/null 2>&1 || true
    docker run -d --name mqttium-broker --network host \
        -v "$conf:/mosquitto/config/mosquitto.conf:ro" \
        eclipse-mosquitto:2 >/dev/null || return 1
    wait_for_port
}

start_native() {
    local mosquitto
    mosquitto="$(command -v mosquitto || echo /usr/sbin/mosquitto)"
    [ -x "$mosquitto" ] || return 1
    "$mosquitto" -c "$conf" -d || return 1
    wait_for_port
}

if port_open; then
    echo "broker already listening on 127.0.0.1:$port"
    exit 0
fi

write_conf

if start_via_docker; then
    echo "mosquitto (docker) is up on 127.0.0.1:$port"
    exit 0
fi

echo "docker broker unavailable, falling back to native mosquitto" >&2
if start_native; then
    echo "mosquitto (native) is up on 127.0.0.1:$port"
    exit 0
fi

echo "failed to start an MQTT broker on 127.0.0.1:$port" >&2
exit 1
