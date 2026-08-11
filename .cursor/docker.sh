#!/usr/bin/env bash
# Ensure a usable Docker daemon inside the nested Cloud Agent VM.
#
# The pod has no pre-running dockerd, no iptables/bridge NAT and unreliable
# overlay layering, so the daemon is started with the vfs storage driver and
# without iptables/bridge management. Containers reach the host with
# `--network host`, which needs no NAT. Idempotent: it no-ops when the daemon
# already answers, and it relaxes the socket so the unprivileged agent user can
# drive Docker without sudo.
set -euo pipefail

if sudo docker info >/dev/null 2>&1; then
    sudo chmod 666 /var/run/docker.sock 2>/dev/null || true
    exit 0
fi

sudo install -d -m 0755 /var/log/mqttium
sudo bash -c 'nohup dockerd \
    --host=unix:///var/run/docker.sock \
    --iptables=false \
    --bridge=none \
    --storage-driver=vfs \
    >>/var/log/mqttium/dockerd.log 2>&1 &'

for _ in $(seq 1 60); do
    if sudo docker info >/dev/null 2>&1; then
        sudo chmod 666 /var/run/docker.sock
        echo "docker daemon is up"
        exit 0
    fi
    sleep 0.5
done

echo "docker daemon failed to start (see /var/log/mqttium/dockerd.log)" >&2
exit 1
