#!/usr/bin/env bash
# Repeat the two-machine commands in distinct network and mount namespaces.
set -euo pipefail
umask 077

if [[ $# != 2 ]]; then
    printf 'Usage: %s NEW_EVIDENCE_ROOT CAMPAIGN\n' "$0" >&2
    exit 2
fi
if [[ $(uname -m) != aarch64 ]]; then
    printf 'The rehearsal requires a native AArch64 host.\n' >&2
    exit 2
fi
kit=$(cd -- "$(dirname -- "$0")" && pwd)
mkdir -- "$1"
out=$(cd -- "$1" && pwd)
evidence="$out/evidence"
mkdir -- "$evidence"
campaign=$2
name="mqttiumd-lan-$$"
network="$name-net"
image="$name:local"
broker_ip=172.28.193.10
peer_ip=172.28.193.20
broker="$name-broker"
peer="$name-peer"

collect() {
    for role in broker peer; do
        docker logs "$name-$role" > "$evidence/$role.log" 2>&1 || true
        receipt="$out/site/$role/run/lan-$role.json"
        if [[ -f $receipt ]]; then cp -- "$receipt" "$evidence/"; fi
    done
    for start in 1 2; do
        events="$out/site/broker/run/broker-$start.events.jsonl"
        if [[ -f $events ]]; then cp -- "$events" "$evidence/"; fi
    done
}

cleanup() {
    collect
    docker rm -f "$peer" "$broker" >/dev/null 2>&1 || true
    docker network rm "$network" >/dev/null 2>&1 || true
    docker image rm "$image" >/dev/null 2>&1 || true
}
trap cleanup EXIT

mkdir -- "$out/image-context"
cp -- "$kit/Dockerfile" "$kit/requirements.txt" "$out/image-context/"
docker build --quiet --platform linux/arm64 --tag "$image" "$out/image-context" > "$evidence/image-id.txt"
python3 "$kit/lan_restart.py" prepare --out "$out/site" --host "$broker_ip" --campaign "$campaign" \
    > "$evidence/provision.log"
docker network create --internal --subnet 172.28.193.0/24 "$network" >/dev/null
docker network inspect "$network" > "$evidence/network.json"
limits=(--init --cap-drop ALL --security-opt no-new-privileges --read-only --user "$(id -u):$(id -g)")
docker run -d --name "$broker" --hostname lan-broker --network "$network" --ip "$broker_ip" \
    "${limits[@]}" --mount "type=bind,src=$out/site/broker,dst=/role" "$image" broker >/dev/null
docker run -d --name "$peer" --hostname lan-peer --network "$network" --ip "$peer_ip" \
    "${limits[@]}" --mount "type=bind,src=$out/site/peer,dst=/role" "$image" peer >/dev/null

docker inspect --format '{{json .Mounts}}' "$broker" "$peer" > "$evidence/mounts.jsonl"
timeout 105 docker wait "$peer" > "$evidence/peer.exit"
timeout 20 docker wait "$broker" > "$evidence/broker.exit"
collect
test "$(cat "$evidence/peer.exit")" = 0
test "$(cat "$evidence/broker.exit")" = 0
python3 "$kit/check_receipts.py" "$evidence" "$broker_ip" "$peer_ip"
