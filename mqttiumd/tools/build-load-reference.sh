#!/bin/sh
# Build the pinned benchmark broker and its matching client library and publisher.
set -eu
out=${1:?usage: build-load-reference.sh NEW_DIRECTORY [2.1.4|2.0.21]}
version=${2:-2.1.4}
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
case "$version" in
    2.0.21) exec sh "$root/preview/build-mosquitto-reference.sh" "$out" ;;
    2.1.4) sh "$root/tests/compat/build-reference.sh" "$out" ;;
    *) echo "unsupported load reference: $version" >&2; exit 2 ;;
esac
for part in lib client; do
    case "$part" in lib) target=libmosquitto.so.1 ;; client) target=mosquitto_pub ;; esac
    make -C "$out/mosquitto-$version/$part" -j2 "$target" \
        WITH_DOCS=no WITH_HTTP_API=no WITH_EDITLINE=no WITH_CONTROL=no WITH_SRV=no \
        WITH_WEBSOCKETS=no WITH_SYSTEMD=no WITH_GMOCK=no >> "$out/build.log" 2>&1 || {
        tail -60 "$out/build.log" >&2
        exit 1
    }
done
