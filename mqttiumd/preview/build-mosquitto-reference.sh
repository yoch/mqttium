#!/usr/bin/env sh
# Build the exact Mosquitto reference used by the interoperability receipt.
set -eu
umask 077
out=${1:?usage: build-mosquitto-reference.sh NEW_DIRECTORY}
mkdir -- "$out"
cd -- "$out"
version=2.0.21
archive=mosquitto-$version.tar.gz
curl --fail --location --silent --show-error --max-time 60 \
    "https://mosquitto.org/files/source/$archive" -o "$archive"
printf '%s  %s\n' 7ad5e84caeb8d2bb6ed0c04614b2a7042def961af82d87f688ba33db857b899d "$archive" | sha256sum -c -
tar -xzf "$archive"
make -C "mosquitto-$version" -j2 binary WITH_DOCS=no WITH_CJSON=no WITH_CONTROL=no > build.log 2>&1 || {
    tail -60 build.log >&2
    exit 1
}
