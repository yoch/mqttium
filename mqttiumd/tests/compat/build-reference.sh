#!/usr/bin/env sh
# Build the Mosquitto reference of the compatibility bench (record 0062).
#
# The source archive is downloaded from mosquitto.org and its digest is
# checked before anything is unpacked. The build includes the common
# library, the TLS-enabled broker and its SQLite persistence plugin, without the
# documentation, WebSocket listener, HTTP API and control tools.
#
# Needs a C compiler, make, and the headers of OpenSSL, SQLite and cJSON, which
# Mosquitto 2.1 requires (Debian and Ubuntu: libssl-dev, libsqlite3-dev, libcjson-dev).
#
# `preview/build-mosquitto-reference.sh` builds 2.0.21 for the frozen
# preview kit and historical comparisons.
set -eu
umask 077

out=${1:?usage: build-reference.sh NEW_DIRECTORY}
mkdir -- "$out"
cd -- "$out"

version=2.1.4
archive=mosquitto-$version.tar.gz
curl --fail --location --silent --show-error --max-time 60 \
    "https://mosquitto.org/files/source/$archive" -o "$archive"
printf '%s  %s\n' ca30b26ccd3041c0e11fc407fe4f2e62bc445e8d8dea94a083cc60c09bd5ea30 "$archive" | sha256sum -c -
tar -xzf "$archive"
for part in libcommon src plugins/persist-sqlite; do
    make -C "mosquitto-$version/$part" -j2 \
        WITH_DOCS=no WITH_WEBSOCKETS=no WITH_HTTP_API=no WITH_EDITLINE=no WITH_SQLITE=yes \
        WITH_CONTROL=no WITH_SRV=no WITH_SYSTEMD=no WITH_GMOCK=no >> build.log 2>&1 || {
        tail -60 build.log >&2
        exit 1
    }
done
