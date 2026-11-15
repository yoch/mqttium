#!/usr/bin/env bash
# Rebuilds the binary of a campaign written by `run --rebuild` from the
# source.tar the campaign holds, and compares its digest with the binary
# the run manifest names (lot L4). Needs only the campaign directory and
# a toolchain with the locked dependencies available offline:
#
#   tools/check_campaign_binary.sh <campaign directory>
#
# Exit 0 when the digests are equal, 1 when they differ, 2 when the
# campaign holds no source or the build fails.
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: tools/check_campaign_binary.sh <campaign directory>" >&2
    exit 2
fi
campaign=$(cd "$1" && pwd)
if [[ ! -f $campaign/source.tar ]]; then
    echo "$campaign holds no source.tar: it was not written by run --rebuild" >&2
    exit 2
fi
digest=$(sha256sum "$campaign/source.tar" | cut -c1-64)
dir="${TMPDIR:-/tmp}/mqttiumd-source-${digest:0:16}"
rm -rf "${dir:?}"
mkdir -p "$dir"
trap 'rm -rf "${dir:?}"' EXIT
tar -x -f "$campaign/source.tar" -C "$dir"
if ! (cd "$dir" && RUSTFLAGS="--remap-path-prefix=$dir=/mqttiumd-source" \
    cargo build -q --locked --offline -p mqttiumd-lab --bin mqttiumd-lab); then
    echo "the source of the campaign does not build" >&2
    exit 2
fi
built=$(sha256sum "$dir/target/debug/mqttiumd-lab" | cut -c1-64)
named=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['artifact']['sha256'])" "$campaign/run-manifest.json")
echo "built from source.tar: $built"
echo "named by the manifest: $named"
if [[ $built == "$named" ]]; then
    echo "the binary of the campaign is the one its source builds"
    exit 0
fi
echo "the binary of the campaign is NOT the one its source builds"
exit 1
