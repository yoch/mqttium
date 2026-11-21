#!/bin/sh
# Create throwaway private credentials outside the demonstration receipts.
set -eu
umask 077
package=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
pki=$(mktemp -d "${TMPDIR:-/tmp}/mqttiumd-demo-pki.XXXXXXXX")
trap 'rm -rf -- "$pki"' EXIT HUP INT TERM
python3 "$package/demo-pki.py" "$pki/credentials"
"$package/will-demo.sh" "${1:-$PWD/secure-demo-data}" "$pki/credentials"
