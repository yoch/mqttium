#!/bin/sh
# The archive verifier passes the digest of the bytes it checked before extraction.
set -eu
umask 077
package=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec "${PYTHON:-python3}" "$package/pilot/durable_restart.py" "$@"
