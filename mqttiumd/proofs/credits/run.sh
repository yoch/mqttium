#!/usr/bin/env bash
# The pilot proof of release_if_current (lot L6, DEM-021), in one command.
#
#   proofs/credits/run.sh            run the preflight, then the proof
#   proofs/credits/run.sh --record   the same, then record the digests
#
# The steps, their order and the exit codes are in run.py.
set -euo pipefail
exec python3 "$(cd "$(dirname "$0")" && pwd)/run.py" "$@"
