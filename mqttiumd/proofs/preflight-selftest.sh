#!/usr/bin/env bash
# Discriminating test of proofs/preflight.sh: the preflight must refuse a
# `cargo-kani` that PATH selects ahead of the pinned one when
#
#   - it reports another version;
#   - it reports a version that only contains the pinned one;
#   - it prints the pinned version and fails;
#   - it prints the pinned version among other lines.
#
# Each case puts a fake tool first on PATH and expects exit code 2, the
# code of "wrong tool or version". The last case runs the real preflight
# and expects it to pass: a preflight that refuses everything would pass
# the first four.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
KANI_VERSION="$(sed -n 's/^KANI_VERSION="\(.*\)"$/\1/p' "$HERE/../tools/install-tools.sh")"
CBMC_VERSION="$(sed -n 's/^CBMC_VERSION=\(.*\)$/\1/p' "$HERE/TOOLCHAIN")"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
status=0

case_with_fake() { # name, body of the fake cargo-kani
    mkdir -p "$work/$1"
    printf '#!/bin/sh\n%s\n' "$2" >"$work/$1/cargo-kani"
    chmod +x "$work/$1/cargo-kani"
    set +e
    PATH="$work/$1:$PATH" PREFLIGHT_OUT="$work/$1/out" "$HERE/preflight.sh" \
        >"$work/$1/stdout" 2>"$work/$1/stderr"
    code=$?
    set -e
    if [ "$code" -eq 2 ]; then
        echo "ok    $1: refused with exit 2"
    else
        echo "FAIL  $1: expected exit 2, got $code"
        status=1
    fi
}

good="Kani Rust Verifier $KANI_VERSION (cargo plugin)"
case_with_fake other-version \
    "echo 'Kani Rust Verifier 0.0.1 (cargo plugin)'; echo 'CBMC $CBMC_VERSION'"
case_with_fake version-as-substring \
    "echo 'Kani Rust Verifier 1$KANI_VERSION (cargo plugin)'; echo 'CBMC $CBMC_VERSION'"
case_with_fake pinned-text-but-failure \
    "echo '$good'; echo 'CBMC $CBMC_VERSION'; exit 1"
case_with_fake pinned-text-among-noise \
    "echo 'error: $good was not found'; echo '$good'; echo 'CBMC $CBMC_VERSION'"

set +e
PREFLIGHT_OUT="$work/real" "$HERE/preflight.sh" >"$work/real.stdout" 2>&1
code=$?
set -e
if [ "$code" -eq 0 ]; then
    echo "ok    real tool: preflight passes"
else
    echo "FAIL  real tool: preflight exited with $code"
    cat "$work/real.stdout"
    status=1
fi
exit "$status"
