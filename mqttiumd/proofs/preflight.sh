#!/usr/bin/env bash
# Preflight of the proof tooling of mqttiumd (lot L6, DEM-021).
#
#   proofs/preflight.sh
#
# It answers one question before any proof result is believed: does the
# tool that PATH selects, at the pinned version, verify a true property
# and refute a false one on the production dependency?
#
# 1. `cargo-kani` is resolved through PATH, and that very file is the one
#    run below. Its version command must succeed and print exactly the
#    pinned lines; a version that merely contains the pinned text, or a
#    command that prints it and fails, is refused.
# 2. The compiler Kani brings and the host target are read and compared
#    with proofs/TOOLCHAIN.
# 3. The true harness of proofs/preflight must be verified, with its two
#    cover properties satisfied.
# 4. The false harness must be refuted by its own check and by no other;
#    a compilation failure or any other error is not a refutation.
#
# Exit codes: 0 pass; 2 wrong tool or version; 3 the true property was not
# verified; 4 the false property was not refuted for its own reason.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
INSTALLER="$HERE/../tools/install-tools.sh"
OUT="${PREFLIGHT_OUT:-$HERE/target/preflight}"

fail() { echo "preflight: FAIL: $2" >&2; exit "$1"; }

pinned() { sed -n "s/^$2=\"\{0,1\}\([^\"]*\)\"\{0,1\}\$/\1/p" "$1"; }
KANI_VERSION="$(pinned "$INSTALLER" KANI_VERSION)"
CBMC_VERSION="$(pinned "$HERE/TOOLCHAIN" CBMC_VERSION)"
KANI_RUST_TOOLCHAIN="$(pinned "$HERE/TOOLCHAIN" KANI_RUST_TOOLCHAIN)"
[ -n "$KANI_VERSION" ] && [ -n "$CBMC_VERSION" ] && [ -n "$KANI_RUST_TOOLCHAIN" ] \
    || fail 2 "a pinned version could not be read"

# --- 1. the tool PATH selects -------------------------------------------
tool="$(command -v cargo-kani)" || fail 2 "cargo-kani is not on PATH"
case "$tool" in /*) ;; *) fail 2 "cargo-kani does not resolve to a file: $tool" ;; esac
version="$("$tool" kani --version 2>/dev/null)" \
    || fail 2 "$tool: the version command failed"
expected="Kani Rust Verifier $KANI_VERSION (cargo plugin)
CBMC $CBMC_VERSION"
[ "$version" = "$expected" ] || fail 2 "$tool reports
$version
and the pinned tool reports
$expected"

# --- 2. what the tool brings --------------------------------------------
kani_home="${KANI_HOME:-$HOME/.kani}/kani-$KANI_VERSION"
toolchain="$(cat "$kani_home/rust-toolchain-version" 2>/dev/null)" \
    || fail 2 "no Kani toolchain record under $kani_home"
[ "$toolchain" = "$KANI_RUST_TOOLCHAIN" ] \
    || fail 2 "Kani brings $toolchain, proofs/TOOLCHAIN pins $KANI_RUST_TOOLCHAIN"
rustc_version="$("$kani_home/toolchain/bin/rustc" --version --verbose)" \
    || fail 2 "the compiler Kani brings does not run"
target="$(printf '%s\n' "$rustc_version" | sed -n 's/^host: //p')"
revision="$(git -C "$HERE" rev-parse HEAD 2>/dev/null || echo unknown)"
if [ -n "$(git -C "$HERE" status --porcelain -- "$HERE/.." 2>/dev/null)" ]; then
    revision="$revision (with uncommitted changes)"
fi

mkdir -p "$OUT"
run() { # harness name -> exit code of the tool, output in $OUT/<name>.log
    set +e
    (cd "$HERE" && "$tool" kani -p mqttiumd-proof-preflight --harness "$1") \
        >"$OUT/$1.log" 2>&1
    code=$?
    set -e
    return 0
}
has() { grep -Fxq -- "$2" "$OUT/$1.log"; }
failures() { grep -Fc -- "- Status: FAILURE" "$OUT/$1.log" || true; }

# --- 3. a true property passes ------------------------------------------
run preflight_true_property
[ "$code" -eq 0 ] || fail 3 "the true property: the tool exited with $code (see $OUT)"
[ "$(failures preflight_true_property)" -eq 0 ] \
    && has preflight_true_property "VERIFICATION:- SUCCESSFUL" \
    && has preflight_true_property " ** 2 of 2 cover properties satisfied" \
    && has preflight_true_property \
        "Complete - 1 successfully verified harnesses, 0 failures, 1 total." \
    || fail 3 "the true property was not verified with its covers (see $OUT)"

# --- 4. a false property fails, for its own reason ----------------------
run preflight_false_property
[ "$code" -ne 0 ] || fail 4 "the false property was accepted"
has preflight_false_property "VERIFICATION:- FAILED" \
    && [ "$(failures preflight_false_property)" -eq 1 ] \
    && has preflight_false_property \
        'Failed Checks: "preflight-false: the subtraction never refuses"' \
    && has preflight_false_property \
        "Complete - 0 successfully verified harnesses, 1 failures, 1 total." \
    || fail 4 "the false property did not fail by its own check alone (see $OUT)"

cat <<RECORD
preflight: PASS
  revision        $revision
  tool            $tool
  tool version    Kani Rust Verifier $KANI_VERSION (cargo plugin), CBMC $CBMC_VERSION
  compiler        $(printf '%s\n' "$rustc_version" | head -n 1) [$toolchain]
  target          $target
  options         cargo kani -p mqttiumd-proof-preflight --harness <name> (defaults)
  dependency      mqttiumd-core by path, as the product workspace builds it
  true property   preflight_true_property: verified, 2 of 2 covers satisfied
  false property  preflight_false_property: refuted by its own check and no other
  bounds          both operands range over all of u64; no loop, no unwinding
  logs            $OUT
RECORD
