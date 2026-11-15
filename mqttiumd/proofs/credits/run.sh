#!/usr/bin/env bash
# The pilot proof of release_if_current (lot L6, DEM-021), in one command.
#
#   proofs/credits/run.sh            run the preflight, then the proof
#   proofs/credits/run.sh --record   also rewrite BODY.sha256 from the source
#
# 1. The preflight: the tool PATH selects, its version, a true property
#    verified and a false one refuted (proofs/preflight.sh).
# 2. The body: the digest of crates/mqttiumd-core/src/credits.rs and of the
#    text of release_if_current must be the ones recorded in BODY.sha256.
#    A result belongs to an exact content.
# 3. Every harness listed in harnesses.txt, one run each: verified, no
#    failed check, and exactly the listed number of cover properties, all
#    satisfied. The list and the source must name the same harnesses.
# 4. Source coverage of the tool under the harness with no assumption:
#    every region of release_if_current is covered.
#
# Exit codes: 0 pass; the preflight's code when it fails; 5 the body is
# not the recorded one; 6 the harness list and the source differ; 7 a
# harness was not verified with all its witnesses; 8 a region of the
# function is not covered.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
PROOFS="$HERE/.."
SOURCE="$PROOFS/../crates/mqttiumd-core/src/credits.rs"
OUT="${PROOF_OUT:-$PROOFS/target/proof-credits}"
PACKAGE=mqttiumd-proof-credits

fail() { echo "proof: FAIL: $2" >&2; exit "$1"; }

# --- 1. preflight --------------------------------------------------------
mkdir -p "$OUT"
PREFLIGHT_OUT="$OUT/preflight" "$PROOFS/preflight.sh" | tee "$OUT/preflight.record"
tool="$(command -v cargo-kani)"

# --- 2. the exact body ---------------------------------------------------
# The function: from its `pub fn` line to the first closing brace in
# column one.
body() { sed -n '/^pub fn release_if_current(/,/^}/p' "$SOURCE"; }
[ -n "$(body)" ] || fail 5 "release_if_current was not found in $SOURCE"
file_digest="$(sha256sum <"$SOURCE" | cut -d' ' -f1)"
body_digest="$(body | sha256sum | cut -d' ' -f1)"
current="file $file_digest
body $body_digest"
if [ "${1:-}" = "--record" ]; then
    printf '%s\n' "$current" >"$HERE/BODY.sha256"
fi
[ "$current" = "$(cat "$HERE/BODY.sha256" 2>/dev/null)" ] || fail 5 "the source is not the recorded one:
$current
BODY.sha256 records:
$(cat "$HERE/BODY.sha256" 2>/dev/null)
Review the change, then run with --record and update PROOF.md."

# --- 3. the harnesses ----------------------------------------------------
listed="$(grep -v '^#' "$HERE/harnesses.txt" | cut -d' ' -f1 | sort)"
written="$(grep -A1 '#\[kani::proof\]' "$HERE/src/lib.rs" \
    | sed -n 's/^ *fn \([a-z0-9_]*\)().*/\1/p' | sort)"
[ -n "$listed" ] && [ "$listed" = "$written" ] \
    || fail 6 "harnesses.txt and src/lib.rs do not name the same harnesses"

total=0
witnesses=0
while read -r harness covers; do
    log="$OUT/$harness.log"
    set +e
    (cd "$PROOFS" && "$tool" kani -p "$PACKAGE" --harness "$harness") >"$log" 2>&1
    code=$?
    set -e
    [ "$code" -eq 0 ] || fail 7 "$harness: the tool exited with $code (see $log)"
    grep -Fxq "VERIFICATION:- SUCCESSFUL" "$log" \
        && grep -Fxq "Complete - 1 successfully verified harnesses, 0 failures, 1 total." "$log" \
        || fail 7 "$harness: not verified (see $log)"
    [ "$(grep -Fc -- "- Status: FAILURE" "$log" || true)" -eq 0 ] \
        || fail 7 "$harness: a check failed (see $log)"
    grep -Fxq " ** $covers of $covers cover properties satisfied" "$log" \
        || fail 7 "$harness: expected $covers of $covers witnesses satisfied, found: $(grep -F 'cover properties satisfied' "$log" || echo none) (see $log)"
    [ "$(grep -Fc -- "- Status: SATISFIED" "$log" || true)" -eq "$covers" ] \
        || fail 7 "$harness: the satisfied witnesses are not $covers (see $log)"
    checks="$(sed -n 's/^ \*\* 0 of \([0-9]*\) failed.*$/\1/p' "$log")"
    printf '  %-48s verified, %s checks, %s of %s witnesses\n' "$harness" "$checks" "$covers" "$covers"
    total=$((total + 1))
    witnesses=$((witnesses + covers))
done < <(grep -v '^#' "$HERE/harnesses.txt")

# --- 4. source coverage of the function ----------------------------------
log="$OUT/coverage.log"
set +e
(cd "$PROOFS" && "$tool" kani -p "$PACKAGE" --harness p1_total_specification \
    --coverage -Z source-coverage) >"$log" 2>&1
code=$?
set -e
[ "$code" -eq 0 ] || fail 8 "the coverage run exited with $code (see $log)"
regions="$(sed -n '/(mqttiumd_core::credits::release_if_current)$/,/^$/p' "$log" | grep '^ \* ' || true)"
covered="$(printf '%s\n' "$regions" | grep -c ' COVERED$' || true)"
all="$(printf '%s\n' "$regions" | grep -c '^ \* ' || true)"
[ "$all" -gt 0 ] && [ "$covered" -eq "$all" ] \
    || fail 8 "release_if_current: $covered of $all regions covered (see $log)"

cat <<RECORD
proof: PASS
  function        mqttiumd_core::credits::release_if_current
  source digest   sha256:$file_digest (crates/mqttiumd-core/src/credits.rs)
  body digest     sha256:$body_digest (text of the function)
  harnesses       $total verified, 0 failed
  witnesses       $witnesses of $witnesses cover properties satisfied
  coverage        $covered of $all regions of the function covered (p1, no assumption)
  options         cargo kani -p $PACKAGE --harness <name> (defaults);
                  coverage: --coverage -Z source-coverage
  bounds          every field symbolic over its whole type (u64, u32, both
                  states); no loop, no unwinding bound, no stub, no contract
  logs            $OUT
RECORD
