# L3 — Oracle, traces, registry

Depends on: L0. Runs with: L1, L2, L5, L6. Followed by: L4. Gate: G0.

## Goal

An independent judge. It reads traces, snapshots and decoded packets, and
decides the assertions of a scenario without sharing any decision logic
with the product.

## Requirements

DEM-014 to DEM-019; V3-003, V3-004, V3-005, V3-017; VAL-002. Decision
records 0001, 0003, 0004.

## Independence

This lot does not read `crates/mqttiumd-core/`. Its expectations come from
the specification, the registry and the observations. It may share
descriptive formats with the product, never the calculation of an expected
answer.

## Deliverables

First: the store snapshot and trace formats, the scenario steps of
P1-02b, and the registry rules lot L1 needs to start. The other lots wait
for them.

1. **A complete registry.** The 39 uncovered cells of
   `contracts/v1/p1.transitions.json` are written and its status becomes
   `complete`. The rules of record 0001 are included: a replayed PUBLISH
   while the outcome is unknown, after a reconnect and on a live
   connection; a deadline for an operation that became terminal.
2. **The oracle**, in Python under `tests/oracle/`, with pinned versions.
   - Its own strict reader and its own semantic rules for the v1 formats:
     the second reader of the semantic level.
   - A trace read as a whole: header, gapless sequence per origin, causal
     parents present, footer count and completeness. A missing record makes
     the conclusion inconclusive, never accepted.
   - Its own identity for a client exchange, from the observed history.
   - The assertions of the catalogue, forbidden effects as well as expected
     ones. Hidden store truth judges a result and never justifies an answer
     of the core.
   - Several interleavings accepted where the contract allows them; local
     order and causes respected.
3. **Contract changes** it owns: the scenario steps of P1-02b, the trace
   records and the assertion for the provenance of broker inputs, the
   store snapshot format, scenario files for QABS-A, B and C.
4. **A corpus of traces written by hand**: good ones, and one per
   violation, each failing for its own reason.
5. **Results**: a qualifying result is produced by the oracle from the
   observations; a result whose statuses differ from the recomputation is
   refused.

## Reception

- The registry is `complete`; `validate` reports no hole and no overlap.
- Each assertion has a good trace it accepts and a faulty trace it refuses
  for that assertion and no other.
- The oracle and the Rust reader agree on the example corpus, semantic
  codes included.
- Control mutations of the oracle itself, each seen to fail: a truncated
  trace accepted; an event moved before its prerequisite; a hidden commit
  used to justify a PUBREC; a count of obligations compared instead of
  their identities (DEM-MU-06); a fresh exchange identity resetting the
  admission count (DEM-MU-04).
- The cases of invalid input of the readers have their own tests.
- Every example is still refused for its declared reason, and the derived
  files are up to date.

## Out of scope

Running scenarios (L4), the packet decoder of the peer (L8).

## Closes

In `../reception/open-conditions.md`.

Alone: in "Before a result may be used as evidence", the conditions whose
only closer is L3, and the first two of "Scenario P1-02b".

Its part only, of conditions shared with L4: the origin of a trace record
(L3 defines the rule and refuses a record that breaks it in a trace; L4
makes the collector assign the origin from the channel), and the single
snapshot of the contracts (L3 makes the readers interpret the bytes that
were hashed; L4 establishes the snapshot of a campaign). A shared
condition is not removed when the part of L3 is done: its row is reduced
to what L4 still owes.
