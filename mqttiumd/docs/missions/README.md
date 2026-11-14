# Missions of the first wave

Lot L0 is the base. The first wave runs five lots in parallel from it: L1,
L2, L3, L5 and L6. Lot L4 integrates L1, L2 and L3 afterwards; L7, L8 and
L9 follow (DEM-024). Each lot has its own mission file here.

A mission states what a lot delivers and how it is received. It does not
replace the specification: `../spec/README.md` and the decision records
come first, and `../reception/open-conditions.md` lists what each lot must
close.

## Base

| | |
| - | - |
| Revision | The revision of lot L0 as received by the integration owner. At preparation: `5f9b458eb2f4cd3cf49f791b9157ab43e276f042`, plus this directory and decision record 0009, which change no code and no contract file |
| Contract digest | `sha256:4e6cef127db777022f68c21b686b1b1ff4f2f2dfdc1526ef1e0b0b014940896b` at preparation. Accepting record 0009 adds it to the requirement register and changes this digest; the wave starts from the revision that holds that change |
| Base branch | `main` once L0 is merged; decided by the integration owner |

Every lot starts from the same revision and names it, with the contract
digest it was verified against, in its report.

## Before the wave starts

1. Lot L0 is received.
2. Decision record 0009 (creation authority for owners and effect boxes) is
   decided. Several lots build on what it fixes.
3. The integration owner chooses the base branch.

## Lots and what each owns

One lot per branch and per session. A lot changes only the paths it owns.

| Lot | Owns | Mission |
| - | - | - |
| L1 Inbound core | `crates/mqttiumd-core/` except `src/credits.rs`; in `crates/mqttiumd-contract/src/`: `effects.rs`, `step.rs`, `ids.rs`, `capacity.rs`, `packet.rs` | [L1](L1-inbound-core.md) |
| L2 Deterministic store | `crates/mqttiumd-lab/src/store/`, `crates/mqttiumd-lab/tests/store_*.rs`; in `crates/mqttiumd-contract/src/`: `command.rs`, `outcome.rs` | [L2](L2-deterministic-store.md) |
| L3 Oracle, traces, registry | `contracts/`, `tests/oracle/`, `tests/scenarios/`, `tests/fixtures/`, `tools/build_corpus.py`, `tools/check_schemas.py`; the validation code of `crates/mqttiumd-lab/src/` (`semantic.rs`, `schemas.rs`, `strict.rs`, `workspace.rs`, `diagnostic.rs`) | [L3](L3-oracle-traces-registry.md) |
| L5 Model M1 | `models/`, the model step of the workflow | [L5](L5-model-m1.md) |
| L6 Pilot proof | `crates/mqttiumd-core/src/credits.rs`, `proofs/` | [L6](L6-pilot-proof.md) |

Shared by all, changed only through the integration owner:
`Cargo.toml`, `Cargo.lock`, `.github/workflows/mqttiumd.yml`, `AGENTS.md`,
`docs/decisions/`, `docs/reception/open-conditions.md`,
`crates/mqttiumd-lab/tests/boundary.rs`. In
`crates/mqttiumd-lab/src/lib.rs` and `crates/mqttiumd-core/src/lib.rs` a
lot adds the line that declares its own module and nothing else. The
remaining files of the bench (`main.rs`, `manifest.rs`, `files.rs`) wait
for lot L4.

## Interfaces between lots

| Between | Interface | Rule |
| - | - | - |
| L1 and L6 | `release_if_current` and its types, in `credits.rs` | L6 delivers the function and its unit tests first; L1 calls it and writes no second variant (DEM-024) |
| L1 and L2 | `Command`, `Recipe`, `StoreReply`, `Input`, `Effect`, `EffectSink` | Neither changes them alone; a change is a contract change |
| L1 and L3 | The transition registry | L3 writes the rules, L1 implements them. A disagreement becomes a decision with a counterexample, never a quiet edit on either side (DEM-025) |
| L2 and L3 | Store snapshots, trace records, injection points | L3 owns the formats; L2 produces them and asks for what it needs |
| L5 and L1, L3 | Model actions against registry rules and scenario steps | L5 drafts the correspondence table; it is completed with L4 |

## Contract changes

A contract change is a change to a public type of `mqttiumd-contract`, a
schema, a catalogue, the registry, the scenario or trace format, an
injection point, or a list of `tests/boundary.rs`.

1. The lot that needs it asks the owner of the file. The owner makes the
   change, on its own small branch, apart from the rest of its lot.
2. The change carries a decision record when it decides something, the
   updated examples, and the rebuilt derived files
   (`python tools/build_corpus.py`).
3. Its description gives the contract digest before and after, and names
   the lots it affects.
4. The integration owner merges contract changes one at a time into the
   base branch. The other lots then take the new base.
5. Two lots never change the same contract file independently.

Contract changes already known:

| Change | Needed by | Made by |
| - | - | - |
| Creation authority, private `Outbox` constructor (record 0009) | L1, L4 | L1, first |
| A refusal of `reserve` that says a table is definitively exhausted | L1 | L1 |
| Credits types for `release_if_current` | L1, L6 | L6, first |
| Routes and capacities among the recovered facts | L1, L2 | L1, with L2 |
| Scenario steps of P1-02b: fire a deadline, release a held command, deliver a held result | L2, L4 | L3 |
| Trace records and the assertion for the provenance of broker inputs (record 0003) | L2, L4 | L3 |
| Store snapshot format | L2, L4 | L3, with L2 |
| Registry rules: the 39 uncovered cells, and the rules of record 0001 | L1 | L3 |
| Attribute and macro lists of the boundary, if a proof tool needs one | L6 | Integration owner, by decision |

## Independence

- The oracle role (L3) does not read `crates/mqttiumd-core/`. Its
  expectations come from the specification and from observations.
- The core role (L1) does not edit `contracts/` or `tests/oracle/`.
- The bench does not lower an assertion to make a run pass.
- A lot's report is a guide for review. The code, the outputs and the
  traces are the evidence.

## Reception

Each mission lists its criteria. For every lot:

- the four checks of `AGENTS.md` pass on the proposed revision, with the
  derived-file check and the second schema reader when contracts changed;
- the report gives the revision, the contract digest, the commands run and
  their results, what was not run, and the limits that remain;
- a defect is fixed test first: the discriminating test is committed while
  it fails;
- each control mutation is seen to fail its test, then reverted; a
  surviving mutation is reported and closed with a test;
- the open conditions the lot closes are removed from
  `../reception/open-conditions.md` in the same change.

A lot is received on the revision actually proposed. If the content changes
afterwards, the evidence it touches is run again.
