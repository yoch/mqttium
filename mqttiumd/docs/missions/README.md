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
| Base branch | `main`, after PR yoch/mqttium#647 (lot L0) is merged |
| Integrated revision | Not fixed yet. A commit cannot hold its own identifier: once L0 is merged, a change to this table alone records the revision of `main` that holds it |
| Contract digest | `sha256:eed8c20e1587c2efc13465efc272b80b740a7634255a056ce938f7279046b93e`. It depends on the content of the contract files only, so it is exact before the merge; the record of the revision repeats what `validate` prints on it |

The wave starts from the commit of `main` that records the integrated
revision. It differs from that revision by this table only. Every lot
names the revision it started from, and the contract digest it was
verified against, in its report.

## Before the wave starts

1. Lot L0 is received and merged into `main`.
2. Decision record 0009 (creation authority for owners and effect boxes)
   is accepted. Done, with amendments.
3. The integrated revision and the contract digest are recorded above.

## Organisation

One session and one branch per lot, and an integrator. The integrator
writes no lot: it merges contract changes one at a time, keeps the shared
files, and receives each lot on the revision actually proposed.

Each session starts with the interface the others wait for:

| Lot | First deliverable |
| - | - |
| L1 | The interface of `OwnerAuthority`, then creation, retirement and their tests |
| L2 | The contract of opening an epoch: durable states and results |
| L3 | The store snapshot and trace formats, the steps of P1-02b, the first registry rules L1 needs |
| L5 | The extension of the TLC runner, then M1a |
| L6 | The preflight, the credits types and the real `release_if_current` with its tests |

## Tools

`tools/install-tools.sh` installs the proof, model and fuzzing tools at
pinned versions: Kani and Verus for L6, the TLC jar for L5, cargo-fuzz for
L8, cargo-deny. A session that starts a lot runs it for the tools it needs
and names the versions in its report. Changing a pinned version changes the
evidence the tool produces and goes through the integration owner. The
installer has two known defects in how it recognises a tool that is
already present; lot L6 has them corrected when it starts (see its
mission). No tool needed a change of network settings.

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
`tools/install-tools.sh`,
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
| L1 and L2 | Opening an epoch and issuing the creation authority (record 0009) | L2 owns the contract of the opening, L1 the authority; the hand-over between them is written by both and changed by neither alone |
| L1 and L3 | The transition registry | L3 writes the rules, L1 implements them. A disagreement becomes a decision with a counterexample, never a quiet edit on either side (DEM-025) |
| L2 and L3 | Store snapshots, trace records, injection points | L3 owns the formats; L2 produces them and asks for what it needs |
| L5 and L1, L3 | Model actions against registry rules and scenario steps | L5 drafts the correspondence table; it is completed with L4 |

## Contract changes

A contract change is a change to a public type of `mqttiumd-contract`, a
public interface of `crates/mqttiumd-core/src/credits.rs`, a schema, a
catalogue, the registry, the scenario or trace format, an injection point,
or a list of `tests/boundary.rs`.

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
| Creation authority, private `Outbox` constructor (record 0009) | L1, L2, L4, L7 | L1, first, with L2 for the opening of an epoch |
| Contract of opening an epoch: durable states, results, hand-over of the authority (record 0009) | L1, L4, L7 | L2, first, with L1 |
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
