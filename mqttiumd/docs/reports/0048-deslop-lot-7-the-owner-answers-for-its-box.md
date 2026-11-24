# The owner answers for its box: deslop lot 7

Report 0039 explains `deslop-please` and the baseline
`.deslop-please.allow`. Lot 7 removes the last field leak of the core,
`Owner::outbox`, as [record 0064](../decisions/0064-the-owner-answers-for-its-box.md)
decides. It changes no behaviour: every event, failure code, file format
and byte on the wire is the same.

## Host and revisions

- **Revision**: branch `refactor/deslop-layers`, on top of `9772112`
  (report 0047): `5775ec9` (the test written first), `4d5f165` (the questions),
  `ad535a5` (record 0064 and the open condition).
- **Host**: Linux x86-64 container, 4 cores, `rustc 1.97.0`.

## What the callers asked

`Owner::outbox()` returned `Option<&Outbox>`, always `Some` since finding
R2. Its callers outside the core:

| Caller | Asked |
| - | - |
| the runtime, when shutdown asks whether everything is done | whether the box is empty |
| the runtime's drain loop, three places | how many effects wait |
| the runtime's decision to dispatch | how many slots are free, how many effects wait |
| the runtime before a dispatch | whether the box is empty |
| the runtime, for a connection whose write is held back | whether that effect still waits |
| the bench (`bench/process.rs`) | the identities of the waiting effects, in order; whether the box is empty |
| the tests of the runtime, 18 places | the waiting effects, their identity and content, in order |

## The change

The owner answers `has_pending_effects`, `pending_effect_count`,
`free_effect_slots` and `holds_effect(id)` by value. `holds_effect` is
the scan the runtime ran (`pending().any(..)`). The owner also lends
`pending_effects`, read only, for the bench and the tests: an
`OwnedEffect` is moved, never copied. The owner of the repository chose
that read view on 24 November 2026, over a copy and over tests that would
observe through `dispatch`.

`Owner::outbox()` goes, with its `Option` and the comment that kept it
for earlier callers. The bench's wrapper, which called it and then
`expect`, goes too. Each call is rewritten one for one, and the counts
match: seven in the runtime, three in the bench, eighteen in the tests.
The condition of the decision to dispatch is the same disjunction without
the closure.

The test `the_owner_answers_for_its_box` was committed first (`5775ec9`),
and failed to compile for want of the five methods. It checks each
answer against the box itself:

- before a publication;
- while its effects wait;
- once they have been handed out;
- while the effects of a second publication wait.

Five throwaway mutations, one per method, each make it fail:

- `holds_effect` answering "the box is not empty";
- `free_effect_slots` answering the capacity;
- `has_pending_effects` answering true;
- `pending_effect_count` capped at one;
- `pending_effects` skipping the first.

The first of these survived the first version of the test. That version
never asked about a handed-out effect while others waited. The last case
of the list above was added to the test before it was committed as `5775ec9`.

The open condition "`Owner::outbox()` still returns an `Option`" is
closed and removed from `docs/reception/open-conditions.md`. Record 0064
is Proposed and amends no record; record 0009 keeps the creation and the
renewal of boxes. A Proposed record is not in the register, and the
contract digest does not move.

`tools/bounded-batch.mutant` names `.outbox()`; its anchors were already
broken (report 0042) and it is left as it is.

## Count

| | Before | After |
| - | -: | -: |
| Entries of `.deslop-please.allow` | 24 | 23 |

The linter does not report `pending_effects`, whose body is a call; the
header of the baseline names it, beside the rows the delivery table lends.
What is left:

- the two entries of the lab kept on purpose;
- the two shallow crates;
- the layout entries: long files and wide directories.

## Performance

`runtime.rs` asks the owner inside the drain loop, so the gate of
report 0046 was played on `dist` builds, `4d5f165` against `9772112`.

| Shape | Lot 7 against `9772112` |
| - | -: |
| QoS 0 bursts, 1 subscriber | 1.024 |
| QoS 0 bursts, 8 subscribers | 1.016 |
| QoS 1, 1 pair | 0.994 |
| QoS 1, 8 pairs | 1.003 |
| QoS 0 blast, 1 (reported) | 0.940 |
| QoS 0 blast, 8 (reported) | 0.999 |

Every deciding shape passes over six passes, and none reaches 1.03, so no
instruction count was taken. The gate resolves about 4 % on QoS 0 bursts
with one subscriber (report 0045).

## Commands run

From `mqttiumd/`, one command after another:

| Command | Result |
| - | - |
| `cargo test -p mqttiumd-core --locked the_owner_answers_for_its_box` at `5775ec9` | does not compile: the five methods do not exist |
| the same at `4d5f165`, and with each of the five mutations | passes; each mutation fails it |
| `cargo test -p mqttiumd-lab --locked --test requirements` at `ad535a5` | 5 passed |
| `deslop-please --allow .deslop-please.allow` | `ok`, 23 entries |
| `cargo fmt --check` | clean |
| `cargo clippy --workspace --all-targets --locked -- -D warnings`, also with `--features mqttiumd-lab/kill-points`, and `-p mqttiumd --features test-hooks` | clean |
| `cargo test --workspace --locked` | 1382 passed (1381 before, and the new test) |
| `cargo test --workspace --features mqttiumd-lab/kill-points --locked` | 1404 passed (1403 before, and the new test) |
| `cargo test -p mqttiumd --features test-hooks --locked` | 288 passed |
| `mqttiumd-lab validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json` | exit 0 |
| `tools/build_corpus.py --check`, `tests/oracle/build_traces.py --check`, the oracle unit tests | exit 0 |
| `conformance/check.py --check` | exit 0 |
| `models/check_models.py` | exit 0 |
| `git apply --check` of every `tests/mutants/*/*.mutant` | all apply |
| `sha256sum -c` of the files `proofs/credits/BODY.sha256` names | all match |
| `tools/run_loop_mutants.sh`, on the simulated store and with `--backend sqlite` | every mutation rejected for its reason |
| `tools/run_pi_core_mutants.py`, `tools/run_expiry_mutants.py`, `tools/check_will_core_mutants.py` | every mutant detected |
| The digests of the 16 traces of `mqttiumd-lab`, against `9b7c05d` | the same |
| `tests/compat/run.py` against Mosquitto 2.1.4 (release build) | 135 scenarios: 105 same, 15 refused by the profile, 6 where the reference does not conform, 9 unspecified, 0 defects; 21 of 21 expectations pass |
| `tools/run_perf_gate.py` (`dist` builds), `4d5f165` against `9772112` | above |

## Not run, not shown

- The proofs (Kani): the files `BODY.sha256` hashes are unchanged.
- `run_secure_mutants.py` and `run_policy_mutants.py`, which the frozen
  drivers of `tests/system` refuse at their first case (report 0047), and
  the mutations whose anchors were already broken (report 0042).
- No qualifying campaign (`run --rebuild`).
