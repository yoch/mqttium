# L5 reception at d76ccc69

Reception of the existing M1Answers delivery on 15 November 2026. This
report concerns the model and its executable replays. The transactional
crash campaigns have their own reception and are not claimed here.
All 52 TLC configurations matched their declared outcomes at the tested
integration revision, and all seven named core replays ran successfully.

## Content received

The GitHub connector resolved `claude/mqttiumd-l5-model-m1` to
`f5c1ee0d9683f1aff65cb080c1860805f4789d97`. It is already an ancestor of
integration HEAD `d76ccc691fcd54324674c07abfb1a5ec8a1312a7`: the connector's
comparison reports zero commits ahead and sixteen behind. Merge
`c5048300d42ae778575a38055753e12ec87a5bf2` integrated the delivery. No
additional merge or reimplementation was necessary.

The delivery adds M1Answers, fourteen configurations, the correspondence
with the seven core replay tests, and consistency checks for those replay
names. Its base contains integration revision
`e0013518a17de964460e7a63d3049515e2d6197b`. The model content at the lot HEAD
and at the tested integration HEAD is identical. In particular:

| File | SHA-256 |
| - | - |
| `models/m1/M1Answers.tla` | `b254e82d8b11b622cdb22b3830128c105d0506919ba483e4079741047b471ba5` |
| `models/m1/M1Answers.json` | `6518c8e6354c9cc7954f8d612afebc43ad70d9f35366e546f23be3ecd66c9fc5` |

The connector returned successful CI, mqttiumd and Formal models workflow
runs for the lot HEAD; the mqttiumd run was
<https://github.com/yoch/mqttium/actions/runs/43742268936>. These are checks
of that SHA, separate from the executions below and from checks of a
future integration SHA.

## Correspondence and scope

The reviewed mapping follows the actual ring positions: the last two
resolution ranks use `rank % 2`, the last four power-of-two ranks use
`ctz(rank) % 4`, and the original Persist remains retained. M1Answers uses
ranks, not elapsed time. Its finite checks cover the configured domains;
they do not prove the core for every possible bound.

I3 requires one uniform finite lag bound, unknown to the core, measured
in newer resolutions of the same operation, with finitely many lost or
unknown answers. It assumes no FIFO delivery. The runtime still owes the
timer and queue assumptions listed in `models/m1/README.md`; a permanently
growing backlog relative to deadline emission is outside I3.

The unbounded configuration remains a limit: at rank 9, answers 3, 5, 6
and 7 were delivered after eviction and absorbed, while the retained
answers 1, 2, 4, 8 and 9 were in transit. No answer was lost or unknown,
and the operation remained unresolved. The safety counterexample is
`NoStarvationWithEveryEvictedAnswerDelivered`; the core test
`review_individually_finite_delays_do_not_imply_a_uniform_lag_bound`
extends this pattern through 512 resolutions and 507 delivered answers.
It is not reported as a defect under I3.

In M1a and M1b, `ApplyCommand` calls the atomic operator `Commit`. M1a's
comment calls this assumption `ApplyCommit`: publication, inbound slot,
obligations, decision and accounts change together. Real K1/K2b/K3 tests
and a sampled K2a sweep can support the SQLite implementation of that
assumption within their stated failure model. A finite family of tests
does not establish universal atomicity for every instant or storage
failure. Process kills preserve the kernel and its page cache; power
loss, torn writes and kernel crashes need separate evidence.

## Reproduction

From the repository root, with the Rust toolchain and a Java runtime on
PATH:

```sh
python mqttiumd/models/check_models.py
python -m unittest discover -s mqttiumd/models -p 'test_*.py'
python tools/formal/run_tlc.py --models-dir mqttiumd/models/m1 --list
python tools/formal/run_tlc.py --models-dir mqttiumd/models/m1
cd mqttiumd
CARGO_TARGET_DIR=/tmp/mqttium-target-l5 cargo test -p mqttiumd-core --locked owner::tests::
```

The runner hash-checks its pinned `tla2tools-1.7.4.jar`:
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.
The jar identifies TLC 2.19 (8 August 2024, revision `5a47802`). The
reception used OpenJDK 21.0.13, breadth-first exploration, one worker,
and no symmetry. A read-only default cache was handled by setting
`XDG_CACHE_HOME=/tmp/mqttium-l5-cache`; no tool or source version changed.
The main TLC invocation and model unit checks used Python 3.12.14; the
separately retained witnesses used Python 3.13.5 with the same runner and
jar. The Rust checks used rustc and Cargo 1.97.0.

The model consistency check passed; its sixteen unit tests passed. The
core owner suite passed 107 tests, including all seven replays named by
M1Answers.

The baseline also passed `cargo fmt --check` and
`cargo clippy --workspace --all-targets --locked -- -D warnings` with a
separate target directory, `/tmp/mqttium-target-l5`. A full workspace test
attempt stopped at the fourteen tests of `qualifying.rs`: their shared
scratch helper assumed that Cargo's `CARGO_TARGET_TMPDIR` was below the
workspace root and panicked on `strip_prefix`. This is an infrastructure
failure for that external target directory, not a failed oracle
assertion and not a successful negative control. The integration work
tracks its correction separately; this report does not claim a passing
full workspace suite at the baseline revision. Before that stop, 459
tests passed and none was ignored. The independent scenario validation
command accepted seven documents and printed contract digest
`sha256:5ab94255d91bfce1d53d18279399c9b8dc055a6e017bfa334941593808cf51a5`.

## Fresh model execution

The full versioned TLC command exited 0. Every configuration terminated
within its 900-second bound; none was interrupted or retried in that
campaign. Matching an expected violation means that the named invariant
was violated, not merely that TLC returned an error.

| Module | Configurations | Matched | Scope |
| - | - | - | - |
| M1a | 19 | 19 | Safety, witnesses, mutations, and the two liveness properties under their declared assumptions |
| M1b | 19 | 19 | The declared groups of faults, witnesses and mutations; no liveness claim |
| M1Answers | 14 | 14 | Five passing configurations and nine expected invariant violations |

For the full I3 configuration, `R=2`, `L=4`, `K=4`, one lost answer, one
unknown answer and one duplicate are allowed; `Bound=20`, `MaxRank=21`.
Both the invariant configuration and its temporal counterpart explored
14,986 distinct states. The reduced configuration has `R=1`, `L=2`,
`K=2`, `Bound=10`, `MaxRank=11` and explored 1,050 distinct states. The
uniform-lag pairs pass; their counterparts without a uniform bound retain
the specified starvation counterexample.

Three configurations were also executed separately through the same
runner's `run_check`, retaining their full TLC output. Each matched its
declared invariant: unbounded lag, 14,080 distinct states; landmark
reachability, 720; the no-landmarks mutant, 1,594,707. These are separate
model runs, and their full traces are labelled accordingly in the archive.

The reception archive is
`mqttium-l5-reception-d76ccc69.tar.gz` (19,670 bytes), SHA-256
`92aecdb6674e833241a2d9a847e642284091e2ab2cfaa7e068e25225ac7313a7`.
It contains the 52-outcome log, the three full witness outputs, source
digests, tool identities, the owner-suite log and the workspace attempt's
actual outcomes. Generated logs and the archive are not committed.

No model behavior, core behavior, contract digest, decision-record status
or merge into `main` changes through this reception.
