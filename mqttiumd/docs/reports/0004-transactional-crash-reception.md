# Transactional SQLite crash reception

Received code: `38db7d6ecd7e2ff2f71402480d3b3105e52cd265`, published on
`claude/optimistic-feynman-fqh947`, PR
[648](https://github.com/yoch/mqttium/pull/648). This is S0 incoming-core,
store and bench work, not a complete network broker. No merge into `main`
or acceptance of proposed ADRs is made by this reception.

## Content and provenance

The GitHub connector confirmed starting head
`d76ccc691fcd54324674c07abfb1a5ec8a1312a7`, ahead of the supplied
`e0013518` reference, against base
`874f298b4de7c3a13006cb1ff767dfb09e4031ac`. The existing K1 test, L3
recovery work and L5 merge were reused. Existing L7 `1474895d` and L3
`4fa0bce9` deliveries were inspected and reused. The local checkout was
first aligned with the confirmed head; local worktrees and build directories
kept the lots separate. R1 to R4 were preserved rather than reimplemented.

The three fresh qualifying campaigns below all record **clean revision
38db7d6e**, identical source archive SHA-256
`2c2daf12d6778dbe6e35e7726bf05c0fa3dac43c4577953f35f17556935a5402`,
and the new oracle identity. The published head and the changed evidence
writer were read back through the connector; blob
`bfebb7cb5a98acae3ffbd77031f9462096b9f14d` matches the tested local blob.
The subsequent commit adding this report and completing the journal changes
documentation only. Its exact published head and CI conclusions belong to
the PR metadata; these source archives continue to identify their actual run.

Contract digests, in order:

- Initial: `sha256:5ab94255d91bfce1d53d18279399c9b8dc055a6e017bfa334941593808cf51a5`.
- Two-point catalogue: `sha256:3c720b161fab91841662b98a22fae898c1ab634d461b209dfd994eaf791d77ed`.
- Final catalogue/assertion: `sha256:d2f238ea6214fec13ad9fcf6c324c609eadd408be695bc15101822167a0cfe0b`.

ADR 0020 remains **Proposed**. `validate` checks documents;
`recompute` judges assertions and qualification conditions. Neither boundary
was weakened. No pure-crate or immutable French-archive file changed.

## Deterministic points

| Case | Actual point and target observation | Signal / same-file restart | Complete reference expectation and result | Oracle / recompute | Negative control |
| - | - | - | - | - | - |
| K1 | `store.commit.prepared`, FaultHit 19; no stable record of the interrupted operation | 9 / yes | Entire state before command, exactly recovered | qualifying PASS / stands as PASS | wrong declared point: FAIL for expected reason |
| K2b | `store.commit.logged`, FaultHit 19; no stable record of the interrupted operation | 9 / yes | Entire state after command, exactly recovered; replay does not admit again | qualifying PASS / stands as PASS | wrong declared point: FAIL for expected reason |
| K3 | `store.commit.applied`, stable record 19 is a parent of FaultHit 20 | 9 / yes | Entire state after command, exactly recovered; replay does not admit again | qualifying PASS / stands as PASS | wrong declared point: FAIL for expected reason |

The complete-state comparisons are **store subprocess tests** against
`MemoryStore`, before opening a new epoch: ledger, collection bound,
revisions, limits, accounts, session/route identities, receipts and recipes,
fences, publications, payloads, inbound slots and obligations. They use
direct `DurableState::eq`, without a projection. Each test waits for the
target acknowledgement, sends SIGKILL, checks wait signal 9, reopens the same
file, checks accounts and compares states. SQLite opening runs `quick_check`.
K3's test parks after `apply` before any delivery to a core. The campaign
then independently exercises actual new-epoch recovery and retransmission;
its oracle checks operation identities and invariants, not a claim of full
state equality calculated by the runner.

The partial-business-state control removes an obligation in an isolated
database. `quick_check=ok`, but complete equality and held-account accounting
detect the defect. This control is an artificial mutation, not a process kill.
The per-campaign negative copies change only the declared point in FaultHit;
the independent assertion `transactional_kill_matches_recovery` rejects each
for `FaultHit does not match its declared point`.

K1/K2b execute a command still inside the SQLite stack. The survivor accepts
an ordered evidence prefix before acknowledging it; the child consumes that
acknowledgement, reports `Parked`, then blocks. Only SIGKILL may follow.
Frames are limited to 65,536 bytes, the evidence window to 64, the shared
receive queue to 512, control reports per action to 64, and silence to 30 s.
These actual constants are in `campaign.json.process_protocol`. K3 uses the
adapter's drained post-commit barrier, before its result is created. All
paths preserve one writer, confirmed death before restart, and error cleanup.

Each point also has a **separate witnessed run**, with its own
`kill-witness.strace` checked against that run's recorded PID and signal-9
wait. Witnessed PIDs are K1 138877, K2b 147952 and K3 160142. The witnessed
application traces equal their qualifying counterparts, but their system
witnesses establish only the witnessed executions. The qualifying runs have
their own supervisor wait observations and reconstruction evidence. No
system witness is transferred from one run to another.

## Features, engine and reconstruction

Linked versions: rusqlite **0.43.0**, bundled SQLite **3.54.1**; WAL,
`synchronous=2` (FULL), 4,096-byte pages, with compile options recorded.
The observed engine test acquires `BEGIN IMMEDIATE` on another raw SQLite
connection inside the callback, then rolls it back: the WAL hook runs after
release of the engine write lock. A later autocheckpoint setting demonstrably
replaces that callback, and installing the callback last restores it.

The armed policy disables autocheckpoint, reads back zero, then installs
the hook last; no checkpoint is performed by that hook. Disarming removes
the hook before restoring the saved threshold. Changing the threshold while
armed is refused, after a committed failing test reproduced its replacement.
K1/K2b record the armed settings at the actual point. K3's normal build uses
the default autocheckpoint policy. A PRAGMA alone is not used as proof that a
callback is installed.

K1/K2b explicitly enable lab `kill-points`; K3 uses no kill instrumentation.
The actual store setting, feature selection, replay and isolated source-build
command agree. Dependency-only feature unification is refused before any
campaign write or source assembly. The normal feature graph is clean.
`--rebuild` and the independent second construction use the same feature
option, locked offline dependencies and remapping flags, ignoring inherited
target/encoded flags. Both normal and instrumented rebuild tests exercise
the independent-directory check.

Second constructions matched these exact binary SHA-256 values:

- K1/K2b: `cb153583809ebbbeefbdde9c735cd56391750cc89c4137fb38ef81dae1789401`.
- K3: `d6396735210c3e7a230e020bb8063903a8b3aac89f6415abad6d5ab0aa4c6f8b`.

## Findings closed during integration

Discriminating tests were committed before fixes. The oracle now rejects
an admission recovered without its matching page read or against a
contradictory durable decision, deduplicates by durable operation identity,
and attributes recovery-only SQLite admissions through their original
submission and observed peer PUBLISH. SQLite startup evidence is joined by
`BrokerRecovered` rank and child/page identity; missing, reused or mismatched
evidence is inconclusive. `Trace.before` and product causality stay unchanged.

The first actual K1 observations exposed the missing startup join, and the
first actual K3 qualifier correctly returned FAIL because the stable write
and applied point were unordered siblings. Test `36d2d021` reproduced the
latter; `7da0b192` records the real stable write as that point's parent by
`WorkId`. The oracle requirement was preserved. All three final campaigns
were regenerated after this fix. A separate historical K3 archive preserves
the valid pre-fix qualifying FAIL for reproduction.

Integration also corrected the external-Cargo-target fixture path helper
(red `930e0e2e`, fix `7fc543cb`) and completed the adapter catalogue test matrix.
The existing SQLite loop reception rejected all 11 applicable code mutations
for their expected reasons; the sim-store-specific mutation was excluded.
That reception was on `fe52ba8e`, before the later K3 ordering fix; the final
SHA's CI reruns the full workflow, rather than assigning that older run to it.

## Bounded complementary experiments

K2a is separate, non-qualifying exploratory evidence. Clean explorer code
`3844d35c5fded5259be4c2759372fbf77ac6fb62` executed **64 calibration trials**,
then exactly **1,000 frozen trials**, seed 1, uniform requested delays
0–800,000 ns, no repeats. There were **600 found / 400 absent**, **zero mixed
states / errors**, and 1,000 signal-9 waits. Among commands with no commit
return observed before signal: **460 found / 400 absent**. The other 140
were established after-return positions. **Zero positions inside COMMIT are
established.** Every usable trial reopens the same files and compares the
whole state before a new epoch. Normal autocheckpoint 16 stays installed;
a separate test observes its real checkpoint of a 65,536-byte multirow
admission. See report `0005`; an independent replay of all archived requested
delays agrees with the bounded-rejection generator. A seed does not reproduce
OS scheduling.

WAL images are separate artificial storage data. The prefix fixture commits
three admission/release pairs, completes and verifies
`wal_checkpoint(TRUNCATE)=(0,0,0)`, captures that coherent main database,
then captures the WAL of exactly one target admission. Its eight frames are
4,120 bytes each, with one final commit frame; WAL length is 32,992 bytes.
The base revision is 12 and the target revision 13. Tests cut within the WAL
header, at every frame boundary, within frame headers and within pages:
**27 images, 26 exact before, one exact after, zero errors**. Metadata keeps
frame/commit boundaries, associated bytes, command and complete references.
L7 ran on `bec93a1f`; the integrator independently ran the versioned tool at
`bf42d85841e96dd8c20314177e85fd5eaa08d707` with the same outcomes. No files of
a real crash campaign are truncated or restored.

M1's abstract `Commit(o,r)` used by `ApplyCommand` maps to one concrete
`BEGIN IMMEDIATE` transaction, guarded writes and meta revision, then
`COMMIT`. These deterministic boundaries, finite timing samples and artificial
images support that implementation. They are not a universal proof for all
instants, kernels or storage devices. Process kills preserve the kernel/page
cache. Power loss, torn media writes and storage outside the stated assumptions
remain out of scope. L5 is already integrated (`c5048300`); report `0003`
records all 52 declared TLC outcomes and preserves I3 and its unbounded-lag
counterexample. Full lease composition and performance targets remain separate.

## Commands and archives

From `mqttiumd/`, with the pinned Rust toolchain and oracle requirements:

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo clippy --workspace --all-targets --features mqttiumd-lab/kill-points --locked -- -D warnings
cargo test --workspace --features mqttiumd-lab/kill-points --locked
cargo run -q -p mqttiumd-lab --locked -- validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json
python -m unittest discover -t tests/oracle -s tests/oracle/tests
python tools/build_corpus.py --check
python tests/oracle/build_traces.py --check
python tools/check_schemas.py
tools/check_kill_points.sh
tools/run_transactional_reception.sh /tmp/new-transactions 1
tools/run_k2a_sweep.sh /tmp/new-k2a 1
tools/run_wal_prefix_experiment.sh /tmp/new-wal-images
```

On the received code: normal Rust **645 passed**, instrumented **653 passed**,
zero failed/ignored; both clippy modes and formatting pass. Oracle **129 pass**;
337 corpus and 399 trace files current; second readers agree on 351 documents.
All three qualifying results are accepted and stand at recompute. The
isolated builds test R4, replay checks R3, and oracle qualification tests retain
R1. R2's authority implementation and core were not changed. CI of the published
documentation head is checked separately through GitHub.

Generated evidence lives outside git in `/workspace/mqttium-evidence/`.
Each point archive contains qualifying, separately witnessed and negative
directories plus its system witness. SHA-256:

| Archive | SHA-256 |
| - | - |
| `mqttiumd-K1-38db7d6e.tar.gz` | `6063936ee90e12d9f5bfbf303214ead84a91cd2249a2fd0fdbce6d5b36b87cf3` |
| `mqttiumd-K2b-38db7d6e.tar.gz` | `1d2a376660a593eda3c4fc3b586c539611f79c5e77073fca79d289864db16d4e` |
| `mqttiumd-K3-38db7d6e.tar.gz` | `ed16d0833201ee0f34c3dec9b53a7d301c1c63bb3da46ec50c90edb0506ff58d` |
| `mqttiumd-k2a-3844d35c.tar.gz` | `21297c51f3b744be56a547624c2973660181a51e9c9f96a863330eb19276907e` |
| `mqttiumd-wal-prefixes-bf42d858.tar.gz` | `9d3bfd1ec3c3edceaae42395fd68af4ebed3c255d2f07989f0200b2682ed2283` |
| `mqttium-l7-wal-final.tar.gz` | `3bf3612cd3a742c34664268dfedfd78d1870d150d30b5566eb04ec9155d1b0d4` |
| `mqttium-l5-reception-d76ccc69.tar.gz` | `92aecdb6674e833241a2d9a847e642284091e2ab2cfaa7e068e25225ac7313a7` |
| `mqttiumd-k1-store-baseline-d76ccc69.tar.gz` | `014da0f9a9a21d7ed5be3388080e0eee113ba05a44d14191789657c8b3f66305` |
| `mqttiumd-k3-order-defect-fb29b0ce.tar.gz` | `02274e71e59afde08ed0b260164fe08e0d0caa5ac2289766ab533cc32539ad4a` |

## Next executable increment

Propagate bounded durable-reader errors through the store trait and child
supervisor, instead of a snapshot panic. First result: an isolated SQLite
image with a malformed durable value still passes quick_check, but startup
returns a typed failure, leaves no child alive and yields an explicitly
INCONCLUSIVE oracle result without any positive acknowledgement. Reception
requires a committed failing test, a coordinated proposed trait/format decision,
both readers, both stores and the existing crash campaigns passing again.
This addresses the remaining snapshot-error row without widening the storage
failure model or implying universal atomicity.
