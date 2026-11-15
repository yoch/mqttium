# K2a: a bounded sample of process kills

Exploratory evidence, 15 November 2026. This is separate from the
deterministic K1, K2b and K3 oracle campaigns and from the artificial WAL
images. It is not a qualifying result or an oracle verdict.

## Content and command

The code executed was the clean revision
`3844d35c5fded5259be4c2759372fbf77ac6fb62`, based on the integration HEAD
`d76ccc691fcd54324674c07abfb1a5ec8a1312a7`. The explorer and its wrapper are
`crates/mqttiumd-store-sqlite/examples/kill_sweep.rs` and
`tools/run_k2a_sweep.sh`. The wrapper builds once before either phase:

```bash
cd mqttiumd
source /workspace/.mqttiumd-tools/env.sh # this machine's installed tools
CARGO_TARGET_DIR=/tmp/mqttium-target-k2a \
  tools/run_k2a_sweep.sh /tmp/mqttium-k2a-exploration-3844d35c 1
```

The release example's SHA-256 was
`4fccbb949bea648ac763ef9d92f4a8592e7b22f7e9082e66223d1126a8c6e239`.
Rust was 1.97.0, host `x86_64-unknown-linux-gnu`, on Linux 6.18.57 and an
overlay filesystem. This is ordinary local build provenance, not the
source-archive reconstruction of a qualifying oracle campaign.

Archive: `mqttiumd-k2a-3844d35c.tar.gz`, SHA-256
`21297c51f3b744be56a547624c2973660181a51e9c9f96a863330eb19276907e`.
It contains the exact parameters, both phases' JSONL observations and
summaries, the frozen selection decision, full deterministic reference
states, binary digest, commands and machine information. Generated data is
not committed.

## Construction and comparison

Each trial creates a real SQLite store through its public API, prepares
the three persistent sessions and routes, opens one epoch, and commits
three admission/release pairs. The target alternates between a further
admission and its release. An admission carries 65,536 payload bytes and
two obligations; a release trial first commits that admission as part of
its prefix. Every command has an explicit durable operation identity.

The child writes `CommandSubmitted` with that identity and the settings,
then waits. The parent receives it, acknowledges by releasing the command,
waits the requested delay, notes the latest report it has received, and
sends SIGKILL. If the command has returned first, the child writes
`CommitReturned` and waits, preserving the writer until the kill. The
parent requires a wait status of signal 9 with no exit code.

There is no inspector, restoration, copy, deletion or alternate file
between the signal and reopening the same store. `SqliteStore::open`
performs `quick_check`. Before opening a new epoch, the explorer compares
the entire recovered `DurableState` directly with the deterministic
store's states immediately before and after the target command. This
includes the ledger, collection bound, revision, limits, accounts,
session and route identities and revisions, operation recipes and
outcomes, fences, publications, payloads, obligations and inbound slots.
No projection removes the facts that carry the business invariants.
Accounting is checked separately. The hashes in each row are diagnostics;
the comparison is direct `Eq` of all fields.

Stores are removed only after the actual recovery, integrity check and
full comparison finish. A mixed state would retain its files and a full
diagnostic. Frames are at most 65,536 bytes, the reader queue has four
places, each child writes at most two valid reports, and command/read and
wait deadlines are 30 seconds. Error paths kill and reap their child.

## Effective SQLite policy

Every row records SQLite **3.54.1**, linked by rusqlite **0.43.0**, the
engine's compile options, WAL mode, `synchronous=2` (FULL), 4,096-byte
pages, `wal_autocheckpoint=16`, and `kill_points=false`. The setting is
applied on the actual writer connection before the prefix. This explorer
installs no custom WAL hook, so it preserves SQLite's autocheckpoint
callback. It records no count of callbacks or checkpoints per trial.

The companion reproducible test
`a_sixteen_page_autocheckpoint_reaches_the_main_database_while_the_writer_lives`
(`00119aac`) observes a real checkpoint for the same API, engine and
65,536-byte admission by comparing an isolated main-database image with
the deterministic full state while the writer remains alive and idle.
That test is distinct from these kills. A PRAGMA readback alone would
not establish that a callback remained installed.

## Finite budget and observed outcomes

The declared budget was 64 calibration trials followed by exactly one
1,000-trial campaign, with no repetition or tuning after the campaign was
frozen. The calibration used eight repetitions at each fixed delay of
0, 25, 50, 100, 200, 400, 800 and 1,600 microseconds. Its outcome was 27
found, 37 absent, zero mixed and zero errors.

The versioned selection rule chooses twice the first calibration delay
whose bucket has at least four found states, clamped to 50–3,200
microseconds; no crossover chooses 3,200. The first such bucket was 400,
so the campaign froze **seed 1**, uniform integer requested delays in
**[0, 800,000] nanoseconds**, alternating target kinds, and zero repeats.
SplitMix64 with bounded rejection sampling generates the choices.

| Phase | Trials | Found target | Absent target | Mixed | Errors |
| - | -: | -: | -: | -: | -: |
| Calibration | 64 | 27 | 37 | 0 | 0 |
| Frozen campaign | 1,000 | 600 | 400 | 0 | 0 |
| Admission targets in campaign | 500 | 335 | 165 | 0 | 0 |
| Release targets in campaign | 500 | 265 | 235 | 0 | 0 |

All 1,000 campaign trials were exploitable: each had a genuine signal-9
wait status, successful reopening and `quick_check`, accounting agreement,
and exact equality with one of the two full reference states.

| Latest report observed before signal | Found | Absent | What it establishes |
| - | -: | -: | - |
| `CommandSubmitted` | 460 | 400 | The command was released; its COMMIT position is not established |
| `CommitReturned` | 140 | 0 | COMMIT returned before the parent sent the signal |

Thus both diversity quotas also hold among commands whose completion the
parent had not observed: **460 found and 400 absent**. This is an
observation of uncertainty at the supervisor, not proof that all 860
commands were still executing at the instant of the signal. For those
trials the possible positions include before execution, inside the
command, inside COMMIT, and after COMMIT before observation. **Zero
positions inside COMMIT are established.** The 140 after-return trials
are classified separately and are not needed for these quotas.

Requested delays ranged from 903 to 798,541 ns. Elapsed time from release
initiation to just before the signal ranged from 79,219 to 25,556,405 ns,
with a median of 567,331 ns and the 990th ordered sample at 5,705,688 ns.
Scheduling and reporting overhead therefore matter. A seed reproduces
generator choices, not scheduling or those elapsed times.

An initial launch stopped before any child trial or kill: the general
conformance helper bounds payload copies at 1,024 bytes. Its local use
was corrected after a committed failing test (`75aa6c83`); the final
explorer constructs its declared larger payload independently, retaining
the fixture identities and targets. That startup failure consumed zero
calibration or campaign trials and is not a successful negative control.

## Validation and limits

Executed on the final explorer content: `cargo fmt --all --check`,
`cargo clippy -p mqttiumd-store-sqlite --example kill_sweep --locked -- -D
warnings`, the discriminating example test
`the_sweep_command_uses_the_declared_large_payload_and_commits`, and the
complete 64 + 1,000 execution above. A separate read of all 1,000 rows
verified the signal/wait fields, target-state diagnostic hashes, complete
comparison flags, distinct child PIDs and the counts above.

The full business comparison supports the concrete atomicity hypothesis
of M1 for this finite sample and this exact engine, workload and system.
Neither diverse outcomes nor zero mixed states locate a kill inside
COMMIT or prove atomicity at every instant. The kernel and page cache
survive every trial. No result here covers power loss, torn media writes,
kernel failure, a network filesystem, or a writer ignoring the lock.
