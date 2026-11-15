# Open conditions

What lot L0 leaves open on purpose. Each condition names the lot that closes
it and the gate or reception it blocks. A lot that closes one removes its
row in the same change; a lot that finds a new one adds it.

Nothing here is satisfied by the present tree.

## Audit of the first wave (head `0daf0eb`)

Defects found by the audit, each with a counterexample that the tests of
the lots did not contain. A row is removed by the change that closes it,
with its failing test first.

| Finding | Reception | Closed by | Blocks |
| - | - | - | - |
| **F3, progress of resolutions: what is left.** Corrected in the core (lot L1, `2255bf7`): an operation retains its command, its two latest resolutions and its four latest landmark resolutions, those whose rank is a power of two, so that an answer delayed by any finite number of resolutions meets a landmark still retained; at most seven effect identifiers per operation; the invariants are in the documentation of `Answers`. Played by the runner as a judged scenario, `tests/scenarios/audit/F3-steady-lag.json`: the result of the Persist is lost, the answer of the first resolution is delivered after six newer ones, and the exchange completes; with the landmarks removed by mutation the run stops before the PUBREC, the runner writes `StepBlocked` and the oracle answers FAIL on `scenario_reaches_its_end`, on both stores (`tests/mutants/loop/m4-landmarks-ignored.mutant`). Scope, to keep (second audit): the correction is received under invariant I3, a delay uniformly bounded by a finite bound that is not known in advance, counted in newer resolutions, with finitely many lost answers; a fair delivery whose delays are each finite but have no uniform bound is another hypothesis, which the landmarks do not cover, and a test of the core shows it as a limit, not as a defect. Left: the contract can now state a permanent delay (repeated faults, a lag, a wait that fires deadlines; record 0018, proposed, lot L3, `8f986b71`), but the bench does not play that vocabulary, and it refuses such a scenario only by failing to read it, where it should say what it does not play; the correspondence of the retention rule and of I3 with the model M1, and with the queue and scheduling hypotheses of the runtime | L4 for the vocabulary in the bench; L1 with L5 for the model | G1 |

## Audit of the second wave (head `a4aea95`)

Defects found by the review of the second wave, each with a reproduction.
A row is removed by the change that closes it, with its failing test
first. The four findings are closed: R1 (`recompute` did not check the
conditions of a qualifying result again; lot L3, `d9eef542`,
`tools/run_recompute_reception.sh`), R2 (a refused `renew_box` reopened
the identity of the owner; lot L1, `bb00da1`, `replace_outbox`), R3 (the
replay command of a campaign did not name its store;
`tests/bench_replay.rs`) and R4 (the binary check followed the build
directory and the flags of its caller; `tests/bench_rebuild.rs`). What
they leave is below.

| Finding | Reception | Closed by | Blocks |
| - | - | - | - |
| **`validate` accepts a qualifying result whose conditions no longer hold.** It checks the document: schema, digests and sizes of the files cited, contracts, assertions and faults of the scenario, coherence of the verdict. It does not judge the trace and does not check the conditions of a qualifying run, so a qualifying result without its `source.tar` is still accepted by it. Only `recompute` says that a result is evidence, which the guide of the oracle and record 0017 state | Either `validate` refuses to be read as that check, by its name or its output, or it runs the conditions too | L3 | A reader who takes `validate` for the check |
| **The public addition of finding R2 is not in a decision record.** `OwnerAuthority::replace_outbox` and `ReplaceRefusal` are new public items of the contract crate; record 0009 describes `create_outbox` and `retire` only. `Owner::outbox()` still returns an `Option` that is now always `Some` | Record 0009 is amended; the bench stops treating the absence of a box | L1 for the record; L4 for the bench | G1 |
| **What the rebuild of a binary depends on.** The path of the directory the source is unpacked in reaches the binary in spite of the remapped prefix: a check in a directory of another name builds another binary. The check and the run therefore use the directory the archive names under `TMPDIR`; two checks of one archive take turns on a lock, but `run --rebuild` does not take that lock, so a run and a check of the same source must not be started together | The run takes the lock; what in the build carries the path is found, or the dependence is stated in record 0017 with the other limits of a rebuild on another machine | L4; L9 for a build elsewhere | Agents rebuilding side by side |

## Before a result may be used as evidence (L3, gate G0)

`mqttiumd-lab validate` checks a qualifying result against the files it
names: digests, contract digest, assertions and faults of the scenario,
verdict equal to the one its assertions determine. It does not check that
the assertions are true.

| Condition | Closed by | Blocks |
| - | - | - |
| **Core, limits of what it remembers** (lot L1, corrections of the review of its core). An operation that ended is kept in a bounded window (`OwnerConfig::retained_operations`); beyond it a contradicting result is absorbed. A connection the owner closed is kept in a bounded window (`remembered_connections`); beyond it what the connection still carries is no longer known as stale. Lot L4, which assigns connection generations, decides whether "older than the current one" closes the second limit | L1, L4 | S0 |
| **Store, limits of the deterministic store** (lot L2, head `35bec750`). It clones its whole durable state per command; the records of terminal sessions stay, bounded by the session counter and not by `max_sessions`; the receipts and fences of epoch 0 are never collected; a recovered limit that does not fit its field, and a route to a session that is not active, are reported as `AllocationFailed`. Stated limits of the deterministic store; the SQLite store states its own | L7 | S0 on SQLite |
| **Store and formats, what is left.** The contract now has the outcome `Violation` with its `violation` (`RecipeMismatch`), carried unchanged by `StoreResultReady`, `StoreResultHandedOver` and `StoreResultDelivered` (lot L3, `ec65934`); the writer of the store writes it on `StoreResultReady` and `StoreResultHandedOver` (`ce64424d`, `evidence::outcome_fields`); the runner writes it on `StoreResultDelivered` with the same content, shown by a composition test in which one operation is presented with two recipes (`crates/mqttiumd-lab/tests/bench_violation.rs`); no scenario of the registry produces a violation, because no declared fault makes the store contradict itself. At `store.fence.transit` a fault on a resolution selects every resolution, because the adapter gives a resolution no source: enough while a scenario has one exchange | L3 if a scenario must produce a violation | G0 |
| **SQLite store, first slice** (lot L7, `430aa728`). Done: `crates/mqttiumd-store-sqlite` realises `DurableStore` on one file with one writer connection, one `BEGIN IMMEDIATE` transaction per command and a real rollback; the thirty contract cases of `conformance` pass on it, among them finding F1, `established`, the opening of an epoch and the collection; a differential test compares it with the deterministic store after each step of 729 sequences. Left: the guard on a payload already referenced and the collection read without an explicit bound (the collection reads every operation and fence identifier to decode its epoch); `SqliteStore::inspect` reads a snapshot through a read-only connection, in one read transaction closed before it returns, and the opening runs `PRAGMA quick_check` (`34a27e45`), and the runner reads every snapshot of a SQLite campaign through it; the six scenarios are played on this store by the runner and judged PASS (`run --backend sqlite`, seed 1), and the loop mutations are rejected on it, `m1s` being the equivalent of `m1` in the SQLite store. Left: `SqliteStore::try_snapshot` and `inspect` return an error for a durable value that does not decode (`82a59b59`), but `DurableStore::snapshot` has no error in its interface and still stops the process on one: changing the trait is a contract change, to be decided with the first real crash, when a damaged file becomes reachable; the crash of a scenario is simulated inside one process on this store too (the adapter and its connection are dropped and the same file is opened again), so nothing is shown about a killed process, a transaction cut by a kill, `fsync` or a power loss; the process topology of a real kill is a plan (`docs/missions/L4-process-topology.md`), not code | L7; L4 for the child process; L9 for the real crash | S0 on SQLite |
| **Oracle, what is left of lot L3** (final head `d86adb0`; the integrator read the corrections of findings 1 and 4 of the review of `3c023bf`, the registry change and the changed test of lot L2, not the rest). No bench plays a scenario: the oracle judged only traces written by hand. The list of contract files is still computed twice, by `manifest.rs` and by `Workspace::open`: both refuse a file of the schema directory that is not a schema, and one list read once needs a change of the shared `manifest.rs` that lot L3 proposed and that is not applied. The Python reader keeps its own copy of the order of recovered facts, on purpose, tied to the Rust reader by twelve shared cases. The registry has no guard on exhausted identities and no session machine. A `ClientBytesReceived` carries one whole packet in v1 | L4 for the bench; L3 with L1 for the guard; integrator for `manifest.rs` | G0 |
| The registry rules that decide a behaviour of the broker which the chapters leave open (record 0013, proposed) are accepted or replaced | Owner of the repository | G0 |

## Before a campaign is run (L4, L7)

The manifest states what is certain about the running binary and what was
observed in the checkout. It does not connect the two.

| Condition | Closed by | Blocks |
| - | - | - |
| The binary of a campaign is built from the recorded content inside the campaign. Done as an option: `run --rebuild` packs the revision, the tracked modifications and the untracked files in a deterministic `source.tar` kept in the campaign, builds the lab binary from that archive alone, offline, and plays the campaign with it; `tools/check_campaign_binary.sh` rebuilds from the archive and compares with the digest of the manifest. A run made with `--rebuild` states the link in its manifest (`artifact.built_from`, lot L4, `a2171db`). Left: a campaign played without `--rebuild` is not linked to its content and stays provisional; that a build on another machine gives the same digest is not established | L9 | Any qualifying run beyond one machine |
| **A qualifying result: what it does not establish.** Reached on a real campaign: on a clean tree, `run --rebuild` of P1-02 on each store, then `mqttiumd_oracle produce`, gives a qualifying PASS that `validate` accepts and that stands at `recompute` (`tools/run_qualifying_campaign.sh`, played by the workflow on both stores). The manifest ties the binary to the archive the campaign keeps (`artifact.built_from`, record 0017, proposed), and the oracle reads that archive as a bounded stream: its digest and size, the contract and oracle files the manifest lists, the untracked files the campaign keeps. Left, as record 0017 states: the oracle builds nothing, so that the binary is the one the archive builds is the declaration of the bench, checked outside the oracle by a second build (`tools/check_campaign_binary.sh`); the revision is not checked against the archive, for lack of git in the oracle; a tracked file that was modified is kept as a patch, which the oracle does not apply; that a build on another machine or toolchain gives the same binary is not established; a run without `--rebuild`, as the six campaigns of the workflow, stays provisional | L3 and L4 if the link must be established by the oracle itself; L9 for a build elsewhere | Any qualifying run beyond one machine |
| For SQLite: the exact engine version and compile options, and the effective `journal_mode` and `synchronous` read back after they are set (DEM-011). The store reads them back at each opening and refuses to open on a difference (`effective_settings()`); the runner writes them in `campaign.json` (`backend_settings`, with the way the inspector reads), which the inventory of the campaign covers; the manifest of a run names the backend only and has no member for them | L3 for a member of the manifest, if the oracle must read them | S0 on SQLite |
| **One snapshot of the contracts.** The contract digest is computed from one read of the contract files and the schemas and catalogues are then read again; a file that changes in between leaves a digest that names other rules than the ones loaded. Either the bytes that are hashed are the bytes that are interpreted, or the snapshot of a campaign is established as immutable. L3: the readers interpret the bytes they hashed. L4: the snapshot of a campaign. The row stays until both parts are done | L3, L4 | Any qualifying result |

## Effect boxes and transitions (ADR 0006): reception of L1

Lot L1 is not received while a condition it closes is open. The creation
authority is decided by record 0009 and split by lot: no part is closed by
another lot's part.

| Condition | Closed by | Blocks |
| - | - | - |
| **Creation authority, the real path** (ADR 0009). Done for the bench: `Process::start` of the runner opens an epoch, takes the grant, builds the authority with `OwnerAuthority::from_grant`, creates the box and the owner, and `bench_composition.rs` shows through that path that a second box for the active owner is refused, before and after a simulated crash and a new epoch. Left: the same for the start-up of the broker binary, which does not exist yet; the authority of the bench holds one owner | The lot that writes the broker binary | S1 |

## Pilot proof (DEM-021): reception of L6

| Condition | Closed by | Blocks |
| - | - | - |
| The proof that is received is on `release_if_current` as the product really executes it. Established at the final head of lot L6 (`709c887`): the proof is on the production body, without a copy; the product calls it at one site, `LeaseTable::release` in `crates/mqttiumd-core/src/leases.rs`, and a test of lot L1 counts that site; the job `pilot proof` was read green on that revision by lot L6 and on the integration branch by the integrator. Whether this receives the lot is the owner's decision | Owner of the repository | Reception of L6 |
| The precondition of the proof, one record per held lease, is not proved and the function cannot check it: with two records of one lease the usage is under-counted (harness p12). The lease table of lot L1 carries it, backed by a test that counts no `from_parts` and one `release_if_current` in the product, not by a proof. The proof covers the function alone, neither `LeaseTable` nor recovery, and rests on Kani, CBMC and the nightly compiler Kani brings | Lot that proves the lease table, not planned in this wave | P1 proofs |

## Model M1: reception of L5, then L4

| Condition | Closed by | Blocks |
| - | - | - |
| The sidecar of a model lists as `tests` only files that exist and replay one of its counterexamples. Until a deterministic test replays a counterexample of M1, the list stays empty and the correspondence table names, row by row, the lot that owes the replay | L4 for the replays; the table is written (lot L5, final head `3d6f562`) | G1 |
| **M1, what its campaign does not establish** (closing report of lot L5). Liveness is claimed on one publication only: on two publications no run of the present module completed. M1b is checked by groups of fault kinds: the run with every kind at once was interrupted at 14.1 million distinct states. M1b has no liveness property. The atomicity of `ApplyCommit` is an assumption the SQLite tests discharge. The fence of both models follows record 0015, and their `Refuse` differs from the registry (record 0013, line 12): both records are proposed | Owner of the repository for the two records; L7 for the atomicity; the bounds stay stated limits | G1 |

## Process topology (ADR 0002)

| Condition | Closed by | Blocks |
| - | - | - |
| The control protocol: bounded frames, one outstanding action, a bounded evidence window, decisive records acknowledged before the child proceeds. Done as a library and tested over socket pairs with a thread in place of the child (`bench/protocol.rs`, lot L4, `c3fc981`). Left: no child process exists yet; the first real kill is a SIGKILL of the child after the commit and outside any transaction, on SQLite, with the death confirmed before a restart and one writer at a time; a kill inside a transaction is the increment after it | L4 with L7 | S0 on SQLite |
| The choice log records elementary choices with stable identities; `max_steps` counts them on both backends | L4 | Exact replay |

## Decisions that stay open in the specification

| Decision | State |
| - | - |
| D1, expiry against uncertain emission | Open; S0 excludes expiry |
| T1, temporal guarantees of the Will | Open; S0 excludes the Will |
| Service targets of the real profiles | Not supplied; needed before G4, useful for the measurement of ADR 0008 |
