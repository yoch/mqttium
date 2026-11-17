# Open conditions

What lot L0 leaves open on purpose. Each condition names the lot that closes
it and the gate or reception it blocks. A lot that closes one removes its
row in the same change; a lot that finds a new one adds it.

Only the remaining conditions are open; received increments and their limits
are linked below.

## Audit of the first wave (head `0daf0eb`)

Defects found by the audit, each with a counterexample that the tests of
the lots did not contain. A row is removed by the change that closes it,
with its failing test first.

| Finding | Reception | Closed by | Blocks |
| - | - | - | - |
| **F3, progress of resolutions: what is left.** Corrected in the core (lot L1, `2255bf7`): an operation retains its command, its two latest resolutions and its four latest landmark resolutions, those whose rank is a power of two, so that an answer delayed by any finite number of resolutions meets a landmark still retained; at most seven effect identifiers per operation; the invariants are in the documentation of `Answers`. Played by the runner as a judged scenario, `tests/scenarios/audit/F3-steady-lag.json`: the result of the Persist is lost, the answer of the first resolution is delivered after six newer ones, and the exchange completes; with the landmarks removed by mutation the run stops before the PUBREC, the runner writes `StepBlocked` and the oracle answers FAIL on `scenario_reaches_its_end`, on both stores (`tests/mutants/loop/m4-landmarks-ignored.mutant`). Scope, to keep (second audit): the correction is received under invariant I3, a delay uniformly bounded by a finite bound that is not known in advance, counted in newer resolutions, with finitely many lost answers; a fair delivery whose delays are each finite but have no uniform bound is another hypothesis, which the landmarks do not cover, and a test of the core shows it as a limit, not as a defect. Left: the contract can now state a permanent delay (repeated faults, a lag, a wait that fires deadlines; record 0018, proposed, lot L3, `8f986b71`), but the bench does not play that vocabulary and refuses such a scenario before it writes anything (lot L4, `ef7615a`); a wait that fires deadlines is in progress on the branch of lot L4, and a repeated fault needs a change of the store adapter asked of lot L2 (`FaultSpec.repeat`, one held item per hit), not started; the retention rule and I3 are now in the model (`models/m1/M1Answers.tla`, lot L5, `6ed04c6`): under I3 the exchange leaves AdmissionPending, and the configuration without a uniform bound keeps its counterexample; what the model assumes of the runtime is the row "M1Answers" below | L4 for the vocabulary in the bench | G1 |

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

The review of `edd8acbb` is received in report
[`0006`](../reports/0006-typed-durable-read-reception.md): typed bounded reads,
first-recovery selection, coherent action termination and post-kill drainage.
The invalid-data receipt is distinct from a qualifying business PASS. The
corresponding snapshot panic and false allocation diagnoses are removed below.

| Finding | Reception | Closed by | Blocks |
| - | - | - | - |
| **`validate` accepts a qualifying result whose conditions no longer hold.** It checks the document: schema, digests and sizes of the files cited, contracts, assertions and faults of the scenario, coherence of the verdict. It does not judge the trace and does not check the conditions of a qualifying run, so a qualifying result without its `source.tar` is still accepted by it. Only `recompute` says that a result is evidence, which the guide of the oracle and record 0017 state | Either `validate` refuses to be read as that check, by its name or its output, or it runs the conditions too | L3 | A reader who takes `validate` for the check |
| **`Owner::outbox()` still returns an `Option`.** Since the correction of finding R2 an owner never lives without a box, and the bench no longer treats its absence (lot L4, `51b0bca`); the signature of the core still says the box may be absent | The signature says what holds | L1 | G1 |

## Before a result may be used as evidence (L3, gate G0)

`mqttiumd-lab validate` checks a qualifying result against the files it
names: digests, contract digest, assertions and faults of the scenario,
verdict equal to the one its assertions determine. It does not check that
the assertions are true.

| Condition | Closed by | Blocks |
| - | - | - |
| **Core, limits of what it remembers** (lot L1, corrections of the review of its core). An operation that ended is kept in a bounded window (`OwnerConfig::retained_operations`); beyond it a contradicting result is absorbed. The product TCP runtime now filters non-reused generations against its live connection map, including before core delivery, beyond the core's bounded closed window (report 0007). This does not extend the terminal-operation contradiction window or prove it universally sufficient | L1, L4 | S0 |
| **Store, remaining deterministic-store scope.** Commands clone the durable state; terminal sessions are bounded by identity exhaustion rather than max_sessions, and epoch-0 receipts/fences are not collected. Typed bounded reads now distinguish invalid data, configured limits and allocation failure (proposed record 0021). These remaining write/storage-growth limits are unchanged | L7 | S0 workloads beyond the stated bounds |
| **Store and formats, what is left.** The contract now has the outcome `Violation` with its `violation` (`RecipeMismatch`), carried unchanged by `StoreResultReady`, `StoreResultHandedOver` and `StoreResultDelivered` (lot L3, `ec65934`); the writer of the store writes it on `StoreResultReady` and `StoreResultHandedOver` (`ce64424d`, `evidence::outcome_fields`); the runner writes it on `StoreResultDelivered` with the same content, shown by a composition test in which one operation is presented with two recipes (`crates/mqttiumd-lab/tests/bench_violation.rs`); no scenario of the registry produces a violation, because no declared fault makes the store contradict itself. At `store.fence.transit` a fault on a resolution selects every resolution, because the adapter gives a resolution no source: enough while a scenario has one exchange | L3 if a scenario must produce a violation | G0 |
| **SQLite store, remaining scope.** Typed bounded readers, semantic decoding and deterministic K1/K2b/K3 remain received. Proposed record 0027 adds bounded live-prefix and closed-epoch product sweeps; the legacy all-at-once administration collector is separate. The already-referenced-payload guard and SQLite internal cache/storage allocation have no new universal bound. Initial legacy-origin registration uses the snapshot budget, while product origins register empty | L7 | S0 workloads beyond the stated bounds |
| **Oracle, what is left of lot L3** (final head `d86adb0`; the integrator read the corrections of findings 1 and 4 of the review of `3c023bf`, the registry change and the changed test of lot L2, not the rest). The bench now plays scenarios through both stores, including real SQLite child kills; hand-built traces remain independent negative controls. The list of contract files is still computed twice, by `manifest.rs` and by `Workspace::open`: both refuse a file of the schema directory that is not a schema, and one list read once needs a change of the shared `manifest.rs` that lot L3 proposed and that is not applied. The Python reader keeps its own copy of the order of recovered facts, on purpose, tied to the Rust reader by twelve shared cases. The registry has no guard on exhausted identities and no session machine. A `ClientBytesReceived` carries one whole packet in v1 | L4 for the bench; L3 with L1 for the guard; integrator for `manifest.rs` | G0 |
| The registry rules that decide a behaviour of the broker which the chapters leave open (record 0013, proposed) are accepted or replaced | Owner of the repository | G0 |

## Before a campaign is run (L4, L7)

The manifest states what is certain about the running binary and what was
observed in the checkout. It does not connect the two.

| Condition | Closed by | Blocks |
| - | - | - |
| **Parallel local suite fixture isolation.** The legacy workspace snapshot test uses a fixed source-relative directory. Normal and instrumented suites in the same checkout can remove each other's fixture even with distinct Cargo targets. Run them sequentially or in separate worktrees; CI already runs them sequentially. A per-process temporary fixture would remove this remaining test-infrastructure limit | Integration/test infrastructure | Concurrent full suites in one checkout |
| The binary of a campaign is built from the recorded content inside the campaign. Done as an option: `run --rebuild` packs the revision, the tracked modifications and the untracked files in a deterministic `source.tar` kept in the campaign, builds the lab binary from that archive alone, offline, and plays the campaign with it; `tools/check_campaign_binary.sh` rebuilds from the archive and compares with the digest of the manifest. A run made with `--rebuild` states the link in its manifest (`artifact.built_from`, lot L4, `a2171db`). Left: a campaign played without `--rebuild` is not linked to its content and stays provisional; that a build on another machine gives the same digest is not established | L9 | Any qualifying run beyond one machine |
| **A qualifying result: what it does not establish.** Reached on a real campaign: on a clean tree, `run --rebuild` of P1-02 on each store, then `mqttiumd_oracle produce`, gives a qualifying PASS that `validate` accepts and that stands at `recompute` (`tools/run_qualifying_campaign.sh`, played by the workflow on both stores). The manifest ties the binary to the archive the campaign keeps (`artifact.built_from`, record 0017, proposed), and the oracle reads that archive as a bounded stream: its digest and size, the contract and oracle files the manifest lists, the untracked files the campaign keeps. Left, as record 0017 states: the oracle builds nothing, so that the binary is the one the archive builds is the declaration of the bench, checked outside the oracle by a second build (`tools/check_campaign_binary.sh`); the revision is not checked against the archive, for lack of git in the oracle; a tracked file that was modified is kept as a patch, which the oracle does not apply; that a build on another machine or toolchain gives the same binary is not established; a run without `--rebuild`, as the six campaigns of the workflow, stays provisional | L3 and L4 if the link must be established by the oracle itself; L9 for a build elsewhere | Any qualifying run beyond one machine |
| For SQLite: the exact engine version and compile options, and the effective `journal_mode` and `synchronous` read back after they are set (DEM-011). The store reads them back at each opening and refuses to open on a difference (`effective_settings()`); the runner writes them in `campaign.json` (`backend_settings`, with the way the inspector reads), which the inventory of the campaign covers; the manifest of a run names the backend only and has no member for them | L3 for a member of the manifest, if the oracle must read them | S0 on SQLite |
| **One snapshot of the contracts.** The contract digest is computed from one read of the contract files and the schemas and catalogues are then read again; a file that changes in between leaves a digest that names other rules than the ones loaded. Either the bytes that are hashed are the bytes that are interpreted, or the snapshot of a campaign is established as immutable. L3: the readers interpret the bytes they hashed. L4: the snapshot of a campaign. The row stays until both parts are done | L3, L4 | Any qualifying result |

## Effect boxes and transitions (ADR 0006): reception of L1

Lot L1 is not received while a condition it closes is open. The creation
authority is decided by record 0009 and split by lot: no part is closed by
another lot's part.

The creation-authority path is now also implemented and received in the product
TCP executable: one durable epoch/grant/authority/Outbox/Owner and complete
recovery before listening. See [report 0007](../reports/0007-tcp-qos2-product.md).
This closes the former broker-startup row; it does not accept proposed ADRs.

## Pilot proof (DEM-021): reception of L6

| Condition | Closed by | Blocks |
| - | - | - |
| The proof that is received is on `release_if_current` as the product really executes it. Established at the final head of lot L6 (`709c887`): the proof is on the production body, without a copy; the product calls it at one site, `LeaseTable::release` in `crates/mqttiumd-core/src/leases.rs`, and a test of lot L1 counts that site; the job `pilot proof` was read green on that revision by lot L6 and on the integration branch by the integrator. Whether this receives the lot is the owner's decision | Owner of the repository | Reception of L6 |
| The precondition of the proof, one record per held lease, is not proved and the function cannot check it: with two records of one lease the usage is under-counted (harness p12). The lease table of lot L1 carries it, backed by a test that counts no `from_parts` and one `release_if_current` in the product, not by a proof. The proof covers the function alone, neither `LeaseTable` nor recovery, and rests on Kani, CBMC and the nightly compiler Kani brings | Lot that proves the lease table, not planned in this wave | P1 proofs |

## Model M1: reception of L5, then L4

| Condition | Closed by | Blocks |
| - | - | - |
| The sidecar of a model lists as `tests` only files that exist and replay one of its counterexamples. Until a deterministic test replays a counterexample of M1, the list stays empty and the correspondence table names, row by row, the lot that owes the replay | L4 for the replays; the table is written (lot L5, final head `3d6f562`) | G1 |
| **M1, remaining model scope.** L5 is already integrated and received again in `docs/reports/0003-l5-reception-d76ccc69.md`: all 52 configurations retain their declared outcomes. Liveness is claimed on one publication only; the combined M1b fault campaign and two-publication liveness remain bounded limits, and M1b has no liveness property. The atomic `Commit` used by `ApplyCommand` maps to one SQLite transaction. Deterministic K1/K2b/K3 full-state comparisons, sampled K2a timings and separate artificial WAL images support its concrete implementation (`docs/reports/0004-transactional-crash-reception.md`); finite tests do not prove the assumption for every instant, system or storage failure. The fence follows proposed record 0015 and `Refuse` differs from registry record 0013, line 12 | Owner for the proposed records; model and storage assumptions remain explicit | G1 |
| **M1Answers, what it assumes of the runtime** (lot L5, `models/m1/README.md`). The model takes I3 as a guard on the environment, not as a property of the core. In the runtime it needs: a deadline of an unresolved operation that eventually fires, with the effect box dispatching `ResolveAndFence` then `ArmDeadline` in order and the timer accepting the second (a box that stays full holds the operation pending); the store writer and the result channel answering within a bounded number of deadline periods, so their queues bounded and the period not shorter than the latency of a loaded writer; finitely many lost or unknown answers. Nothing is assumed of FIFO delivery. Not modelled: the saturation of `emitted`, the bounded window of terminated operations (I4, I5), an effect box shared by several operations. No temporal property is claimed outside I3: the counterexample there is a safety one, prolonged by the test `review_individually_finite_delays_do_not_imply_a_uniform_lag_bound` | The runtime states its queue bounds and its deadline period, and a campaign measures the latency of a loaded writer against that period | L8 for the runtime; L7 for the measure | G1 |

## Process topology (ADR 0002)

| Condition | Closed by | Blocks |
| - | - | - |
| **Process crashes, remaining scope.** K1/K2b/K3 are implemented in the SQLite child topology, with an exclusive writer, signal-9 wait before same-file restart and child cleanup on error. K1/K2b retain and acknowledge `FaultHit` in the surviving collector while the command is in flight, then accept only SIGKILL; K3 halts after commit before adapter result creation. Normal builds exclude instrumentation; feature unification inconsistencies are refused, and reconstruction/replay preserve the explicit feature. The transaction scenarios and independent identity-based admission/recovery checks are in proposed record 0020. The executed reception, qualifying/recomputed results, per-run system witnesses and negative controls are recorded in `docs/reports/0004-transactional-crash-reception.md`. Separate K2a data comprise 64 calibration and 1000 frozen trials with diversity of found/absent outcomes (`0005`); no position inside COMMIT is established. WAL prefix images are artificial storage experiments. Left: kernel/page-cache survival, no power-loss/torn-write evidence, no universal all-instant guarantee or cross-machine binary reproducibility. A separately traced run witnesses only itself, and the oracle does not read that system witness | L9 for broader system/storage failures; finite scenario and workload bounds remain | S0 qualification beyond the received scope |
| The choice log records elementary choices with stable identities; `max_steps` counts them on both backends | L4 | Exact replay |

## Decisions that stay open in the specification

| Decision | State |
| - | - |
| D1, expiry against uncertain emission | Open; S0 excludes expiry |
| T1, temporal guarantees of the Will | Product zero-delay QoS1/non-retained Will received under Proposed0034; nonzero delay and universal timing guarantees remain open |
| Service targets of the real profiles | Not supplied; needed before G4, useful for the measurement of ADR 0008 |

## Product TCP: remaining scope

| Condition | Closed by | Blocks |
| - | - | - |
| Outgoing QoS 2, shared subscriptions, Subscription Identifiers and authentication remain outside the local hierarchical pub/sub preview. Wildcards and No Local are implemented by Proposed0031; the fresh system receipt has its own scope. Report 0010 adds incoming/outgoing QoS 0 and grants 0, preserving the durable QoS 1/2 admission path and separate system-test scope. Incoming QoS 1 retains lawful repeat-admission behavior. This does not extend fixture-oracle qualification or complete P1/P2 | Product follow-up lots | A complete or deployable broker |
| SQLite schema 7 refuses schemas 1..6 before write-affecting pragmas; main/WAL bytes remain unchanged. An explicit migration/export path is not implemented. General multiowner delivery ordering remains outside this single-owner FIFO product | Store/runtime follow-up | Upgrade of existing databases and broader operating profiles |
| Live receipt retirement removes the cumulative 8,192-command ceiling within the received profile (report0011, proposed0027). Proposed0028/report0012 add protected outgoing QoS 1 PI reuse after known Finish application, with immutable observation targets at runtime reader-channel dequeue. The 8,192 physical receipt slots, 1,024/512 per-origin window, 512 control reserve and 64 origin rows remain finite. Session capacity (128 active incarnations, with explicit zero/infinite lifecycle in proposed0029), active claims, checked causal identities, typed read budgets and storage availability still bound service life. Upstream unobserved bytes and arbitrary identical wire ACKs first observed after reuse are outside the internal-observation correlation guarantee. No indefinite-service or loaded-writer latency/performance guarantee is claimed | Store/runtime follow-up | Broader long-running operation and G4 |
| A storage syscall can outlive graceful shutdown's deadline; the product reports incomplete shutdown/unknown outcome and process exit releases OS resources. Process-kill evidence is not power-loss evidence | Runtime/storage follow-up | Stronger system/storage failure guarantees |


## Finite session expiry (Proposed0030)

The product now implements all Session Expiry Interval values with durable
fixed detachment deadlines, guarded Open/Expire ordering and one bounded clock
anchor. This removes the former intermediate-finite-interval profile exclusion;
it does not accept Proposed ADRs or qualify temporal behavior with the fixture
oracle. The system receipt and report0014 identify the final tested content.

Remaining temporal limits: clock-origin continuity must be established; otherwise
only committed progress crosses restart. Unknown downtime is conservatively
ignored on domain change. Checkpoint target30s includes actual scheduling/commit
delay and reports degradation after31s; unavailable storage has no unconditional
time bound. Sessions still active at process loss are first detached at recovery
observation. Message Expiry uses the separate policy below; Will has the distinct zero-delay profile described below. Schema6 has no migration
from1..5, and there is no automatic eviction of valid persistent sessions.

## Hierarchical local preview (Proposed0031)

The common bounded selector aggregates eligible exact/+/# contributions per
SessionId, applies No Local before maximum QoS, sorts targets canonically and
checks the 16-distinct-destination budget before reservation/output. QoS/options
survive literal replacement, unsubscribe, session recovery and restart. Valid
RAP/RH now govern durable retained replay and live flags under Proposed0032; the retained system receipt remains separate from fixture qualification. There is no silent fanout
truncation or fixture-oracle qualification of these new routing decisions.

A bounded scan of 128 routes remains the implementation. The exploratory
measurement is useful for selecting an optimization, not a throughput promise.
The normal x86_64 Linux archive is executed after extraction; ARM execution,
TLS, authentication, packaging for system services and format migration remain
separate. Valid persistent sessions are never evicted to free capacity. Quotas
are unchanged, including the existing 16 targets per literal filter. Retained
messages and Message Expiry are implemented under their separate decisions below.


Retained values are implemented under Proposed0032 and report0016:32current
topics/1MiB, exact and wildcard replay, ordered RH, live RAP and ClientID No Local.
Replay is frozen transactionally and drained by existing delivery machines.
Old owed values remain charged under the existing128-reference/4MiB budgets;
a complete subscription update may therefore be refused. This does not provide
an unbounded state archive or format migration. Current schema10 refuses historical schemas. Product system tests do not qualify allP1/P2.


## Message lifetime (Proposed0033)

Message Expiry is implemented for retained and non-retained publications,
including offline responsibilities and outgoingQoS0/1. Its system reception is
separate from fixture qualification. Raw-domain continuity is required for fresh
expiring delivery. Unrecognized continuity quarantines existing expiring values;
no elapsed time is invented, and no strict cross-domain compliance is claimed.
Quarantined references remain charged until explicit replacement/clearing or
session termination releases the corresponding responsibility. An unmarked
recovered claim can be transmission-uncertain; expiry does not free a possibly
started QoS1 PI or payload. A known Started claim can finish after its lifetime.
Physical wire-departure timestamps, arbitrary host scheduling bounds, universal
MQTT conformance and power-loss guarantees remain outside the local preview.
The sweep has bounded work and one outstanding request; no expiry timer or row
accumulates per historic connection/publication. Current SQLite10 has no general migration.


## Will product scope (Proposed0034)

Zero-delay non-retained QoS1 Will installation, cancellation, durable pending
admission, startup recovery and capacity deferral are implemented. Tests cover
real sensor SIGKILL, normal disconnect suppression, source-session deletion and
offline recipients. These system tests do not extend independent fixture
qualification. Delayed/retained/other-QoS Wills and Will properties except delay0
remain open. An uncommitted cancellation cannot be recovered after a crash;
server failure can defer notification to restart. Pending intent reserves are
bounded, so arbitrarily many blocked vanished clients are not promised. Will
receipt/inventory retention is bounded and old schema formats are refused.
TLS, authentication and measurable local deployment service targets are the next
separate increment; no unauthenticated remote deployment is received here.
