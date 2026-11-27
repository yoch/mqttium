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
| **M1, remaining model scope.** L5 is already integrated and received again in `docs/reports/0003-l5-reception-d76ccc69.md`: all 52 configurations retain their declared outcomes. Liveness is claimed on one publication only; the combined M1b fault campaign and two-publication liveness remain bounded limits, and M1b has no liveness property. The atomic `Commit` used by `ApplyCommand` maps to one SQLite transaction; a group of proposed record 0045 is K `ApplyCommand` steps in one transaction, with no crash between them, and has no model yet. Deterministic K1/K2b/K3 full-state comparisons, sampled K2a timings and separate artificial WAL images support its concrete implementation (`docs/reports/0004-transactional-crash-reception.md`); finite tests do not prove the assumption for every instant, system or storage failure. The fence follows proposed record 0015 and `Refuse` differs from registry record 0013, line 12 | Owner for the proposed records; model and storage assumptions remain explicit | G1 |
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
| T1, temporal guarantees of the Will | Product zero-delay QoS1 Will with both Retain values under Proposed0034 and Proposed0037. Nonzero delay and universal timing guarantees remain open |
| Service targets of the real profiles | Not supplied; needed before G4, useful for the measurement of ADR 0008 |

## Product TCP: remaining scope

| Condition | Closed by | Blocks |
| - | - | - |
| Shared subscriptions, Subscription Identifiers and dynamic authorization remain outside the product profile. Wildcards and No Local are implemented by Proposed0031. Report 0010 covers incoming and outgoing QoS 0. Proposed 0041 covers outgoing QoS 2 with separate system receipts. Incoming QoS 1 retains lawful repeat-admission behavior. Fixture-oracle qualification retains its own scope | Product follow-up lots | A complete or deployable broker |
| SQLite schema 7 refuses schemas 1..6 before write-affecting pragmas; main/WAL bytes remain unchanged. An explicit migration/export path is not implemented. The product has one owner by default. With `--delivery-shards K` (record 0066, proposed) deliveries are spread over `K` more owners; what that mode still lacks is in the section on the validation of the lot 10 head, below | Store/runtime follow-up | Upgrade of existing databases and broader operating profiles |
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
Packaging for system services and format migration remain separate. Valid persistent sessions are never evicted to free capacity. Quotas
are unchanged, including the existing 16 targets per literal filter. Retained
messages and Message Expiry are implemented under their separate decisions below.


Retained values are implemented under Proposed0032 and report0016:32current
topics/1MiB, exact and wildcard replay, ordered RH, live RAP and ClientID No Local.
Replay is frozen transactionally and drained by existing delivery machines.
Old owed values remain charged under the existing128-reference/4MiB budgets;
a complete subscription update may therefore be refused. This does not provide
an unbounded state archive or format migration. Current schemas 12 and 13 refuse historical schemas. Product system tests do not qualify allP1/P2.


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


## Will product scope (Proposed0034 and Proposed0037)

Zero-delay QoS1 Will installation with optional Retain, cancellation, durable pending
admission, startup recovery and capacity deferral are implemented. Tests cover
real sensor SIGKILL, normal disconnect suppression, source-session deletion and
offline recipients. These system tests do not extend independent fixture
qualification. Delayed and other-QoS Wills and Will properties except delay0
remain open. An uncommitted cancellation cannot be recovered after a crash;
server failure can defer notification to restart. Pending intent reserves are
bounded, so arbitrarily many blocked vanished clients are not promised. Will
receipt/inventory retention is bounded and old schema formats are refused.
Proposed 0035 adds a private MQTT 5 listener using TLS 1.3 and mandatory
client certificates. Durable ClientID ownership and immutable authorization
policy bound session recovery. See [the secure preview guide](../../preview/SECURE.md).

Proposed 0036 provides a read-only plan and a single-transaction offline apply
for monotone authorization changes on current secure schema13. The tool preserves historic
MQTT state and makes a verified standalone backup before changing authority.
Grant removal and principal or ClientID transfer still need explicit
reconciliation. See [the offline operator guide](../../preview/POLICY.md).

Username/password, certificate and ACL reload, remote administration,
automated rotation and revocation, tenant isolation, and restrictive policy changes
remain separate. System reception reports finite service distributions for
the verified environment. MQTT conformance and storage power-loss behavior
retain their separate evidence requirements.

## Pre-pilot operating decision

The source 51be4079 executable passed the healthy 50/s workload and targeted
recovery/security receptions. Direct publication under one persistent dashboard
outage admitted 127 messages and refused 373 of 500 at 50/s. The application
outbox and durable recipient ledgers now receive deferred delivery for one
30-minute dashboard outage, with explicit acceptance of measured freshness
and finite storage before pilot activation. Independent real-time dashboard
supervision remains NO-GO under the shared broker quota. The measured online
gap was 1,798.11 seconds. See `docs/pilot/forward-decision.md` and the results
manifest. The original 49bbaaa4 runtime races were corrected and requalified.
Full-PR independent review and power-loss reception remain open.

## Version 0.1 on the reactor runtime (records 0044 to 0046)

Found while the runtime of record 0044 was built, by the differential
bench `tests/compat/`, by the statement register `conformance/` and by the
runtime models. Each row is open; none is repaired by a frozen reception.

| Condition | Closed by | Blocks |
| - | - | - |
| **Order under late store results, beyond the witnesses.** Four witnesses keep the two orders apart. The order of the PUBLISH packets: with the result of a first admission held back, a subscriber still reads the publications in the order of their reception, and a session that was offline reads twelve messages in order across the pages of its window (runtime tests, both versions, QoS 1 and 2). The order of the PUBRELs: the core sends them in the order of the PUBRECs when their preparations are answered in reverse (core test), and on real sockets no PUBREL leaves while the preparation of an earlier PUBREC is held unknown, the two leaving in order once the SQLite store resolves it (runtime test with the barrier `hold-pubrel-ready-result`, both versions); a resumed session receives its PUBRELs in the order of its PUBRECs. Not asserted: either order at a takeover, across a crash inside a group commit, or when the write itself is late. The broker's own PUBACK and PUBREC keep the order of reception since record 0050 (row below) | Witnesses at a takeover and across a crash inside a group | Conformance |
| **The profile refuses what Mosquitto accepts.** Empty ClientID, username and password, Will QoS 0 and 2 and Will Delay, shared subscriptions, subscription identifiers, topic aliases, `$SYS`, MQTT 3.1, packets over the announced 1,048,584 bytes (15 scenarios of class `profile-refused` in the matrix against Mosquitto 2.1.4; the bounds of 8 filters per SUBSCRIBE, 128 subscriptions, 32 retained topics and 65,536-byte packets this row named were raised by record 0051) | Product lots of version 0.1. The application properties of a PUBLISH, which this row named, are carried since record 0053 | Version 0.1 |
| **Pressure on QoS 1 and 2 beyond the store's rate.** Refusals 0x97 in the PUBACK, more than half of the publications at 3,000 per second offered to 64 connections on a loaded host. No connection is closed for it: a publication that finds every exchange credit lent waits for one, and is closed only when the broker has returned none for five seconds (record 0046 as amended after the counter-audit of `bbe1453`). The exchange table holds eight entries for every session (record 0061), so clients that fill their window together no longer wait for one another's credit | A store that commits faster | Load profile of version 0.1 |
| **Durable QoS 1 throughput.** One store worker commits a few thousand publications per second on tmpfs; report 0027 has the figures against Mosquitto. A fifth of the worker's samples were still statement preparation and a quarter the re-reading of work for online subscribers | Store (cached statements everywhere, direct delivery), Volatile profile of ARC-020 | Performance target |
| **A reactor thread held by a TLS test barrier.** In a build with `test-hooks` a held TLS frame barrier blocks the reactor thread that serves it | Not planned: the barriers belong to the frozen secure reception | Nothing current |
| **Fuzzing of the codec.** `crates/mqttiumd/tests/codec_properties.rs` keeps the decoder total and the header reader consistent on generated inputs with a fixed seed; `tools/install-tools.sh` installs a fuzzer, and no coverage-guided target exists | Runtime | Evidence tier of record 0044 |
| **Loom and a deterministic simulation of the runtime.** The wake-up protocols are modelled in TLA+ and exercised by timing tests; no interleaving explorer runs the Rust | Runtime (V4) | Evidence tier of record 0044 |

## Independent audit of `8e4303d` (21 November 2026)

An independent reviewer answered the open questions and audited the tree
at `8e4303d97778`. The owner keeps its full report. What it established
that this list did not say, or said wrongly:

| Condition | Closed by | Blocks |
| - | - | - |
| **Keep Alive suspended by a held packet.** Found by the audit, fixed after it: the clock runs from the last complete packet received (record 0046). The counter-audit of `bbe1453` found that taking an old packet still moved the deadline; it now depends on the reception alone. With Keep Alive 0 a held packet has no bound | Done for Keep Alive greater than 0; a bound for Keep Alive 0 is open | Record 0046 |
| **A quarter of the lane does not reserve the way of an acknowledgement.** Found by the audit, changed after it, and changed again after the counter-audit of `bbe1453`, whose duplex bench closed clients that respected their window. As it stands (record 0046): the last eighth of the ordinary lane is kept for the packets that release capacity; a connection has at most its announced window of business packets waiting in the core, so the PUBREL it sends after them is read; the lane has a place for the window of every connection; a wake tries the deferred packets from the oldest; no count of them closes a connection. A hundred clients filling their window together complete (executable test). The slots of the effect box are still not reserved, and an acknowledgement behind a held publication of its own connection waits with it | Done for the window of a client; the effect box belongs to the core cost model | Record 0046 |
| **The broker's acknowledgements keep the order of reception.** Found by the audit of `8e4303d` as the opposite: with the result of the first admission held back, PUBACK and PUBREC left as 8 then 7 for publications 7 then 8. Decided by the owner on 22 November 2026 (record 0050): the core holds an acknowledgement that would overtake the answer of an earlier publication of the same connection and releases it in its turn, whatever the reading of MQTT-4.6.0-2 and -3. Shown by tests of the core, by the runtime witness on sockets in both versions at QoS 1 and 2, and on the bench by scenario `ORDER-A`. Still open: the oracle has no assertion on the order of two acknowledgements, so the bench reads it on the trace by a Rust test and no mutation of the loop is rejected for it; the order is that of one connection, each connection of a takeover having its own; with a result that stays unknown, the acknowledgements of the later publications of that connection wait for its resolution, seconds, and a client that fills its Receive Maximum behind it waits with them | An assertion of the oracle with its mutation; a witness across a takeover | Record 0050 |
| **Group commit and an uncertain commit.** Found by the audit, answered after it: the deterministic store of the lab judges every differential test of `apply_group`, and the lost answers of a committed group, the transaction the engine drops and the commit that fails have permanent tests (record 0045, accepted with amendments). Done since, after the counter-audit of `bbe1453`: the real worker is killed by SIGKILL after none to four of the five answers of a group, and the reopened store holds every member and recognises the group presented again without a write (`crates/mqttiumd/tests/writer_group_kill.rs`); a commit that is durable while its call returns an error is answered unknown and reconciled by identity without a second effect, for a group and for `apply` (`crates/mqttiumd-store-sqlite/src/commit_error_tests.rs`, feature `kill-points`); the model has that outcome, the reconciliation and a mutant that applies the group again; a campaign on the backend `sqlite-group` commits through `apply_group`, reaches a group of two on the scenarios of `tests/scenarios/group/` and passes the oracle up to PUBREC and PUBCOMP, and mutation `g1` (a group rolled back and answered as committed) is failed by it (`crates/mqttiumd-lab/tests/bench_group.rs`). Still open: a group that is not committed answers unknown where a member alone would answer `Violation(RecipeMismatch)`; the judged groups have two members and are formed by the store adapter of the bench, not by the worker of the product; no campaign kills a process inside or after the transaction of a group (`--process child` is refused in that mode), and none is qualifying (`run --rebuild`); the kill test presents the commands again to a new worker, where the product recovers by pages, and no test drives the runtime with client connections through that kill or through an uncertain commit of a group (the executable is killed with client connections for the end of a session only, a single command: `crates/mqttiumd/tests/disconnect_durability.rs`, record 0059); the model and its extension have not been reviewed independently | A qualifying campaign in group mode with a child process; a runtime test of the kill | Record 0045 |
| **Profile refusals that break the protocol.** Found by the audit, fixed after it: in 3.1.1 a `$share/` filter is an ordinary filter and its UNSUBSCRIBE is acknowledged; a SUBSCRIBE the profile cannot serve (more than eight filters, a filter longer than 1,024 bytes) is answered filter by filter in both versions, as is the MQTT 5 UNSUBSCRIBE; in MQTT 5 a shared subscription is closed with 0x9E, a subscription identifier with 0xA1, reserved subscription-option bits with 0x81, an authentication method with CONNACK 0x8C; a refused fixed header is answered 0x95 or 0x81 before the close, and the CONNACK announces the packet size the payload limit allows. The counter-audit of `bbe1453` then found that refusing every filter of a request with more than eight diverged from its unit requests (an UNSUBSCRIBE left active what a unit request removes) and that a packet a byte larger than the announced size was served: such a request is now served as its unit requests and acknowledged once, in both versions, and the announced size is a limit | Done | Compatibility matrix: no `defect` left |
| **The judge of the compatibility bench can miss a new process incident** in a scenario already classified `defect`, and its transcript equality has blind spots. Found by the audit, fixed after it: an unrecorded incident fails the run whatever the class, the sequence of acknowledgement identifiers and the kind of closure (end of stream or reset) are compared, raw transcripts are kept for both brokers, and six expectations judge the broker's own transcript without the reference. The differential matrix now includes two persistent graceful restarts. The listener checks exercise mTLS and common grants. Process-kill cut points and scheduled TLS takeover have mqttiumd-specific executable witnesses | Done for the judge; the scope of the scenarios is open | Trust in the matrix |
| **Three of thirty sampled `covered` citations do not test the distinctive precondition** (MQTT-3.1.3-5 and MQTT-4.3.3-7 of 5.0, MQTT-3.3.1-7 of 3.1.1); eleven more need an inference. Found by the audit, corrected after it: the three rows are requalified, and a covered row can state whether its evidence is direct or partial. 35 of the 215 covered rows have been reviewed that way; the others are not | Review of the remaining covered rows | Trust in the register |
| **Load figures are those of one host.** On the auditor's host the executable of report 0027 received 56,000 to 57,000 QoS 0 publications per second at 60,000 offered with 7 to 12 % missing, not 59,500 with 1 %; 2,200 to 2,700 QoS 1 at 3,000 offered; the collapse at 64 connections is confirmed. Mosquitto in that bench persists by snapshot (autosave 3,600 s): its QoS 1 answer is not a synchronous commit per publication, so the two QoS 1 columns of report 0027 compare different contracts. Report 0028 adds, for `038ce23` on the host of report 0027, the account of every QoS 1 publication at 8 and 64 connections, the percentiles and the return to nominal after five seconds of overload; it also shows that the broker acknowledges fewer publications at 6,000 offered a second than at 3,000, the excess being refused late | A measurement on the target disk, with a generator on another machine, longer plateaux and equal persistence contracts | Any throughput claim |
| **A reactor thread that left its loop kept receiving registrations**, and two construction errors could panic. Found by the audit, fixed after it: a dead reactor finishes what is registered with it, the pool is an error and not a panic, `Limits` are brought within their bounds | Done | — |
| **Text the code has overtaken.** Record 0044 (accepted) says no loop polls on a timer and gives decoding and encoding to the I/O threads; three 100 ms rescans remain and MQTT decoding and encoding run on the core thread. One of the rescans is needed: `queue_work` leaves without remembering the session when the ordinary lane is full | Owner, for the accepted record; runtime, to replace the rescan by a mark per session | Record 0044 |

## Scaling, Volatile Store, PUBLISH Properties and Configuration Subset (Proposed records 0051 to 0054)

Implemented under proposed records `0051`–`0054` and validated by workspace unit, store-conformance, boundary, and end-to-end tests:

- **Core scaling, indexed routing & aligned credits (Proposed 0051)**: raised ceilings to 4,096 active sessions/connections, 16,384 subscriptions, 1,024 fanout targets per publication, 4,096 retained topics / 64 MiB, 1 MiB payloads (~1.06 MiB wire packet), and `RECEIPT_WINDOW = 4,096`; replaced linear scans with an exact + wildcard trie route index and O(1)/O(log N) core/store indexes; aligned exchange credit account to `connections * receiving::MAXIMUM` (since record 0061: `SESSIONS * receiving::MAXIMUM`, whatever the connection limit); added `--io-threads N` and opportunistic write/TLS coalescing; upgraded SQLite to Schema 14/15 with `obligations_by_target_order` and `PRAGMA data_version`-guarded caches.
- **Volatile store with optional atomic snapshots (Proposed 0052)**: added `mqttiumd-store-volatile` (`VolatileStore`, `VolatileHandle`) passing the full store conformance and differential `apply_group` suites, exposed via explicit `--storage-mode durable|volatile` and `--snapshot-interval SECS` with `DUR-001` readiness disclosure and zero implicit fallback (`DUR-002`).
- **End-to-end MQTT 5 PUBLISH application properties (Proposed 0053)**: carries `0x01` Payload Format Indicator (enforcing RFC 3629 UTF-8 when `1`, `0x99`), `0x03` Content Type, `0x08` Response Topic, `0x09` Correlation Data, and ordered `0x26` User Properties across QoS 0/1/2, retained restarts (SQLite and Volatile snapshots), and strips them for MQTT 3.1.1 subscribers via canonical recipe kind `23` and `publications.properties BLOB`.
- **Native `mosquitto.conf` subset reader (Proposed 0054)**: added `mqttiumd::config`, `-c`/`--config PATH`, and `mqttiumd check-config PATH` classifying every encountered directive (`CFG-001`) and blocking unknown or security-weakening options before opening sockets or databases (`CFG-002`, `CFG-006`, `SEC-005`).

Still open: records `0051`–`0054` remain `Proposed` pending owner acceptance; Will properties beyond zero Will Delay, Topic Aliases, Shared Subscriptions, Subscription Identifiers, username/password authentication, `$SYS`, multi-listener configs, and cross-host power-loss receptions remain outside the implemented profile.

## Validation of the lot 10 head (`9dfd568`, 26 November 2026)

The branch `refactor/lot10-validated-base` checks the proofs and the code
of `9dfd568` and corrects what it finds; reports 0043 to 0047 have the account. The
defects of the default path it found are fixed, each by a test committed
while it failed. What it leaves open:

| Condition | Closed by | Blocks |
| - | - | - |
| **The sharded mode is experimental.** With `--delivery-shards K >= 1` the inline-claim lease and its dirty barrier have a model since the third part (`models/inline-lease`); the routing of acknowledgements, the end of a generation and the stop have none, and every other runtime model describes `K = 0`. Fixed, each by a failing test first: in the first part (report 0044) a shard panic not reported, a stop lost on a full lane, the wake hold kept through a wait, silent skips at attach, unbounded delegated QoS 0, a lease count that could wrap, a retained replay before its SUBACK; in the second (report 0045) a lease published without a committed claim's identifier, and a shard claim that took a reserved identifier, now prevented by the read fence of the third; in the third (report 0046) a lease never eligible again after a stored publication, a stored publication committed by a resolution never delivered, the order of a subscriber's publications after an unknown outcome (suspect B1), a delivery sent twice after a read during an unknown outcome, reads stopped while the acknowledgement credit is spent, and three divergences of the shard's effect sink (journal origin, journal failures, PUBREL bindings). Still open: the rest of that divergence (no `frame_queued`, `frame_failed` or `effect_retained` entry from a shard, a too large PUBLISH answered otherwise, no close notice for a transport that is gone, writes to a connection the coordinator is closing, no producer signal when a socket drains, so a held effect is retried every millisecond); store worker head-of-line blocking across owners (D8); detach scans quadratic in pending detaches (D9); an answer of the store stepped after a later one when the core lacks capacity, outside the model; the TLS read turn without credit, which has no unit test | Lot 3 of the validation, third part | The mode leaving "experimental", which the owner decides |
| **Records applied while proposed.** 0051 to 0057 and 0064 to 0067 are applied, some of them changing a contract, and are still proposed. 0066 departs from 0044 (accepted) without amending it; it now names where | Owner of the repository (report 0043 has a table) | Acceptance of those records |
| **Performance figures of lots 8 to 10.** Reports 0039 to 0041 are single runs, on code that later commits changed, two of them with a load generator that could write past its buffer (fixed by `675128d`), against an unpinned Mosquitto 2.1.2 kept in memory while mqttiumd synchronised a log | New measurements under the gate of report 0043 | A performance claim of those lots |
| **A log image recovery cannot tell from a torn tail.** A committed frame whose first sector reads as zeros, followed only by bytes within the reach of one frame, is cut without an error (record 0055) | A frame format that can be read backwards | A power-loss claim of `--storage-mode log` |
| **A connect beside an old peer whose binding ended.** When a frame of a task that already ended has failed, the owner ends the binding while the peer is still in the table; a new connection of the session is then no takeover. Exploratory runtime runs showed no defect (CONNACK and Session Present correct); `MTakeoverInFlight` does not contain the path, and `OldClosedBeforeNewBound` was not checked on it | Model and replay | Scope of `models/takeover` |
| **Volatile store, what the validation did not change.** The operation table keeps an owner's ring after its run (32 rings at most, then the ordered fallback), and a ring grows with the span of sequences from its oldest live entry. A recovery page reads the whole inventory, and an expiry sweep scans every delivery of each expired publication | Store lot | Recovery and expiry at the session bound |
| **The oracle judges one owner on two stores.** It reads recipes 24 and 25 since this validation, and leaves them out of the scope of a verdict. The log store, the volatile store and the sharded mode are judged by their conformance suites and by the bench, not by the oracle | Oracle lot | Qualifying campaigns on those stores |
| **`tools/bounded-batch.mutant` no longer applies.** It belongs to the frozen receptions of report 0026 | Not planned: frozen evidence | — |
| **What the cleanup left.** `drain_inputs` and `Sink::try_submit`, about 1,100 lines each, keep their arms inline: the large ones are the per-publication paths, and splitting them relies on the inliner. `net.rs` and `writer.rs` were not split (report 0047) | A lot that splits them arm by arm with instruction counts for each | — |

## Follow-up of the external audit of `47e1d12`

The integration owner accepted decisions 0058 through 0063, with amendments
to 0060, 0061 and 0063, and referred six migration choices to product review.
The migration guide and audit register identify each decision separately.
Real-network timing, platform-specific packaging, clock changes, storage
volume behaviour, physical power cuts and deployment-scale load have named
follow-ups. The local evidence covers the executable, the stated loopback
loads and the two qualifying store configurations. At the 4,096-session
limit an earlier build completed all 32,768 exchanges and reported 977
connection closes still pending at its shutdown deadline. The final build
completed the same workload and exited zero. Decision 0067 on validating
operational journal fields is proposed. [Load and shutdown follow-up](https://github.com/yoch/mqttium/issues/683)

[Audit disposition](../reports/0042-external-audit-disposition.md) ·
[Migration decisions](../migration-from-mosquitto.md) ·
[External audit tracking](https://github.com/yoch/mqttium/issues/672)
