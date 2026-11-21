# Explicit bounded session lifecycle

`MSessionLifecycle` is a finite specification model for Proposed
[ADR 0029](../../docs/decisions/0029-explicit-session-lifecycle.md). It checks
incarnation identity, attachment-token guards, zero/infinite retention and
target-only cleanup. The existing 52 M1, ten receipt-closure and twelve PI-reuse
configurations are unchanged. This is neither a Rust refinement nor a proof of
global MQTT correctness, SQLite atomicity, bounded latency or liveness.

From the repository root, using the pinned, hash-checked TLC runner:

```bash
python tools/formal/run_tlc.py --models-dir mqttiumd/models/session-lifecycle --list
python tools/formal/run_tlc.py --models-dir mqttiumd/models/session-lifecycle
cd mqttiumd
python models/check_models.py
python -m unittest discover -s models -p 'test_*.py'
```

## Boundary and identities

There are two ClientIDs and at most two live sessions. The other destination,
SessionId1, stays persistent. The reusable ClientID starts with SessionId2.
`nextSession` increases, never wraps and never reuses a removed identity; the
cleanup witness supplies identities through6, exceeding the live capacity2.
At the end of this finite issuance domain further fresh opens suspend. This
domain is a finite test workload, not a proposed production counter limit.

`current` identifies the reusable name's durable incarnation. The fields
`policy`, `token` and `revision` model active row metadata. A successful open
uses its fresh OperationId as an attachment token. Resuming the same persistent
SessionId may change policy and token; its business revision stays1. The token
is separate from that revision. A close compares both the stored SessionId and
the original attachment token. A saved close starts with the authentic initial
transport's guard `(2,0)` and cannot look up a later ClientID binding.

`SubmitOpen` first closes the old transport. `CommitOpen` then makes the atomic
session cut. CS0 of an existing Never session resumes it/SP1, including when
the new policy is Zero. CS1 replaces it/SP0. An old Zero attachment expires
before a new CS0 lookup, so this case also creates a fresh identity/SP0. The
model collapses a zero-attachment close followed by open into this atomic cut;
it does not model their separate transport/store messages or claim that the
product persists an Open before closing the old transport.

`SubmitClose(FALSE)` represents an explicit durable end, including DISCONNECT
expiry0 or a zero-policy transport end. An ordinary Never transport detach
requires no durable deletion and is abstracted by takeover/transport loss.
`SubmitClose(TRUE)` carries the saved initial Close guard through the same
receipted store path. Malformed MQTT encodings, CONNECT/DISCONNECT property
validation, negative CONNACK mapping and finite unsupported expiry intervals
remain codec/system-test obligations outside this model.

## Atomic responsibility and accounting

The initial state includes one already committed, still unknown QoS2 admission
by SessionId2. Its immutable shared payload has two destination obligations,
one claim for target2 and one inbound marker for source2: two payload references
and two exchange-slot credits.
This initialized admission is a finite fixture for the specification, not a
product fixture or an observation fabricated after execution.

Closing/replacing a session atomically removes its physical row, its inbound
marker, its own destination obligation/claim and corresponding references.
The other destination's obligation remains even after the producer disappears.
`FinishOther` models its independent durable terminal action and returns its
last reference once. Claim state is a subset of obligations and adds an exchange slot, not a
second payload reference. An inbound marker adds an exchange slot and keeps
publication metadata, not another payload reference. Exact counters distinguish cancellation from delivery
success. The model abstracts finite preflight and the existing bounded cleanup
helpers; it does not establish their Rust iteration or allocation bounds.

`rows` is physical session metadata, while `Active(current)` is the live set.
The KeepTerminal mutation preserves removed rows and exceeds the same live
capacity after replacement. Active fields are cleared on removal. Fixed arrays
over the finite supplied SessionId/OperationId workload use absent cells for
notation; absent cells have no modeled row cost and are not a proposed
historical array in the product. `h` fields are verifier histories used only
for invariants, witnesses and recording history; no product action's enabling
guard reads them.

## Receipts, unknown results and startup

One lifecycle flight moves through Submitted, Unknown, Known and applied/Idle.
Commit changes durable state atomically but does not supply terminal knowledge
to the owner. `ResolveResult` models direct authentic receipt delivery or
reconciliation of the same OperationId; a delay or an absent observation does
not allocate a new command or authorize CONNACK. `ApplyResult` consumes the
result, updates bounded owner rows and attaches only a still-live request with
the exact current SessionId/token. An open whose transport disappeared may
leave a durable zero session awaiting later close/startup; it never becomes
ready on the dead request.

The unknown initial admission remains a receipt hole after its source closes.
`ResolveAdmission` consumes that same terminal admission without repeating it
or attaching it to a later incarnation. `CollectPrefix` can pass only consumed
positions. It atomically establishes the retired floor and removes one result
and recipe, allocating no new operation. Submission is limited to three
positions beyond that floor. The cleanup witness must resolve the original
hole and collect before it can create more incarnations. `ReplayCommand`
represents the existing receipt contract: a retained result or retired floor
prevents reexecution, including when the detailed recipe/result was collected.
Exact recipe comparison, namespace floors and physical control reserves are
received assumptions here and remain modeled independently by
[`MReceiptClosure`](../gc/README.md).

An authentic reply can be saved after resolution, then replayed after its
flight has gone. Its immutable operation correlation cannot become a new
flight's authority. The saved copy is a test transport slot, not an unbounded
runtime tombstone. Arbitrary unobserved network bytes, old ACK association and
PI reuse remain within the separate
[`MPacketReuse`](../pi-reuse/README.md) boundary.

The startup configuration permits one process loss. `Crash` closes transports
and clears owner readiness; durable state and outstanding operation identities
survive. `OpenEpoch` atomically removes all Zero sessions before the recovered
owner can serve. The outstanding terminal result remains available for
consumption after recovery, but cannot attach the dead request. This abstracts
the existing origin recovery/retirement machinery: it does not require the
product to recover a former owner's volatile flight. `DuplicateOpenEpoch`
returns an already-opened epoch without re-expiring Zero sessions established
in that current epoch. No wall clock, elapsed outage or TTL is modeled.

Authentic correlated results, serial durable session cuts, atomic SQLite
transactions and non-reused causal identifiers are assumptions. Corrupted
reads, writer transport, Outbox reservations, shared-page byte budgets, full
receipt collection and other business transitions stay with their existing
tests/models. `Idle` permits no progress and no fairness is assumed. TLC
safety/non-vacuity is not the512-cycle product test, a crash experiment or a
universal proof over all possible executions.

## Invariants and expected outcomes

| Invariants | Meaning |
| - | - |
| `SessionRowsBounded`, `SessionIdentitiesMonotone` | At most two physical/owner rows; removed incarnations never become active or reuse an allocated identity. |
| `BusinessRevisionStable`, `ActiveMetadata` | Policy/attachment changes do not invalidate business revisions; claims and responsibilities have active destinations. |
| `CreditsExact`, `OtherResponsibilityPreserved` | Payload references and exchange slots match their distinct holders; source/target cleanup cannot cancel the other destination. |
| `NoStaleClose`, `OwnerReadyAuthentic` | Frozen old guard/reply cannot authorize deletion or readiness for a new binding. |
| `NoZeroAfterRestart` | Every Zero session selected at actual startup is absent after that cut and cannot resurrect. |
| `UnknownResultsRetained`, `ClosedPrefixConsumed` | A removed source does not erase unresolved receipts, and retirement cannot cross an unconsumed result. |
| `NoReexecution`, `ReceiptWindowBounded` | Same operation cannot execute twice; held results/consumption state are limited by the finite span. |

| Configuration | Expected result |
| - | - |
| `safety` | All invariants pass over three lifecycle operations, four available fresh identities and no crash. |
| `startup` | All invariants pass over two lifecycle operations, startup cleanup, current-epoch opens and duplicate epoch opening. |
| `reach-cleanup` | Violate `NoCleanupWitness`: six identities have been allocated with at most two rows, persistent resume changed to Zero, old-Zero CS0 allocated a fresh identity, the source's unknown receipt survived its closure, collection passed that hole, and the other destination still holds the payload. |
| `mut-terminal-history` | Violate `SessionRowsBounded`: replacement keeps an old physical Terminal row. |
| `mut-old-token` | Violate `NoStaleClose`: a saved initial Close is rebound to the current incarnation/attachment instead of checking its immutable guard. |
| `mut-delete-other` | Violate `OtherResponsibilityPreserved`: cleanup cancels another destination's obligation. |
| `mut-resume-zero` | Violate `NoZeroAfterRestart`: actual startup keeps a prior-process Zero session. |
| `mut-double-credit` | Violate `CreditsExact`: cleanup returns one reference twice. |

The runner must observe the exact invariant named by each negative
configuration. Syntax errors, another invariant, timeout or incomplete
exploration are campaign failures. These are specification mutations. A
compiled product mutation has its own distinct reception and is not implied
by any TLC counterexample.

## Correspondence

The shared types are in
[`session.rs`](../../crates/mqttiumd-contract/src/session.rs). Store lifecycle
regressions are in
[`session_lifecycle.rs`](../../crates/mqttiumd-store-sqlite/tests/session_lifecycle.rs),
owner tests in
[`owner/sessions.rs`](../../crates/mqttiumd-core/src/owner/sessions.rs), and
transport tests in
[`runtime_tests.rs`](../../crates/mqttiumd/src/runtime_tests.rs).

The table identifies concrete implementation checks and the separate model
configurations. Test names describe coverage, not a claim that this model-only
lot executed the Rust/system suites or established a trace refinement. The
integrated reception records those executions against its own SHA. The sidecar
therefore retains `specification` and `tests: []`: no exact TLC-counterexample
Rust replay is registered as a refinement. Gaps remain explicit below.

| Model action | Implementation predicate | Replay |
| - | - | - |
| `SubmitOpen` | Canonical SessionLifecycle Open; old transport closes before the durable cut. | Core `five_hundred_twelve_ephemeral_lifecycles_leave_only_bounded_rings`, `full_capacity_refuses_a_new_name_but_clean_start_replaces_in_one_slot`; actual close-before-open transport ordering is received separately by the system suite. |
| `SubmitClose` | Immutable SessionGuard from confirmed transport, through the receipted path. | Store `lifecycle_replay_and_attachment_cas_preserve_business_state`; core `resuming_an_infinite_session_with_zero_expiry_rebinds_before_later_closure` checks the old token/generation. |
| `CommitOpen` | Store compares expected SessionId/token; resume preserves business revision and replacement needs no extra active slot. | Store `lifecycle_replay_and_attachment_cas_preserve_business_state`, `replacement_at_the_full_session_quota_reuses_only_its_own_slot`; safety/cleanup configurations. |
| `CommitClose` | Atomic destination-only removal and exact payload-reference/exchange-slot cleanup. | Store `loaded_termination_cancels_only_its_target_and_keeps_shared_payloads`; cardinality regressions `memory_session_cycles_do_not_retain_terminal_history`, `sqlite_session_cycles_do_not_retain_terminal_history`; terminal/other/credit mutants. |
| `ResolveResult` | ResolveAndFence returns the same OperationId terminal facts. | Core `unknown_closure_keeps_capacity_and_resolves_the_same_operation`; genuine lost writer outcomes remain separate system evidence. |
| `ApplyResult` | Correlated known result updates owner/cache before CONNACK and retains a dead-request guard. | Core `a_new_ephemeral_session_ends_and_releases_the_owner_slot`, `closing_an_unknown_open_survives_eviction_from_the_closed_generation_ring`; safety/startup configurations. |
| `EndPendingTransport` | Pending request cancellation survives closed-generation ring eviction. | Core `closing_an_unknown_open_survives_eviction_from_the_closed_generation_ring`; actual sockets remain a runtime/system obligation. |
| `SaveReply` | A test retains an authentic already correlated response. | Safety/startup configurations; product retention/duplication is an explicit system injection, not synthesized by a different operation. |
| `ReplayReply` | An old response cannot acquire a newer flight's OperationId or attachment. | Core `unknown_closure_keeps_capacity_and_resolves_the_same_operation` checks an old duplicate after application; replay after another incarnation must also be received by the system suite. |
| `ResolveAdmission` | Detached old business flights consume their own results without recreating work or emitting to the retired source. | Core `lifecycle_retired_unknown_inbound_result_closes_its_receipt_hole_without_ack` and `lifecycle_retired_claim_finish_and_consume_results_close_receipt_holes_without_emitting`; cleanup witness. The model's single admission hole abstracts those separate tested command paths. |
| `CollectPrefix` | Receipt consumer advances only a contiguous consumed prefix. | Store `receipted_ephemeral_cycles_collect_without_reusing_session_or_attachment_identities`; the independent receipt-closure model checks the detailed floor contract. |
| `ReplayCommand` | Retained canonical receipt or compact retired floor prevents old-operation execution. | Store `lifecycle_replay_and_attachment_cas_preserve_business_state`, `receipted_ephemeral_cycles_collect_without_reusing_session_or_attachment_identities`; exact recipe and retired-result checks remain distinct. |
| `FinishOther` | Guarded Finish on the surviving destination returns its last ref independently of the source's deletion. | Store `loaded_termination_cancels_only_its_target_and_keeps_shared_payloads` terminates the surviving obligations and checks final zero counters. |
| `Crash` | Old process connections disappear while durable objects/receipt guards survive. | Startup configuration; store reopen/failed-commit tests do not replace the separate real-process SIGKILL witnesses. |
| `OpenEpoch` | A genuinely new epoch removes Zero sessions before grant and recovery. | Store `only_an_actual_epoch_open_expires_zero_sessions_and_commit_failure_is_atomic`; `mut-resume-zero`. |
| `DuplicateOpenEpoch` | An already-opened epoch response does not repeat zero-expiry cleanup. | Store `only_an_actual_epoch_open_expires_zero_sessions_and_commit_failure_is_atomic` creates a current-epoch Zero session before repeating the opener; startup configuration. |
| `Idle` | No fairness, timing or useful-progress assumption. | Every configuration;512-cycle public-client capacity recovery remains system evidence. |
