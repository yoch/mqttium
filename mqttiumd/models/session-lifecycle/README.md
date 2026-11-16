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
one claim for target2 and one inbound marker for source2: three references.
This initialized admission is a finite fixture for the specification, not a
product fixture or an observation fabricated after execution.

Closing/replacing a session atomically removes its physical row, its inbound
marker, its own destination obligation/claim and corresponding references.
The other destination's obligation remains even after the producer disappears.
`FinishOther` models its independent durable terminal action and returns its
last reference once. Claim state is a subset of obligations and does not add a
second payload reference. Exact counters distinguish cancellation from delivery
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
| `CreditsExact`, `OtherResponsibilityPreserved` | Reference count matches the remaining responsibilities, and source/target cleanup cannot cancel the other destination. |
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

This table records intended implementation predicates, not executed Rust
replays. At the public interface freeze used to create this model the shared
types exist in [`session.rs`](../../crates/mqttiumd-contract/src/session.rs)
and the historical cardinality regression exists in
[`session_lifecycle.rs`](../../crates/mqttiumd-store-sqlite/tests/session_lifecycle.rs).
The latter was still red before the implementation. The sidecar deliberately
has no `tests` entries or refinement status. Complete and receive the exact
implementation-test correspondence on the integrated revision; do not infer
it from this specification run.

| Model action | Implementation predicate | Replay |
| - | - | - |
| `SubmitOpen` | Canonical SessionLifecycle Open, old transport closed before the durable cut. | Safety/cleanup configurations; concrete core/runtime replay owed at integration. |
| `SubmitClose` | Immutable SessionGuard from confirmed transport, same-operation receipt path. | `mut-old-token`; concrete saved-Close replay owed at integration. |
| `CommitOpen` | Store transaction compares expected SessionId/token, resumes or atomically replaces without an extra active slot. | Safety/cleanup configurations; concrete Open store replay owed at integration. |
| `CommitClose` | Guarded destination-only removal and exact ref cleanup. | Terminal/other/credit mutants; existing512-cycle cardinality regression needs the integrated store fix. |
| `ResolveResult` | ResolveAndFence returns the same OperationId terminal facts. | Safety/startup configurations; lost lifecycle reply replay owed at integration. |
| `ApplyResult` | Correlated known result updates owner/session cache before CONNACK, preserving dead-transport guard. | Safety/startup configurations; late Open result replay owed at integration. |
| `EndPendingTransport` | Pending request cancellation retained despite closed-generation ring eviction. | Safety/startup configurations; runtime early-close replay owed at integration. |
| `SaveReply` | Test retains an authentic correlated lifecycle response. | Safety/startup configurations; response-copy replay owed at integration. |
| `ReplayReply` | Old response cannot acquire a new flight's operation/token. | Safety/startup configurations; stale response replay owed at integration. |
| `ResolveAdmission` | Detached old admission consumes its own result without recreating input or emitting to retired source. | Cleanup witness; unknown old admission replay owed at integration. |
| `CollectPrefix` | Existing receipt consumer advances only a contiguous consumed prefix. | Cleanup witness; existing receipt-closure model independently covers its detailed contract. |
| `ReplayCommand` | Existing retained receipt/retired floor prevents any execution with that old identity. | Both safety configurations; detailed canonical replay remains covered by receipt reception. |
| `FinishOther` | Guarded Finish on the surviving destination returns its last ref independently. | Safety/startup configurations; shared-payload termination replay owed at integration. |
| `Crash` | Prior process connections disappear while durable objects and receipt guards remain. | Startup configuration; actual process/signal scenarios owed to system reception. |
| `OpenEpoch` | Actual new epoch transaction removes zero-policy sessions before grant and recovery. | Startup and `mut-resume-zero`; actual startup replay owed at integration. |
| `DuplicateOpenEpoch` | Already-opened epoch response does not run zero-expiry cleanup again. | Startup configuration; duplicate-opening store replay owed at integration. |
| `Idle` | No fairness or latency assumption. | Every configuration; useful product progress remains system evidence. |
