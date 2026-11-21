# Bounded receipt closure and compact identity guards

`MReceiptClosure` is a focused finite specification model of the live-prefix
mechanism of proposed ADR 0027 and ARC-019/021. It does not extend the broker's
M1 proof, prove MQTT liveness, or replace SQLite/crash/system reception. The
existing 52 configurations under `models/m1/` remain unchanged. This directory
adds ten expected outcomes: two safety passes, three reachability witnesses
and five mutations.

From `mqttiumd/`:

```bash
python models/check_models.py
python ../tools/formal/run_tlc.py --models-dir models/gc --list
python ../tools/formal/run_tlc.py --models-dir models/gc
```

## Scope and assumptions

An origin is a fixed abstract value for `(StoreIncarnation, RunEpoch,
LogicalOwnerId)`. Its checked operation authority never reuses a sequence.
The safety configurations use one origin/four operations or two origins/two
operations. The reachability and mutation witnesses use one origin and two to
four operations.
This is not an unbounded-origin or epoch-allocation model; startup collapse
of old origins behind `EpochLedger.collected_through` is outside it.

The scaled origin window is three issued sequences beyond its **acknowledged
absolute floor**. New business can use only span two; a continuation operation
can use the third position. The production bounds are 1024/768, with receipt
reserve and queue limits described by ADR 0027. The model checks a bounded
window and independent maintenance capacity, not that those production values
prove scheduling liveness for arbitrary traffic. No fairness or latency is
assumed, and `Idle` permits a scheduler that does not make progress.

Operation 1 represents a subscription/business command, 2 an admission, 3 a
continuation and 4 another admission. Publication and payload counters advance
at admission issuance independently of the operation counter. The finite
`IssuedData(n)` workload is the immutable issuance boundary captured for
operation position `n`, **not the maximum committed identity**. Adversarial
fresh operations may present an old publication or payload identity separately.
A correct store refuses them. Normal source issuance/order and canonical
scope validation remain assumptions of the producer interface.

`ConsumeResult` represents application of an authentic correlated terminal
result with its consequences retained by the owner. A result in a transport
queue, timeout, absence, MQTT ACK, history eviction or deletion of a live
object is not consumption. The `results` set is a bounded abstraction of
terminal replies; `LoseResult` can hide a truly committed result and `Resolve`
reads its existing outcome again. `Fence` creates a non-executable outcome
for an absent operation without executing its delayed command.

There are three distinct durable frontiers: operation, publication and payload.
The origin requests an exact consumed-prefix boundary. The store checks every
newly closed position has a terminal receipt/fence and that the logical advance
is bounded. Honesty of the private origin's consumption proof is essential:
terminal rows alone cannot prove their results reached the owner. `CrossHole`
shows that difference with a committed but unconsumed earlier result.

`ApplyCollection` is one atomic store transition: advance the compact guards,
delete at most **one physical row** and subtract the actual deletion count.
The model's maximum logical advance is two, independently of this smaller
physical sweep. Production advances at most 128 positions and removes at most
128 rows in a pass. Atomicity is an assumption supplied by the store contract;
real transaction cut/crash witnesses are received separately. Physical rows
can remain below the logical floor. Such an old OperationId is already retired
and cannot execute, regardless of whether its canonical bytes differ.

Lost, duplicated and reordered maintenance requests/answers are represented
by one active request/reply lane per origin, `RetryCollection`,
`LoseCollectionReply`, `InjectLateReply` and monotone `ObserveCollection`.
An unresolved request retains its original closure; old already-known
closures can be repeated after a newer one. There is no maintenance OperationId or receipt.
The origin uses absolute acknowledged closure, never the answer's removed-row
count as an additive credit. The scheduler's empty-producer/no-spin behavior
and transport correlation checks remain implementation/system-test obligations.

Retained nonretired receipts compare exact recipes. `ReplayOld` abstracts a
matching replay or mismatch as no new execution; it does not prove the concrete
error enum or byte codec. Separate live publication/payload metadata survives
receipt collection, and `Settle` can remove it later. Namespace frontiers then
prevent identity reuse after both the object and its full receipt disappear.
No per-operation tombstone is substituted for collection. The fixed-size TLA+
arrays encode partial tables with absent cells costing no row; h-prefixed
verifier histories are never read by guards and are not implementation state.

## Invariants

| Invariant | Meaning |
| - | - |
| `ClosedResultsConsumed` | Every logically closed position's result was consumed by its origin. |
| `DeletedOnlyAfterClosure` | A physical receipt/fence cannot disappear before its durable guard. |
| `CreditsExact` | The physical receipt account equals the actual retained row count, even after repeated maintenance. |
| `WithinWindow`, `OriginMetadataBounded` | Origin issuance and unacknowledged consumption metadata remain bounded. |
| `ClosureKnowledgeSound` | An origin never learns an absolute closure beyond the durable floor. |
| `AdmissionIdentityUnique` | One OperationId cannot execute twice. |
| `PublicationIdentityUnique`, `PayloadIdentityUnique` | Separate compact guards prevent old data identities being admitted under a new operation. |

## Expected outcomes

| Configuration | Expected result and scope |
| - | - |
| `safety` | All safety invariants pass for one origin, four operations and independent data identities. |
| `two-origins` | All safety invariants pass with two independently bounded origins and unknown gaps. |
| `reach-gap` | Violate `NoUnknownGap`: operation 2 consumed while operation 1 has a terminal row but no consumed result. This is a non-vacuity witness, not a defect. |
| `reach-window-reuse` | Violate `NoWindowReuse`: collection and absolute acknowledgement allow more operations than the fixed window, without raising the bound. |
| `reach-live-after-close` | Violate `NoClosedLiveObject`: a live admission object can outlast its closed receipt. Business completion need not pin unrelated outcome history. |
| `mut-cross-hole` | Violate `ClosedResultsConsumed`: use the highest consumed result rather than the contiguous prefix. |
| `mut-delete-unclosed` | Violate `DeletedOnlyAfterClosure`: delete a row while its durable closure update is omitted. |
| `mut-old-publication` | Violate `PublicationIdentityUnique`: forget the publication frontier, retaining only live/full-receipt checks. |
| `mut-old-payload` | Violate `PayloadIdentityUnique`: forget the independent payload frontier. |
| `mut-double-credit` | Violate `CreditsExact`: a single-row pass with at least three retained rows returns two credits. |

The mutation configurations must compile and reach their named invariant.
Compilation errors, a timeout, a different invariant or an unreachable fault
are not successful detections. The concrete Rust/system mutations remain
separate evidence, not inferred from these model configurations.

## Correspondence

| Model action | Contract / implementation predicate | Replay |
| - | - | - |
| `Issue` | Pure owner issuance tracker; `ReceiptOrigin`, acknowledged floor, data/control spans in `receipts.rs`. | Safety configurations and `reach-window-reuse`; bounded-pressure concrete reception remains separate. |
| `ApplyCommand` | `DurableStore::apply`; exact retained recipes plus live and closed publication/payload identity guards. | Identity mutation configurations; corresponding concrete old-publication/old-payload probes. |
| `Fence` | `DurableStore::resolve_and_fence`; absence is not rejection until a non-execution fence is established. | Safety configurations; old S0 fence tests remain unchanged. |
| `Resolve` | Reconcile the same OperationId through its existing terminal outcome. | `reach-gap`, safety configurations; concrete held-result resolution. |
| `LoseResult` | Delivery failure does not confirm terminal-result consumption. | `reach-gap`, `mut-cross-hole`; concrete held committed result. |
| `ConsumeResult` | Owner application of a correlated validated terminal result, not generic `StepOutcome::Applied`. | `mut-cross-hole`; core mutation `MUTANT_CONFIRM_UNKNOWN`. |
| `RequestCollection` | `Effect::CollectReceipts` with three exact captured namespace frontiers. | Closure/gap configurations and the concrete same-operation/unknown-hole controls. |
| `RetryCollection` | Replay the same metadata-only closure after uncertainty or while physical rows remain. | Both safety configurations; concrete duplicate/lost maintenance result. |
| `ApplyCollection` | Atomic `DurableStore::collect_receipts(ReceiptCollection)`: durable logical retirement before visible deletion, exact physical counts. | `mut-delete-unclosed`, `mut-double-credit`; SQLite `MUTANT_DELETE_WITHOUT_CLOSURE` and `MUTANT_DOUBLE_RECEIPT_CREDIT`. |
| `ObserveCollection` | `Input::ReceiptsCollected` applies absolute monotone closure, never adds `removed` as a fresh grant. | Both safety configurations and concrete late/repeated maintenance results. |
| `LoseCollectionReply` | A committed collection can remain uncertain at its origin. | Safety configurations; concrete crash/commit and lost-result reception. |
| `InjectLateReply` | An authentic old maintenance answer cannot regress absolute known closure. | Both safety configurations; concrete delayed/duplicate maintenance replies. |
| `Settle` | Independent business lifetime: obligations/claims/inbound state can finish after their command receipt is retired. | `reach-live-after-close`; concrete offline/claim/PUBREL and consumed-QoS0 cases. |
| `ReplayOld` | Retired operation check precedes physical rows; retained nonretired commands keep canonical-byte mismatch protection. | Safety configurations; concrete same/different old operation recipes. |
| `Idle` | Scheduler choice, without a liveness promise or busy-loop proof. | Every configuration; production scheduling and maintenance reserve are independently tested. |

The source constants `MUTANT_ALLOW_OLD_PUBLICATION` and
`MUTANT_ALLOW_OLD_PAYLOAD` are the concrete counterparts of the two separate
identity mutants. This table names contracts and intended predicates; the model
remains a specification model rather than asserting a machine-checked Rust
refinement or a global MQTT/storage proof.
