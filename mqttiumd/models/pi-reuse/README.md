# Protected outgoing Packet Identifier reuse

`MPacketReuse` is a focused finite specification model for Proposed
[ADR 0028](../../docs/decisions/0028-outgoing-packet-identifier-reuse.md).
It checks reservation, uncertainty, immutable ACK
association and exact terminal credit. It does not establish Rust refinement,
global MQTT correctness, storage atomicity or scheduling liveness. The existing
52 M1 and ten receipt-collection configurations are unchanged.

From `mqttiumd/`:

```bash
python models/check_models.py
python ../tools/formal/run_tlc.py --models-dir models/pi-reuse --list
python ../tools/formal/run_tlc.py --models-dir models/pi-reuse
```

## Model boundary

There is one durable target session, two publication/delivery occurrences and
one or two Packet Identifiers. The pure owner can hold two delivery credits,
independently of the number of PIs in the reduced test domain. Selection uses
the smallest PI not reserved by a live occurrence. Reservation starts before
the Claim command and includes `ClaimPending`, an unknown committed Claim,
`AwaitPuback`, and `FinishPending`, including an unknown committed Finish.
Only applying the correlated known Finish result removes the reservation.

Claim and Finish operations have distinct non-reused identities. The finite
workload assigns `ClaimOperation(d) = 2*d-1` and `FinishOperation(d) = 2*d`.
Frame effects use a checked, increasing counter; it is never recycled with a
PI. Connection generations increase at most once and never wrap. Exhausting
the finite causal-effect domain disables another emission; it does not reset
the counter. These abstract identities stand for the full production scopes,
not MQTT Packet Identifiers or a proposal to change their persistent encoding.

`ClaimCommit` and `FinishCommit` are atomic store facts. They do not imply the
owner knows the result. `ClaimKnown` and `FinishKnown` model either a directly
correlated terminal reply or reconciliation of that **same OperationId**.
They can be delayed forever. Timeouts and observed absence cannot free a PI.
The model covers committed-but-unknown commands, not the separate certain
refusal path. Its underlying store atomicity and authentic terminal facts are
assumptions received by the store/system tests, not proved by TLC here.

The recovery configuration starts with an existing durable claim. Recovery
metadata reserves its original PI and causal claim identity before loading
the payload. Availability waits for all modeled claims to be reserved. The
existing claim is conservatively eligible for DUP=1 on its next generation;
a fresh occurrence starts with DUP=0 even when it reuses the same PI. `Takeover`
discards the connection's emission map while keeping durable responsibilities
and reservations. This is a reconstruction/takeover abstraction, not a SIGKILL
or SQLite transaction-cut model.

## Authoritative ACK association

`QueueRawAck` puts unassociated PI/generation bytes in the logical reader
channel. `DequeueAck` is the authoritative runtime dequeue of that channel,
**before** the runtime input queue. It copies an immutable `Target` containing
delivery, claim-operation and frame-effect identities, or copies `NoTarget`.
`ProcessAck` uses that copy with its original generation and PI. It never
looks up a replacement occupant for an old associated observation.

`AcceptPublish` collapses production of a PUBLISH effect and its successful
adapter acceptance. A refused transfer has no model transition: the existing
bounded Outbox retains its effect and no emission mapping is registered. This
model checks accepted effects, not the Outbox's separate reservation protocol.
`AcceptPublish` installs the bounded accepted-emission mapping atomically before
`WirePublish` can expose the frame to the peer. Consequently an ACK can be
associated and processed before `CurrentWrittenCallback`. A local write result
is not evidence of peer receipt and cannot finish a durable responsibility.
The `reach-ack-before-written` witness requires the PUBLISH to have been exposed
to the peer while its local callback is still pending.

One reader slot, one runtime-input slot, one saved observation copy, one saved
Finish reply and one saved frame callback are enough for the bounded adverse
orderings. The saved copies model a test retaining existing authentic messages;
they are not a proposed production history table. Replaying a saved input does
not pass through `DequeueAck` again and cannot change its copied target. A
`NoTarget` input stays without authority after a later emission becomes mapped.

The guarantee deliberately starts at `DequeueAck`. Raw bytes still upstream in
the reader channel or socket do not yet have a causal association. MQTT PUBACK
carries no DeliveryId or claim identity, so an arbitrary identical ACK first
observed after PI reuse cannot universally be distinguished from a current
ACK. The model makes no such promise. It does not assume the peer obeys ACK
ordering to turn an associated old copy into a fresh one.

## Bounded state and receipts

The owner maps and reservations range over live deliveries/PIs. After known
Finish, its active occurrence fields become absent/zero. No per-message runtime
tombstone is used to reject an old callback. The h-prefixed verifier histories
record facts only for invariants and non-vacuity checks; guards never read them.
The two-element arrays describe the finite supplied workload, not cumulative
production metadata. At most two terminal Finish outcomes are modeled until
their owner consumes them; maintenance itself creates no business operation.

The receipt-collection mechanism, old full-recipe protection, namespace floors,
control reserves, physical SQLite schema and 8192-receipt production budget are
outside this model and remain covered by their existing reception. A pending
unknown outcome is assumed retained/reconcilable under that contract. A late
authentic result can still be rejected against the live causal identity after
its old business occurrence has disappeared; a historical receipt is not an
authorization to act on the current PI occupant.

`Idle` permits no progress, so there is no fairness, latency or busy-loop claim.
Finite safety passes do not prove unbounded service or the >65536-delivery
product reception. That run and its independent client/wire evidence remain
separate system tests.

## Invariants

| Invariants | Meaning |
| - | - |
| `UniqueLivePacket`, `ReservationHeld` | Live pending/unknown/active claims retain distinct target-session PIs. |
| `ReleasedOnlyAfterKnownFinish`, `CreditsExact` | The allocator cannot expose a retained claim's PI and terminal credit is returned exactly once. |
| `RecoveredClaimsReserved` | A ready owner has reserved the stored claim's original PI before loading its payload. |
| `NoWrongFinish`, `NoWrongWritten` | A copied ACK or callback cannot affect a different causal occurrence. |
| `EmissionRegistered`, `ActiveFrameGuard` | Socket-eligible output has its accepted target and a known claim phase. |
| `FreshFrameDupZero`, `RecoveredFrameDup` | A fresh first-generation occurrence starts DUP=0; the modeled recovered exchange resumes with DUP=1. |

## Expected outcomes

| Configuration | Expected result |
| - | - |
| `live` | All invariants pass with two PIs and two occurrences. |
| `recovery` | All invariants pass with one PI, a recovered claim and a possible takeover. |
| `reach-reuse` | Violate `NoReuseWitness`: a new causal occurrence emits DUP=0 with the known-finished occurrence's PI and different claim/frame identities. This is non-vacuity. |
| `reach-ack-before-written` | Violate `NoAckBeforeWrittenWitness`: a legitimately observed PUBLISH is acknowledged before the local write callback. |
| `reach-stale-copy` | Violate `NoStaleCopyWitness`: an old immutable observation copy is ignored after a new occurrence reuses its PI. |
| `reach-none` | Violate `NoNoneWitness`: an observation frozen without a target stays ignored after a mapping appears. |
| `reach-recovery-before-payload` | Violate `NoRecoveryBeforePayloadWitness`: readiness includes the recovered PI reservation while its payload is still unloaded. |
| `mut-early-free` | Violate `ReleasedOnlyAfterKnownFinish`: the allocator considers a FinishPending PI free before the owner applies its terminal result. |
| `mut-occupied` | Violate `UniqueLivePacket`: ignore an occupied PI held by another pending/unknown claim. |
| `mut-rebind-copy` | Violate `NoWrongFinish`: rebind an old associated observation to the new occurrence at processing time. |
| `mut-rebind-none` | Violate `NoWrongFinish`: promote a targetless observation after a new mapping appears. This is the same Rebind mutation with a different boundary case. |
| `mut-old-callback` | Violate `NoWrongWritten`: ignore a Written callback's old effect identity and update a newer frame. |

The last five configurations represent four mutation classes. They must reach
their named invariants; a syntax error, timeout or another failure is not a
detection. Concrete successfully compiled code mutations are independent
evidence and are not inferred from these finite specification mutations.

## Correspondence

The owner predicates and named core tests are in
[`owner/pubsub.rs`](../../crates/mqttiumd-core/src/owner/pubsub.rs). The TCP
predicates are `Runtime::packet`, `Runtime::prune_outgoing_bindings` and
`Sink::submit(EmitPublish)` in
[`runtime.rs`](../../crates/mqttiumd/src/runtime.rs); reader-channel regression
tests are in [`runtime_tests.rs`](../../crates/mqttiumd/src/runtime_tests.rs).
The exact finite minimum over PIs models the production allocator's policy;
its live-count-plus-one search bound is established separately by the code,
not by an unbounded complexity claim from this tiny domain.

| Model action | Implementation predicate | Replay |
| - | - | - |
| `Reserve` | `Owner::free_packet_id` and `Owner::reserves_packet_id` count all live claims. | `mut-occupied`, `reach-reuse`; `an_applied_finish_reuses_a_free_identifier_after_the_historical_ceiling`, `a_submitted_unknown_claim_reserves_its_identifier_before_any_publish_or_callback`. |
| `ClaimCommit` | Canonical guarded Claim commits with the original OperationId. | Safety configurations; store test `lost_claim_and_finish_reply_resolve_without_new_identity`. |
| `ClaimKnown` | Validated direct result or same-operation resolution makes output eligible. | Safety configurations; core test `an_unknown_claim_keeps_its_packet_identifier_unavailable`. |
| `RecoverClaim` | Complete recovery inventories stored outgoing claims before payload loading. | `reach-recovery-before-payload`; `the_complete_small_domain_counts_unknown_finish_and_unloaded_recovered_claims`. |
| `RecoveryComplete` | Readiness waits for complete metadata, not only loaded payload pages. | Recovery configurations; store test `recovery_reserves_unloaded_claim_and_failed_finish_cannot_free_its_pi`. |
| `LoadPayload` | Bounded work read supplies data without granting another PI. | Recovery configurations; `the_complete_small_domain_counts_unknown_finish_and_unloaded_recovered_claims`. |
| `AcceptPublish` | `Sink::submit` registers accepted target before the outgoing queue transfer, rolling back refusal. | `reach-ack-before-written`; `a_valid_puback_can_precede_the_local_write_callback_and_duplicates_do_not_finish_twice`. |
| `WirePublish` | Peer-visible PUBLISH remains distinct from local Written notification. | ACK-before-Written witness; independent raw-wire product reception. |
| `CurrentWrittenCallback` | Correlated local frame result does not finish the obligation or free its PI. | Safety configurations; `a_valid_puback_can_precede_the_local_write_callback_and_duplicates_do_not_finish_twice`. |
| `OldWrittenCallback` | Local callback lookup must match the exact effect and connection generation. | `mut-old-callback`; `old_finish_results_and_frame_callbacks_cannot_change_a_reused_slot`. |
| `QueueRawAck` | Raw reader-channel input has no causal claim identity yet. | Safety configurations; runtime test `a_no_target_puback_observed_before_the_first_claim_cannot_bind_to_it_later`. |
| `DequeueAck` | `Runtime::packet` copies an accepted-emission `PubackTarget`/None before enqueueing owner input. | Rebind/None configurations; runtime test `a_no_target_puback_observed_before_the_first_claim_cannot_bind_to_it_later`. |
| `ReplayObservedCopy` | Reinject an authentic associated observation unchanged. | `reach-stale-copy`, `mut-rebind-copy`; `an_observed_puback_target_cannot_be_rebound_after_identifier_reuse`. |
| `ProcessAck` | `Owner::plan_observed_puback` validates copied delivery/claim/frame/generation. | Both Rebind configurations; `an_immutable_target_requires_every_causal_identity_and_the_current_connection`, `an_observed_puback_without_a_target_cannot_acquire_a_future_claim`. |
| `FinishCommit` | Guarded durable Finish can precede terminal-result knowledge. | `a_finish_committed_but_not_known_and_its_resolution_keep_the_identifier_reserved`. |
| `FinishKnown` | Only application of the correlated known terminal result removes the live slot. | `reach-reuse`, `mut-early-free`; `a_finish_committed_but_not_known_and_its_resolution_keep_the_identifier_reserved`. |
| `OldFinishCallback` | An already-applied old command result cannot return credit for a newer occupant. | Both safety configurations; `old_finish_results_and_frame_callbacks_cannot_change_a_reused_slot`. |
| `Takeover` | New generation invalidates emission associations while durable claims retain their PIs. | Recovery safety; `takeover_keeps_claim_and_number_but_replaces_the_connection_and_emission_target`. |
| `Idle` | No scheduling fairness is assumed. | Every configuration; finite product liveness/pressure reception remains separate. |

The mutation classes correspond to `MUTANT_EARLY_PI_RELEASE`,
`MUTANT_IGNORE_OCCUPIED_PI`, `MUTANT_REBIND_OBSERVED_PUBACK` and
`MUTANT_STALE_OUTGOING_FRAME_RESULT`. The last is an old callback update guard,
not a claim that a local Written callback normally returns delivery credit.
Exact-credit checks still cover ordinary and duplicate old Finish outcomes.
This correspondence is reviewed documentation: the status remains
`specification`, not a machine-checked Rust refinement.
