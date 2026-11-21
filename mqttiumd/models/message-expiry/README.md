# Finite Message Expiry and started-delivery ownership

`MMessageExpiry` is a finite specification model tracked by integration PR
[#648](https://github.com/yoch/mqttium/pull/648). It adds a separate model;
the retained, session-expiry, receipt and packet-identifier models are unchanged.
It is not an implementation refinement, a universal proof, an MQTT conformance
proof, a real crash experiment or independent-oracle qualification. Its sidecar
therefore says `specification` and `tests: []`.

```bash
# Repository root, with the pinned TLC tool environment:
python tools/formal/run_tlc.py --models-dir mqttiumd/models/message-expiry --list
python tools/formal/run_tlc.py --models-dir mqttiumd/models/message-expiry
cd mqttiumd
python models/check_models.py
python -m unittest discover -s models -p 'test_*.py'
```

## Finite workload and time

There is one original publication, one exact retained topic and two recipient
obligations. Recipient 1 is initially pending; one optional retained replay
creates recipient 2's independent obligation to the same immutable payload.
The producing session is distinct from both recipients. Its destruction has
no authority over their work or the retained reference. At most three references
and two fixed packet identifiers coexist. Packet identifier reuse, per-filter
selection and global identity allocation remain in their existing models.

`origin` contains the publication's original clock domain, raw tick and finite
interval. Each delivery's `stamp` must equal that original record, including
after replay, retry and recovery. Neither replay selection nor a later socket
attempt admits a newly timed publication. A retained replay rechecks freshness
at selection; a still-present expired retained slot grants no permission.

Configurations separately use intervals 0, 2 and `MaximumInterval`. Zero is
finite zero: it never satisfies `Fresh` and is due immediately. The maximum
MQTT interval, 4294967295, is represented by finite abstract value 4 because
the pinned TLC's machine integers cannot represent that wire value. It becomes
due at the matching abstract tick 4. There is no infinity sentinel and no
non-expiring publication in this workload. The five sampled raw instants are
0, 1, 2, 3 and 4. `Tick` can jump forward between these instants, including
while the process is stopped. This ordered finite abstraction does not test
the actual wire maximum, nanosecond conversion, checked arithmetic, tick
precision or numeric exhaustion; concrete provider/store tests must do so.

The original domain is 1. Recovery may retain that known domain, in which case
elapsed raw ticks remain applicable, or lose continuity and observe domain 2
with a new tick origin. Domain 2 means an origin whose relation to the old one
is unavailable; its raw ticks cannot measure the old publication's age. Recovery
quarantines old pending work and the retained slot, and never reanchors their
publication metadata. Quarantine retains charges and prevents selection and
first send. No unknown downtime, UTC elapsed time or fabricated fresh interval
is used. A zero interval remains independently known to be due without elapsed
time. Recognizing boot/time-namespace domains is a separate provider obligation.

## Send boundaries, ambiguity and ownership

The durable delivery states include `pending`, `claim`, `known-started`,
`expired` and `temporal-uncertain`. `retained` separately records
`retained-current`, expiry or temporal uncertainty. `referenced` is the explicit
set of recipient owners; a socket attempt separately enters
`socket-start-uncertain`. These are different dimensions of one responsibility,
not interchangeable publication statuses.

`Claim` reserves the delivery's durable claim and packet identifier. A delayed
claim still needs a fresh publication at `SocketStart`, the atomic last
authorization before a possible first socket byte. A durable claim by itself
does not permit a stale first send. `SocketStart` marks the attempt ambiguous;
`PositiveSend` observes its positive outcome later. Time may advance between
these actions because that later observation is not a new first-byte decision.
The model assumes that the implementation serializes this final check with
socket authorization; checking only when placing work in an earlier queue does
not discharge the obligation.

`DurableStarted` records the positive start. A QoS1 delivery then keeps its own
reference, durable claim and packet identifier through an ACK and
`DurableFinish`, including after expiry. `Ack` may precede `DurableStarted`;
the ACK does not return ownership. A later `Retry` and another `SocketStart`
may retransmit known-started work past TTL using the original stamp. Each
delivery has at most two attempts, a finite workload bound rather than a
production retry limit. Correlated ACK/transport identities and exact result
resolution remain separate packet-identifier and implementation obligations.

Expiry returns only the expiring retained reference or an unambiguously
unstarted delivery's own reference and identifier. Other recipient references
keep the payload alive. An ambiguous claim instead enters temporal uncertainty
and remains charged. If a still-correlated positive outcome arrives in that
live process, `DurableStarted` can record it and the ordinary ACK/Finish path
remains available.

`Crash` loses socket outcomes, volatile ACK knowledge and volatile ambiguity.
It preserves committed business state. `Recover` reconstructs every unresolved
durable claim conservatively as possibly started. If such a claim is expired
or incomparable, recovery quarantines it with its reference and identifier;
it never treats the absence of a durable Started record as proof that no socket
byte escaped. Fresh recovered claims remain eligible under the same freshness
check. Known-started claims stay known-started and can complete beyond TTL or
after loss of clock continuity. There is no automatic release or first-send
path out of a recovered expired ambiguous claim in this model.

`references` is independently updated by each ownership delta.
`ReferencesExact` recomputes owners from delivery and retained state;
`PayloadLifetimeExact` keeps the payload exactly while a reference remains.
`StartedReferenceHeld` uses the verifier's positive-send history to require
all three resources until a legitimate durable Finish. `NoLostObligations`
checks that admitted work disappears only through its own expiry or Finish,
and that retained expiry has its own terminal action. Source death cannot
create those terminal histories.

## Expected outcomes and proof boundary

| Configuration | Expected result |
| - | - |
| `safety` | All nine invariants pass over replay, delayed claim, socket uncertainty, positive send, ACK, expiry, retry, Finish and source destruction. |
| `recovery` | The same invariants pass with one process loss and same-domain clock advancement while stopped. |
| `domain-change` | The same invariants pass with one optional loss of clock continuity; old unstarted work is quarantined. |
| `zero` | The same invariants pass with a finite zero interval, including recovery and an optional changed domain. |
| `maximum` | The same invariants pass for the finite maximum class, including its abstract exact expiry boundary. |
| `mut-ttl-reset` | `PublicationExpiryFixed` fails when replay refreshes the stamp from the current raw tick. |
| `mut-retry-ttl-reset` | The same invariant independently fails when only retry refreshes the stamp; replay cannot mask this defect. |
| `mut-stale-send` | `NoStaleFirstSend` fails when an expired delayed claim can start its first socket attempt. |
| `mut-free-started-ref` | `StartedReferenceHeld` fails when TTL frees a known-started claim before ACK and durable Finish. |
| `mut-domain-assumption` | `NoAssumedContinuity` fails when recovery treats a changed origin's raw ticks as comparable and restores old work as fresh. |
| `reach-past-ttl-finish` | `NoPastTTLFinishWitness` is intentionally violated after the same started delivery retries past TTL and durably finishes. |
| `reach-ambiguous-expiry` | `NoAmbiguousExpiryWitness` is intentionally violated after an observed positive send, a crash before durable Started, same-domain expiry and recovery with the ambiguous claim still charged. |
| `reach-domain-quarantine` | `NoDomainQuarantineWitness` is intentionally violated with retained and recipient references quarantined after continuity is lost. |
| `reach-zero`, `reach-maximum` | `NoFiniteBoundaryWitness` is intentionally violated when retained and pending owners independently expire at the exact finite interval, returning the final reference and payload. |

Negative configurations must fail their exact named invariant. A parse error,
timeout, unexpected invariant or incomplete exploration is not detection.
The mutants change this specification; they do not mutate compiled product
code or supply system-client reception evidence.

Atomic durable transitions and the survival of committed records through a
process loss are assumptions. There is one original publication, one replay,
at most two attempts per delivery, five sampled ticks, two domain labels and
at most one crash. `h` variables record verifier history only; ordinary product
guards do not consult them. `Idle` permits no progress. There is no fairness,
universal liveness, real-time scheduling bound, garbage-collection guarantee
for quarantine, or proof about SQLite, fsync or power loss. Concrete stores,
the final send check, durable Started/Finish engagement and crash barriers need
their own tests and independent evidence.

## Correspondence

The table states implementation obligations, not completed Rust trace replays.

| Model action | Implementation predicate | Replay |
| - | - | - |
| `Tick` | Same recognized clock domain advances publication age, including while the broker is stopped. | Deterministic provider/restart tests; `recovery`, `maximum` and exact-boundary witnesses. |
| `SelectReplay` | Check retained eligibility and inherit the original publication domain/tick/interval when creating a distinct recipient obligation. | Both-store frozen replay/expiry tests; `mut-ttl-reset`. |
| `Claim` | Fresh pending work acquires its own durable claim and packet identifier. | Store claim/expiry ordering tests and final-send tests. |
| `SocketStart` | Recheck freshness at the last authorization before a possible first byte; only durable known-started work can retry beyond TTL. | Controlled delayed writer and positive-send boundaries; `mut-stale-send`, `reach-past-ttl-finish`. |
| `PositiveSend` | A correlated positive transport result supplies start evidence without returning the reference or identifier. | Writer result/expiry race tests and `reach-ambiguous-expiry`. |
| `DurableStarted` | Persist positive start knowledge before allowing expiry-independent QoS1 continuation; a live correlated result can resolve uncertainty. | Both-store Started engagement and lost/delayed-result tests. |
| `Retry` | Preserve the original publication expiry metadata on another known-started attempt. | Reconnect/retry stamp tests; `mut-ttl-reset`. |
| `Ack` | An ACK alone does not release the claim, payload reference or packet identifier. | ACK-before-Started and ACK-before-Finish barriers. |
| `DurableFinish` | A correlated durable QoS1 Finish returns exactly its own resources, including after TTL. | Both-store Finish and reference-accounting tests; `reach-past-ttl-finish`. |
| `ExpireDelivery` | Expire only proven unstarted work; quarantine ambiguous claims and preserve started claims until Finish. | Pending/claim/positive-start expiry ordering and recovery tests; `mut-free-started-ref`, `reach-ambiguous-expiry`. |
| `ExpireRetained` | Remove only the retained reference; recipient owners remain independently charged. | Retained/pending shared-payload expiry tests; finite-boundary witnesses. |
| `SourceDeath` | Ending the producing session cannot erase retained or other recipients' obligations. | Source-session destruction with retained, pending and inflight recipient work. |
| `Crash` | Lose volatile observations while preserving committed claims, ownership, expiry stamps and durable Started records. | Separate same-file process-cut tests before/after Started and Finish. |
| `LoseContinuity` | Treat an unrecognized clock relation as unavailable, rather than inventing elapsed or remaining lifetime. | Provider changed/unavailable-domain tests; `mut-domain-assumption`. |
| `Recover` | Rebuild known-started claims, quarantine expired ambiguous claims and old unstarted work with incomparable time. | Both-store typed recovery tests; recovery and quarantine witnesses. |
| `Idle` | Safety does not assume a fair scheduler or a bounded maintenance delay. | Every configuration; no liveness result is declared. |
