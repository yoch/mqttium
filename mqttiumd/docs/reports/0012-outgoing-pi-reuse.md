# Protected outgoing QoS 1 Packet Identifier reuse

mqttiumd reuses an outgoing Packet Identifier after the owner applies the
correlated known Finish commit. The durable pub/sub, receipt collector and
MQTT profile remain in place. PI numbers are finite reusable resources;
DeliveryId, claim OperationId, EffectId, generations and durable identity
frontiers are causal identities and never wrap or move backwards.

## Starting content and executable regressions

The GitHub connector confirmed PR #648 at
66d2f616256e7f69f0d0483ddb7fabb513b625d8, base main
874f298b4de7c3a13006cb1ff767dfb09e4031ac, draft/unmerged and green checks.
There were no human reviews/threads; the only discussion entry was the docs
bot. Native fetch and the inspected allocator blob cdbc3cf657dd51d485b82bd2f9be5e5b6b51404b
matched the connector. No receipt-GC general re-audit was undertaken.

The committed fast red test 1611a4e7 positions the old cursor at 65535,
executes Claim → PUBLISH → PUBACK → known Finish, then demands another Claim
on the same connection. The historical owner instead closes CapacityExceeded,
detected as outgoing_pi_historical_exhaustion. Its fix removes the obsolete
cursor and consults only live reservations. The regression preserves its
next-Claim/no-capacity-close assertion; its private historical cursor setup
has no counterpart once that scalar is removed.

A separate actual-socket red test 76049091 exposes late binding on the old
runtime: a no-target PUBACK is dequeued into its owner-input queue, copied,
then delayed until a real new claim uses PI1. The old raw packet path wrongly
finishes it (no_target_puback_rebound). The fix introduces the immutable input
and an accepted-emission registry, rather than treating a later PI lookup as
the original observation.

The first integrated normal diagnostic received and durably finished 128
numbered mqttium→broker→Paho deliveries, one connection each, with 127 observed
PI1 reuses and seven receipt-collection applications. Rust build content was
7ac2433775d84ac142a66212dd52ef651c92f1ce, harness9e9ec6e2df22f1aa68582d7896cb62130e30027e,
binary e28c6d56fca6babdcde9c0c1ef444693285f65b213863067b91d216d27478c89.
All 1,962 actual product-event records passed the new schema/semantic reader,
including 128 distinct delivery/claim/frame targets. This diagnostic explicitly
has long_requirement_met=false; it is not relabeled as the final 65537 run.

## Reservation, observation and recovery

Proposed [ADR 0028](../decisions/0028-outgoing-packet-identifier-reuse.md) gives
the precise authority and normative references. The smallest-free selection
checks every target-session claim, regardless of its phase, window or loaded
payload. It checks at most live claims+1 candidate numbers against bounded
metadata (product maximum 129). Unknown Claim/Finish outcomes retain the same
operation and their reservations. A valid negative PUBACK terminates through
the same durable Finish. QoS 0 never enters this accounting.

Existing SQLite uniqueness and conditional Claim/Finish guards are reused.
There is no SQL schema, canonical recipe or recovery encoding change; schema 4
and its explicit historical-format refusal remain. A committed Finish whose
reply is lost can remove the SQL row while the live owner still reserves its
number. Only same-operation known resolution/application releases it. After a
crash, durable state decides whether that claim still exists. Claim metadata
reserves the original number before online payload reads, independently of
whether its historical receipt is retained or collected.

The product's authoritative association occurs at runtime dequeue/validation
of the reader-channel packet, before the owner-input queue. The adapter has
already registered accepted PUBLISH metadata before its socket writer can
send. No local Written prerequisite rejects a legitimate fast peer PUBACK.
ObservedPuback carries the full ConnectionKey, PI, reason and immutable
target (delivery, claim operation, frame effect), or explicit None. Exact
occurrence/generation guards prevent a copy or delayed callback affecting a
new occupant. A no-target observation never gains a target retroactively.

A targeted integration review also found and reproduced a takeover race. If
an older Persist blocks FIFO Outbox dispatch, the owner's new session binding
can apply before the old socket's Close effect. The former adapter tried to
derive its PUBACK connection key from the current owner binding and stopped
the entire reactor (puback_connection_unbound). Red b6f5120f/4eef57d1 preserves
that real queued/wire schedule. Fix f100d8ae retains each peer's original,
writer-confirmed SessionId with its non-reused generation; the old observation
stays targetless/stale while the replacement connection and PING progress.
This is distinct from releasing the old delivery's resource number.

The earlier reader queue/socket still contains unassociated bytes. Two
indistinguishable arbitrary wire ACKs first observed after reuse cannot be
assigned an invented historical claim. The scope is protection of facts
already associated at the documented point, not universal wire deduplication.
The normal case keeps MQTT PUBACK order and does not hold an unknown operation
across the long run, which would legitimately stop the receipt window first.

## Finite reception and retained evidence

The versioned launcher is autonomous:

```bash
# From mqttiumd/, with the pinned toolchain and system requirements installed:
tools/run_pi_reception.sh /tmp/mqttiumd-outgoing-pi-reception
```

The long normal case requires at least 65,537 distinct outgoing QoS 1 messages,
public mqttium 1.3.0 publishing and Paho 2.1.0 receiving. A separate bounded TCP
observer records the numbered payload, PI, DUP and actual PUBACK frames. The
driver streams claim/Finish mappings, deduplicates terminal observations by
their identities, verifies connection counts and final durable completion,
and retains periodic SQL/resource windows. A short public-client inversion
checks Paho→broker→mqttium. It neither uses SQL cleanup nor changes quotas or
restarts to release capacity. Receipt collection stays active.

Short cases cover occupied/unknown claims, unknown Finish with same-operation
resolution, reduced-domain exhaustion, independent controls/destinations,
incoming/outgoing namespaces, real queued target/None copies, takeover and
old results/callbacks, including ACK before Written. Separate same-file crash
cases distinguish claim committed before emission, emitted before Finish,
Finish committed before result consumption and a new reused occurrence.
Live claims keep identity/PI/DUP replay behavior; finished/consumed work cannot
resurrect. Each process/signal witness belongs only to its own run.

The root of every receipt contains source identity, clean source archive,
actual binaries/features, public client import identity, effective parameters,
commands, wire/durable facts, process statuses, reached injections and hashes.
The final index and PR #648 receipt bind the final publication and CI to those
newly executed runs; diagnostic outcomes above retain their own source labels.
Updating this narrative never retrospectively changes an archived witness.

Four targeted mutation classes cover early release (finish_unknown_freed_pi),
occupied-claim omission (occupied_unknown_pi_reused), runtime rebinding of an
old associated/targetless fact, and stale frame callback application
(stale_outgoing_frame_result_applied). The first, second and fourth are pure
Owner transition mutants; the fourth asserts an incorrectly changed local
write-history fact, not an invented network Finish/credit failure. The runtime
mutant exercises the real queues. A compilation error, unrelated refusal,
unexplained timeout or missed injection does not count as detection.

The focused models/pi-reuse directory adds 12 expected outcomes to the unchanged
62 M1/gc outcomes. It separates durable commit, owner knowledge, reservations,
raw reader bytes, frozen observations and callbacks, with finite PI resources
and non-wrapping causal identities. It includes two safety cases, five witnesses
and five configs for four mutation classes. Authentic association, SQLite
atomicity and bounded execution are explicit assumptions; no global liveness
or Rust-refinement proof is claimed. The model-only 040df332 archive has SHA256
c87e563121e677126dd84c3872bfd320301cb89fc3ea48bef1c13cc418b301b7.

Public types and bounded product observations/registry/examples change under
Proposed 0028. Contract digest before is 162a9cd3b9e1f5a623ebcba47f75ac852e1c5c02674867dd74496b03a5e6126d;
after is 8884608f0e66e905307a546b6296934099db688ae5b2829d187dc816dda0bb05.
No fixture-oracle qualifying scope is added. Required Rust/oracle/schema,
existing receipt/pubsub/QoS 0/1/2, models/proof and K1/K2b/K3 gates keep their
separate results, exact identities and limits. Final counts/check identities
are recorded by the final receipt/index rather than inherited from 66d2.

## Remaining bounds and next increment

The existing 128-session persistent/infinite-expiry bound remains; absent
sessions are never silently deleted. Physical receipts 8,192, metadata 128,
references 128, active peer windows, two 16 MiB event segments, checked identities,
typed read budgets and storage availability still bound service. This removes
two cumulative ceilings, not every operating limit or general MQTT/QoS 1
delivery duplication. Performance, power loss, migration, outgoing QoS2,
wildcard/retained/Will/authentication remain separate. PR #648 remains draft and
unmerged; Proposed ADRs remain Proposed; archived French chapters are unchanged.

The next useful operational increment is an explicit session lifecycle. Its
first result should release an eligible terminal session and accept a new
ClientID beyond 128 historical creations, while refusing to discard offline
obligations, claims or uncertain operations without their specified protocol.
