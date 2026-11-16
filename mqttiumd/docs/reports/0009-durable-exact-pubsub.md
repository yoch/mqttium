# Durable exact-topic pub/sub product

The TCP product connects real mqttium and Paho clients: incoming QoS 1/2
publications create atomic recipient obligations and leave as QoS 1 PUBLISH.
Exact subscriptions are durably created, replaced and removed over MQTT.
Offline subscribers resume their persistent session without SUBSCRIBE and
discover queued work, including after broker restart. The publisher's positive
acknowledgement waits for admission, not for recipients.

This is finite **system reception**, not full MQTT conformance, completion of
P1/P2 or an extension of the fixture oracle's qualifying assertions. The
existing K1/K2b/K3 qualifier remains separate. PR 648 stays draft and unmerged;
ADRs 0024 and 0025, like historical proposed decisions, remain Proposed.

## Source and executable evidence

The connector confirmed starting head
`bd579da64fbc6904efbba95608142fdcf3fbf191`, main
`874f298b4de7c3a13006cb1ff767dfb09e4031ac` and the draft PR. The first
published increment is the Receive Maximum fix
`fe90df3ac6c58820d06a48ecd3a196bd4ac187bc`; its four changed blobs were read
back and exactly matched Git. The subsequent integrated source is identified
by each receipt's `source_head`, not by relabeling the preliminary runs.

Run from `mqttiumd/`, with the pinned toolchain and system requirements:

```bash
tools/run_pubsub_reception.sh /tmp/mqttiumd-pubsub-reception
tools/run_tcp_reception.sh /tmp/mqttiumd-tcp-regression
tools/run_qos1_reception.sh /tmp/mqttiumd-qos1-regression
tools/run_transactional_reception.sh /tmp/mqttiumd-transactions 1
```

The pub/sub command requires a clean checkout. It retains the full repository
source archive, imported client identity (mqttium 1.3.0 from that source and
Paho 2.1.0), features, executed binary hashes, commands, each process status,
useful wire bytes and independent read-only SQLite observations. `index.json`
links the normal and instrumented receipts and three mutants. `SHA256SUMS`
and the adjacent `.tar.gz.sha256` identify all artifacts. A strace witnesses
only the run it actually traces; compilation is outside that trace.

Simple broker, publisher, two-subscriber and unsubscribe commands are in
[the maintained product guide](../product-tcp.md). The new CI job runs the
same command and uploads `mqttiumd-pubsub-product-<HEAD>`; the final published
SHA, check conclusions and archive hashes belong in the PR receipt. A failed
build or unrelated tool error is never a mutation detection.

## Receive Maximum correction

Seven held QoS 2 exchanges plus sequential fresh QoS 1 identifiers exposed
the old accounting race: bytes of PUBACK were visible to the peer before
the reactor collected Written, so the next valid PUBLISH falsely hit 0x93.
The red reactor regression precedes the fix. A bounded per-connection wire
progress lane releases the exact occurrence's credit after successful local
write. The ordinary callback retains its connection/effect/occurrence/QoS
guards and cannot free a newer exchange.

The additional preemption gap between actual write and publication of that
progress retains one decoded PUBLISH for at most 500 ms while a matching
terminal ACK is pending. Queuing an ACK alone frees nothing. A genuine ninth
held exchange still receives 0x93. Tests cover both deterministic reactor
positions and 32 fresh identifiers under seven held QoS 2 exchanges on the
normal binary. Existing C-late-receipt, C-late-same-qos and stale-receipt
mutation gates remain required.

## Durable operations and recovery

Canonical recipes gain ChangeSubscriptions, ClaimDelivery and FinishDelivery.
The first batch upserts/removes accepted exact filters atomically, preserves
request result order and installs the routing revision before SUBACK/UNSUBACK
or dependent admissions. Deferred business inputs retain their connection
order; a later unsubscribe cannot overtake a held subscribe. Reusing an
active sender Packet Identifier, even with identical control bytes, closes
the connection without discarding the already submitted durable command.

Outgoing state is Queued → ClaimPending → AwaitPuback → FinishPending →
Terminal. The durable claim stores DeliveryId, target/revision, outgoing PI
and original OperationId as its slot generation before emission. Only known
claim commit permits PUBLISH. A recipient PUBACK, including a valid negative
reason, conditionally finishes that claim. Only known finish commit releases
window/slot/reference/payload. A local write receipt proves no peer receipt.
An earlier unresolved claim cannot be overtaken at its destination.

Lost results resolve/fence with their original OperationId. Recovery restores
all obligation/claim metadata; bounded eight-item work reads fetch payloads
online. Independent 100 ms discovery finds durable work without notifications
and causes no periodic live-connection retransmission. Old pages, generations,
results and callbacks cannot resurrect or finish another occurrence. Claimed
replay retains PI and identity with DUP 1. QoS 1 incoming repeats remain
distinct from replay of an existing outgoing obligation.

SQLite schema 2 adds outgoing claims and bounded fact bytes. Schema 1 is
refused with InvalidData before write-affecting pragmas, preserving main and
uncheckpointed WAL bytes. There is no silent reset or migration. Both stores
share conformance, atomic capacity/refusal, same-operation uncertainty,
identity/reuse and last-reference collection checks. Typed bounded read
causes remain distinct; orphaned claims cannot masquerade as an empty page.

## Finite system cases

The receipt runs nine normal cases and fifteen instrumented cases. Its case
records are the evidence of execution and exact binary/source identities.
Preliminary integration runs passed all of these before the clean receipt.

| Group | Required observed behavior |
| - | - |
| A | mqttium publishes to Paho, then roles reverse; incoming 1/2 becomes outgoing 1; neighboring topics do not match. |
| B | Durable SUBACK, offline publication, same-file restart, Session Present and delivery without new SUBSCRIBE. |
| C | Two obligations admitted before publisher success; one actual manual PUBACK held, other destination and PING progress. |
| D | Actual SQLite prepared barriers before claim and finish; no early emission/free; separately dropped committed claim/finish results resolve under the original operation. |
| E | Actual PUBLISH observed, PUBACK held, signal 9 and same-file recovery; same DeliveryId, claim and PI, appropriate DUP, no new obligation. |
| F | Peer Receive Maximum, negative PUBACK, both-direction same integer, same-sender collisions, takeover; small peer packet limit preserves work for a capable resume; original receive-window regression. |
| G | Replacement without duplicate, durable removal blocking new work while preserving old obligations, routing/admission cut. |
| H | Finished destination absent after crash while another resumes, last-reference release, idle discovery with notifications suppressed, atomic two-destination capacity refusal. |

The three deliberate product mutations must fail exactly as
`premature_outgoing_publish`, `premature_delivery_release` and
`durable_work_lost_without_notification`. The finish control observes either
premature Claim2 submission or applied finish knowledge while the actual SQL
commit is held; it does not mistake a blocked single writer for safe volatile
accounting. The discovery control retains the dropped notification, unchanged
durable obligation, live process and successful control traffic.

Integration also found two discriminating regressions: an adjacent DISCONNECT
discarded a decoded PUBACK before its generation was applied, and ResultSlots
deferral reordered later filter mutations. Their tests first failed for the
missing finish and wrong final route respectively. Applying ordered packets
before close and retaining deferred business order correct these cases.
Canonical subscription equality also ignores container capacity, as admission
equality already did; incomplete/trailing encodings remain refused.

The first published integration at `7411400c` passed the product, models and
proof jobs, but the historical M3 mutation did not compile: its injected
recipe match omitted kinds 3/4/5. That compiler error is not a semantic
detection. The patch now explicitly excludes those non-admission recipes;
its premature PUBREC mutation and required assertion/reason are unchanged.
The repaired loop must execute on both stores before final CI reception.

## Contracts and remaining scope

Contract digest changes from
`sha256:a0b4fb2cab5c1eb001083aa897e6c03875489813e19f99da65c550784f966c42`
to `sha256:d789be0eb72021c4c77b60132c097c558b81cc72bd5899fdea74a5fb26e42193`.
Examples, two semantic readers, canonical descriptors, product profile and a
separate incomplete product transition registry evolve together. The oracle
refuses qualifying claims about unexamined new operations/outgoing facts;
corroborated queued metadata alone preserves existing S0 qualification.
The proof BODY files and immutable French archive are unchanged.

The profile remains MQTT 5, explicit ClientID, Clean Start 0, infinite expiry,
non-retained publications, no Will/authentication. QoS 0, outgoing QoS 2,
wildcard/shared filters, retained replay and finite expiry remain open.
Bounds include 128 sessions/subscriptions, 16 targets, eight filters/request,
eight outgoing slots further constrained by the peer, 128 references and
128 shared durable exchange slots. Separate outgoing volatile metadata/payload
budgets are 128 entries/4 MiB. Receipts/epochs still bound service life.
Outgoing PI exhaustion requires reconnect; schema-1 migration, general
multiowner ordering and performance/conformance/power-loss reception remain
separate work. The next useful increment is QoS 0 delivery/admission with its
explicit non-acknowledged lifecycle and mixed-subscription tests.
