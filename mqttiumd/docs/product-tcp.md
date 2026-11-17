# Local MQTT 5 preview with hierarchical pub/sub and durable retained

The product receives MQTT 5 QoS 0/1/2 publications, including durable retained values, and delivers
at QoS 0 or 1 to persistent exact and hierarchical subscriptions. Sessions and filters
are durable. Initially non-retained QoS 0 publications use a bounded volatile path;
QoS 1/2 admissions atomically create durable recipient obligations. This is
an experimental subset, not complete MQTT conformance, a Mosquitto replacement,
or reception of all P1/P2 behavior. Outgoing QoS 2 remains outside it.

Effective delivery QoS is the minimum of the published and granted QoS:

| Published QoS | Granted subscription 0 | Granted subscription 1 |
| - | - | - |
| 0 | Outgoing 0 | Outgoing 0 |
| 1 | Outgoing 0 | Outgoing 1 |
| 2 | Outgoing 0 | Outgoing 1 |

A request for subscription QoS 2 grants QoS 1. Replacing a filter changes
future matching decisions, not the QoS of an existing obligation.

## Downloadable local preview

The PR's `mqttiumd-local-preview-linux-x86_64` artifact contains the normal
standalone archive. Extract it and run `./start.sh ./data`; no laboratory,
Git or compiler is required. The bundled [quick start](../preview/README.md)
starts temperature and archive receivers with the Mosquitto MQTT 5 clients.
Keep the data directory for restart; logs are fresh for every start. The
artifact's BUILD.json and extraction receipt identify its actual platform.

`atelier/+/temperature` selects one level; `atelier/#` includes the parent and
all descendants. Empty levels are significant. Leading wildcards exclude `$`
topics; explicit `$SYS/#` is allowed but creates no metrics service. Matching
preserves UTF-8 bytes and case. A session with several eligible matching
filters gets one delivery at their maximum granted QoS, capped by published
QoS and outgoing QoS 1. No Local is applied separately to each contribution.
UNSUBSCRIBE removes a literal stored filter only. The destination set is frozen
at admission; later replacement or removal never reroutes an old obligation.

The 16-destination limit applies to distinct sessions after aggregation, not
matching filters. Exactly 16 is allowed. A seventeenth refuses the complete
durable admission or explicitly drops the whole volatile occurrence before any
output. Existing per-literal route limit 16 and total subscription limit 128
remain in force. [ADR 0031](decisions/0031-hierarchical-subscriptions.md) records
the decision and versioned canonical options; schema 7 refuses older formats
without modification or implicit migration.

## Launch and external clients

From `mqttiumd/`, with the pinned Rust toolchain:

```bash
cargo build --locked -p mqttiumd
./target/debug/mqttiumd --db /tmp/mqttiumd.sqlite \
  --listen 127.0.0.1:1883 --events /tmp/mqttiumd.events.jsonl
python -m pip install -r tests/system/requirements.txt
```

Database and event paths in these examples must be fresh. The database may be
absent, with an existing parent directory. A failed open never falls back to
creating an empty replacement. Event files are created exclusively so earlier
observations cannot be overwritten. Readiness follows exclusive writer startup,
one durable broker epoch and complete owner recovery, not exhaustion of every
recipient queue. An epoch belongs to the broker lifetime, not each connection.

For initially QoS 0, keep the subscribers online. In two separate terminals,
run these commands and wait for `subscription_ready` in their new event files:

```bash
python tests/system/external_pubsub.py --client mqttium --action receive \
  --port 1883 --client-id demo-zero --filter demo/exact --qos 0 --count 1 \
  --expected-granted-qos 0 --expected-delivery-qos 0 --events /tmp/demo-zero.jsonl
python tests/system/external_pubsub.py --client paho --action receive \
  --port 1883 --client-id demo-one --filter demo/exact --qos 1 --count 1 \
  --expected-granted-qos 1 --expected-delivery-qos 0 --events /tmp/demo-one.jsonl
```

Then publish in another terminal:

```bash
python tests/system/external_pubsub.py --client mqttium --action publish \
  --port 1883 --client-id demo-publisher --topic demo/exact --payload volatile --qos 0
```

Both subscribers receive QoS 0, including the subscriber granted QoS 1.
Neither submits manual PUBACK. A successful QoS 0 publisher receipt means
`local-write-only` completion; it proves neither broker processing nor
subscriber receipt. Each subscriber's message is the positive observation.
QoS 0 has `packet_id: null`; a separately recorded Paho receipt token is
not a wire Packet Identifier.

To try mixed durable fanout, run the subscriber commands again with fresh event
paths. Keep expected delivery QoS 0 for `demo-zero`, change it to 1 for
`demo-one`, then publish with Paho:

```bash
python tests/system/external_pubsub.py --client paho --action publish \
  --port 1883 --client-id demo-durable-publisher --topic demo/exact --payload durable --qos 2
```

One admission creates distinct QoS 0 and QoS 1 obligations. Only the QoS 1
recipient sends PUBACK. `--allow-mixed-delivery-qos` also permits observing
both output QoS values on one connection. Receipts retain each message's actual
QoS and identity. Swap `--client mqttium` and `--client paho` to exchange roles;
the matrix reception exercises both implementations.

For offline durable work, create a QoS 0 subscription and disconnect, then
publish an incoming QoS 1 or 2 message:

```bash
python tests/system/external_pubsub.py --client mqttium --action subscribe \
  --port 1883 --client-id demo-offline --filter demo/offline --qos 0
python tests/system/external_pubsub.py --client paho --action publish \
  --port 1883 --client-id demo-offline-source --topic demo/offline --payload queued --qos 1
python tests/system/external_pubsub.py --client mqttium --action receive \
  --port 1883 --client-id demo-offline --resume-without-subscribe \
  --expected-session-present 1 --expected-delivery-qos 0 --count 1
python tests/system/external_pubsub.py --client mqttium --action unsubscribe \
  --port 1883 --client-id demo-offline --filter demo/offline --expected-session-present 1
```

The same offline example with an initially QoS 0 publisher deliberately loses
that occurrence. It is not retained for a later connection.

The helpers use only public mqttium/Paho APIs and wait for subscription results
and client publication receipts. QoS 1 reception uses actual manual ACK:
`--hold-acks --release-acks PATH` withholds it until that file exists within
`--wait-timeout`. Without a release path it disconnects after the requested
count without acknowledging those QoS 1 messages. QoS 0 never enters this ACK
policy. Sleeping after automatic acknowledgement is not proof of held work.

## Accepted profile

CONNECT requires MQTT 5 and an explicit nonempty UTF-8 ClientID. Both Clean
Start values are supported. Session Expiry Interval may be absent (zero) or any u32 value: zero, finite
1..0xfffffffe, or `0xffffffff` (infinity).
The existing command-line pub/sub helpers request infinity explicitly.

| Request | Existing session | Public result |
| - | - | - |
| Clean Start 0, infinity | Persistent | Resume same SessionId, Session Present 1 |
| Clean Start 0, zero/absent | Persistent | Resume state/SP1, set zero retention; end on subsequent close |
| Clean Start 0 | Absent | New SessionId, Session Present 0 |
| Clean Start 1 | Present or absent | End old incarnation if any; new SessionId, SP0 |
| DISCONNECT, expiry zero | Persistent connected | Explicit durable termination |
| Transport close, infinity | Current binding | Preserve session and outstanding responsibilities |
| Transport close, zero | Current binding | End incarnation and recover capacity |
| Transport close, finite | Current binding | Persist original close observation and fixed deadline |
| Reconnect before finite deadline | Detached current incarnation | Resume state/SP1 and invalidate old deadline |
| Reconnect at/after finite deadline | Detached current incarnation | Atomically retire it and create fresh incarnation/SP0 |

CONNACK success waits for a correlated known commit applied by the owner. Open,
replace and close retain one OperationId through unknown outcomes and resolution;
a lost answer cannot cause a second replacement. The durable attachment token
and transport generation guard later closes independently of reusable ClientID.
A takeover closes the old transport first. An old zero-retention session ends
before Clean Start handling, so Clean Start 0 then creates a new session/SP0.

DISCONNECT without the property preserves the CONNECT policy; explicit infinity
is supported only when CONNECT was nonzero. CONNECT zero followed by any
nonzero DISCONNECT expiry is Protocol Error 0x82. Duplicate properties and
malformed encodings remain protocol/malformed errors. Finite overrides are
supported; they do not reset a deadline when a command result is delayed. DISCONNECT has no MQTT ACK:
local client completion alone does not establish durable termination.

Proposed [ADR 0029](decisions/0029-explicit-session-lifecycle.md) defines the
atomic target-only cancellation and lifecycle authority. Ending a session
cancels its own filters, deliveries, claims and incoming markers. Publications
already owed to other destinations survive their producer's termination.
Cancelled obligations are not counted as successful deliveries. No terminal
session row is retained per historical ClientID; SessionIds never repeat.
Actual broker-epoch opening ends previous zero-retention sessions before owner
recovery/readiness. Persistent sessions remain until explicitly ended; there is
no automatic eviction and the active quota remains 128.

For a short lifecycle demonstration against the running broker:

```bash
python tests/system/external_sessions.py --client mqttium --port 1883 \
  --client-id demo-ephemeral --clean-start 1 --expiry 0 \
  --topic demo/lifecycle --qos 1 --expected-session-present 0
python tests/system/external_sessions.py --client paho --port 1883 \
  --client-id demo-persistent --clean-start 0 --expiry infinite \
  --expected-session-present 0
python tests/system/external_sessions.py --client paho --port 1883 \
  --client-id demo-persistent --clean-start 0 --expiry infinite \
  --disconnect-expiry 0 --expected-session-present 1
```

These helpers use the public APIs. The reception command
`tools/run_session_reception.sh /tmp/mqttiumd-session-reception` additionally
waits for the broker's known lifecycle result and independent durable cleanup.

The profile accepts ordinary and retained QoS 0/1/2 PUBLISH, PUBREL, hierarchical SUBSCRIBE and
UNSUBSCRIBE, recipient PUBACK, PINGREQ and ordinary DISCONNECT. Completed valid
packets reset the 1.5-times-Keep-Alive deadline; fragments do not. Keep Alive 0
disables that MQTT timer, not partial-packet/write deadlines. PINGREQ passes
through the core and produces PINGRESP.

Will, authentication, shared subscriptions,
subscription identifiers, topic aliases, PUBLISH properties and outgoing
QoS 2 are not implemented. No Local, Retain As Published and valid Retain
Handling values are accepted and durably preserved per subscription. No Local
removes only its own matching contribution before aggregation. RAP is combined across eligible live contributions; retained replay always sets
RETAIN1. RH is evaluated per ordered filter occurrence as described below. Subscription properties remain outside the profile.
Malformed options are distinguished from well-formed unsupported options.

Necessary CONNECT properties are validated. Peer Maximum Packet Size constrains
every output. Peer Receive Maximum constrains QoS 1 output in addition to the
local limit; QoS 0 neither consumes nor returns receive-window credit in either
direction. Topic Alias Maximum and Request Problem Information are checked;
no alias or diagnostic string is emitted. CONNACK advertises Receive Maximum 8,
Maximum Packet Size 65,536, wildcard and retained availability, and no subscription-identifier, shared,
wildcard or topic-alias support. Maximum QoS is omitted: that property can
only encode 0 or 1, while this profile accepts incoming QoS 2.

Malformed encodings, invalid protocol/context and valid unsupported features
have distinct stable error/event codes. Complete properties are validated before
classifying an unsupported feature. A refusal attempts a bounded MQTT response
when the peer's limit is known, then closes the unusable connection. Ignoring
a feature never produces a false success.

## Bounds and transport

`contracts/v1/product-profile.json` identifies `tcp-qos0-exact-pubsub-v1`.
Limits are 16 connections, 128 sessions, 128 exact subscriptions, eight filters
per request and 16 targets per publication/topic. Packets are at most 65,536
bytes, ClientID/topic/filter 1,024 bytes and payload 32,768 bytes. Per-connection
input/output queues hold four items; writer queue 32, commands in flight 16,
timers 512 and physical receipts 8,192. Maintenance and the single lifecycle flight share reserved four-slot
writer request/reply lanes. Maintenance has four owner control inputs; closure
has a separate bounded two-facts-per-transport lane. The core remembers
eight closed generations. Known terminal operation timers are removed only
after their corresponding owner transition applies.

There are 128 volatile incoming entries and a separate 128-entry outgoing
metadata table with 4 MiB of loaded payload. Initially QoS 0 fanout has a separate
128-entry/reference and 4 MiB volatile budget, conservatively charged per
recipient reference although immutable payload bytes are shared. A new fanout
that does not fit is dropped as a batch with `capacity_exceeded`. The durable
account has 4 MiB payload, 128 references and 128 exchange slots shared by
incoming QoS 2 and claimed outgoing QoS 1. Each connection has at most eight
active outgoing QoS 1 slots, further bounded by the peer. QoS 0 uses no such slot.

Packet bodies are reserved after canonical bounded Remaining Length. Socket
send buffers request 4,096 bytes; the effective OS value is recorded. Separate
reader/writer tasks serve each connection. SQLite runs on its exclusive worker,
never in the owner reactor. Partial packets have a 5-second deadline, CONNECT
3 seconds, and each entire write an absolute 250 ms budget even if partial
writes progress. Cancellation closes both socket halves. A started frame
completes or its transport closes.

The bounded Outbox retains effects with the same identity until acceptance or
a correlated outcome. QoS 0 output saturation returns a typed refusal to the
owner for an explicit drop; it does not silently erase an effect. Durable
results remain correlated after socket closure. Generations do not repeat,
and the runtime checks its live map before core delivery even after the core's
bounded closed-generation memory evicts an old entry. Late store results may
reconcile knowledge but do not authorize output on a retired connection.

## Filters, ordering and initially QoS 0

SUBSCRIBE grants QoS 0 for a request of 0 and QoS 1 for requests of 1 or 2.
Unsupported shared/wildcard filters are refused per filter, with results in
request order. The durable SessionId/revision owns the subscription. Replacement
of the same session/filter, including 0→1 and 1→0, is atomic without duplicate
or remove/add gap. An unknown mutation retains its OperationId and authorizes
no positive SUBACK.

FIFO submissions and routing revision define the cut. Earlier admissions
precede a mutation. Dependent new admissions, including initially volatile
QoS 0 routing, wait rather than use its stale projection. A known mutation
installs the new projection before SUBACK/UNSUBACK and subsequent admissions.
UNSUBSCRIBE prevents new matching work while preserving obligations already
admitted, including their original effective QoS.

ARC-025's non-retained initially QoS 0 path creates no publication Persist,
durable receipt, claim or AwaitPuback state. Initial session and subscription
writes are separate. PUBLISH has no MQTT identifier, requires DUP 0 and receives
no ACK. Content equality does not merge occurrences; a typed volatile identity
uses the source ConnectionKey and ingress order. The owner reuses the routing
projection, captures destination generations and shares immutable bytes.

Absent subscribers get no retained volatile work. A later connection or restart
cannot replay it. The owner records `qos0_dropped` with origin, target when
applicable, and one of `no_subscribers`, `destination_absent`,
`capacity_exceeded`, `packet_too_large`, `connection_closed`, `output_saturated`
or `write_failed`. An unavailable fanout budget drops the new batch; a closed,
absent, slow/saturated or undersized destination can lose its own delivery while
independent destinations and controls progress. A local or ambiguous write
outcome never grants a new attempt on another connection.

## Durable obligations delivered at QoS 0

QoS 1/2 admissions atomically include all recipient obligations, also those
whose effective QoS is 0. Publisher PUBACK/PUBREC success depends on known
admission, not recipient completion. In this bounded profile an absent
recipient's QoS 0 obligation may wait durably for its next connection. This
choice differs from initially QoS 0 traffic and is not a general MQTT mandate.

The owner loads payload into bounded RAM, captures the current ConnectionKey,
and submits conditional, canonical, idempotent ConsumeDelivery. Only known
commit of obligation/reference removal permits one volatile EmitQos0 attempt
on that captured generation. RAM retains the bytes after durable release.
No outgoing Packet Identifier, QoS 1 claim/window or AwaitPuback is introduced.

| Interruption | Result for the same obligation |
| - | - |
| Before consumption commits | Work stays durably discoverable. |
| After consumption, before emission | Loss is allowed; recovery does not reconstruct an emission. |
| After PUBLISH was emitted | No resurrection or retransmission of the consumed obligation. |
| Consumption result lost | No emission while unknown; resolve/fence the original OperationId, then at most one attempt if the original process/connection still permits it. |

Close/takeover invalidates the old permission instead of moving it to a new
generation. Duplicate results, callbacks, wakeups and old pages do not create
a second permission. A historical consumption receipt is command-replay
evidence, not startup emission work. A lawful new incoming QoS 1 admission can
create another obligation with identical bytes; that is a different identity.
Consumption releases its resources once and collects payload after the final
reference, independently of command-result receipt retirement.

## Incoming and outgoing QoS 1/2 guarantees

A positive incoming PUBACK/PUBREC requires known atomic admission. Certain
refusal may use Quota Exceeded (0x97) or Implementation Specific Error (0x83);
unknown outcome allows neither success nor certain rejection and keeps the
OperationId for resolution. Incoming QoS 1 has no durable AwaitPubrel marker,
no durable exchange charge and no ReleaseInbound; its admission/pending PUBACK
still holds a bounded volatile lease. Unreferenced publications are collected.

After PUBACK is sent another PUBLISH with the same identifier is a new
occurrence, even with DUP or equal bytes (MQTT-4.3.2-5). Lost PUBACK/restart can
therefore create another admission. Before knowledge of admission, internal
replay keeps its operation identity. Retained ACK effects freeze ingress order;
old receipts cannot finish a new occurrence or release its credit. Completion
also checks QoS/packet kind: PUBACK for 1, PUBCOMP or negative PUBREC for 2.

Receive Maximum uses actual bounded terminal-ACK wire progress rather than
waiting for ordinary Written callbacks. Connection/effect/occurrence/QoS guards
remain. The write-to-progress publication gap can retain one decoded PUBLISH
for at most 500 ms while a matching accepted terminal ACK is pending. Submission
alone returns no credit. A genuine ninth unfinished exchange remains refused.

Outgoing QoS 1 progresses Queued → ClaimPending → AwaitPuback → FinishPending
→ Terminal. Known ClaimDelivery commit associates DeliveryId, target/revision,
PI and claim operation before PUBLISH eligibility. Claim identity also guards
the slot generation. Fresh known claims emit DUP 0; recovered claims retain PI
and identity and conservatively use DUP 1. Intent, local write and peer ACK
are distinct, and no live-connection retransmission timer is added.

A valid recipient PUBACK, including a negative reason, starts conditional
idempotent FinishDelivery. Only known finish commit frees its window/slot and
reference, collecting the final payload. Unknown outcomes retain their operation.
Peer packet limits apply before output; oversized QoS 1 work closes that
connection but remains durable for a suitable reconnect. A full outgoing window
leaves ACK, PING, closure and results runnable. Each destination owns its window.

Incoming and outgoing PI spaces are independent. The client's active PUBLISH,
SUBSCRIBE and UNSUBSCRIBE share its sender space; active collisions are errors.
Server PIs are recyclable resources: all live claims reserve their number,
including unknown Claim/Finish outcomes and recovered claims before payload
loading. Known durable Finish application makes the number available to a new
claim. A reconnect restores outstanding claims with their original PIs and
fences old generation callbacks/observations. Causal identities never wrap.

## Recovery, discovery and formats

Recovery restores sessions, granted subscriptions, obligations and claim
metadata before readiness. Typed work pages load at most eight items in durable
DeliveryId order for this one-owner FIFO product. Useful pages, results,
readiness and available credits rearm a bounded deduplicated fair continuation;
ready backlog does not wait for a 100 ms tick per message. Empty or blocked
work is not rearmed unconditionally. The periodic 100 ms scan remains a
lost-notification fallback and creates no live MQTT retransmission.

Correlations/stamps reject stale pages after finish, consumption or takeover.
Readers validate durable domains before QoS-specific selection, so an invalid
obligation cannot appear as absent work. Artificial SQL changes after readiness
are corruption controls, not supported product operations; typed failures stop
the runtime safely. Payload reads obey separate byte budgets.

Canonical format 1 adds kind 6 ConsumeDelivery, kind 7 UpdateSubscriptions and
kind 8 SessionLifecycle.
The latter encodes granted QoS in its decision. Existing kinds 1–5 preserve
their bytes and meanings; kind 3 remains implicit QoS 1 rather than silently
being reinterpreted. Proposed [ADR 0026](decisions/0026-qos0-volatile-and-consumed.md)
specifies the layouts and separate volatile/consumed cycles.

SQLite schema **6** stores compact receipt guards, retention/attachment, durable
detachment/deadline and one temporal anchor. Schemas 1/2/3/4/5 and other
incompatible versions are refused with typed InvalidData before write-affecting
pragmas. There is no implicit migration, checkpoint, reset or empty replacement.
Fresh and existing schema 6 files are supported; older-format files remain
historical inputs until a separate migration is supplied. Invalid/over-budget current files also refuse
readiness without resetting partial recovery.

Publication metadata is collected after its final marker/obligation reference;
a zero-target admission does not archive payload. Live command-result retirement
removes the cumulative receipt ceiling described below. Outgoing QoS 1 numbers
are recycled after known durable Finish application; session capacity, active
claims, checked causal identities and storage budgets remain finite.

SIGINT/SIGTERM closes sockets and drains accepted writer work before releasing
the writer. Accepted commands are not called certainly rejected. Shutdown has
a finite deadline; a stuck storage operation reports incomplete shutdown with
unknown outcome. SIGKILL has no graceful drain and depends on SQLite recovery.
Normal shutdown gives zero-policy session ends two seconds to complete while
preserving persistent state. An unresolved end is reported as incomplete;
the next actual epoch opening still finalizes previous zero-policy sessions.

## Live receipt retirement

Proposed [ADR 0027](decisions/0027-live-receipt-retirement.md) separates result
consumption from completion of the responsibility a command created. Only an
applied, correlated terminal store result whose consequences are retained by
the pure owner can close its sequence position. Unknown outcomes, disconnection,
timeouts and history eviction cannot. An offline obligation, an active claim
or an incoming QoS 2 marker can outlast the full admission/claim receipt.

Each origin has at most 1024 issued but not durably confirmed positions. New
business stops at span 512, leaving 512 positions for inbound, outgoing and
session-termination continuations (128 each). Ordinary
business cannot spend the final 512 of the unchanged 8192 physical receipt
slots. Resolution uses its original operation identity. Maintenance itself
creates neither an OperationId nor another receipt. Its reserved paths can
progress when ordinary result/input queues or network output are blocked.

The owner closes only a contiguous consumed prefix, with the PublicationSeq
and PayloadSeq issuance boundaries at that exact position. At most 128 new
positions and 128 physical receipt/fence rows are handled per pass. Prefix
validation has a separate 2 MiB aggregate/1 MiB field budget, sufficient for
128 maximum-profile subscription recipes; ordinary work/recovery pages keep
their original 1 MiB budget. A known absolute closure
returns issuance capacity once; repeated/lost replies never grant credits twice.
The usual trigger is 64 consumed results. Hole-blocked or empty producers do
not spin. A deferred fenced Release can consume its terminal result at a full
window and prepare its new attempt only after collection makes room.

SQLite atomically installs all three monotone guards, deletes bounded receipt/
fence rows and updates actual receipt accounting. Old OperationIds return
RetiredOutcomeUnavailable, including with different recipe bytes: their detail
may be unavailable, but they cannot execute again. Retained nonretired receipts
still compare canonical bytes. New operations cannot reuse old publication or
payload identities after the business rows and original receipt have vanished.
Live business rows keep their own identities/guards and are not deleted by GC.

There are at most 64 compact origin rows. On startup, after complete validated
recovery, 32 retained origins or 4096 receipts trigger bounded sweeping of closed
epochs before readiness. This does not rotate an active owner's epoch. Initial
opt-in compatibility uses the existing snapshot read budget; normal product
origins register empty before accepting business. Legacy epoch-zero fixtures
remain outside this live-origin protocol. No incompatible database is reset.

The optional product event log retains at most a 16 MiB active segment and one
16 MiB `.previous` segment, with continuous sequence numbers and explicit
rotation. A receiver requiring a complete long-run history must drain it during
the run; bounded retention is not a permanent archive. Actual writer SQLite
settings and receipt policy are emitted before readiness. WAL measurements in
the receipt reception use the normal build and its normal autocheckpoint.

## System reception

```bash
tools/run_tcp_reception.sh /tmp/mqttiumd-tcp-reception
tools/run_qos1_reception.sh /tmp/mqttiumd-qos1-reception
tools/run_pubsub_reception.sh /tmp/mqttiumd-pubsub-reception
tools/run_qos0_reception.sh /tmp/mqttiumd-qos0-reception
tools/run_receipt_reception.sh /tmp/mqttiumd-receipt-reception
```

mqttium 1.3.0 is primary through public APIs; Paho 2.1.0 is a second
implementation. Receipts retain imported identity/version/hash, executed binary
hashes/features, commands, wire/message observations, durable facts, reached
injection points and process statuses. A system witness belongs to its own run.
Hooks require `--features mqttiumd/test-hooks` and are absent by default; the
SQLite feature is recorded independently. Holds are acknowledged with synced
events, and the supervisor releases or kills that same process.

The QoS 0 receipt observes the six matrix cells with roles reversed and an
independent frame parser. It checks no-commit pure traffic, drops/budgets,
consume barriers/lost results, three crash positions, mixed fanout, filter
replacement and credit separation. Three negative controls target emission
before consumption, repeated output of a consumed obligation and wrong QoS 1
accounting. Ready-backlog and post-readiness invalid-QoS regressions, including
lost notifications, have their own retained observations. The index and case
verdicts determine what actually ran; publisher QoS 0 completion is not the
positive broker/destination observation.

Earlier receipts retain incoming QoS 1/2, claim/finish, stale-generation/receipt
and no-premature-ACK controls. The QoS 1 proxy independently observes PUBACK
withheld from a living mqttium client with its original reconnecting receipt.
Critical crash/collision schedules use raw TCP; manual ACK behavior is checked
on the wire. Quota/corruption interventions use isolated copies or test runs and
are labeled rather than presented as ordinary product configuration changes.

These are finite system tests. Existing K1/K2b/K3 qualifications and proof/model
limits stay separate; no new path automatically qualifies all P1/P2. ADRs
0024/0025/0026/0027/0028 retain Proposed status and guard partitions stay incomplete.
Legacy epoch-zero/session collection, indefinite operation, outgoing QoS 2,
shared delivery, Will/authentication, deployment
security and performance remain separate. Process SIGKILL is not a power-loss
or universal storage guarantee.

## Recyclable outgoing QoS 1 identifiers

Proposed [ADR 0028](decisions/0028-outgoing-packet-identifier-reuse.md) removes
the cumulative 65535-delivery connection limit. The owner selects the smallest
free PI in 1..65535 for the target session. Selection consults all live claims,
including pending/unknown Claim, pending/unknown Finish and recovered metadata
before payload loading. It tries at most the live claim count plus one
candidates (129 with this product's unchanged 128-entry metadata budget).
There is no per-historically-sent-message allocation table or wrapping causal
counter. QoS 0 does not reserve, release or await an acknowledgement for a PI.

A known Finish commit must be applied by the owner before its PI can be
reused. A writer submission, timeout, PUBACK alone, local Written callback or
physical receipt deletion is insufficient. The next occupant has another
DeliveryId/claim OperationId/frame EffectId and its initial PUBLISH has DUP 0.
An interrupted live claim instead recovers with its original PI/claim and
conservative DUP 1. There is no periodic in-session retransmission. Valid
negative PUBACK reasons keep the existing durable terminal behavior.

The TCP adapter freezes a PUBACK's full occurrence target, or explicit absence
of target, after dequeuing and decoding the reader-channel packet and before
enqueueing the owner input. Accepted emission metadata is registered before
the socket writer can send the frame, so a legitimate early PUBACK does not
wait for the local Written callback. Already associated observations, store
results, write callbacks and connection generations cannot migrate to a later
occupant of the same resource number. Bytes still in the earlier reader
channel/socket are outside this observation boundary. Arbitrary identical
wire ACKs first observed after reuse carry no internal claim identity; the
broker cannot promise to distinguish them as old packets.

The autonomous finite reception uses normal mqttium/Paho public clients and a
separate bounded TCP observer, requiring at least 65537 numbered outgoing QoS 1
receives and known durable terminations on one subscriber connection, process
and file, with active receipt collection and unchanged quotas:

```bash
tools/run_pi_reception.sh /tmp/mqttiumd-outgoing-pi-reception
```

Its index identifies the exact source/binaries, client modules, connection
counts, wire PI/DUP/ACKs, new claim identities, distinct Finish facts, final
SQL state, resource windows, separate crashes and precise mutation reasons.
See [report 0012](reports/0012-outgoing-pi-reuse.md). This is system reception,
not an extension of the fixture oracle's qualifying scope. The existing
receipt, QoS 0/1/2 and pub/sub receptions remain separate regression gates.

The simultaneous session limit remains128. Explicit termination and finite
expiry recover eligible capacity; valid offline infinite sessions are preserved.
No automatic eviction or unlimited-service claim follows from these mechanisms.


## Finite session expiry

Finite retention starts at the first observed transport close, never while the
connection is active. The original policy, observation and attachment survive
pending Open and unknown Detach results. A known committed detachment installs
one fixed logical deadline. Reconnection is ordered with conditional expiration:
Open itself checks whether the old offline deadline is due; a delayed timer
cannot cause an expired session to resume. A valid earlier resume invalidates
the old detachment. All cancellations use target-only cleanup, preserving work
already owed to other destinations.

```bash
# Resume/create a session, keep it for five seconds after close:
python tests/system/external_sessions.py --client mqttium --port 1883 \
  --client-id demo-finite --clean-start 0 --expiry 5 --expected-session-present 0
# Paho exposes the supported DISCONNECT expiry override through public APIs:
python tests/system/external_sessions.py --client paho --port 1883 \
  --client-id demo-override --clean-start 0 --expiry infinite \
  --disconnect-expiry 2 --expected-session-present 0
tools/run_expiry_reception.sh /tmp/mqttiumd-expiry-reception
```

The mqttium helper uses only public APIs; it does not expose DISCONNECT expiry
properties, so Paho and the independent TCP probe exercise those overrides.
Local DISCONNECT completion is not proof of durable detachment or deletion.

Safe Linux CLOCK_BOOTTIME supplies checked nanoseconds outside the core.
Continuity requires the same boot UUID and proven effective BOOTTIME namespace
origin; boot UUID alone is insufficient. Missing/unverified namespace evidence
uses a fresh process domain, reported in `session_clock_anchor`. Same-domain
process restarts count elapsed BOOTTIME. Changed/unproven domains preserve
committed progress but count no unknown downtime, so expiry may be delayed.
No UTC jump authorizes deletion. A session already durably detached never starts
its TTL again. A finite connection lost in a crash gets one persisted initial
detachment at the first reliable recovery observation.

A singleton anchor coalesces temporal commits and checkpoints otherwise-idle
finite offline state every30s. The target includes actual scheduling/commit
delay; an observed age beyond31s emits `session_clock_degraded` rather than
claiming the target still holds. At most one checkpoint is in flight and at
most128 keyed timers/facts exist, with four expiration attempts per reactor
turn. No historical timer heap, per-tick receipt or terminal-session row grows.
Unknown lifecycle commands retain their OperationId. A known certain failure
of Detach/Expire stops with `session_detach_rejected`/`session_expire_rejected`,
so unavailable continuation is observable instead of silently losing expiry.

See proposed [0030](decisions/0030-finite-session-expiry.md),
[the continuation journal](reception/finite-expiry-journal.md), and
[the finite model](../models/session-expiry/README.md). Expiry reception is
system evidence, separate from the fixture oracle's K1/K2b/K3 qualification.
Message Expiry, Will and general migration remain separate increments.


## Durable retained values (Proposed0032)

The normal preview now stores up to32 exact retained topics and1MiB of current
retained payload. Each topic owns one reference independently of subscribers
and producer session lifetime. It records the original producer ClientID for
No Local. The existing4MiB payload and128 reference quotas also apply; old
versions still owed to recipients remain charged. Replacing or clearing a value
releases only the retained-owned reference. Empty RETAIN1 clears and remains a
normal live publication; RETAIN0 does not mutate the stored value.

QoS1/2 retained changes, complete live fanout, reference/account updates and
receipt are one admission transaction. QoS2 changes at admission before PUBREC;
PUBREL only releases its inbound marker. Retained QoS0 uses its own durable
operation without a Packet Identifier, acknowledgement, inbound slot or Receive
Maximum credit. Ordinary QoS0 remains volatile. Client QoS0 completion alone is
not evidence of broker commit; observe the value at another client or after
restart. Unknown commands keep their exact operation and recipe; historical
lookup precedes execution and collection preserves non-execution frontiers.

A subscription update transaction considers up to8ordered filter occurrences
against32retained entries. RH0 always requests replay; RH1 only when that literal
filter is absent at that step; RH2 never. Repeated filters are evaluated in order,
including intermediate effects. The final route keeps the last options. Each
eligible occurrence/topic creates a distinct replay: overlapping requested
filters can intentionally create several replays, unlike live one/session
aggregation. No Local excludes the stored producer ClientID's contribution.
Replay always carries RETAIN1; live delivery uses the OR of RAP on eligible
contributions and the incoming RETAIN flag.

Replay materialization freezes the publication version, topic, shared immutable
payload, effective QoS, flag and a fresh causal delivery identity. Up to256
publication identities are reserved before submission; unused identities are
burned and covered by receipt closure. Existing reference capacity can refuse a
whole update with negative SUBACKs; it cannot announce success and truncate the
replay. Thirty-two topics to one subscriber are supported:16 is the per-live-
publication destination bound, not a retained inventory bound. Existing paged
work discovery and claim/Finish or consume-before-QoS0-emission drain the work;
notifications remain an optimization. A later replace/clear does not rewrite it.

Schema8 adds the retained map, replay version and frozen delivery RETAIN flag.
Historical SQLite schemas are refused before modification; no migration/reset
is provided. New canonical tags10/11/12 preserve tags1–9 byte meanings. Readers
validate bounded inventory, producer fields, QoS, causes, flags and version
relationships; physical quick_check alone is insufficient.

Run `tools/run_retained_reception.sh /tmp/retained-reception` for source-pinned
normal/public-client tests, real writer barriers, same-file SIGKILLs, authentic
lost results, compiled negative controls and a standalone extracted-archive
demo. These are product system tests, separate from the existing qualifying
K1/K2b/K3 fixture scope. The package's `demo.sh` executes the short Mosquitto
publish-before-dashboard, restart, replace and empty-retained-clear guide.
Message Expiry, Will, TLS/authentication, shared subscriptions, Subscription
Identifiers, outgoing QoS2, MQTT3.1.1 and general migration remain unsupported.
