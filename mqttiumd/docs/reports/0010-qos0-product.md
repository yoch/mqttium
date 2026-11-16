# Volatile and consumed QoS 0 product

Real mqttium and Paho clients now publish/receive the complete incoming
QoS 0/1/2 × granted QoS 0/1 matrix. QoS 2 subscription requests still grant 1.
Independent TCP observations verify QoS 0 has DUP 0 and no Packet Identifier.
The product remains a bounded MQTT 5 exact-topic subset; this reception is
system-test-only, not MQTT certification or full P1/P2 qualification.

## Two initial corrections

The confirmed base is PR 648 head
673302b4398d408058234ece20906f74880dbec7, main
874f298b4de7c3a13006cb1ff767dfb09e4031ac. The connector confirmed draft,
unmerged status, green checks and no review threads. The attached review
archive supplies observations, not additional authority or qualification.

First, ready work used to wait for each 100 ms scan. A committed red reactor
test reproduces that dependence. The owner now exposes bounded eligibility;
the reactor coalesces FIFO continuations after useful page/result/credit/bind
progress. Empty, window-blocked and uncertain destinations do not busy-loop.
An invalidated page keeps one rescan requirement, avoiding the message-8/9
stall. A deterministic test drains twelve offline messages across pages with
no discovery ticks, verifies idle/full-window stability and independent PING
and destination progress. The scan remains the lost-notification fallback;
the notifications-only mutation remains detected for its original reason.

The small normal-binary witness, same finite parameters as review, observed
1,352.27 ms before and 40.88 ms after the first correction. These are separate
runs, not a universal throughput result. The final wrapper repeats the witness
on its own retained normal binary and records all intervals and finish time.

Second, SQLite selected qos=1 before validating unclaimed obligations. The
red test reads an artificial qos=99 row as empty. Bounded target preflight
now validates QoS/cause/phase before QoS or cursor selection. Valid QoS 0/2
are distinguished from invalid values. The live control mutates a disposable
database after readiness, keeps quick_check=ok, and requires InvalidData/Work,
termination and writer release. Restart still refuses InvalidData/Snapshot.
Artificial corruption is not a supported product operation.

## Separate lifecycles

Initially QoS 0 uses a typed volatile occurrence (source ConnectionKey and
ingress order), immutable shared payload, bounded fanout and continuations.
It creates no publication Persist/receipt, MQTT identifier, receive credit,
claim, AwaitPuback or reconnect replay. Twelve identical-content messages
are twelve distinct occurrences. Prepared sessions/subscriptions are accounted
separately: client receipt completion alone proves only local progress;
positive reception observes actual destination messages and unchanged SQL.

Absent destinations are dropped for this path. Capacity, peer packet-size,
output saturation, closed transport and write failure produce explicit owner
loss decisions. The adapter accepts each attempt once and reports exact
terminal progress/refusal; it never silently discards accepted effects or
retries an ambiguous frame on another connection. Partial frames finish or
close their transport. A test-first integration correction retires pending
write receipts on close, including QoS 0 whose Written callback was uncollected.

Incoming QoS 1/2 still atomically admits all recipients with effective QoS
min(published, granted), before positive PUBACK/PUBREC. Its QoS 0 obligations
may wait durably for an absent subscriber, a deliberate profile distinction.
They load bounded bytes into RAM, then ConsumeDelivery conditionally removes
the obligation/reference before authorizing one volatile attempt on the saved
generation. No outgoing PI, QoS 1 claim or ACK window is involved. Last-reference
collection remains exact while RAM owns bytes required for the attempt.

Before consumption, crash leaves work pending. After consumption but before
emission, loss is allowed. After an observed emission, the consumed obligation
never reappears. Unknown consumption retains OperationId and resolves/fences;
no emission occurs while uncertain, and a late result cannot authorize a new
generation. Historical receipts never rebuild an emission after restart.
Repeated results/pages/callbacks do not repeat permission. Tests first exposed
and corrected duplicate terminal history and a late refusal closing a newer
takeover generation.

## Executed finite reception and reproduction

The diagnostic integration runs passed eight normal and sixteen instrumented
cases, including twelve public-client matrix cells, exact raw-wire verification,
no-commit traffic, absent/slow/saturated/undersized recipients, actual SQLite
barriers, lost and duplicated results, three crash positions, takeover, mixed
atomic capacity, both grant replacement directions, restored grants, routing
cuts and shared-window non-regression. Existing pub/sub, QoS 1/2, typed-read,
generation and stale-receipt suites remain required.

The clean final command records its own exact source/binary/client identities,
features, commands, actual injection points, wire/durable observations and
process statuses. It includes the ready-backlog witness and live corruption
control. A strace belongs only to the run it actually observes; compilation is
outside it. Archives have SHA256SUMS and adjacent tar.gz.sha256 files. Final
publication/check identities and hashes are kept in the PR receipt and indexes,
not assigned retrospectively to preliminary runs.

```bash
# From mqttiumd/, pinned toolchain and tests/system/requirements.txt installed:
tools/run_qos0_reception.sh /tmp/mqttiumd-qos0-reception
tools/run_pubsub_reception.sh /tmp/mqttiumd-pubsub-regression
tools/run_qos1_reception.sh /tmp/mqttiumd-qos1-regression
tools/run_tcp_reception.sh /tmp/mqttiumd-qos2-regression
tools/run_transactional_reception.sh /tmp/mqttiumd-transaction-regression 1
```

Three compiled negative controls must fail for premature_qos0_publish,
consumed_qos0_reemitted and qos0_entered_qos1_accounting. The last charges
an actual wrong receive-window path while capacity is available in case B;
the positive H case independently verifies QoS 0 under eight held QoS 2
exchanges. A timeout, missed injection or compilation error is not detection.
The older M3 patch explicitly excludes new recipe variants and retains its
premature PUBREC assertion/reason.

## Formats, limits and next increment

Proposed ADR 0026 preserves ARC-025. Kind 3 retains legacy implicit grant 1;
new canonical kind 7 carries remove/grant0/grant1. Kind 6 consumes a delivery
without PI/claim; DeliveryConsumed stores only delivery and target. Work and
recovery facts carry QoS. SQLite schema 3 refuses schema 1/2 before writable
opening; no reset or implicit migration. Both stores share atomic resource,
identity, uncertainty and replacement checks. Old obligations keep admitted
QoS after 0→1 or 1→0 replacement/removal.

Contract digest changes from
sha256:d789be0eb72021c4c77b60132c097c558b81cc72bd5899fdea74a5fb26e42193
to sha256:6d1a0045c03e2d85f8bd89bc2759e4f6974a226e8184a3d2bc95ae0a6c3042c7.
Corpus/readers/examples and incomplete
product transitions agree. New QoS 0 facts, recipes and snapshots explicitly
refuse fixture qualification; assertions and R1 recomputation are preserved.
Existing K1/K2b/K3 qualify their original S0 scope, not these new paths.

Separate volatile fanout bounds are 128 entries/references and 4 MiB charged
conservatively per reference. Other queue/session/receipt/window limits remain
explicit in product-profile.json. Receipt/epoch collection still limits service
life. QoS 2 outgoing, wildcard/shared subscriptions, retained, Will, auth and
finite expiry remain open, as do schema migration and general multiowner order.
No performance, power-loss or full conformance claim is made. Proposed ADRs
stay Proposed; PR 648 stays draft/unmerged. The next useful increment is
wildcard matching with bounded subscription indexing and exact/wildcard fanout.
