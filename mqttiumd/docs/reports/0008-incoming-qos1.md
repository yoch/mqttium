# 0008 — Durable incoming QoS 1 on the TCP product

The existing product now receives non-retained MQTT 5 QoS 1 and QoS 2.
QoS 1 uses the same pure owner, bounded Outbox, resolution/fencing path and
dedicated SQLite writer. There is no parallel admission machine in the network
adapter. mqttium 1.3.0, imported from the repository through its public API,
is the primary reception client; Paho 2.1.0 remains a second implementation.

## Content and publication

The GitHub connector confirmed the starting PR #648 head
`1dea5b59864baefebce567729d781af44c80cf22`, branch
`claude/optimistic-feynman-fqh947`, and main
`874f298b4de7c3a13006cb1ff767dfb09e4031ac`. The checkout and two inspected
source blobs matched the remote; reviews and threads were empty and the
starting checks successful. No general broker or proof audit was repeated.

The implementation was first published at
`530c5adfe12a2ca8011a84b99062283936872a82`. All 31 changed blobs were read
back through the connector and matched the tested tree. The local complete
receipts below belong to that SHA. A subsequent test strengthens the explicit
same-process/original-receipt assertions in the PUBACK-loss case; it changes
no executable behavior. Its targeted loss case was executed successfully.
Final publication/check identities are kept in the PR description and CI
artifacts' `source_head`, rather than relabeling an older run with a new SHA.
The PR stays draft and unmerged. Proposed decisions stay proposed.

The full CI at `9f997cda` received both product suites, models and proof, but
stopped the contracts job because historical mutant M3 omitted the new
EmitFrame.publish_order field and did not compile. Static patch applicability
was insufficient. Its initializer is refreshed with `publish_order: None`;
the premature-PUBREC mutation and expected assertion/reason are unchanged.
The complete simulator and SQLite mutation loops were executed: all 5 and 11
applicable mutants were rejected for their expected reasons. M3 now compiles
and fails no_positive_ack_before_confirmed_commit for its premature PUBREC.
The earlier compiler error is not a semantic detection.

## Lifecycle and contracts

[Proposed ADR 0023](../decisions/0023-incoming-qos1.md) defines the occurrence,
unknown outcome, acknowledgement and resource boundaries. A new occurrence
draws fresh exchange/publication/operation identities. Before an admission's
outcome is known, replay retains its OperationId. A known commit alone makes
positive PUBACK eligible; a certain refusal can use 0x97 or 0x83. Unknown,
timeout, absence and socket closure never authorize positive PUBACK or certain
rejection. The separate PubackReason type cannot represent PUBCOMP-only 0x92.

QoS 1 creates no durable AwaitPubrel marker, charges zero durable exchange
slots and submits no ReleaseInbound. Its admission/pending PUBACK still holds
one bounded volatile lease. An ACK-pending occurrence no longer excludes a
new use of its Packet Identifier. Its own full frame EffectId/ConnectionKey
selects its terminal write/failure; an old or repeated receipt cannot end a
new occurrence. Closure/takeover releases pending ACK leases while unresolved
admissions keep their uncertainty. The bounded cleanup scan is charged before
reservation and mutation.

EmitFrame now freezes an optional PUBLISH ingress order in the core. A retained
effect never looks up a later use of an identifier when the queue accepts it.
Receive Maximum completion checks both occurrence and QoS: PUBACK for QoS 1,
PUBCOMP or negative PUBREC for QoS 2. Mixed reuse, negative completions and old
receipts recover exactly their own credits. Bounds were not increased.

After PUBACK becomes eligible, a new PUBLISH is admitted afresh even with DUP
or equal bytes. This supports a peer that received PUBACK before the reactor
collects the internal Written result. MQTT-4.3.2-5 forbids permanent
deduplication after PUBACK. In particular, lost PUBACK and broker restart may
produce another application admission. Internal command replay remains
idempotent. Local socket write progress never proves that the peer received it.
Publications without obligations are collected through the existing store
path; no archive or fake QoS 2 termination is introduced.

The product profile is `tcp-inbound-qos1-qos2-v1`. Product schemas/examples
include the bounded asynchronous `ack.receipt` test point. The fixture-based
QoS 2 profile, assertion qualification and oracle are not extended to qualify
QoS 1. SQLite schema and durable encoding are unchanged. The contract digest
changes from `sha256:775794a5e47f5e1d881f0572cdc3e64fa250d46e796e7474d9c5854c102fa5cc`
to `sha256:a0b4fb2cab5c1eb001083aa897e6c03875489813e19f99da65c550784f966c42`.
No proof BODY file or immutable French archive changed.

## Executed finite reception

| Criterion | Normal product | Instrumented product / targeted evidence |
| - | - | - |
| A | mqttium creates/resumes sessions, completes repeated QoS 1 and mixed 1/2; Paho completes QoS 1 and resumes; both survive a clean broker restart | Same cases; independent SQLite admission/release and normal metadata collection |
| B | Core/codec regressions require durable-before-ACK behavior | Synced actual SQLite admission.prepared point and OperationId; independent read sees the old state, no positive PUBACK; release permits raw PUBACK and the original mqttium receipt |
| C | Equal content, DUP, repeated PI, fragmentation/concatenation and cross-QoS reuse | Raw same-QoS control: after receiving the first PUBACK, an equal/DUP QoS 1 PUBLISH with the same PI reaches its own SQL-prepared point; the old receipt cannot release its credit or authorize PUBACK before the second commit. Separately new PI1 QoS 2 plus seven exchanges fill the window; the old PUBACK cannot admit a ninth. A normal real-reactor test also covers this scheduling |
| D | Session/restart in A | SIGKILL -9 after admission commit before PUBACK, same-file restart and retransmission; separately a proxy captures a complete server PUBACK, forwards none to the client and cuts transport. The same mqttium process/application publish/original receipt reconnects under a finite policy and completes. Both constructions expect two QoS 1 admissions |
| E | Two clients and mixed QoS progress; partial sender does not block the other | Certain payload-quota refusals produce 0x97/PublishRejectedError, repeated >8 times with reusable credits. An exhausted receipt budget produces PendingOrUnknown and no ACK; restoring it allows the same OperationId's resolution to fence and return 0x83. Takeover during a held admission reconciles its late result safely |
| F | Existing QoS 2 A/B/E/F/G-concurrency | All QoS 2 A–H, both G cases, typed invalid/over-budget startup, writer cleanup and historical controls retained |

Quota interventions are explicit isolated storage controls **after readiness**,
not supported live production configuration. They retain before/after facts
and restore the original limits. A restart never bypasses profile validation.

The actual four-slot output channel was filled in a focused real-reactor test
while its receiving adapter was held. The committed PUBACK retained its exact
effect through QueueFull and then explicit terminal failure after closure;
the admission stayed durable. This is a controlled channel test. Separate
normal and instrumented QoS 2 G runs reached real TCP nonreader saturation,
kept each retained effect's terminal witness, and let a healthy client progress.

| Isolated control | Required failure observed |
| - | - |
| Positive PUBACK before the acknowledged precommit hold | premature_puback |
| Remove occurrence/QoS guards from an old write receipt | stale_receipt_released |
| Historical early PUBREC / stale ingress controls | premature_pubrec / stale_generation_forwarded |

These are system receipts, not qualifying verdicts for QoS 1. Each run keeps
its own binary, effective features, client source/version/import path, network
bytes, SQLite facts, reached points, PID/wait status and strace. The proxy's
independent observation places the lost PUBACK; an internal receipt alone is
not substituted for that evidence. No client internals were changed or used.

## Commands and retained evidence

From `mqttiumd/`:

```bash
cargo build --locked -p mqttiumd
./target/debug/mqttiumd --db /tmp/mqttiumd.sqlite \
  --listen 127.0.0.1:1883 --events /tmp/mqttiumd.events.jsonl
python -m pip install -r tests/system/requirements.txt
python tests/system/external_mqttium.py --port 1883 --client-id example-client --qos 1 --qos 2
python tests/system/external_paho.py --port 1883 --client-id second-client --qos 1
tools/run_qos1_reception.sh /tmp/mqttiumd-qos1-reception
tools/run_tcp_reception.sh /tmp/mqttiumd-tcp-reception
```

The normal local A run actually invoked `external_mqttium.py --host 127.0.0.1
--port 35755 --client-id qos1-mqttium --expected-session-present 0 --qos 1
--qos 1`, then resumed with `--qos 1 --qos 2`; Paho used `--client-id qos1-paho
--qos 1` on that same port. Full interpreter/module paths and the restarted
port are retained in its receipt, not substituted into other runs.

At `530c5adf`, QoS 1 has 3 normal / 10 instrumented cases; QoS 2 has 5 normal
/ 9 instrumented cases. Both wrappers and all four controls pass. All 461
retained files were independently checked against SHA256SUMS.

| Local archive | SHA-256 |
| - | - |
| 530c5adf-qos1.tar.gz | 624f86d9a297eef9e3d0001ebb3e51415c73e4b19b75bc9546926248d40a0efc |
| 530c5adf-qos2.tar.gz | 3931d43700f9dc2a56693f1d2a9da4146b1f912bd71942543c9f21ebe4c52626 |

The wrappers executed identical normal and hooked binaries across these two
receipts: `e0ed180ec62e04f0a217447a31fe589a419be5c9267d2cdf1a906059fd3478ab`
and `6dd47d45433d53e9ab7bdd797f0d4f8bf0c32d5833e904b9508fc190fd6409fa`.
Their shared source archive is
`7b6135e0bd6770ca9575baa9d63ee1bb08057a79a2d209148cc38c103e5a2caa`.
CI independently rebuilds/runs both wrappers for its own exact head, with
artifacts `mqttiumd-tcp-product-<SHA>` and `mqttiumd-tcp-qos1-<SHA>`.
The current instrumented suite additionally includes C-late-same-qos (11
cases). Its positive run and the stale-receipt mutation were executed for
PASS / stale_receipt_released respectively. Historical 10-case archives above
retain their own scope; the final CI artifact records the expanded suite.

Executed local checks: formatting, strict workspace Clippy, **766 normal** and
**774 instrumented** Rust tests, **47** product-hook tests, **142** unchanged
oracle tests, **17** client/proxy/pilot tests; 367 derived corpus and 405 trace
files current; 382 schema documents agree between readers. Full normal and
instrumented suites ran sequentially in this checkout. Normal executable
features exclude hooks and the lab. The new core has 208 tests plus 2 doctests.

K1/K2b/K3 reconstruction/recompute, typed reads, K2a/WAL, 52 declared TLC
results and proof gates remain CI obligations for the final published SHA.
Their historical runs are not relabeled. Power loss, complete MQTT conformance,
performance and long-running collection are not received by these tests.

The next small product increment is incoming QoS 0 with an explicit source
identity that does not invent a Packet Identifier, followed by subscription
and outgoing delivery. General receipt/epoch collection, finite session expiry,
Will/authentication and deployment security remain separate work.
