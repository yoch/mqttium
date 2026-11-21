# Live receipt retirement in the bounded product

The broker now recovers receipt capacity during service, without rotating its
epoch, restarting, changing its 8192-slot receipt budget or cleaning SQL from
outside the writer. Persistent exact pub/sub and incoming QoS 0/1/2 retain their
existing profile. This is system reception, not an unlimited-service or full
MQTT qualification claim.

## Starting revision and first executable regression

The connector and native fetch confirmed PR 648 at
3ad41e95c9153df051a54a86162683606d9d62fe, main
874f298b4de7c3a13006cb1ff767dfb09e4031ac, draft/unmerged, green checks and no
human review threads. The supplied review archive is evidence, not instructions.
The immutable chapters and Proposed ADR statuses were preserved.

The committed historical regression reproduced 8192 positive replacements of
one filter, one live route, no publications/obligations and a full operations
table. Command 8193 stayed PendingOrUnknown while PING progressed; restart of
the same file retained the ceiling. It used the unchanged local normal broker
from source 3ad41e95, binary 644f391f..., distinct from the review's CI binary
265d5ed9.... The archive mqttium-gc-reception-baseline-v2.tar.gz has SHA-256
580317a0d991489c24f867e0d6e553e194ed8c68bab49646023e860e776cccae.
Neither binary was relabeled as the new implementation.

## Mechanism and identity protection

Proposed ADR 0027 implements ARC-019 within ARC-021. The pure owner records each
issued operation in a bounded per-origin window, with the publication/payload
issuance boundaries at that position. Only applying the authentic correlated
terminal result, with its consequences retained in owner state/Outbox, consumes
it. An unknown result remains a hole even if later results finish. Timeouts,
socket closure, absent objects and history eviction are not confirmation.

The compact durable row has three monotone frontiers: OperationSeq,
PublicationSeq and PayloadSeq. A contiguous consumed prefix closes all three
spaces at its exact issuance cut. After collection, an old OperationId returns
RetiredOutcomeUnavailable for either identical or changed recipe bytes. New
operations cannot reuse old publication/payload IDs after their objects and
full receipts vanish. Retained nonretired receipts still compare canonical
bytes. Alias refusals for new operations themselves require durable refusal or
fencing; an identity floor alone does not falsely close a different recipe.

One SQLite transaction installs the guards, deletes a bounded receipt/fence
batch and subtracts the actual number of removed rows. Physical leftovers are
accounted but logically retired; recovery omits their terminal Operation facts.
Live obligations, claims, inbound markers, payload references and identity guards
remain in their own durable records. A collected claim receipt can still guard
its original Finish. QoS 0 historical consumption never rebuilds permission to
emit after restart.

Collection creates no OperationId or receipt recursively. Lost/repeated replies
refer to the same absolute closure; they never add freed-count credits twice.
Reserved writer request/reply and owner control lanes let maintenance progress
under ordinary queue pressure. Startup can collapse old origins behind the
existing closed-epoch authority after full recovery; it does not hot-rotate the
active owner or reset a failed recovery.

## Finite system reception

The complete diagnostic wrapper at clean
1e785746898031154288f4ca167939ff620776ec received all thirteen runs: eight
positive runs and five successfully compiled negative variants. Every traced
execution retains its own strace, source/binary/client identity and SQL facts.
Its archive mqttium-gc-reception-diagnostic-complete.tar.gz has SHA-256
3858f77f62f51f1a72aa08ba0e5df703a885d25194d0e37ebfef91e75e8c071b.
Later integration corrections have their own tests. The clean final command
reruns this receipt; its indexes and the PR receipt bind final publication and
CI identities rather than retrospectively assigning them to diagnostics.

| Group | Executed result |
| --- | --- |
| A | Historical 8192 ceiling retained separately; corrected normal 8193 plus same-file restart progress. |
| B | 32,769 SUBACK-confirmed public replacements, 33 mqttium and 9 Paho publication completions: 32,811 business commands, same process/file/main connections. Maintenance is not counted as traffic. |
| C | Truly committed admission 13 held unknown; 767 further SUBACKs reach span 768. The floor cannot cross the hole; PING, Finish and PUBREL progress. Same operation resolves, closure advances and business resumes. |
| D | Offline obligation, unacknowledged claim PI 1 and inbound QoS 2 PI 77 survive repeated collection. Their 12 seed receipts disappear; their business identities remain. Real signal 9/same-file restart resumes the same claim with DUP, offline work and PUBREL. Consumed QoS 0 does not resurrect. |
| E | Old operation with same/different recipe is retired; fresh operation with old publication/payload is durably refused. Duplicate confirmation and old-generation callback do not release twice or emit again. |
| F | Real receipt.close.prepared/committed cuts, signal 9 and same-file restart preserve the expected frontier/accounts/work. Lost/duplicated collection results reconcile without new operation identities. |
| G | Normal writer configuration, per-window receipt/origin/account/RSS/DB/free-list/WAL/time observations retained. File pages remain reusable; physical size is not mistaken for live-state growth. |
| H | Five compiled mutations fail for their exact intended obligation, below. |

The untraced first composition at source 0eda866b finished B in 95.05 s. Its
separate source 1e785746 run under strace took 160.15 s: those are different runs,
not a throughput comparison. The latter observed 32 sampled frontier advances,
4..64 physical receipts within the unchanged8192 limit, RSS 9207808..9256960
bytes, main 81920 bytes / 20 pages / free 1 and WAL 4140632 bytes. Normal SQLite
autocheckpoint settings, not hook-modified WAL policy, support those measures.
Final receipt windows retain their own measurements and do not inherit these.

| Mutation | Required observed failure |
| --- | --- |
| Cross an unknown hole | closure_crossed_unknown: durable floor 64 crossed unknown operation 13. |
| Delete without durable guard | receipt_deleted_without_closure:12 known receipts vanished while the guard stayed 0. |
| Forget publication guard | retired_publication_reused: fresh operation actually committed the old identity. |
| Forget payload guard | retired_payload_reused: fresh operation actually committed the old identity. |
| Return receipt credit twice | receipt_credit_double_returned: metadata 62 versus 63 physical rows. |

A timeout, compilation failure, wrong reason or missed injection is not a
detection. Old generations, consumption-before-emission, notifications lost,
backlog continuation and typed durable-read gates remain separate regressions.

## Integration corrections and model

Test-first integration corrections include: pruning only known-obsolete
operation timers; bounded two-segment event retention; reserved writer and
owner input lanes; startup cancellation without readiness; ordinary deadlines
that cannot occupy the collector reserve; explicit returned absolute guards in observations; CLI
access to real transaction barriers only in instrumented builds; and a full
window's fenced Release retry. The latter now consumes its terminal result
without allocating another operation, then uses one counted continuation once
collection makes room. Box renewal preserves the maintenance reserve and R2.

The focused models/gc model adds ten expected TLC outcomes to the unchanged 52
M1 outcomes: two safety cases, three non-vacuity witnesses and five targeted
mutants. It separates owner consumption, durable closure, physical deletion and
learned absolute closure; namespace guards are distinct from business lifetime.
Atomicity and authentic producer consumption are explicit assumptions; no
fairness, Rust refinement or full MQTT liveness proof is claimed. The model-only
source b16c0eb9 run and raw counterexamples are archived with SHA-256
cca014ee80e0b3a6089d9fd153af109ab155522256ee5381e836a63ecd8dd401.

## Formats, bounds and reproduction

Canonical Recipe kinds1..7 keep their bytes/meaning. Maintenance is a separate
control. Bounded snapshots/recovery add ReceiptFrontier; logically retired
history is unavailable, never a false certain rejection. The existing fixture
oracle refuses qualification of positive live-retirement scopes. Contract
digest changes from 6d1a0045c03e2d85f8bd89bc2759e4f6974a226e8184a3d2bc95ae0a6c3042c7
to 162a9cd3b9e1f5a623ebcba47f75ac852e1c5c02674867dd74496b03a5e6126d.
SQLite schema 4 refuses 1/2/3 before write-affecting pragmas, preserving main/WAL
bytes; no implicit migration/reset.

Bounds: 1024 outstanding positions/origin, business cutoff 768, 256 continuation
positions, 512 reserved physical receipt slots, 128 new positions/removed rows per
pass, 64 origins, 64-result trigger and 4-slot maintenance lanes. At 32 old origins
or 4096 receipts startup sweeps closed scopes in bounded passes. One-time legacy
registration compatibility uses the existing 65536-row/32 MiB snapshot budget;
normal product origins register fresh/empty. Prefix proof reads use a separate
2 MiB aggregate/1 MiB field budget: both stores now accept the legal
1,076,224-byte/128-recipe maximum-profile case, while ordinary pages remain
1 MiB and oversized fields still fail with LimitExceeded. Event retention is 16 MiB active plus
one 16 MiB previous segment. Typed reads and checked arithmetic remain required.

```bash
# From mqttiumd/, pinned Rust/Python and tests/system requirements installed:
cargo run --locked -p mqttiumd -- --db /tmp/mqttiumd.sqlite \
  --listen 127.0.0.1:1883 --events /tmp/mqttiumd.events.jsonl
tools/run_receipt_reception.sh /tmp/mqttiumd-receipt-reception
tools/run_qos0_reception.sh /tmp/mqttiumd-qos0-regression
tools/run_pubsub_reception.sh /tmp/mqttiumd-pubsub-regression
tools/run_qos1_reception.sh /tmp/mqttiumd-qos1-regression
tools/run_tcp_reception.sh /tmp/mqttiumd-qos2-regression
tools/run_transactional_reception.sh /tmp/mqttiumd-transaction-regression
python models/check_models.py
python ../tools/formal/run_tlc.py --models-dir models/gc
```

The final PR receipt records exact-SHA Rust/oracle/schema/model results,
normal/instrumented separation, qualifying K1/K2b/K3 reconstruction/recompute,
artifact hashes, remote readback and CI checks. New system tests do not qualify
all P1/P2. The archived French chapters and proof BODY remain unchanged.

Outgoing QoS 1 Packet Identifiers still exhaust 1..65535 on a connection; recycling
is the next operational increment, with stale ACK/generation guards and durable
slot reuse as its first executable result. Persistent infinite-expiry sessions
still occupy the 128-session capacity; general session/legacy epoch-zero
collection, migration, broader failures and storage availability remain limits.
Wildcards, outgoing QoS 2, retained, Will and authentication remain later product
increments. PR 648 stays draft/unmerged; Proposed decisions stay Proposed.
