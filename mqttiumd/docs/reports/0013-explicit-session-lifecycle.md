# Explicit bounded session lifecycle

The experimental MQTT 5 TCP product adds Clean Start0/1 and zero/infinite
session retention. Ending an eligible session recovers its slot and removes
its physical row, routes and volatile tracking; persistent offline sessions
are preserved until explicitly ended. This is a system-tested product increment,
not extension of fixture-oracle qualification or complete MQTT conformance.

## Public behavior and authority

[Proposed0029](../decisions/0029-explicit-session-lifecycle.md) contains the
request matrix and ordering. An absent CONNECT expiry is zero. CS0 can resume
an existing persistent session with SP1 and change its policy to zero without
first discarding its state. Closure then ends it. CS1 replaces its incarnation
with SP0, including at the unchanged128-session quota. A takeover first closes
the old transport; if its policy was zero the later CS0 open creates a new
incarnation. DISCONNECT absent preserves policy; explicit zero ends it.
CONNECT0→DISCONNECT nonzero is Protocol Error0x82; intermediate finite intervals
are outside the profile. Duplicate and malformed properties keep distinct codes.

ClientID is reusable. SessionId is allocated by a checked durable counter and
never reused. Each Open has a new OperationId attachment token independent of
the stable business revision. Close compares that original token and SessionId;
the Peer keeps its initially confirmed identity and generation. Cache lookup
cannot substitute a newer same-name incarnation.

Canonical format1 kind8 adds receipted SessionLifecycle Open/Close, preserving
kinds1–7. Submitted/unknown/terminal/applied are distinct. Unknown results resolve
under the same OperationId; they cannot cause blind re-creation/replacement or
a positive CONNACK. Owner application of a correlated known result precedes
cache installation/CONNACK. A bounded lifecycle lane can progress with ordinary
writer replies unread. Lifecycle cuts serialize with subscription changes;
other destinations' continuations and control remain independent.

## Cleanup, responsibilities and storage

Replacement and termination are atomic within existing finite limits. Cleanup
cancels the ended session's subscriptions, destination obligations/claims and
incoming markers, returning each held reference/slot once. These cancellations
are separate from successful delivery. Deleting a producer does not delete
publications owed to another target: the surviving reference keeps its shared
payload until that target terminates normally. Old unknown admissions and
Claim/Finish/Consume results remain bounded detached flights until their terminal
knowledge closes the original receipt hole, without old ACKs or emissions.

SQLite schema5 stores retention and attachment explicitly. Schema1–4 files are
refused before write-affecting pragmas, with no reset or silent migration.
Initial typed snapshot validation precedes actual OpenEpoch's atomic expiry of
previous zero-retention sessions. Recovery/cache use the post-opening snapshot.
Duplicate AlreadyOpened never re-expires sessions created in the current epoch.
Physical removal is protected by monotone SessionId allocation and receipt
retirement, rather than one tombstone per historical ClientID.

The product retains128 active sessions,8192 physical receipts and512 reserved
control receipts. The1024-position origin window admits business through640,
leaving384 positions for128 inbound,128 outgoing and128 session-termination
continuations. Existing receipt collection remains active. Runtime work/views,
emission registrations, caches and requests are bounded by live/configured
state. Closed-generation and finished-operation rings remain bounded; identity
safety does not depend on retaining all historical transports.

## Executed discriminants and finite reception

The first committed red test (`9b04a9ad89b1706ffedeabf0dc150a54d82c98ba`)
compiled against the former Admin interfaces and failed both stores exactly
`session_terminal_history_grew`:512 create/delete cycles left512 Terminal rows.
Functional store cleanup removes that cumulative history while SessionIds keep
increasing. Differential tests additionally receive attachment CAS, full128
replacement, shared references, collection/replay, startup and typed bad policy.

Early source-specific normal diagnostics established the following before the
final source freeze. They remain diagnostics of those exact sources; no earlier
binary is relabeled as the final publication.

| Group | Executed diagnostic |
| - | - |
| A | Public mqttium/Paho matrix, SP0/SP1, absent expiry, finite/duplicate/context refusals |
| B |512 distinct ephemeral ClientIDs, same broker/file, unchanged quotas;18.676s,52 collections,63 retained operation rows; sessions Active0/Terminal0 and all postdrain owner/cache/work/frame/timer counts0, closed ring≤8; same-file restart succeeds |
| C |128 persistent sessions retained,129th refused; existing resume and same-name replacement at full capacity; explicit end frees a slot; offline witness filter/message survives |
| D | Loaded target cancellation, own incoming QoS2 and claim removed; another destination's byte-identical obligation/shared payload survives and completes |
| E | Genuine committed Open/Replace/Close results withheld, same-operation unknown/known resolution, independent PING; old copied lifecycle effect/original Close replayed through real guards after resume/replacement |
| F | Five separate product SIGKILLs: replacement/close before and after real commit, and connected zero-expiry producer crash with another persistent target; same DB, correct identities, no resurrection |
| G | Four independently compiled exact controls: early capacity, stale Close, terminal history, unrelated destination cancellation |

B/A/C/D use agent integration88a667c1; its B ledger SHA256 is
72719fa5c9417c59f5d4e47c90a15971836b71fa80f3d9a02bacbb56795d97d1.
E held-result diagnostics use88a667c1, actual takeover/copy diagnosticsfccb42a8,
and F uses0253eeb5. An additional authentic source-unknown case atc8ca500a
holds committed admissionseq4, ends sourceSID2, creates same-nameSID3, then
resolves the original operation without old ACK. The other targetSID1 keeps
its byte-identical obligation/payload and receives it through public mqttium. All retain their process/client/SQL observations separately.

The final launcher below reruns the complete A–G scope from its own clean pinned
source and keeps its exact source SHA, binaries/features, imported clients,
parameters, client/network facts, SQL/resource windows, each kill/wait status,
compiled-mutant hashes/logs and restoration witnesses. `complete_mission_scope`
is true only after all criteria, strict512 cycles and all four expected mutation
reasons are received. Its `index.json`, archive and SHA256SUMS are the final-run
authority; the PR receipt binds those artifacts and CI to the published HEAD.

| Compiled control | Required detection |
| - | - |
| Free owner capacity before known Close |session_capacity_released_before_close_commit|
| Apply original attachment's old Close |old_session_close_rebound|
| Keep every terminal row |session_terminal_history_grew|
| Cancel another target's obligation |session_other_destination_cancelled|

Pure Owner/store controls have their stated scope. Build failure, generic timeout
and missed injection are never detections. Six additional store multiprocess
cuts (Close/Replace×prepared/logged/applied) use actual signal9, same-file reopen,
full deterministic DurableState equality and replay. They do not substitute for
product F's own process witnesses.

The focused finite model adds8 expected outcomes to the unchanged74 previous
outcomes. It separates payload references from inbound/claim exchange slots and
maps the related focused Rust regressions. This is a specification model with
explicit assumptions, not a Rust refinement or universal network/storage proof. The pilot
proof's protected files and French archive remain unchanged.

## First publication and reception compatibility

Published201c037b8da7757cb4a097b434e6239a54314204 independently completes the
full A–G launcher:512 cycles on PID1320550/inode2924382 in34,820.287ms,52 receipt
collections, zero active/terminal/business rows and all zero postdrain volatile
inventories except the closed-generation ring≤8.63 operation receipts remain
before and after restart. The local archive SHA256 is
`3c545f6ce271b6d000e2c22413d6602fe8c612fc431e5817b3600b33e48e74f3`.
Independent audit matches2082 source Git blobs,832 retained hashes/833 packed
files and all five same-supervisor SIGKILL/wait witnesses. Normal binary is
`4f7e4d73aee0f811c1a496ac8d59f57cd73ecc7a4f70e694b0571edf3e3df057`.
This is evidence of201c, not an implicit receipt of any successor.

Its isolated gates pass922 normal Rust,937 instrumented Rust,89 product-hook
Rust,207 oracle and71 system-pilot tests; formatting, three strict Clippy
contexts, typed readers,515 schema examples, corpus/traces and8 focused TLC
outcomes pass. CI also receives the new session reception, durable pub/sub and
pilot proof. Its historical reception jobs expose compatibility defects in
the witnesses; their original failures remain retained and are not negative
control detections:

- TCP still called now-supported Clean Start1 an unsupported feature. Use a
  valid intermediate finite expiry and require its exact unsupported cause;
  malformed flags retain their separate exact cause.
- QoS0 matrix expected all eight historical admissions still to have detailed
  SQL receipts. Extra lifecycle traffic legitimately crosses the collection
  trigger. Count distinct actual known Admission commits and separately
  corroborate retained rows, without disabling collection.
- QoS1 refusal/unknown probes confused new lifecycle receipts with the targeted
  publication result. Match the actual operation and preserve other committed
  facts. A held committed result lets writer-backed takeover progress without
  pretending the new durable Open can bypass a blocked writer.
- The old premature-ACK mutation did not cover canonical kind8 and failed to
  compile. Its narrow exhaustive-arm update restores actual compiled detection
  of `no_positive_ack_before_confirmed_commit` on both stores.
- The long receipt observer keyed offsets only by inode and observed stat
  before open. A rotated/reused log inode produced an invalid mid-record read
  after more than30,720 commands. Six real-file red tests and the correction
  use the opened inode plus first sequence, atomic cursor/counter commit,
  bounded live-generation pruning and explicit gap rejection. The strict
  32,769-command requirement remains unchanged.

These follow-ups change reception/mutation files only: broker Rust, contracts,
models, quotas and the contract digest are byte-identical to201c. Actual retained
201c binaries pass TCP5 normal/9 hooks, QoS0 eight normal/16 hooks plus its three
exact mutants, and QoS1 three normal/11 hooks plus both exact mutants. The
receipt observer's1024-replacement normal smoke is explicitly not the long
requirement. The final source-pinned launcher and full CI must run afresh on
the published successor; their source-head/index/archives provide that binding.

## Reproduction and remaining limits

From `mqttiumd/`, with the pinned toolchain and system requirements installed:

```bash
cargo run --locked -p mqttiumd -- --db /tmp/mqttiumd.sqlite \
  --listen 127.0.0.1:1883 --events /tmp/mqttiumd.events.jsonl
tools/run_session_reception.sh /tmp/mqttiumd-session-reception
cargo test --locked -p mqttiumd-store-sqlite --test session_lifecycle
cargo test --locked -p mqttiumd-store-sqlite --test kill_points --features kill-points
python tools/run_session_core_controls.py /tmp/session-core-controls
python tools/run_session_store_controls.py /tmp/session-store-controls
```

The product links SQLite3.54.1 in these diagnostics (WAL, FULL,
autocheckpoint1000); each final run records its actual linked version and policy.
Client mqttium1.3.0 is imported from the identified source, and Paho2.1.0 is the
independent second public implementation. A local DISCONNECT completion is not
proof of broker observation/commit: the reception waits for known lifecycle
facts and independent durable/cardinality observations.

The previous contract digest is
`8884608f0e66e905307a546b6296934099db688ae5b2829d187dc816dda0bb05`;
the resulting digest is
`c249cd468b460b8304b043a5ced515fee14ac13d3f5bb676d5dd0f0180b5c897`.
Final `validate` and run artifacts bind that identity to each exact source. Lifecycle evidence deliberately remains
OUT_OF_SCOPE for the fixture oracle. Existing K1/K2b/K3 qualifications retain
their own assertions, source/rebuild/features and system witnesses.

Intermediate finite expiry, general migration, automatic eviction, Will,
authentication, retained/wildcard/shared subscriptions and outgoing QoS2 remain
separate. At most128 persistent sessions can coexist; none is silently evicted.
Storage, configured live windows, reader budgets, origin limits and checked
causal identities remain finite. No indefinite-service, general performance,
complete MQTT conformance or power-loss claim is made. The next useful increment
is finite Session Expiry Interval using this explicit termination authority and
a bounded external timer/recovery policy.
