# Finite session expiry and early DISCONNECT reception

MQTT5 clients can now select any Session Expiry Interval. mqttium1.3.0 and
Paho2.1.0 actually exercise active sessions beyond their TTL, offline state
resumption before expiry, fresh incarnations after expiry, and policy changes.
The broker preserves the other recipient's obligation and payload when a
producer or recipient expires. The128-session/8192-receipt quotas are unchanged.

This report records the local reception at
`78d2aeb55ef8fec52204d2a346a7fc04c9c78c74`. Its manifests identify the exact
source, clients, normal/hooks binaries and each process. Subsequent publication
checks and their own fresh CI artifacts are recorded on PR648, without
relabeling this run. The PR stays draft/unmerged; Proposed0030 and previous
proposals retain their statuses. No French archive or proof-protected file
changed. SQLite is3.54.1; the new durable schema is6, with typed pre-mutation
refusal of old formats rather than migration/reset.

## First correction and discriminating implementation checks

`5bf0b006` commits the actual-writer RED: an Open result is relayed into
control_inputs, then DISCONNECT0 applies it during its own drain. The old code
retains the session, failing
`disconnect_zero_during_open_drain_retained_durable_session`.
`6b583782` retains the original disconnect policy in the living-Peer branch;
the same test and the previous post-Peer-loss permutation pass. SQLite, owner
and cache become empty and reconnect reports SP0. Published6b583782 has green
mqttiumd/client/formal CI (mqttiumd run44028547894).

The generalized close intent preserves one effective policy and observed close
instant, including while Open is unknown or internally queued. It is always
associated with the original transport generation and durable attachment.

A focused integration review produced three additional local RED/GREEN checks:

| Defect | Discriminating outcome and repair |
| - | - |
| Blocked expiry moved into ordinary inputs | `fc6768df` fails `blocked_expiry_escaped_its_reserved_lane` with128 ordinary facts and a genuine held committed Open; blocked expirations now stay in their separate128 lane. |
| Impossible deadline interpreted as expiry | Store RED0e2252ea/core REDa1e6c33b accept finite deadline0; both readers and recovery now require deadline at least the nonnegative interval, refusing InvalidData before cleanup. |
| Certain no-effect Detach silently lost continuation | RED95fd3644 uses a real writer Resolve/fence before the withheld Persist; runtime now stops observably with session_detach_rejected/session_expire_rejected after consuming terminal knowledge. Unknown outcomes still resolve the same operation. |

A further actual-writer test confirms that a stale checkpoint CAS response
cannot reset the checkpoint age unless its absolute anchor covers the requested
observation. Receipt-pressure tests derive the changed512 business threshold
from the pinned profile, preserving the total1024 window and8192 physical quota.
A compile failure, missed hook or unexplained timeout counts as no detection.

## Implemented temporal contract

Proposed0030 defines the authority. Canonical lifecycle tags2/3/4 are OpenAt,
Detach and conditional Expire; legacy tags0/1 keep their bytes. Finite sessions
record an immutable detachment operation/deadline. The pure owner serializes
lifecycle requests, preserves their operation under uncertainty and validates
known results before effects/capacity change. A timer never deletes cache state
or a session by ClientID.

Immediately before accepted Open planning, the adapter reads admissible logical
time. The store atomically decides due-versus-resume using the original session
and detachment guards. An unpolled timer cannot produce SP1 after its deadline.
A valid earlier resume replaces the attachment and invalidates the old timer.
Delayed Detach COMMIT/Resolve cannot rearm from a later time.

The external safe provider uses CLOCK_BOOTTIME with checked u64 nanoseconds.
One durable anchor stores the recognized domain, boot tick and logical progress.
Recognition includes boot UUID and effective BOOTTIME namespace origin; boot ID
alone is insufficient. When origin evidence is unavailable, the provider reports
that fact and uses a unique process domain. This managed environment takes that
conservative path; normal local restart results do not assert proven continuity.

Same-domain injected tests count elapsed boot time through repeated crashes.
Changed-domain tests preserve committed progress and ignore unknown downtime;
UTC is not an input. Already detached deadlines remain unchanged. A connection
still attached at process loss is first durably detached at recovery observation.
Temporal commits coalesce progress; idle finite-offline state checkpoints every30s,
with one outstanding CAS and no receipt/history chain. The actual delay includes
scheduling and commit; age over31s reports degradation. No unconditional storage
or unknown-downtime bound is claimed.

Timers are keyed by live SessionId, at most128, with four attempts per turn and
no historical heap. Open/retirement removes old timers. The receipt business
threshold512 leaves512 continuation positions for128 inbound,128 outgoing and
256 detach/expiry controls. Resolution, clock CAS and receipt collection do not
create another cumulative receipt chain. Target-only cleanup returns each
resource once, preserving obligations already due to other destinations.

## Actual system reception

Command, from mqttiumd with pinned tools:

```bash
tools/run_expiry_reception.sh /tmp/mqttiumd-expiry-reception
```

The launcher pins builder, harness and client source together, builds normal and
hooks separately, traces the executions rather than compilation, and archives
client/wire/durable/process facts. This is system evidence, not independent
oracle qualification of temporal semantics.

| Group | Observed result |
| - | - |
| A | Authentic queued-Open DISCONNECT0 RED/GREEN and all prior permutations pass. |
| B | mqttium/Paho active beyondTTL, offlineSP1 with state/message, dueSP0 with fresh SessionId, finite/infinite overrides and zero/infinite cases pass through public APIs. Paho supplies DISCONNECT properties absent from mqttium's public API. |
| C | Injectable exact before/at/after boundaries, delayed detach and unpolled-timer Open pass. A stopped SQLite deadline0 corruption image passes quick_check but exits2 with InvalidData/Snapshot, no readiness, released writer and unchanged SQL/anchor/main-file hash. |
| D | Actual committed Detach/Expire replies remain PendingOrUnknown, then resolve their same operations. Five separate own-process SIGKILL9/same-file runs pass: before/after Detach COMMIT, before/after Expire COMMIT and connected finite session at process loss. |
| E | Expiring target and producer leave the other byte-identical obligation/payload alive; reference count2→1→0 and no resurrection. Existing lifecycle cleanup and unknown-business reconciliation remain shared code. |
| F | Full128 is retained;129th identity gets0x97. Actual expiry frees capacity; newcomer has SessionId129. The infinite witness survives.64 rearms retain timer index0/1; final state has one witness, zero terminal rows, one anchor row. |
| G | Four compiled controls each execute and fail their exact invariant, after passing their positive test; private source is restored. Eight finite-model configurations retain their exact expected outcomes. |

The normal capacity run uses PID1419517 and SQLite inode2664246 throughout.
Killed PIDs1421721/1421741/1421761/1421858/1421943 each have their own executed
hooks binary, kill/wait witness, same-file restart and clean later exit.
The normal healthy run's observations do not substitute for these crash runs.

| Compiled mutation | Required detected reason |
| - | - |
| Open waits for timer to notice expiry | expired_open_resumed_without_timer |
| Old timer rebound to new attachment | old_timer_rebound_after_open |
| Detach deadline recomputed | detach_result_recalculated_deadline |
| Same-domain restart loses progress | continuous_domain_progress_lost |

Archive `mqttiumd-expiry-reception-78d2aeb5.tar.gz` SHA256:
`a7fea47894cbc4575a6b93ca55ffd97b344b58896a0be7b9f8f407f3b3db61ff`.
Its source.tar is `cabc5aa478352131c8ae702a00d0d47953f5d90aa7899ca9690adeab973162d3`.
Normal binary is `354ccb2a19465ebc602313f95fb0bae8ced4c1491840f140482070f3b0574fd8`;
hooks binary is `1eb094a3e6eeb2e099635805dfe62bee54bade707805c76e49e68ea9dd2f1723`.
The artifact audit matches2131 Git blobs,368 retained hashes and369 packed files,
plus each of the five own exec/kill/wait witnesses. These identities belong to
this local run; final CI builds retain their own different identities.

## Contracts, verification and remaining limits

Contract digest changes from
`sha256:c249cd468b460b8304b043a5ced515fee14ac13d3f5bb676d5dd0f0180b5c897` to
`sha256:5fada9d9c4f9e461671ce4063722ecf95730fd6104f154011766f3c2d7630ad0`.
This was the digest of the local78d reception. Its actual-stream audit then
found that generic SessionLifecycle submission events were missing from the
schema enumeration, although their emitted shape was unchanged. A separate
schema RED/GREEN and bounded stream-reader gate correct that description.
Final contract digest is
`sha256:9269cee70ac3eaa90d78458e2227f2854bae0045d4c28c7842d264dc375493c7`;
final-source CI uses this identity and fresh campaigns, never the old digest.
Finite JSON policy is `{"After":seconds}` alongside legacy strings; detachment
and singleton clock facts are bounded and independently parsed. The fixture
oracle explicitly refuses temporal scope rather than silently extending PASS.
Its new canonical reader checks exact action bytes and time overflow. The
partial transition register covers the newly implemented decisions only.

Targeted validation passes160 contract tests,294 core tests/two doctests,
21 lifecycle differential tests, strict pure/store Clippy and the new product
regressions. Local public reception and all four controls pass. The clean938d gate receipt
passes968 normal and987 instrumented Rust tests sequentially, strict Clippy,
validation13,218 oracle tests,91 system pilots and537 schema documents. Its
corpus517/traces405 checks pass; newexpiry8 and unchangedGC10 model outcomes
match. Further unchanged models were stopped under the agreed local budget,
not counted as detections; CI retains the complete90-configuration obligation.
Gate archive SHA256 is
`abaa2eb2168ab6d106bd04beda48be6cec97c5366552ffbe25f20bf09077d734`.
Later changes add one checkpoint test, a receipt-pilot compatibility fix, the
observed-event schema/stream reader and documentation. Each keeps its own
subsequent verification. Final CI runs the complete suites on publication; the workflow also preserves the existing receipts, PI, QoS0/1/2,
pubsub, typed reads, K1/K2b/K3/reconstruction, models and pilot-proof gates.
Each later gate and qualifying campaign names its own SHA in PR648/artifacts;
this report does not transfer an earlier result to a later source.

No power-loss, universal clock continuity, MQTT-wide certification, performance
qualification or indefinite-service claim follows. Sessions remain bounded128;
valid offline infinite sessions are never evicted. Schema1..5 migration is not
provided. Message Expiry, Will, authentication, retained and outgoingQoS2 remain
separate. The next functional increment is bounded wildcard subscription
matching for `+` and `#`, using the existing durable subscription cut.
