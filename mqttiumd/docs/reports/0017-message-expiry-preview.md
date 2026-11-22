# 0017 — Durable Message Expiry in the local MQTT 5 preview

The normal Linux x86_64 preview accepts Message Expiry for retained and ordinary
publications. An expired, unstarted copy is not newly transmitted; retained and
offline references are reclaimed when expiry is established. Retained replay
keeps the original lifetime. A fresh value without expiry remains usable after
expired state has been removed or temporally uncertain state has been replaced.

The short package guide starts with the executed Mosquitto demonstration:

```sh
./expiry-demo.sh ./new-expiry-demo-data
./demo.sh ./new-retained-and-expiry-data
./start.sh ./data
```

The normal executable needs no Git, Cargo or laboratory. Only the optional
demo needs Python3 and the Mosquitto client executables. `start.sh` preserves
SQLite and creates new logs; it never repairs or resets an incompatible file.
Download the normal archive and its separately kept system evidence from the
exact published SHA's **durable Message Expiry and local preview** job in
[PR648](https://github.com/yoch/mqttium/pull/648).

## Received content and formats

The complete local source-pinned candidate is
`66361b74dbde52c670907586359a5152290071e7`, descended from revalidated draft PR648
head `795d200929db643618be22cd01c5538f78ed1d96` and main
`874f298b4de7c3a13006cb1ff767dfb09e4031ac`. This report is the following delivery
record. Fresh CI, downloadable archives and the independent extraction receipt
must identify the subsequent published SHA; local evidence is not relabeled.
The PR remains draft/unmerged and Proposed 0033 remains Proposed.

Contract digest changes from
`sha256:948a42f7161f97e3cd0cfe4d5b37bee5f9b56325b1a4b69f1c7124b97aac5412` to
`sha256:36a15f70c77acaf2611ccd5a955d9898fc65de2bcac93b861d607ac3b2d95f1e`.
New canonical tags13..16 add timed admission, timed retained replay, Started
and exact certainly-unsent expiration. Historical tags1..12 preserve their
bytes and meaning. SQLite schema 9 refuses schema 8 and other versions before
write-affecting setup; no migration or implicit reset is supplied. The normal
product links bundled SQLite 3.54.1, WAL, synchronous FULL (2), autocheckpoint 1000
pages and page size 4096. Fault instrumentation is absent from its feature graph.

## Time authority and uncertainty

Property `0x02` is accepted once, as u32 seconds, only in PUBLISH. Absence is
unlimited; zero is a zero lifetime; 0xffffffff is a finite duration. Deadline,
differences and conversions use checked arithmetic. Remaining wire seconds
subtract whole elapsed seconds. Exact deadline means expired. Other unsupported
properties and malformed/duplicate-property refusals are preserved.

| Observation | Implemented authority |
| --- | --- |
| Same process and domain | Original raw CLOCK_BOOTTIME domain/tick/interval, frozen at full TCP frame completion before any queue |
| Restart with recognized continuous origin | Checked raw elapsed time can count process downtime |
| Changed/unrecognized origin | Quarantine old expiring values and unstarted work; neither fresh delivery nor invented elapsed time |
| Unreadable/regressive clock | Observable refusal/suspension of the affected path; no resampling an already queued error into freshness |
| Invalid/overflowing durable stamp | Typed read refusal, never immediate expiry by saturation |

The real managed Linux run reports `boottime_namespace_unavailable`; a restart
has a new private domain. The packaged demo therefore records
**temporal-quarantine-not-proven-expiry**, separately from physical expiry in
the uninterrupted process. An independent BOOTTIME observation does not grant
unproven domain continuity. UTC is diagnostic, never deletion authority.
Quarantined references remain charged until replacement/clearing or applicable
session termination. This is a deliberate availability limit and not strict
cross-domain MQTT expiry compliance. Session Expiry keeps its separate logical
retention policy; it never supplies a new message lifetime.

## Durable responsibilities and transmission

QoS1/2 atomically commit the original expiry, retained mutation, frozen live
fanout, references/accounts and operation result. QoS2 still admits before
PUBREC; a late PUBREL only releases its marker. Retained QoS0 has a durable
operation without a fictitious Packet Identifier, ACK or Receive Maximum
credit. Ordinary QoS0 remains volatile. No client-side QoS0 receipt is treated
as proof of broker processing or subscriber receipt.

Retained replay and old owed versions carry their own immutable original stamp.
RH, RAP, ClientID No Local and overlap semantics are preserved. A replay never
re-reads the current value at emission and never renews its lifetime. Unknown
commands retain the same OperationId and canonical bytes through resolution;
retired identities cannot remutate newer state.

| Delivery fact | Lifetime/resource decision |
| --- | --- |
| Queued/loaded/unclaimed | Established expiry removes only its own responsibility |
| Claim committed or frame queued | Does not prove transmission; expiry is checked again before every zero-progress socket attempt |
| First positive local write | Correlated Started fact; the frame finishes or its transport closes |
| Started result pending/unknown | Keep the exact claim, payload and PI; early PUBACK waits for known Started |
| Known Started | QoS1 can complete after TTL, including a same-claim retransmission with remaining0 |
| Recovered unmarked claim | Transmission-uncertain; expired/unknown-domain work is quarantined, not freed or newly presented as fresh |
| Certain first-attempt NotStarted | Exact guarded tag16 can expire the claim; an earlier ambiguous attempt cannot be erased |

The local authority is the pre-write clock sample, not a timestamp of physical
network departure. Arbitrary host preemption after that sample is not bounded.
The positive-write-to-durable-Started crash window remains conservative and
explicit. ACK bindings precede socket submission; effect, claim, target and
connection guards prevent old internal facts from acting on a new occurrence.

A receiptless writer lane scans at most4 publications per request with a live
key cursor, one frozen observation per complete scan and one request in flight.
It removes expired retained references and unclaimed copies, preserving inbound
QoS2 markers and every claimed copy unless exact unsent cancellation succeeds.
Lost-result retries retain the same scan header, have a three-attempt budget,
and apply absolute results once. Duplicate/stale responses do not return credit
twice. Newer admissions than an older as-of sweep are skipped until a fresh pass.

## Finite system reception

| Group | Executed local candidate result |
| --- | --- |
| Minimal public path | mqttium retained QoS1 without subscribers; source destroyed; physical expiry; no stale wildcard replay; succeeding timeless value |
| Live matrix | 120 cells: both mqttium/Paho directions, input0/1/2 × grant0/1 × retained/plain × absent/0/1/2/u32max; independent frame decoding |
| Offline work | 24 cells, expiry and physical reference reclamation while the session remains valid; fresh-message witnesses after reconnection |
| Original lifetime/options | Remaining interval decreases; RH0/1/2, RAP, No Local, duplicate/overlapping filters;32 valid retained values replay and finish |
| Started QoS1 | Both public clients hold the actual PUBACK until after TTL, then complete; shared payload stays owned until known Finish |
| Capacity/invalid inputs |32 retained topics fill the unchanged inventory; exact typed0x97 refusal leaves business state unchanged; expiry physically returns capacity;24 malformed-property cases |
| Crashes |11 separate SIGKILL runs at prepared/committed Q1, Q0 and replay admission, claim, Started and receiptless cleanup; own PID/barrier/signal9/inode-preserving restart witnesses |
| Unknown and unsent | Authentic committed admission withheld past expiry then resolved with the original operation and public receipt; exact zero-byte refusal reaches tag16 before physical release |
| Normal package | Same normal bytes in reception and extracted archive; real Mosquitto retained replacement/clearing and Message Expiry demos pass |

The normal restart and14 instrumented boundary cases are kept separately. A
frozen replay legitimately owns a distinct publication identity sharing its
original payload: its exact version/stamp/obligation/references are checked
through quarantine, not reduced to a misleading publication count.
Three existing wildcard and9retained boundary cases also pass independently at
the earlier exact pin d635dffb; these are not relabeled as the final candidate.

Six compiled controls each execute a passing positive before requiring the
mutant's exact assertion, a distinct executed binary and complete restoration:

| Mutation | Required detection |
| --- | --- |
| Replay renews lifetime | `replay_refreshed_expiry_origin` |
| Expired retained reaches socket | `expired_retained_reached_socket` |
| Sweep releases a started reference | `started_claim_payload_released` |
| Offline expiry ignored | `offline_expiry_ignored` |
| Outgoing property corrupted | `outgoing_message_expiry_remaining_wrong` |
| Unknown origin treated as fresh | `unknown_origin_replayed_as_fresh` |

All 6 are received at this candidate. Earlier catalog drift is retained as an
unreached injection, not a detection. A source-site integrity regression now
checks every mutation's exact production anchor. Other corrected harness errors
(historical reused PUBACK selection, incomplete recipe fixtures and a wait
shorter than the real10s resolution delay) remain identified in the journal.

Implementation regressions commit their failing tests first. Reader timestamp
tests6c1a5647/3c0907ee precede668c0ed3. A previously unchanged child-directory race
is exposed by482dbe05 and fixed separately by9a39cb5c with exclusive owned
directories. New store test407dffdf reproduces an invalid Started commit on a
non-expiring claim in both stores;66361b74 refuses it. Positive9b81f5e9 ensures
that a real Started result remains admissible after TTL, without a freshness
guard at its durable commit.

## Gates, bounds and provenance

At this candidate: formatting, both strict workspace Clippy configurations,
1106 normal Rust tests and 1121 instrumented tests pass,
with the full suites run sequentially. Validator confirms the digest above.
Corpus577, independent schema reader600documents, historical405derived traces,
255oracle tests and171 system-pilot tests pass. Healthy-physical,
invalid-semantic expiry fixtures are rejected with InvalidData. Historical
K1/K2b/K3 remain incoming-QoS2 oracle qualifications and must be regenerated by
the final CI; the new receptions are system tests, not added qualification.

All119 finite-model expected outcomes match at d635dffb:30passes,87 specified
invariant violations and2 specified liveness violations, no mismatch/timeout.
The15 new expiry configurations model pending/started/expired/retained/reference
ownership and temporal uncertainty. This is finite specification evidence, not
an implementation refinement or universal runtime/conformance proof. Their
maximum-interval abstraction is finite; actual u32maximum is tested in code.
The model inputs are unchanged in this candidate.

Existing quotas stay32retained topics/1MiB,128references/4MiB shared payload,
16live destinations,8filters,256replay candidates,128sessions and8192receipts.
An expiring publication adds44 logical metadata bytes (domain32/tick8/u32interval)
plus SQLite representation overhead; a claim adds one Started discriminator.
Old owed versions remain charged.544 terminal-input slots accommodate Started
and completion facts;208 frame registrations and business quotas are unchanged.
No timer heap, per-sweep history or per-reading row is added.

```sh
cd mqttiumd
python -m pip install -r tests/system/requirements.txt
# Install your distribution's mosquitto-clients package.
tools/run_message_expiry_reception.sh /tmp/message-expiry-reception
```

Local evidence: `/workspace/message-expiry-66361b74-full`; system archive SHA256
`9b863620ea6fb4d7ab8770404e7089543b02ab3318bdce1a20f9a175107e509c`; normal tar SHA256`64f457a3cfe45927b6860fe2a2939c8997e167b2811731e84b2821def8dc9519`; executed normal binary
SHA256`a6ec7ac9596e8a8510468d9074d605d25b3e2a8200b9a8ad46ee11100dc8ebfe`. The receipt checks14832 strict product events in
43 streams,982 kept member hashes,6 compiled controls and11
own-run signal9 witnesses. Source archives and build options/features are kept.
Clients: exact-source mqttium1.3.0 (62Python files, digest
`f9d009f27270e491d05f4c37c3378f7ba4cc1a52fca1d6174c815c8f9cc319cd`), Paho2.1.0,
Mosquitto2.0.21. Local build and extraction are separately recorded Linux
x86_64/glibc2.41. The final CI package has its own build platform, hashes and
independent download/execution receipt; no ARM execution is claimed.

No strict cross-domain expiry, physical wire deadline, performance qualification,
power-loss proof or general MQTT certification. Will, TLS/authentication, shared
subscriptions, Subscription Identifiers, outgoingQoS2, MQTT3.1.1 and migration
remain separate. The next concrete functional increment is non-retained QoS1
Will on ungraceful transport loss, reusing lifecycle and admission authority.
