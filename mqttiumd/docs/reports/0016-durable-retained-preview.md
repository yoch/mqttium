# 0016 — Durable retained values in the local MQTT 5 preview

The normal Linux x86_64 package now supports publishing state before any
subscriber exists, ending the producer, restarting on the same database and
recovering that state through a new wildcard dashboard. Replacing one value and
publishing another with an empty retained payload produces exactly the new
current inventory. The archive includes the executable, database-preserving
launcher, actual-client demonstration, short guide, licenses/NOTICE, dependencies
and separate build/execution identity records.

## Received content and scope

The complete local source-pinned candidate is
`90736706400e58db97b4da9bd6826d6f4c0b586a`, descended from revalidated draft PR648
head `e8d446ebd0c3e9b753b9910f85bfb3d9e838bc47` and main
`874f298b4de7c3a13006cb1ff767dfb09e4031ac`. This report is the subsequent delivery
record. The published commit and its fresh CI artifacts must be read from PR648;
local candidate evidence is not relabeled as a later SHA's execution.

Contract digest changes from
`sha256:1889d1d070b3923095f17c6decb7e0cb29d8dfd8662e753b20a50f8c099228c5` to
`sha256:948a42f7161f97e3cd0cfe4d5b37bee5f9b56325b1a4b69f1c7124b97aac5412`.
Proposed0032 stays Proposed. Recipe tags10/11/12 extend format1 without changing
tags1–9. SQLite schema8 refuses historical formats before mutation; there is no
migration or reset. The executed product links bundled SQLite3.54.1, WAL,
synchronousFULL(2), autocheckpoint1000pages, page size4096. Normal instrumentation
is absent. New receptions are system tests; historical K1/K2b/K3 qualification
does not automatically qualify retained semantics.

## Behavior and authority

One exact topic owns one independent reference to its immutable current value.
The retained entry preserves producer ClientID even after that session ends.
QoS1/2 admission atomically mutates retained state, freezes complete live fanout,
accounts and receipt. QoS2 mutates at admission; late PUBREL only releases its
marker. Retained QoS0 has a real durable operation but no MQTT Packet Identifier,
ACK, exchange slot or Receive Maximum credit. Non-retained QoS0 remains volatile.
Client-side QoS0 completion is not evidence of broker commit.

Each ordered SUBSCRIBE filter occurrence applies RH0(always), RH1(only when the
literal filter is new at that step), or RH2(never). Every eligible occurrence and
retained topic may create a distinct replay, including intentional duplicates
from overlaps or repeated RH0. No Local compares producer and target ClientID.
Replay freezes version, bytes, QoS, RETAIN1 and a fresh delivery identity before
SUBACK. Live overlaps remain one/session with maximum eligible QoS and OR of
eligible RAP contributions. A later replacement/clear cannot rewrite old work.

Replacement/clear releases only the retained-owned reference; owed payloads
survive until their own terminal consumption/Finish. Replay reuses shared payload
storage but reserves fresh causal publication identities. The complete256-ID
range is burned, protected even on refused replay admission and covered before
receipt collection. Unknown results retain their operation and canonical bytes;
lookups/retirement guards prevent replayed operations from remutating newer state.
No consumed QoS0 permission is reconstructed from an old receipt.

## Finite reception

| Group | Actual local result |
| --- | --- |
| Demonstration | mqttium retained QoS1 with no subscriber, ephemeral producer destroyed, same-file restart, new wildcard recipient with RETAIN1; real Mosquitto publisher/subscriber and Paho inversion also pass |
| Matrix | 24 cases: incoming0/1/2 × granted0/1 × RAP0/1 × both mqttium/Paho directions, independent packet taps observing live, replay and empty clear |
| Options | RH0/1/2, ordered duplicate filters, overlaps, replacement and ClientID No Local after producer incarnation destruction |
| Replace/clear | New recipients see current values; RETAIN0 leaves them intact; older owed work retains its frozen version and payload |
| Capacity | 32topics/1MiB, net replacement at full, explicit Q1/Q2 negative reasons and Q0 capacity drop, all-or-refuse256candidate replay,32durably finished QoS1 replays to one target, clear returns capacity |
| Crashes | Six separate real SIGKILLs at prepared/committed admission(Q1/Q0) and subscription replay; each has its own PID, barrier operation, signal9, inode-preserving same-file restart and system witness |
| Unknown results | Three actual committed results dropped and resolved under the same operation: Q1, Q0 with source destroyed before resolution, and subscription replay; no second emission/materialization |

The optimized diagnostic run legitimately dropped one QoS0 replay under actual
output saturation, with named owner event `output_saturated`. It is not counted
as a durability failure. The decisive32-value test uses ordered QoS1 PUBACKs;
QoS0 loss under pressure remains explicit and observable. A demo-harness filter
initially created a legitimate offline obligation and was corrected to use
separate scopes, without discarding that responsibility.

Five compiled core/store controls first run a passing positive, then execute the
mutated test and require its exact business assertion:

- `retained_publish_did_not_enter_durable_admission`
- `replay_reread_current_retained`
- `clear_released_owed_payload`
- `old_retained_operation_remutated`
- `no_local_forgot_producer_client_id`

All five are detected, return101 only after a test executes, and restore their
isolated source trees. Compilation errors, unwrap panics, zero tests, missed
injections and timeouts are excluded. They are implementation controls, separate
from the four finite-model counterexamples. The new ownership model has seven
expected TLC outcomes (safety740states; one-crash abstraction2220); all match.
Existing97 model configurations are unchanged, total104 for the final CI gate.

Typed-reader regressions include physically healthy retained data with invalid
QoS, invalid/oversized producer fields, dangling versions and contradictory
replay cause/RETAIN. Both stores reject invalid data without treating it as an
empty inventory. Default extension fields are omitted from legacy snapshots;
the original14 canonical-evidence tests remain unchanged and pass with two new
serialization tests. The packaged log filename `events.jsonl` is now included
in directory schema scans, with an actual red/green regression. Startup console
text reflects hierarchical retained pub/sub; extraction records actual execution
platform independently of build platform.

## Bounds and retained limits

Current retained inventory:32topics and1MiB. Existing128references,4MiB payload,
128sessions,128subscriptions,8filters/request,16live destinations/publication and
8192receipt slots remain unchanged. Old owed versions remain charged. One
retained-QoS0 owner flight holds at most32768payload bytes and16targets while
uncertain. One subscription transaction plans at most256replays; reference
pressure can refuse the whole update. Existing eight-item work pages drain it.

QoS0 output uses consumption before one captured-generation emission; a crash
or saturation after consumption can lose it. Retained QoS0 also uses durable
live obligations, including bounded offline work. A fresh requested replay is a
new occurrence, not resurrection of a consumed obligation. QoS1 retransmissions
and lawful repeated admissions remain possible; this is not exactly-once
application delivery. Retained values survive session expiry and receipt GC.

No Message Expiry, Will, TLS/authentication, shared subscriptions, Subscription
Identifiers, outgoingQoS2, MQTT3.1.1 or general schema migration. Persistent
sessions are never silently evicted. Clock continuity retains the previously
received conservative scope. Linux x86_64 is exercised; no ARM execution claim.

## Reproduction and artifacts

```sh
cd mqttiumd
python -m pip install -r tests/system/requirements.txt
# Install your distribution's mosquitto-clients package.
tools/run_retained_reception.sh /tmp/mqttiumd-retained-reception
# Or extract the normal archive and run, without Git/Rust/laboratory:
./start.sh ./data
./demo.sh ./new-retained-demo-data
```

`demo.sh` records actual Mosquitto argv, client versions, fresh broker logs and
exact output comparisons; it preserves the SQLite file across its restart. The
normal launcher itself needs no Python/client/build tools. The short guide's
Mosquitto commands publish two temperatures before any dashboard, restart,
recover both, replace one and clear the other with `-r -n`.

Local candidate package SHA256
`6db04d12de570f5e3303056167b188d497b6e1343dab293275e6285f5e7cc70a`;
normal executable
`6c7af0a3439534ac4a27647d2066080998968bc567adc4ada525dd079bf8e530`;
separate system archive
`5c5af1b7a76a1d8d12cb6ff33459320e731054a987104e22eb0868cb460c1780`.
The receipt verifies5921 product events in26files, every retained member hash,
same normal bytes in reception and extracted package, six own-run signal9
witnesses and all five compiled controls. Build and extraction execution are
separately recorded Linux x86_64/glibc2.41. Clients: exact-checkout mqttium1.3.0
(62Python files, source digest
`f9d009f27270e491d05f4c37c3378f7ba4cc1a52fca1d6174c815c8f9cc319cd`),
Paho2.1.0 and Mosquitto2.0.21. Final CI records its own client/platform identities,
source-specific package/evidence hashes and downloadable artifacts; after
publication those exact bytes must be downloaded and exercised separately.

Local candidate gates:1024 normal Rust tests,1039 instrumented tests (sequential
in one checkout),108 product-hook tests, strict corresponding Clippy, formatting
and scenario validation. Independent oracle240tests,534corpus/405trace derivations,
556schema documents,130system-pilot tests and16model-reader tests pass; all
model metadata is consistent. The independent local audit verifies2212Git blobs
and modes,520kept file hashes and each of the six actual kill/wait witnesses. K2a/WAL, the long receipt and PI runs are not repeated locally for each
change; their existing CI gates remain required on the published SHA. No claim
of complete MQTT conformance, performance qualification or power-loss safety.

The next recommended functional increment is Message Expiry: first retain a
value, pass its deadline, then verify a new subscriber receives no expired
value. Its concrete integration points are recorded separately; it remains
explicitly refused in this preview.
