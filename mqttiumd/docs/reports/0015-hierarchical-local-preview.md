# Hierarchical subscriptions and a runnable local preview

The normal Linux archive is published separately as the PR648 workflow artifact
`mqttiumd-local-preview-linux-x86_64`. Its inner tarball contains the executable,
SHA/source/platform metadata, runtime dependencies, upstream licenses and NOTICE,
a launcher preserving its database, and a short Mosquitto-client guide. The PR
publication record supplies the exact SHA-specific artifact link and checksum;
this is a local plaintext MQTT5 preview, not complete conformance or a Mosquitto
replacement. Proposed0031 and all earlier proposals remain Proposed.

## Use it

Extract the inner archive, enter its directory and run `./start.sh ./data`.
In two other terminals, use the bundled README commands for
`atelier/+/temperature` and `atelier/#`, then publish numbered temperatures and
commands with `mosquitto_pub -V mqttv5`. Both subscribers use explicit identities,
Clean Start0 and infinite expiry. Wait for SUBACK before publishing. The guide
also replaces a wildcard with an exact filter using a literal UNSUBSCRIBE.
Stopping/restarting the launcher preserves `data/broker.sqlite` and creates a new
log directory. No Git, laboratory, hooks or compiler is a runtime dependency.

## Implemented behavior

One allocation-free selector scans at most128 routes for both volatile QoS0 and
durable QoS1/2 admission. It matches levels without regex, recursion, backtracking
or normalization, including parent `a/#`, empty levels, Unicode and leading `$`
rules. No Local removes each matching subscription's own contribution before
maximum eligible QoS is taken. One distinct, current-revision SessionId contributes
one target, capped by published QoS/outgoing1. Targets are sorted by SessionId.

UpdateSubscriptions preserves QoS, No Local, RAP and RH durably. Replacing a
literal updates one row, duplicate filters in a packet use the final options,
and UNSUBSCRIBE removes only the recorded literal. Known commit installs the
prepared projection before positive SUBACK/UNSUBACK; both publication paths wait
for an unresolved routing cut. Existing admissions retain their original targets
through resolution, replacement, reconnect and unsubscribe.

Quotas remain128 sessions,128 subscriptions,8 filters/request,16 distinct
publication targets and8192 receipt slots. The existing per-literal16 target
limit also remains. Dynamic fanout is checked after eligible aggregation and
before reservation/output. Seventeen eligible targets drop the complete volatile
occurrence observably; durable overflow closes the source with a recorded
`capacity_exceeded`, no positive publication ACK and no partial admission.
Sixteen targets work. No Local can reduce17 raw matches to16 eligible targets.

Valid RAP/RH values are preserved within the non-retained profile; they cannot
create retained replay or retained output. Reserved bits, QoS3/RH3 and shared
NoLocal are protocol errors. Shared subscriptions and Subscription Identifiers
remain unavailable; CONNACK advertises wildcards available, retain/shared/IDs
unavailable. Session expiry, live receipt retirement, protected PI reuse and
typed durable reads retain their separate contracts.

## Executed reception and source attribution

The complete source-pinned local candidate is
`4a3ddefb7dfcd102d36bb0c7cf35bfb7c82fc057`. All eight normal groups, three
actual-writer groups, four compiled controls, strict product-stream validation
and extracted-archive smoke pass. Its normal binary is
`sha256:a721606de921899a1df961db7e80a87075df54bcf5f503bece5ebcc2f5e59be9`;
its runnable tarball is
`sha256:77baf7ff3c723b486e7125f73d01adcd091e136a71906d0275f65273b13da9e8`;
the separate system archive is
`sha256:709df44a81f8200a5c10fa6e0c3af32194d7f176d4a3a2e0fd1a0236f93e5d0d`.
The final CI publication rebuilds fresh artifacts at its own source SHA; the PR
record provides their exact downloadable links and hashes.

An earlier2abe162e run passed the functional groups but failed strict validation:
`qos0_dropped` contained a digit excluded by the event-name pattern. It remains
a failed reception. RED030965c9 fails specifically
`qos0_event_name_rejected_by_schema`; GREEN3b39cba4 permits bounded internal digits
and still refuses a leading digit. The complete4a3ddefb run above verifies the
correction on8598 actual product events with no skipped records.

| Group | Observation actually exercised |
| - | - |
| Three clients | mqttium1.3.0 from the identified checkout, Paho2.1.0 and Mosquitto clients2.0.21 exchange numbered messages; independent packet taps count one output under exact/+/# overlap. Roles are reversed in the matrix. |
| QoS matrix | All six published0/1/2 × granted0/1 cells in each mqttium/Paho direction, with actual subscriber receipt and independent outgoing QoS/RETAIN/PI observations. |
| No Local/options | Self versus other producer, high-QoS excluded contribution plus low-QoS eligible one, literal replacement, valid RAP/RH, and restart preservation. |
| Grammar | Independent edge table, malformed filters/options, shared refusal and ordered SUBACK results. |
| Offline/expiry | Original wildcard obligations survive a real SIGKILL and same-file restart; same session resumes without SUBSCRIBE. Finite expiry later removes its routes and a new incarnation gets SP0. |
| Fanout |17 eligible destinations: complete QoS0 drop and QoS1/2 refusal with zero business-state delta;16 actual deliveries durably finish.17 raw NoLocal matches reduce to16 eligible destinations. |
| Pressure | A real nonreader with1024-byte requested receive buffer causes `output_saturated` after bounded32768-byte publications. Independent numbered traffic and PING progress. |
| Update boundaries | Real SQLite `subscription.prepared`, old projection read independently, no SUBACK/volatile bypass, PING progress. Genuine lost committed update resolves the same OperationId and canonical recipe. An older QoS2-created QoS1 claim survives replacement/unsubscribe and finishes with its own manual PUBACK. |
| Archive | Fresh extraction executes the packaged normal bytes, overlap-once delivery, offline work, clean same-file broker restart/SP1 and final durable drain. |

The normal reception and archive smoke use the **same packaged release bytes**.
Each process has its own status/events and optional same-run strace; the restart
case records signal9. Commands, source archive, imported-client identities,
effective parameters, network bytes, SQLite facts and file hashes are retained.
Client-library receipts are not substituted for recipient observations.
The local target was x86_64 Linux/glibc2.41; the downloadable CI build records its
own Ubuntu24.04 environment. No ARM execution was performed.

## Discriminating tests and exploratory measurement

Core RED8f9cd909/GREEN07a20b32 establishes aggregation; store
RED621157a2/GREEN1793e406 establishes persisted options. Size-diagnostic
REDb6d0ad00/GREEN5b9aee0e checks a field limit before grammar classification.
Independent generated language tests compare909 grammar-built filters against583
topics (529947 pairs) using level substitution/suffix expansion as the expected
set, not a second call to the implementation matcher. Core tests additionally
cover120 route permutations,16/17 targets, No Local maximum QoS and immutable
admissions. SQLite/Memory tests cover options, replacement, typed corrupt reads,
same-operation resolution and old-obligation QoS. Schema6 refusal preserves bytes.

Four compiled mutations require exact assertion failures: missing aggregation
(`wildcard_routes_not_aggregated`), lost maximum QoS
(`wildcard_maximum_qos_lost`), silent fanout truncation
(`wildcard_fanout_truncated`), and whole-session self suppression
(`no_local_applied_after_aggregation`). Each exact positive runs first; the changed
binary must compile and fail the expected test. Source is restored between probes.
Compile errors, missing injections and unexplained timeouts are no detections.

A finite model adds seven expected TLC outcomes: safety, non-vacuity and five
intentional counterexamples. It abstracts a fixed match relation, route-order
folding, No Local, update cuts and frozen obligations. It is not a universal
matcher or runtime proof; existing90 configurations are unchanged (total97).

Eight exploratory runs compare exact/wildcard tables of1/16/64/128 routes with
one destination,16 numbered QoS1 publications and16 known Admission/Finish
commits per run. The source4a3ddefb run measured median publisher-ACK latency
(ms), exact6.154/6.145/6.499/6.245 and wildcard6.364/6.556/6.368/6.513.
It includes TCP, FULL SQLite commits and shared-host scheduling, not isolated
matcher CPU time or a performance qualification. RSS, database pages/free pages,
WAL, effective SQLite parameters and all samples are retained. No comparison
with differently configured Mosquitto is made; this does not justify a trie refactor.

## Formats, checks and reproduction

Default-options kind7 canonical bytes and legacy kind3 remain unchanged.
Nondefault options use explicit kind9; all-default kind9 is noncanonical and
removal cannot carry ignored options. Default0 JSON fields are omitted for
legacy fixture compatibility; nondefault fields are validated. SQLite schema7
adds route options and refuses1..6 before mutation, without migration/reset.
Historical `LiveExact` cause byte1 now explicitly denotes the live routing cut,
including hierarchical selection. The transition register is descriptive and
partial, never a complete guard partition.

Starting contract digest:
`sha256:30874671202259bb8387e30aa427f1067d685e9b195d37ed3ba4a685e249fdb7`.
New contract digest:
`sha256:1889d1d070b3923095f17c6decb7e0cb29d8dfd8662e753b20a50f8c099228c5`.
`validate`, every fresh qualifying campaign and the PR publication record check
this identity. No historical fingerprint is bypassed.

Local integrated checks: formatting, strict workspace Clippy,995 normal Rust
and1010 instrumented Rust tests (sequential),228 oracle tests,112 system-pilot
tests after the event-name regression, corpus/trace regeneration and both schema
readers. Boundary purity checks pass and proof-protected/French-archive files are
unchanged. Final CI repeats its full gates, K1/K2b/K3 qualifying campaigns,
receipt/PI/pubsub/session/expiry receptions and all97 model outcomes on its exact
SHA. Those qualifying campaigns retain their own scope; these new routing
scenarios are system tests and cannot automatically qualify P1/P2.

```bash
# Install pinned Rust/Python requirements and Mosquitto clients first.
cd mqttiumd
python -m pip install -r tests/system/requirements.txt
tools/run_wildcard_reception.sh /tmp/mqttiumd-hierarchical-preview
# The output includes preview/*.tar.gz, normal/boundaries/preview-smoke receipts,
# four mutations, product-event-streams.json, index.json and SHA256SUMS.
```

General migrations, retained messages, Will, TLS/authentication, shared filters,
Subscription Identifiers, MQTT3.1.1 and outgoing QoS2 remain separate. Valid
persistent sessions are not evicted to free capacity. Clock-domain continuity
remains conservatively unrecognized in current test environments; no new
wall-clock expiry guarantee is claimed. The single next functional increment is
[durable retained messages](../reception/retained-next-increment.md).
