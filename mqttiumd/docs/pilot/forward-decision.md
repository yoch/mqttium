# Q9 deferred-delivery qualification

**GO with restrictions for deferred telemetry.** Eight sensors durably enqueued
the full 30-minute outage workload in their local outboxes. The same normal mTLS
broker and SQLite database recovered capacity when the absent persistent
dashboard resumed. With generation continuing at 50/s, all 113,470 accepted
events ultimately reached durable application commits on both dashboards.
Opening this pilot requires explicit operator acceptance of the measured
freshness and finite storage envelope below.

**NO-GO for independent real-time dashboard supervision.** The connected
dashboard went 1,798.11 seconds between new events when the shared reference
budget was exhausted. The outbox preserves data and delays admission until
capacity returns. A separate live and historical flow would need independent
routing and failure policies. Per-recipient admission would require a contract
for durable responsibility, publisher acknowledgements and missing recipients.

The original [direct-publication decision](decision.md) records the 128-reference
limit and the shared effect of an offline dashboard. Its measured evidence
remains attached to the earlier package revision.

For the deferred profile, the source records each event in its own SQLite
outbox before attempting MQTT. The producer's confirmed sequence advances after
a correlated positive PUBACK. Each dashboard separately records the event and
its envelope in a durable ledger before returning its own PUBACK. A negative
quota PUBACK only ends the current attempt. An unknown attempt retains the same
event ID on a fresh MQTT session. An identical repeat is acknowledged after the
dashboard has matched its existing committed bytes. A contradiction stops the
affected dashboard and preserves the original application row.

## Measurements on the reference machine

The control run at 50/s admitted 127 of 500 direct publications and refused the
remaining 373 with PUBACK `0x97`. The short outbox run accepted all 500 events
locally. At the outage boundary 127 had positive PUBACKs and 373 were pending
locally. After resuming the absent dashboard, both ledgers contained the 500
exact events and the producer rows and broker obligations drained to zero.

The decisive run used the frozen normal broker in one process with eight
separate producer processes. The absent dashboard was offline for 1,801.34 s
and resumed with Session Present 1. Producers paced the first 90,000 events
over the 1,800-second generation window. At the first outage-boundary sample,
90,018 events were committed locally, 127 had positive PUBACKs and 89,891 were
pending. The online dashboard had applied 127 events and the absent dashboard
zero. Generation continued at about 50/s throughout catch-up.

| Boundary or measurement | Observed result |
| --- | ---: |
| Events generated and independently enqueued | 113,470 each |
| Publication attempts and positive PUBACKs | 157,790 and 113,470 |
| `0x97` replies and new attempts | 44,320 each |
| Unknown publication results and other refusals | 0 each |
| MQTT messages received and unique applied events per dashboard | 113,470 each |
| Repeats, contradictions and invalid events in the normal run | 0 |
| Broker admission and recipient Finish commits | 113,470 and 226,940 |
| Maximum aggregate queue and final producer rows | 89,901 and 0 |
| Final broker obligations, claims, payloads and references | 0 each |
| Catch-up while generation continued | 466.86 s |
| Admissions, generation and net clearance during catch-up | 242.56/s, 50.02/s and 192.54/s |
| Best observed 60-second window while backlog remained | 253.16 admissions/s |

Both final consumer databases passed `quick_check`. The receiver checked every
sensor, stream and sequence, the full envelope and the generated body against
both ledgers. A separate read found zero per-sensor first-receipt order
reversals in either ledger. The normal broker event stream had no gaps or
duplicate command commits. The broker recovered the references needed for admission
capacity without a restart or quota change.

From event creation to receipt on the shared host, p99 was 1,779.83 s for the
live dashboard and 1,782.29 s for the returning one. Maxima were 1,800.29 s and
1,800.63 s. These timestamps precede the recipient transaction and the ledger
counts independently establish committed application. The separate inter-receipt
gap above uses the monotonic clock. The demonstrated freshness envelope
includes the outage and finite catch-up. Its acceptability is an operator
condition for delayed delivery.

The reference machine was Linux x86_64, kernel 6.18.57 and glibc 2.41, with an
AMD EPYC 9V74, a four-CPU cgroup quota, 16 GiB memory and overlayfs on an
unidentified backing medium. Python was 3.13.5 with SQLite 3.46.1 and Paho 2.1.0.
The broker and application SQLite writers used WAL and FULL synchronization.

| Store or process | Sampled peaks and total cost |
| --- | --- |
| Broker | 9.41 MiB RSS, 212 KiB main database, 4.02 MiB WAL, 684.01 CPU seconds and 23.49 GB process writes |
| Eight producers | 27.64 MiB largest process RSS, 4.21 MiB largest SQLite footprint out of 64 MiB, 256.19 CPU seconds and 4.96 GB process writes summed |
| Two dashboards | 26.75 MiB largest process RSS, 38.09 MiB SQLite footprint per dashboard, 96.36 CPU seconds and 4.78 GB process writes summed |

The largest producer backlog was 11,251 events on a single sensor. After
drainage the producer images had 805–806 reusable pages each, zero queued
rows and zero WAL bytes at rest. Producer truncate checkpoints counted 354–362
per writer. Each dashboard recorded 1,551 checkpoints and retained its full
deduplication ledger. The 2,203 samples cover SQLite page and WAL sizes,
oldest queued age, process counters and broker credits. Raw process write
counters include logs and SQLite and do not establish SSD wear. The JSON
report retains the per-process counters, budget observations, delivery latency
distributions and evidence hashes. See [forward-results.json](forward-results.json).

## Process recovery and controls

The normal mTLS broker was exercised with Paho 2.1.0. Process cuts used actual
SIGKILL and recorded the actor's PID, reached boundary and wait status. The
producer's sent cut ran after flushing the PUBLISH with the public write pump
and before pumping the read side. The broker and consumer ledgers independently
confirmed the first admission. The replay created one further QoS 1 admission
with the same event identity. Each durable dashboard applied it once and counted
the repeat. The positive-result cut preserved the committed local high-water
mark and only collected the old row on restart.

The combined quota, broker and drain case held a real `0x97` result before retry
scheduling, killed the producer, then killed the broker while the second dashboard
was absent. An independent authorized publication was acknowledged under pressure.
Both dashboards resumed on the same broker database. A further producer cut
during drainage recovered the same event ID and the final ledgers contained all
145 IDs. The dashboard cut after local commit withheld PUBACK. A restored inbox
recognized the broker retransmission with its original Packet Identifier and
`DUP=1`. The broker finished the responsibility after the replay was acknowledged.

A foreign publication closed the transport with a broker authorization event
at `0x87` and created no admission. An independent TLS reader observed
`e0028700`. The Paho callback reported its default code `0` for that short
DISCONNECT. The sender classifies the missing correlated PUBACK
as an unknown outcome. Actual QoS 1 PUBACK reason `0x97` reached the producer
callback and the durable retry logic. A different envelope with an existing ID
was rejected after the dashboard recorded the contradiction. Both original
application rows remained and neither conflicting obligation was acknowledged.

All six compiled Python controls failed on their intended assertions. They
covered collecting on a negative PUBACK, forgetting an uncertain event on
reopen, reapplying a duplicate, overwriting a contradictory payload, exceeding
the local disk budget and retrying without backoff. Syntax errors and timeouts
were excluded as detection criteria.

The default 64 MiB producer budget was also exercised to refusal in an isolated
local capacity test. Its 7,896-page main-image cap held 110,222 events. The
largest sampled main+WAL+SHM footprint was 33,104,920 bytes and the next enqueue
was refused. After 512 confirmed rows were collected, sequence 110,223 was
accepted in the same stream. The small-budget regression tests independently
check the bound and reuse. These local transitions establish storage behavior.
The mTLS campaigns establish actual MQTT admissions and recipient effects.

## Operating envelope

Use both provisioned persistent subscriptions, a stable database for each
producer and each dashboard, and a canonical private storage path per writer.
Track oldest queued age, enqueued-to-confirmed and confirmed-to-applied gaps,
quota and unknown results, conflicts, and disk space. On local pressure, pause
the generation source or keep its refused events externally. On a missing
dashboard session or a contradictory ID, stop and reconcile affected state
before restarting. A new empty consumer database would lose prior deduplication
authority. Restoring a stale producer image also requires sequence reconciliation.

The finite pilot ledger retains all event identities and payloads inside a
256 MiB SQLite budget per dashboard. Plan the number of events for the pilot
using measured account growth and preserve the ledgers for any replay horizon.
There is no automatic archival or pruning protocol. Keep the eight 64 MiB
producer budgets separate from host capacity for the broker, logs and backups.
The Linux overlayfs measurements give process I/O and logical image growth.
Flash endurance, Raspberry Pi and physical power-loss behavior require their
own evidence. See the [operator guide](../../preview/FORWARD.md).

## Provenance

The broker is the frozen normal executable from
`51be4079f7ee42f46500e993315eddc971f0a8d4`, SHA-256
`9ef097977c2cb86669e11e98d7fbceeac87a4f86fd6a24c7deb76ea9c31bf119`.
The measured producer, dashboard and campaign come from
`acdaf4641912ab7fb42f40f8159f79ed789efa67`. The sealed package checks the
component hashes in this report and identifies its own source separately in
`PACKAGE.json`. The broker contract hash is
`bf9bbc0a9d74a4a5b72330f60038b2f51189eca58b5784c6c40a54bfe7e22d1a`.
The observed schema was 13 and its reference limit remained 128. The system
receptions have application scope. The historical qualifying crash campaigns
retain their scope. The preceding sealed TAR, GitHub artifact `14448387514`,
is identified by
`83f0ab354cfd2d39193e3f8f1c9669a13088977c60a736575232fde54a5c47a5`.
