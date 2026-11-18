# Split-pilot qualification

| Decision | Scope |
| --- | --- |
| A — GO with restrictions for deferred collection | Both historical ledgers recovered every accepted event across a 30-minute dashboard outage at 50 events/s and drained while generation continued. The result is bounded by the received workload and declared storage. |
| B — GO for best-effort live supervision | The protected private topology kept LIVE fresh through HISTORY pressure and its separate restart. Sequence gaps remain visible and repairable from verified history. |
| C — NO-GO for strong real-time supervision | A LIVE failure produced an observable silence and missing QoS 0 deliveries. A single host remains a failure domain for both lanes. |

## Received results

| Observable | Result on the normal binary |
| --- | --- |
| Nominal rate and duration | 50/s for a 1,800 s HISTORY outage. Session resumed at 1800.82 s from the first scheduled event. Final drain observed at 2272.16 s. |
| HISTORY at return | 90,031 local commits, 127 positive PUBACKs, 89,904 pending and 19,023 Quota Exceeded replies. |
| Broker HISTORY pressure | Peak 128 outstanding references in the samples against a limit of 128. Two destinations need two free references for each new admission. |
| LIVE during the outage | 90,000 of 90,000 events. Creation-to-reception p50 4.08 ms, p95 6.02 ms, p99 8.85 ms and maximum 251.33 ms. |
| LIVE gap under pressure | At most 0.264 s between observations within the saturated interval. An independent TLS client obtained CONNACK and PINGRESP. |
| HISTORY catch-up | 469.29 s to the catch-up boundary. 241.59 admissions/s with 50.02 newly generated/s, a net 191.57/s. |
| LIVE after the outage | 23,606 observations with p99 7.60 ms and largest gap 93.69 ms. |
| Total at drain | 113,606 durable outbox enqueues and 113,606 positive HISTORY publishes. 43,940 quota replies, 43,940 retries and 0 unknown replies. |
| Durable processing | Each dashboard verified 113,606 identities and exact payloads. The broker counted 227,212 durable Finishes. Both historical obligation queues and all eight outboxes drained. |
| LIVE and history reconciliation | 113,606 observed LIVE events and 0 missing, all accounted for by each HISTORY ledger. Display state agreed with the monotone LIVE-only trace. |

The 500-event extracted-package witness observed 109
Quota Exceeded replies during saturation, 500 LIVE
deliveries and a LIVE p99 of 17.32 ms.
It drained both HISTORY destinations. The ten-second witness and the long run
have distinct receipts.

### Faults, rotation and restore

- SIGKILL after durable enqueue and before LIVE submission left the original
  event recoverable from both history ledgers. The same device's retained Will
  reached the LIVE subscriber in 25.26 ms while HISTORY
  was saturated. A late viewer obtained retained `offline`. The returning
  device restored `online`, which survived a separate LIVE restart. Normal
  disconnect removed the armed Wills and preserved the last application state.
- A HISTORY stop and restart left 41
  fresh LIVE observations in its 0.837 s
  interval. Stopping LIVE left the history pair advancing from
  [281, 203] to [336, 336].
  Both histories reconciled 64 LIVE omissions in
  the incident run. The recovered history did not update the LIVE view.
- Both brokers refused the foreign ClientID with CONNACK `0x87`. An actually
  retransmitted application identity from an archived range was acknowledged
  by both historical consumers without a second applied effect.
- Killing the archivist during file creation, after publication and after
  registration reached three distinct barriers. Each recovery retained all
  16 original events. Exact old duplicates and contradictory bytes were
  distinguished. Missing data and altered manifests refused old confirmations.
- The old ledger physically reached `SQLITE_FULL` at
  384,072 committed events and
  32,017 occupied pages under its
  32,018-page ceiling. This was a SQLite witness
  with the original schema, budgets and write path.
- The reduced new ledger rotated 128 times over 2,048 events with 32 hot
  rows allowed. It peaked at 10 allocated pages
  and exposed 2 reusable pages without
  VACUUM. A verified export restored all 2,049
  archived and hot records. A one-segment archive plus 32 hot rows preserved
  48 accepted events and refused the 49th. An out-of-order fixture left the
  archived prefix at zero until its missing predecessor arrived.
- Four compiled controls reached their faults and failed their intended
  checks. They tied LIVE to blocked HISTORY, collected before registration,
  skipped a missing sequence, and trusted an old identity without checking
  its original envelope.

### Observed storage and resources

The largest outbox occupied 4.21 MiB with a 64 MiB budget.
The long receiver configuration allowed 1,024 hot rows and 4 MiB of SQLite
plus sidecars. Peak warm rows were 455. The final compaction left
zero hot rows on both dashboards. Dashboard 1 retained
1,776 verified cold segments and dashboard 2
retained 1,776. Dashboard 1 charged
27.75 MiB against a 512 MiB archive budget.
Its compressed files and manifests occupied
10.77 MiB of logical file length
and 20.81 MiB of reported
file blocks. Record bytes compared to file lengths give a 2.74x
reduction for this fixture. SQLite page and WAL sizes, CPU, process writes and
RSS peaks for both planes and all sampled actors are in the evidence record.
The brokers peaked at 9.43 MiB RSS on LIVE
and 9.41 MiB on HISTORY.
[Measured results and trace hashes](split-results.json)

### Pilot restrictions

| Restriction | Signal | Response |
| --- | --- | --- |
| Bound capture to the received outage and storage envelope | Outbox age and depth, repeated `0x97`, `archive_pressure` or history apply lag | Reconnect the missing historical subscription, check net drain, throttle the source and export verified backups before exhaustion. |
| Treat the freshness view as best effort | Time since last LIVE arrival, `live_gap` ranges and a mismatch with the source's final frontier | Flag stale or missing measures and reconcile historical ranges separately from the displayed live value. |
| Reserve space for both authorities on the host | Free disk and WAL levels, archive charge and segment count, missing or unverifiable segments | Suspend an affected receiver and recover a verified copy. Rotate logs, protect backups and keep accepted records. |
| Review storage before a longer deployment | Measured bytes/day and usage against cold and warm limits | Use the finite retention plan in the operator guide. The campaign qualifies the measured run, with no multi-day or power-loss evidence. |

This lot uses the normal Linux x86_64 broker from
`51be4079f7ee42f46500e993315eddc971f0a8d4`. Its SHA-256 is
`9ef097977c2cb86669e11e98d7fbceeac87a4f86fd6a24c7deb76ea9c31bf119`.
The measured application is
`e44be3569379b21eec806e08c6173bb432c8c4cc`. The sealed package identifies
its own assembly revision separately. The executable, broker schemas,
MQTT quotas and canonical recipes have their previous identities.
[Application and evidence fingerprints](split-results.json)

## Workload and method

The reference host was Linux x86_64 with kernel 6.18.57, glibc 2.41,
AMD EPYC 9V74, four CPU equivalents under cgroup quota and a 16 GiB memory
limit. The filesystem was overlayfs with unspecified backing storage.
The broker used embedded SQLite 3.54.1 with WAL, FULL synchronization and
autocheckpoint at 1,000 pages. The clients were Python 3.13.5 with
SQLite 3.46.1 and Paho 2.1.0. Both sockets negotiated TLS 1.3.
Short functional and validation jobs shared the host for parts of the
outage. Catch-up ran without those jobs.
[Measured machine and source manifest](split-results.json)

At 50 events/s, eight independent producer processes committed 256-byte
telemetry envelopes to their own outboxes. Each producer offered the same
envelope to LIVE at QoS 0 without waiting for HISTORY. Each HISTORY sender
had one QoS 1 attempt in flight. Two HISTORY consumers committed effects
before sending PUBACK. The LIVE subscriber used a fresh ephemeral session.
Both brokers used schema 13, each on its own file with its own PID and
listener. No broker knob changed.

The outage starts with both historical subscriptions established and one
dashboard disconnected. The source runs 30 minutes at the nominal rate.
The dashboard then resumes its original session while generation continues.
After catch-up we stop generation and verify every event's identity and bytes
against both committed history ledgers and the independent LIVE trace.
The broker journal tally is read across rotation and checked against the
final durable counts. Latency uses creation and arrival from the same host.
Spacing between arrivals uses the monotonic clock. RSS and SQLite/WAL sizes
are sampled, so peaks are observed values at sample times. Process I/O
counters include logs and other writes and give no measurement of flash
wear. On this single host, both processes share hardware and storage.

## What the archive commits

A dashboard advances an archived prefix by contiguous ranges per stream.
It keeps out-of-order rows hot until the hole is filled. Every segment has
a bounded set of complete original envelopes and a manifest identifying its
range and SHA-256s. Publishing a segment synchronizes its files and directory.
Registration advances the frontier in a SQLite FULL transaction. Only after
that commit does a second transaction delete covered hot rows. A published
orphan is rechecked against its still-durable hot rows. A registered range
with a remaining hot copy is collected after verification. Missing or
contradictory evidence suspends the affected receiver without a successful
ACK.

Repeated events are checked against exact hot or archived envelope bytes.
The archive index stays bounded by the finite stream and segment limits.
After exhausting its cold and then warm budgets, the receiver preserves
accepted records and applies backpressure. Operator export and restoration
preserve the SQLite image and the referenced segments. Export retains the
original files. The operator must plan the finite retention window and
protect the backup separately.
[Operator guide](../../preview/SPLIT.md)

The process-cut tests do not exercise power loss. Cold storage is bounded,
and 72 hours is a sizing horizon rather than a measured run. The default
allocation reserves 8 GiB archive and 32 MiB hot SQLite per dashboard plus
64 MiB outbox per sensor. Add both broker images, WAL, rotated logs,
filesystem overhead and separate backup copies. At 50 events/s the
uncompressed event envelopes alone total 1.106 GB/day per full historical
copy. The received compression ratio reflects the synthetic fixture.

## Reproduction

Run from an extracted sealed pilot package. Use a private output directory
and keep any real keys and telemetry out of shared diagnostics.

```sh
sha256sum -c SHA256SUMS
python3 -m venv .venv
.venv/bin/pip install -r pilot/requirements.txt
PYTHON="$PWD/.venv/bin/python" ./run-split-reception.sh ./qualification-short "$PWD"
PYTHON="$PWD/.venv/bin/python" ./run-split-reception.sh ./qualification-full "$PWD" full
```

The operator launcher check starts both lanes from the sealed package, probes
TLS and MQTT, and restarts both on their original SQLite files with fresh
journals. Candidate `289ca304` failed with `events_unavailable` because the
launcher created the journal before the broker's exclusive open. The updated
launcher creates a private directory and passes an unused journal path.

The 30-minute outage acceptance is a system test. It leaves the historical
K1/K2b/K3 qualifications at their original scope. No broker or contract
change was required.
