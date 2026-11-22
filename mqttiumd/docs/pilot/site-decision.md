# Private pilot release candidate

**GO with restrictions for the bounded reference pilot.** The supervised
loopback mTLS deployment, HISTORY backlog and exact identities were recovered
within the received campaign. The application is the paced reference source.
A deployment on a designated physical host needs its own storage reception.
The 72-hour endurance target and autonomous recovery without the original
authority are still open. Strong real-time use remains NO-GO.

The measured operation layer is `5f37e61735a6590675e7ed8b1bf103e4050c28bf`. Its
component digests and the sealed package tie are in the results. Both roles
use the qualified Rust executable from `51be4079f7ee42f46500e993315eddc971f0a8d4`
with SHA-256
`9ef097977c2cb86669e11e98d7fbceeac87a4f86fd6a24c7deb76ea9c31bf119`.
The new layer runs the application and operator processes. The broker,
quotas, MQTT contracts and ADRs retain their baselines.
[Raw receipt fingerprints and machine details](site-results.json)

## Endurance and incidents

| Check | Received result |
| --- | --- |
| Observed reference campaign | 43 min 38 s from the start of paced generation to the first complete drain, including the planned component restarts. Eight sources at a scheduled total of 50/s. |
| Nominal then HISTORY saturation | Five minutes scheduled with both consumers, then the second consumer intentionally offline for at least 1800.87 s. Saturated sample had 89,910 pending and 127 broker references. |
| LIVE freshness in the outage | 90,000 of 90,000 expected, p99 8 ms rounded up, longest interval 385.88 ms. |
| HISTORY catch-up | 502.84 s at 228.80 admissions/s while generating 50.00 new events/s. Net drain 178.80/s. 45,796 Quota Exceeded replies observed by that point. |
| Isolated lane restarts | Both planned broker stops preserved measurable progress on the other lane. Each resumed its endpoint and reopened its original file. |
| Process supervision | The killed sensor's child exit was SIGKILL and the supervisor relaunched it. An independent HISTORY consumer restarted and recovered its original session. Zero unexpected primary service restarts were observed. |
| Acceptance and uncertainty | 130,893 distinct source events drained before the charged snapshot workload. 0 unknown source results had been seen by that drain. The restored ledgers were checked against the complete sequence of envelopes. |
| Sampled invariants | 408 observations, peak 128 HISTORY references. SQLite and cold byte and row bounds held in those observations. Foreign ClientIDs with valid client certificates got MQTT 5 `0x87` on both listeners. |

The LIVE test checks the scheduled outage ordinals. The latency buckets use
the same host's clocks, with millisecond ceilings. The observed gap and p99
met the 2 s and 250 ms objectives. The LIVE broker's own planned stop
interrupted that stream. Source and consumer restart semantics keep
the durable outbox, receipts and ledgers as their previous authorities.
[Phase witnesses](site-results.json)

## Operated recovery

The charged backup preserved 273 outbox items while
one HISTORY receiver was held. The site quiesced with its known broker backlog
still present. The tool kept twelve separately consistent SQLite backups,
policy digests, two cold ledgers, and the source and applied frontiers.
No private key was packaged. There is no distributed database transaction.

The original remained available offline for the explicit handoff. Its raw
database and WAL signatures had to match the snapshot boundary. The tool
fenced that site before activating the restored copy. Replay of the same
handoff was idempotent. All roles started from the copy and delivered the
remaining records, then added 200 events on the same source streams.
Both HISTORY ledgers verified 131,493 full event
envelopes and exactly the same identity sequence. The common envelope digest
is `c72a49cda39b95e4320597956a9190a086af8bcd4e48cd40bad78abe5440ae69`.

The refused-restore case used an older backup after the original accepted new
events. It left the original active-capable and the restored copy fenced.
A missing broker SQLite was refused without a replacement file. The separate
session-loss witness observed an explicit alert and suspended producer
admission. An altered segment suspended its consumer. The outbox pressure
witness verified each 70/85/95% notice on the effective SQLite page ceiling,
then observed a typed refusal with all 1,260
committed events retained. A test also reproduced the transient absence of
the active journal during rotation. The fixed collector observed all three
fixture records without restarting or skipping one.
[Operator workflow](../../preview/SITE.md)

## Archive geometry and storage

The accelerated single-ledger fixture archived **2,097,152 events** in 4,096
full segments. Hot batches used FULL transactions and the normal archive
publication path. Cold files used 179.95 MiB of apparent bytes and
224.19 MiB of reported file and directory blocks. A verified open took
8.83 s. Verified duplicate lookup took 2.49 ms at p50 and 3.22 ms at p95
over 72 lookups.
Export and restoration took 33.08 s and 24.91 s.
Both missing and altered cold files were detected.

The dense fixture also attempted to add enough small segments to fill the
catalog. That combined run stopped at its free-space floor after the dense
restore completed. A separate geometry witness reached **32,768 segments**
and 65,536 files with mostly one-event segments. It observed
`archive_budget_exhausted` at the next compaction, preserved one hot event
and verified that event again after reopening the ledger in 2.48 s. The
dense and catalog limits were exercised on separate databases. Endurance
at the full archive geometry remains open.

In the operated campaign, the sampled peaks per receiver were 4,044
hot rows, 256 cold segments and 14.00 MiB charged cold space. The largest
sampled producer footprint was 4.19 MiB. The lowest available filesystem
space was 270.33 MiB on the shared reference volume, past the critical
95% alert. The HISTORY processes peaked at 9.94 MiB RSS. Summing the last
sampled process counters by PID gives at least
23.94 GB of logical writes and 26.87 GB
reported as process write bytes across the observed HISTORY generations,
including recovery. Final writes between samples are excluded.

The execution mount was `overlay[/workspace]`, filesystem `overlay` and
device tuple `0:27`, with no observable mapping to a physical drive. The
local 256-sample probes observed fsync p99 1.202 ms and SQLite FULL WAL
commit p99 0.841 ms.
The bench records its passive checkpoint cadence and visible I/O counters.
These are overlay and process observations. Physical throughput attribution,
SSD wear and power-loss durability remain unqualified. Shared validation
work ran during some early portions of the campaign. The reference host had
four CPU equivalents under its cgroup quota.
[Storage and per-process observations](site-results.json)

## Scope of use

- Keep LIVE and HISTORY on the two named authorities. Admit the bounded
  scheduled workload on a monitored single-host loopback site. Validate the actual
  device source adapter and physical volume before field operation.
- Alert on missing view updates, undrained queues, unexpected causal gaps,
  70/85/95% storage or page pressure and lost archive integrity. A genuine
  integrity fault suspends admission. Keep enough storage for both cold
  copies, both SQLite families, checkpoints, logs and the verified export.
- At the configured limits, plan for 16 GiB of charged cold space,
  512 MiB of producer SQLite budgets and 64 MiB of hot ledger budgets.
  Add both brokers, WAL, journals, filesystem overhead and a separate
  verified backup. Those are configuration ceilings; the exercised disk
  and file counts are in the results.
- Qualify longer operation on identified storage. The bundled full kit
  schedules a 72-hour run and uses disposable seven-day certificates. The
  result here qualifies only the elapsed campaign. An unverified cold
  rollover, or stale/wounded authority recovery, still requires a separate
  operational procedure.

Run from the extracted package for the short live witness and the actual
archive and storage tools.

```sh
PYTHON="$PWD/.venv/bin/python" ./run-site-reception.sh ./site-check "$PWD"
.venv/bin/python pilot/rc_reception.py ./measured-site --duration 2100 --outage 1800
.venv/bin/python pilot/archive_scale.py ./dense --events 2097152 --ceiling 4104
.venv/bin/python pilot/archive_scale.py ./catalog --events 4096 --ceiling 32768
.venv/bin/python pilot/storage_check.py ./storage
```
