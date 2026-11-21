# Private live and history pilot

Use the sealed Linux x86_64 pilot archive with a compatible glibc. It carries the
qualified normal broker and a separate Python 3.11+ application. Install OpenSSL
and the utilities `stat` and `findmnt`. The included reception uses Paho 2.1.0
and generates temporary loopback credentials. Keep real CA and device keys
outside the archive.

```sh
sha256sum -c SHA256SUMS
python3 -m venv .venv
.venv/bin/pip install -r pilot/requirements.txt
PYTHON="$PWD/.venv/bin/python" ./run-split-reception.sh ./split-short "$PWD"
```

The short reception starts two brokers, eight sensor processes, an ephemeral
LIVE dashboard and two persistent HISTORY dashboards. One HISTORY subscriber
goes offline. After ten seconds it resumes and both historical receivers
reconcile all 500 events. The LIVE dashboard observes the new measures while
HISTORY returns Quota Exceeded. The incident suite kills a sensor and each
broker separately. The archive suite checks durable handoff, rotations and a
restored ledger. Use the final argument `full` and a new output directory to
also reproduce the old ledger's physical ceiling and the 30-minute outage
with a measured catch-up. The detailed decision is in
[qualification/split-decision.md](qualification/split-decision.md).

## Start a private fixture

Generate separate demo keys and policies and start the two processes in
separate terminals. The working directory must be new for provisioning.

```sh
.venv/bin/python pilot/split_site.py provision ./private-split
.venv/bin/python pilot/split_site.py broker ./private-split live
.venv/bin/python pilot/split_site.py broker ./private-split history
```

LIVE listens at 127.0.0.1:8883 and HISTORY at 127.0.0.1:8884. The launcher
creates a new event log on each start and reopens that lane's SQLite file.
Both listeners require TLS 1.3 and a verified client certificate. Each device
publishes only below its lane's device grant. Each broker has its own
certificate pins, authenticated sessions, writer and quotas, including the
limit of 16 TCP connections. A matching ClientID on the other listener has no
authority over this store. Keep the brokers' files, credentials and backup
sets distinct.

Start both history receivers before generating telemetry. Their logs must
show `history_ready` after SUBACK on first use. On reconnect they require
Session Present. The live receiver always requests a fresh ephemeral session.

```sh
.venv/bin/python pilot/split_client.py history --id dashboard-1 --port 8884 \
  --pki ./private-split/history --db ./private-split/dash-1-history.sqlite
.venv/bin/python pilot/split_client.py history --id dashboard-2 --port 8884 \
  --pki ./private-split/history --db ./private-split/dash-2-history.sqlite
.venv/bin/python pilot/split_client.py live --id dashboard-1 --port 8883 \
  --pki ./private-split/live --db ./private-split/viewer
```

There is no database for the ephemeral viewer. It emits `live_metrics` and
reports sequence ranges in `live_gap` when a later observation exposes a gap.
The schedule in `pilot/split_reception.py` drives eight copies of the producer
below. For a product integration, the sensor calls `SplitOutbox.enqueue` with
its data after constructing the same live and history workers as the reference
actor.

```sh
.venv/bin/python pilot/split_client.py producer --id capteur-1 \
  --pki ./private-split/history --port 8884 \
  --live-pki ./private-split/live --live-port 8883 \
  --db ./private-split/capteur-1.sqlite --schedule ./private-split/schedule.json
```

The schedule has `start_ns` in this host's monotonic clock, `rate` in events
per second across eight participants and `events` as the total cutoff. Each
actor generates its assigned ordinals. Start with a new fixture when changing
the synthetic workload. Use `live_producer_ready` and `producer_ready` as
separate readiness signals. The packaged reception handles scheduling and
affinity to the two endpoints automatically.

## Track the separate outcomes

| Observation | Meaning |
| --- | --- |
| SQLite outbox commit | Local acceptance. The sensor can resume the same event after a process kill. |
| QoS 0 LIVE observation | A fresh best-effort sample. LIVE may drop under delay, pressure or disconnection. |
| Positive HISTORY PUBACK | The broker accepted the historical publication. |
| HISTORY receiver commit | This event has been durably applied, or its exact bytes verified as a repeat. The client then acknowledges. |
| Verified history segment | Still the same application effect. Hot rows can be collected after registration. |

Both topics carry the exact same 256-byte envelope and its stable
`ClientID/stream/sequence` identity. The LIVE connection sends previously
unattempted, recent samples from a queue of 32. A sample older than 200 ms in
that queue is dropped. A LIVE failure leaves the outbox and HISTORY sender
working. HISTORY retries `0x97` with bounded jitter using the existing FIFO.
Local disk pressure refuses new outbox enqueues. Slow the source or fix its
storage before retrying a refused input.

The viewer's latest values only advance on LIVE input. Use recovered HISTORY
records to fill analytical gaps in a separate view. For tail losses, compare
the viewer's final per-stream sequence against a known producer frontier.
QoS 1 can retransmit. Both hot and archived duplicates must agree with the
original envelope to avoid reapplying an effect.

LIVE publishes retained `online` and installs retained Will `offline`. A
late subscriber gets durable presence. Voluntary MQTT DISCONNECT cancels the
Will. Publish retained `offline` explicitly before a planned departure when
required by the application.

Monitor `live_gap`, age of the last observation, `history_retry`,
`archive_pressure`, outbox depth and the last positive PUBACK. LIVE can
itself become unavailable. Independent writers leave the host, disk, CPU,
network and PKI as shared failure domains.

## History retention and recovery

Per dashboard the defaults are 32 MiB of budget for SQLite plus WAL and SHM,
8,192 hot rows, 512 events per segment, 64 streams, 32,768 segment references
and 8 GiB of charged archive space. The reception uses a smaller hot row cap
and segment size to exercise rotation. A segment is charged for the data and
manifest rounded to 4 KiB and for directory overhead. Monitor actual available
disk as well. Keep sufficient space for SQLite, backups, the filesystem's
metadata and other writers.

The design horizon for this pilot configuration is at most 72 hours at the
nominal 50/s, subject to free space and all budgets. This is a planning ceiling
with no automatic time-based deletion. The measured duration is in the
qualification report. Plan scheduled export and surveillance early, and
stop admitting new measurements before any storage budget is exhausted.
Keep all accepted records until operator retention is satisfied. The reference
export does not reclaim cold files on the serving ledger. A deployment that
requires indefinite admission needs an archive retention and handoff policy. The
split fixture starts each stream at sequence 1. Existing Q9 dashboard ledgers
use the old format and must be kept separately. This package does not convert
them or seed a prefix from unverified history.

On archive pressure the receiver keeps committed hot rows. When the warm
budget can no longer admit a new event, it closes without ACK and retries;
HISTORY keeps existing responsibilities and producers eventually backpressure.
On a missing or corrupted segment suspend the affected ledger and restore
a verified copy. Never advance a contiguity frontier across a missing sequence
or reset a ledger to work around pressure. A lost source stream or permanent
gap may exhaust the warm space; reconcile the missing sequence under
operator control before continuing.

Stop the history receiver before verifying, exporting or restoring its store.
Keep the SQLite file beside its `.segments` directory and do not rename
only one. SQLite backup includes committed WAL state. Immutable files and
their manifests are checked against the catalog. Destination parents must be
private and destinations fresh.

```sh
mkdir -m 700 ./private-exports
.venv/bin/python pilot/segment_admin.py archive ./private-split/dash-1-history.sqlite
.venv/bin/python pilot/segment_admin.py backup ./private-split/dash-1-history.sqlite \
  ./private-exports/dashboard-1-backup
.venv/bin/python pilot/segment_admin.py restore ./private-exports/dashboard-1-backup \
  ./private-exports/dashboard-1-restored
.venv/bin/python pilot/segment_admin.py verify ./private-exports/dashboard-1-restored/history.sqlite
```

Run `segment_admin.py repair DATABASE --sensor capteur-1 --stream HEX --first N
--last M` for a known gap. It reads verified historical identities and returns
their envelopes and any remaining gaps. Protect its output as telemetry data.

Back up each broker with its corresponding policy and credentials using
the offline SQLite procedure in [PILOT.md](PILOT.md). Producer outboxes use the
separate SQLite backup command in [FORWARD.md](FORWARD.md). A restore is an
exclusive, coordinated replacement of the affected role. A stale copy may
lack effects acknowledged later, so do not use it to rewind a live dashboard
or producer. The process-cut tests give no result about loss of the machine or
power-loss durability on a storage device.
