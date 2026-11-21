# Deferred telemetry with durable producer outboxes

Use `qualification/forward-decision.md` for the Q9 decision and the measured
outage, recovery and freshness limits. The package preserves the qualified
normal executable identified by `BUILD.json`. `PACKAGE.json` identifies the
packaged source and the application components that ran the qualification.

## Run the demonstration

On Linux x86_64 with a compatible glibc, install Python 3.11 or newer, OpenSSL
and the utilities `stat` and `findmnt`. Use an empty private directory for
each reception. The scripts create local demo keys at runtime and connect over
loopback mTLS. Use the sealed pilot archive linked from the pull request for
this reception. The harness checks the exact qualified executable hash.

```sh
sha256sum -c SHA256SUMS
python3 -m venv .venv
.venv/bin/pip install -r pilot/requirements.txt
PYTHON="$PWD/.venv/bin/python" ./run-forward-reception.sh ./q9-short "$PWD"
```

The short reception offers 500 direct QoS 1 publications at 50/s while one
persistent dashboard is offline. It then generates 500 events through eight
independent SQLite outboxes on a fresh instance of the same broker version.
Both dashboards must contain the 500 IDs after the absent dashboard resumes.
The process-cut suite checks replay and deduplication on the same databases.
Run `./run-forward-reception.sh ./q9-full "$PWD" full` with the same `PYTHON`
environment variable for the 30-minute outage and the measured catch-up.
The full run allows up to an hour of catch-up while generation continues.

## Install the reference applications

Use the ten-identity PKI and `start-tls.sh` commands in [PILOT.md](PILOT.md).
Provision both dashboard subscriptions before starting producers. Start these
commands in separate terminals from the extracted directory:

```sh
.venv/bin/python pilot/forward_client.py dashboard --id dashboard-1 \
  --db ./private-data/dashboard-1.sqlite --pki ./private-pki
.venv/bin/python pilot/forward_client.py dashboard --id dashboard-2 \
  --db ./private-data/dashboard-2.sqlite --pki ./private-pki
```

Wait for both `dashboard_ready` events before sending telemetry. Each dashboard
establishes a persistent exact-depth wildcard subscription to
`atelier/+/telemetry`. An existing dashboard database requires Session Present
on reconnect. A missing broker session stops the receiver and calls for
reconciliation of the affected responsibilities.

Queue a sample with the producer stopped, then start the sender. The network
commands default to port 8883. Set `--host` and `--port` to the broker address
covered by its certificate when using a private network with real credentials.

```sh
.venv/bin/python pilot/forward_client.py enqueue --id capteur-1 \
  --db ./private-data/capteur-1.sqlite 'temperature=21.4'
.venv/bin/python pilot/forward_client.py producer --id capteur-1 \
  --db ./private-data/capteur-1.sqlite --pki ./private-pki
.venv/bin/python pilot/forward_client.py stats ./private-data/capteur-1.sqlite
```

For a continuous telemetry source, call `forward_store.Outbox.enqueue(bytes)`
from the application's generation thread and run `forward_client.send_pending`
with the same store in its sender thread. The paced producer in
`pilot/forward_client.py` shows this integration. The `enqueue` command takes
the writer lease and is intended for a stopped sender.

The 256-byte MQTT payload is a `Q9E1` envelope. It carries a persistent random
128-bit stream identity, a positive 64-bit sequence, creation time, up to 186
data bytes, zero padding and a SHA-256 fingerprint bound to the exact sensor
topic. The event ID is `ClientID/stream/sequence`. Creation time is informative.
Each sensor owns a FIFO. The oldest unconfirmed event has at most one active
transport attempt. No sensor can send another sensor's telemetry through the
broker's configured prefix grants.

## Track the three boundaries

| Boundary | Evidence |
| --- | --- |
| Local acceptance | `enqueue` returned after a SQLite FULL commit. The sender can read the same event ID after process restart. |
| Broker admission | The correlated positive MQTT 5 PUBACK advances the producer's durable `confirmed` sequence. Collection of its row can follow in a separate transaction. |
| Dashboard application | The dashboard inserts the event ID and exact envelope in SQLite, then submits PUBACK. Its `unique_count` counts applied events. `mqtt_received` includes repeats and anomalies. |

The receiver's application effect is the insertion into `applied`. An
integration with additional effects needs its own transaction or idempotent
effect keyed by `event_id`. Keep the consumer ledgers for the full pilot and
for every event still eligible to be replayed. The reference has a fixed disk
budget and no automatic expiry of the deduplication ledger.

## Reconnect, inspect and recover

- On PUBACK `0x97`, the sender stores the negative result and releases the
  attempt. The FIFO head keeps its bytes and ID. A fresh MQTT publication uses
  Paho's public packet-ID allocator after jittered backoff. The first delay is
  50–100 ms, doubling to a 500–1,000 ms range under sustained pressure.
- A missing correlated PUBACK leaves an unknown outcome. The sender discards
  that Paho object and uses a fresh Clean Start session with expiry zero. The
  persisted event survives and can be admitted again with the same ID. An
  explicit broker DISCONNECT also leaves a missing PUBACK. Check the broker
  authorization log if a producer repeatedly loses the transport.
- Other negative PUBACKs stop the affected FIFO with the durable reason and
  queued bytes intact. A `0x10` PUBACK means no matching subscribers and also
  parks the head for inspection. The pilot relies on both provisioned persistent
  subscriptions and checks them with the consumer ledgers. Resolve configuration
  and reconcile the pending file before resuming a blocked stream.
- On identical replay the dashboard counts a repeat and acknowledges it after
  the deduplication transaction. An ID with different bytes is a reported
  conflict. The stored application row survives and the conflicting MQTT
  delivery stays unacknowledged.
- `stats` shows `pending`, `oldest_age_ms`, `occupied_bytes`, `last_ack_ns`,
  `quota`, `unknown`, `attempts`, `retries` and the current stop reason. The
  applications emit periodic counts with checkpoint and SQLite settings. Track
  the gap between producer enqueues and each dashboard's unique applications.
- On disk saturation the producer rejects new local input with visible
  backpressure. Pause or spool at the source before offering more input. Keep
  the existing files. A checkpoint blocked by an SQLite reader also returns
  backpressure. Close long read transactions. Restore free disk in the private
  data volume and resume the same producer database. Completed rows give space
  back to SQLite for reuse, and freed pages may remain in the main file.

The default budgets are 64 MiB per producer and 256 MiB per dashboard for
SQLite's main file, WAL and SHM. `--budget` sets bytes at store creation and the
same value is required on reopen. The page cap reserves a main image and space
for WAL growth. Each small transaction uses SQLite WAL with `synchronous=FULL`
and begins after a bounded checkpoint when the WAL prefix reaches 1 MiB. Use
one writer per canonical file and keep observation transactions short. Log,
backup and operating-system disk capacity need separate provisioning.

## Save and restore application state

Stop the application with SIGINT. Keep its SQLite database and all associated
files together until the writer has stopped. Use a private directory and a
new destination for each backup:

```sh
install -d -m 0700 ./private-backups
.venv/bin/python pilot/forward_client.py backup \
  ./private-data/capteur-1.sqlite ./private-backups/capteur-1.sqlite
```

The command acquires the writer lease, takes a coherent SQLite backup of the
committed image and checks it. Restore the latest consistent snapshot into a
new private directory and start only one writer with the matching ClientID
and credentials. Verify the outbox stream, sequence and queue before sending.
For a dashboard, retain its broker session and reopen the corresponding inbox.
Restoring a stale snapshot needs reconciliation with later committed events
and sequence numbers. An older producer image cannot safely allocate new IDs
in the original stream once later sequence numbers have been issued. Follow
[POLICY.md](POLICY.md) for the separate broker backup procedure.

## Pilot restriction

Once an offline dashboard fills the broker's shared responsibility budget,
the online dashboard also stops receiving new telemetry for that fanout. The
outbox preserves the accepted source data and retries delivery when capacity
returns. The operator must explicitly accept the measured delay envelope before
using the deferred-delivery pilot. The measured Linux overlayfs environment
does not establish storage endurance for Raspberry Pi, SD cards or other flash
devices. The process cuts exercise SIGKILL recovery.
