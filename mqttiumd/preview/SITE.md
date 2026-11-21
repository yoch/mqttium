# Operate a private mqttiumd pilot site

The site runs two mTLS brokers, eight producers, two persistent HISTORY
consumers and one LIVE dashboard. A Linux supervisor starts each tier after
the previous tier is ready. It owns the site lease, limits child retries and
publishes a local JSON health report. Its two listeners bind to loopback.
[Qualification and pilot decision](qualification/site-decision.md)

## Run the received workflow

On Linux x86_64 with compatible glibc, Python 3.11 or later and OpenSSL,
extract the sealed archive and work from the package directory.

```sh
sha256sum -c SHA256SUMS
python3 -m venv .venv
.venv/bin/pip install -r pilot/requirements.txt
PYTHON="$PWD/.venv/bin/python" ./run-site-reception.sh ./reception "$PWD"
```

This runs a disposable mTLS deployment, HISTORY saturation and independent
restart, process failure and recovery, a charged site backup and real
restoration, integrity faults, outbox thresholds and short archive and storage
checks. The report records the exact component hashes. The `full` argument
selects 72 hours of scheduled production through the same system, plus drain,
incidents and restoration. Run the full kit on a dedicated storage volume
with enough measured capacity. Its disposable certificates are valid for seven
days. Each invocation needs a new private output directory.

## Start and supervise one site

Use a dedicated unprivileged Linux account and a mode-0700 site root. For a
local rehearsal, create disposable credentials with `--demo-pki`. For an
operator-owned site, use `--credentials /path/to/private/pki` and provision
both PKIs locally. Each contains `ca.crt`, `server.crt`, `server.key` and
`identities.json`, then the role certificates and mode-0600 private keys.
Each sensor can publish to its own telemetry topic on each lane and to status
on LIVE. The two dashboard identities subscribe under `atelier`. Pin the
`site-health` certificate to its own MQTT identity with empty publish and
subscribe grants. Retain a protected copy of these credentials separately.
The sealed release and the site backup contain no private keys.

```sh
.venv/bin/python pilot/operate.py init ./site --demo-pki
.venv/bin/python pilot/operate.py run ./site
```

Leave the foreground supervisor running. From another terminal in the package:

```sh
.venv/bin/python pilot/operate.py status ./site
.venv/bin/python pilot/operate.py schedule-demo ./site --events 3000 --rate 50
.venv/bin/python pilot/operate.py pause ./site broker-history
.venv/bin/python pilot/operate.py status ./site
.venv/bin/python pilot/operate.py resume ./site broker-history
```

`init` creates the databases once and seals their identities. Existing stores
are checked again at startup and before child restart. A missing store
requires recovery from an identified copy. Changing a policy or port under an
existing seal requires an explicit migration. The schedule command drives the
paced reference source. Device deployments need a source adapter using the
existing split outbox interface and its single-writer lease. The reception
qualifies the bundled paced source.

Start order is storage and policy verification, both brokers, MQTT 5 TLS 1.3
CONNACK and PINGRESP, the HISTORY consumers, the LIVE dashboard, then producers
connected to both lanes. The supervisor uses a private control file. Clients
reconnect to the same endpoints and consumers verify the original sessions.
Unexpected process exits restart that component with exponential backoff
bounded at eight seconds and a maximum of five failures per 60 seconds.
Planned pauses preserve the intended role identity and data. A component
suspended for an integrity fault requires offline reconciliation.

The supervisor is suitable for a Linux service manager without one unit per
child. For systemd, set `User` to the account that owns the site, use
`ExecStart=/opt/pilot/.venv/bin/python /opt/pilot/pilot/operate.py run /var/lib/pilot/site`,
`Restart=on-failure`, `RestartSec=5s`, `KillMode=mixed` and
`TimeoutStopSec=45s`. Use a start limit such as three tries per minute. The
child processes receive SIGTERM if their Linux supervisor dies. The shipped
mechanism does not install a service or change network settings.

## Read the operating signals

A current `status` response has `fresh: true`. Check each broker's actual
`mqtt_probe`, readiness and live data ages. The retained presence field is the
last observation on LIVE. Measure a stale value by its age even when its
device last announced `online`.

| Signal | Example response |
| --- | --- |
| LIVE age and receive interval by sensor, sequence gaps and local QoS 0 drops | Mark the view stale and reconcile the relevant HISTORY ranges. |
| TLS or authorization refusal, missing ping, frequent reconnects | Check the separate lane's certificate, pin, ClientID and policy. |
| Outbox enqueue/confirm divergence, oldest pending, `0x97`, retry rate and net drain | Restore the HISTORY consumer and reduce generation until the backlog decreases. |
| Consumer transaction freshness, archive prefixes, scrub and catalog state | Verify the last applied sequence and inspect a retained segment or recent backup. |
| CPU, RSS, process write bytes, SQLite and WAL footprint, disk and inode counts | Budget both lanes and the backup copies. Watch device counters only when the mount maps to a known device. |

Capacity notices start at 70%, warnings at 85% and critical alerts at 95%.
The site reports the effective SQLite page ceiling as well as allocated bytes
because SQLite reserves space for WAL and an atomic transaction. It also
reports cold bytes, the segment count and filesystem use. These are starting
thresholds. Reserve enough disk for the chosen workload, WAL, two bounded
broker journal generations per lane and verified backup copies. Disk latency
and FULL WAL timings are measured separately with `pilot/storage_check.py`.
An overlay or virtual counter alone cannot identify an SSD or its wear.

Loss of a required persistent HISTORY session or a permanent integrity fault
suspends producer admission. The site retains existing responsibilities.
Checks sample four published segment hashes per ledger per health pass.
A full open or restore verifies every original envelope. Suspected corruption
requires an explicit operator repair. Notifications name the cause, severity
and recovery action. Logs for each supervised application rotate locally.
Preserve incident receipts separately if longer log retention is required.

## Backup and controlled recovery

The backup command asks a running site to quiesce. It stops producers first,
gives known HISTORY obligations a bounded drain window, then stops consumers,
the view and both brokers. A missed drain window is recorded. Remaining
messages and outbox items stay durable. The command takes the site lease and
uses SQLite backups for the twelve authorities. It verifies the two cold
archives, policies and per-stream frontiers. This is a stopped-site protocol
with individually consistent snapshots.

```sh
mkdir -m 700 ./private-backups
.venv/bin/python pilot/operate.py backup ./site ./private-backups/snapshot-one
.venv/bin/python pilot/operate.py restore ./private-backups/snapshot-one ./private-backups/recovered \
  --credentials ./site/credentials
.venv/bin/python pilot/operate.py handoff ./site ./private-backups/recovered
.venv/bin/python pilot/operate.py run ./private-backups/recovered
```

Restoration always writes a new directory. The restored site starts suspended.
An explicit handoff requires the stopped original, its identical store
signatures and the verified restored copy. It fences the original durably
before letting the restored site start with the existing producer streams.
Retry the same handoff after an interruption. Keep the original and its fence.
Never force activation by deleting the fence or editing a sequence. A stale
backup or an unavailable original requires a reconciliation procedure before
any same-stream production. The received procedure is planned recovery with
the original authority present.

After starting the restored site, check the two consumers' backlog and run
a new reference workload if using demo credentials. The charged reception
checks all original and new sequence identities and envelopes in both ledgers.

Use archive budgets agreed for the pilot. The default is 512 events per segment,
32,768 segments and 8 GiB of charged cold storage per consumer. Backpressure at
a budget leaves the owned events intact. Operator export does not remove old
identities. Cover the entire replay horizon with local storage or a separately
verified retention procedure. Shared hardware requires independent planning for
host loss, and a LIVE interruption remains a best-effort gap. [Archive guide](SPLIT.md)
