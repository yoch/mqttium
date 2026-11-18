# Private pilot evaluation

The qualification dossier identifies each tested executable and its source SHA.
Read the decision in `docs/pilot/decision.md` in the source repository before
using this preview for a private pilot. The original candidate is
`49bbaaa4d1d080f458980e3498e90638e317668d`. Its normal load passed the traffic
counts, and its shutdown exposed a false Will. Three deterministic regressions
cover corrected transport retirement and simultaneous session opening.

## Start and observe

Verify `sha256sum -c SHA256SUMS`. Use Linux x86_64 with a compatible glibc.
`BUILD.json` records the build machine and source. The execution receipt records
the separate machine that actually ran this archive. Install Python 3.11 or
newer, OpenSSL and Mosquitto clients. Generate private credentials locally.

```sh
python3 -m venv .venv
.venv/bin/pip install -r pilot/requirements.txt
.venv/bin/python -c 'import sys; from pathlib import Path; sys.path.insert(0,"pilot"); from common import provision; provision(Path("private-pki"))'
MQTTIUMD_LISTEN=127.0.0.1:8883 ./start-tls.sh ./private-data ./private-pki
```

The generator creates eight sensor identities and two dashboard identities.
Each sensor owns its ClientID and `atelier/capteur-N` publication prefix. Both
dashboards can read `atelier`. Keep the two-day demo CA and all private keys in
this private directory. Provision real credentials before an operational pilot.
The secure launcher requires client certificates and retains SQLite schema13.
The plaintext launcher is restricted to loopback and uses schema12.
An older format is refused without modification. There is no automatic migration.

In another terminal, connect a dashboard and publish with the first sensor.

```sh
mosquitto_sub -V mqttv5 -h 127.0.0.1 -p 8883 --tls-version tlsv1.3 \
  --cafile private-pki/ca.crt --cert private-pki/dashboard-1.crt --key private-pki/dashboard-1.key \
  -i dashboard-1 -c -x 4294967295 -t 'atelier/+/status' -q 1
mosquitto_pub -V mqttv5 -h 127.0.0.1 -p 8883 --tls-version tlsv1.3 \
  --cafile private-pki/ca.crt --cert private-pki/capteur-1.crt --key private-pki/capteur-1.key \
  -i capteur-1 -c -x 4294967295 -t atelier/capteur-1/status -m online -q 1 -r
```

A sensor that uses a retained Will publishes `offline` after unexpected loss.
A voluntary MQTT DISCONNECT cancels its Will. Before a planned departure, the
application must publish retained `offline` itself if that is its desired state.
Use `python3 retained-will-demo.py ./presence-demo-data` for the process SIGKILL
example, or run the ten-identity perturbation below with Paho.

## Repeat the qualification

Use new output directories. These commands run the normal packaged executable
on loopback, create local temporary keys and preserve the database and receipts.

```sh
.venv/bin/python pilot/load.py ./load-30m --rate 50 --seconds 1800 --size 256
.venv/bin/python pilot/load.py ./load-1k --rate 50 --seconds 120 --size 1024
.venv/bin/python pilot/perturb.py ./perturbations
.venv/bin/python pilot/restore.py ./charged-restore --mode charged
```

The load writes a compressed sequence ledger, one-second resource and SQLite
samples, process identities and an aggregate event inventory across log rotation.
It requires 90,000 positive PUBACKs, both complete received sequences and 180,000
durable Finishes in the default run. Its closure check requires zero Will
admissions after the normal disconnections. It uses public Paho APIs and strict
TLS1.3. The charged restore uses the existing independent socket probe to retain
an actual PUBACK and PUBREL at precise boundaries. Normal application clients
verify restoration in the quiet procedure and other packaged demonstrations.

## Backup, onboarding and restore

Stop the broker before offline maintenance. The same database pathname must be
used by every process. Keep the data directory private, use a regular canonical
file, and keep event paths in the launcher's separate per-run log directories.
The offline tool refuses final symlinks and hardlinks. The broker's adjacent
lease requires operator discipline against aliases and uncoordinated writers.

Follow `POLICY.md` to generate a ninth device and make a monotone policy update.
`policy apply` creates a verified SQLite backup of the full committed image,
including committed WAL pages. Protect backup directories with mode0700 and
backup files with mode0600. Restore into a new private directory with its matching
old identity configuration, leaving the original file intact. A copied main file
from a running broker is insufficient. Reopen the restored broker and verify
Session Present, retained values and delivery completion with real clients.

The quiet qualification accepts a served load database and its retained local
credentials. It applies onboarding, restores the backup with the old policy, and
reopens the original with the new policy. It leaves both files for inspection.

```sh
.venv/bin/python pilot/restore.py ./quiet-restore --mode quiet \
  --db ./load-30m/broker.sqlite --pki ./private-pki
```

For this command, create the load with `--pki ./private-pki`. The file supplied
must have been created by the pilot generator and the default ten identities.
Do not mix credentials or schema versions from unrelated demonstrations.

## Operational limits

Use this package on an explicitly provisioned private host. The quotas remain
16 transports, eight unauthenticated handshakes,128 sessions,128 subscriptions,
16 live destinations,32 retained topics,128 references and8,192 receipt slots.
An offline dashboard can fill the durable reference budget quickly at50 messages
per second. Pause or slow sensors before exceeding the reported capacity and
resume after drainage. A rejected publication requires application handling.

Monitor free disk, RSS, writer errors, receipt pressure, backlog and clock events.
Each process retains two event segments of at most16MiB each. Launches keep their
own directories, so archive and rotate old logs under a host disk policy. Preserve
incident evidence before removing old log directories. Logical live quotas do
not cap physical database size, WAL, backup files or filesystem use.
Offline policy maintenance caps the committed SQLite image at128MiB and its
logical inventory at32MiB. Run `policy plan` on the stopped served file before
approaching either bound. A large WAL alone does not equal a large committed
image. Do not use VACUUM or manual SQL cleanup to manufacture a passing result.

If clock continuity is unknown after restart, Message Expiry can quarantine
messages. Report temporal quarantine separately from proven expiration. Keep
session-expiry conservative fallback within its published assumptions.
TLS credentials and authorization are static during a process. Certificate pin
rotation preserves a principal's existing rights. Reductions and ownership
transfers are refused by the offline transition tool. Keep copies of both policy
files and the plan beside each private backup.
