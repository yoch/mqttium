# Field pilot preflight

**Hardware qualification withheld.** The Raspberry Pi 5 is a real candidate
host, and its recent GitHub receipt identifies a native AArch64 execution.
The sealed broker is an x86-64 ELF. The accessible x86-64 rehearsal environment
uses virtual storage and has insufficient free space. No field deployment,
LAN exchange, device acquisition or 72-hour run was performed.
[Field observations](field-preflight-results.json)

## Decisions

| Scope | Decision and evidence |
| --- | --- |
| Local synthetic pilot | Existing GO with restrictions, on the documented x86-64 cloud host with a four-CPU cgroup quota and overlayfs. Two independent loopback mTLS brokers, eight scheduled sources at 50 events/s and two HISTORY ledgers were exercised for 43 min 38 s through the first drain. Per broker the 16 connections, 128 sessions, 128 references and 8,192 receipts remain the bounds. |
| Remote device pilot | Not qualified. The frozen site supervisor and reference clients use loopback. A staged LAN exchange and a second-machine acquisition adapter are still needed. |
| 72-hour endurance | Not qualified. The only measured site endurance is the preceding 43 min 38 s; the Raspberry Pi receipt belongs to a Python-client fuzz job. |
| Planned recovery on another host | Not qualified. The preceding coordinated restore resumed the twelve authorities in a fresh directory on the same host. |
| Recovery after permanent loss of the original | NO-GO for same-stream activation with the current handoff. |
| Strong real-time supervision | NO-GO. LIVE is best effort and it can be interrupted by loss of its own broker or host. |

The Raspberry Pi job ran on kernel `6.18.50+rpt-rpi-2712`, glibc 2.43,
four cores and 8,453,947,392 bytes of RAM. The runner receipt contains no
storage mapping for a pilot, no power or UPS receipt and no external MQTT
exchange. GitHub denied runner-inventory access to the integration. The
repository restricts execution on this persistent runner to trusted
default-branch workflows. Existing jobs qualify the Python client; the PR
contains no broker workflow approved for the Pi.
[Recent ARM64 job and manifest](https://github.com/yoch/mqttium/actions/runs/44267745156)
· [Runner trust boundary](https://github.com/yoch/mqttium/blob/874f298b4de7c3a13006cb1ff767dfb09e4031ac/.github/ARM64_RUNNER.md)

The kit pins the original `91e53652` archive. A native ARM64 build would
have its own executable, dependencies, provenance and target-host receipts.
[Sealed reference package](https://github.com/yoch/mqttium/actions/runs/44426277613/artifacts/14485370226)

## Run the gate from the target environment

Run as the service account. Select existing private directories on the actual
data, backup and recovery volumes visible to the service. The probe uses
Python 3.11 or newer and the Linux `findmnt`, `lsblk`, `ip` and OpenSSL
commands. The Paho 2.1.0 dependency is checked with the chosen interpreter.
The kit opens no listeners, installs nothing and uses no SSH. It verifies the
release checksum and ELF identity without executing the broker.

```sh
umask 077
python3 field_preflight.py \
  --archive /path/to/mqttiumd-prepilot-51be4079-91e53652-x86_64-linux.tar.gz \
  --data-root /srv/mqttiumd/data --backup-root /mnt/backup/mqttiumd \
  --restore-root /srv/recovery/mqttiumd > preflight.json
```

Exit 2 reports HOLD with reasons in JSON. To clear the applicable checks,
add the local private IP with `--lan-address`, the existing operator PKI
root with `--credentials`, and an operator disk assessment with
`--write-assessment`. Use `--smart` for a read-only SMART attempt on
directly mapped devices. The CLI records accessibility of counters and any
reported critical warning. It cannot identify wear on an underlying virtual
or layered volume. Treat `READY_FOR_STAGED_RECEPTION` as eligibility to
begin an isolated test. The JSON always marks field qualification
`not_executed`.

The capacity floor covers 16 GiB cold, 512 MiB of outbox budgets and 64 MiB
of hot journals per site copy, plus **4 GiB per copy** by default for the
brokers, WAL, journals and filesystem headroom. Increase this reserve if
measured overhead requires it. Three concurrent copies sharing one
filesystem require **61.69 GiB** free and **602,112** free inodes by default.
The probe groups by filesystem device so that free space is counted once.
Separate partitions on one drive can still share a failure domain. On a
layered or unmapped mount, resolve the backing storage from the physical
host before qualifying it. Keep headroom to act at the 70/85/95% thresholds.

The write assessment is a JSON object keyed by the backing disk's
`major:minor` value. For each disk supply `model`, integer
`projected_72h_write_bytes`, integer `available_write_budget_bytes` and
`evidence_sha256`. Bind the estimate to a representative short run on that
device, include backup and restore traffic, and record the drive rating,
current health, power source and UPS if present. Keep the cited evidence.
These fields are operator estimates. Record filesystem options, device
counter deltas, boot identity and elapsed time separately from the
process-write counts. The preceding 23.94 GB of HISTORY process writes is
an overlay observation. It cannot size the Pi's card or SSD.

## Stage an isolated LAN and recovery test

1. Provision both operator PKIs offline with a server IP SAN matching the
   selected interface and a distinct certificate for each allowed MQTT
   identity. Keep keys in the service account's private directories. Run
   preflight with the actual IP. It checks the server chains, IP SAN,
   certificate horizon and key association. The broker validates client
   chains and pins on connection.
2. On an isolated private segment, use the received `start-tls.sh` for a
   **broker-only** smoke. Select different data directories, policies and
   explicit `MQTTIUMD_LISTEN` endpoints on that assigned private IP, with
   ports 8883 and 8884. Keep the coordinated site on its own stores. Peer
   checks use the appropriate CA and client certificate, a permitted
   ClientID and a foreign ClientID. Capture accepted CONNACK, PINGRESP and
   the cross-principal refusal before any load. The packaged supervisor and
   reference clients still require an authorized LAN configuration change
   and reception for a full-site deployment.
3. Add a distinct acquisition host with the existing outbox format and a
   per-stream lease. Verify local commit before emission, independent LIVE
   and HISTORY connections, `0x97` retry, the retained Will after loss, and
   exact sequence continuation after SIGKILL. Include that producer's
   separately owned SQLite authority in any planned handoff. Only start the
   72-hour run after the physical-volume, LAN and short recovery gates.
   Measure cross-host latency with a recorded error bound on clock alignment.
4. Reuse coordinated backup while a backlog is present. Restore with
   separately provisioned credentials matching the sealed certificate and
   policy fingerprints. Keep the original stopped and accessible. Reconcile
   both ledgers and outboxes, record the handoff and verify new measurements
   end to end. Test a second host explicitly and retain its own evidence.

For the broker-only smoke, the example below uses an isolated address and new
directories. Replace the IP with the one assigned and verified in preflight.
Run each broker command in its own terminal as the service account.

```sh
# In the extracted reference package on the authorized isolated host.
ip=192.168.42.10
MQTTIUMD_LISTEN="$ip:8883" ./start-tls.sh ./smoke-live /path/to/operator-pki/live
MQTTIUMD_LISTEN="$ip:8884" ./start-tls.sh ./smoke-history /path/to/operator-pki/history
```

On the approved peer, set the same `ip` and exercise the LIVE policy with
the operator's LIVE credentials. Wait for SUBACK in the subscriber's debug
output before sending the publication. Repeat the
identity control on HISTORY using its own CA and client credentials.

```sh
mosquitto_sub -d -V mqttv5 -h "$ip" -p 8883 -i dashboard-1 \
  --cafile ca.crt --cert dashboard-1.crt --key dashboard-1.key \
  -t atelier/capteur-1/status -q 1 -C 1 -W 10
mosquitto_pub -V mqttv5 -h "$ip" -p 8883 -i capteur-1 \
  --cafile ca.crt --cert capteur-1.crt --key capteur-1.key \
  -t atelier/capteur-1/status -q 1 -r -m online
# The same certificate claiming capteur-2 must receive Not authorized.
mosquitto_pub -V mqttv5 -h "$ip" -p 8883 -i capteur-2 \
  --cafile ca.crt --cert capteur-1.crt --key capteur-1.key \
  -t atelier/capteur-1/status -q 1 -m refused
```

Keep the current same-stream recovery disabled if the original has been lost.
A future loss-recovery procedure needs an independently verifiable fence
covering the original host, its disks and producer credentials on both
networks, plus reconciliation of the captured sequence fronts. Without that
proof, continue only under freshly allocated stream identities and keep the
uncertain old history visible. This kit does not implement either activation
path. [Current backup and handoff](https://github.com/yoch/mqttium/blob/91e53652a7e4f2b95cabc34476048538e9a64ad7/mqttiumd/preview/SITE.md#backup-and-controlled-recovery)

## Targeted source inspection

The inspection compared the released Python component fingerprints and the
frozen broker bytes with their named source before reading the paths below.
The independent review of a **deployed** field candidate remains outstanding.
The historical security review covers the frozen broker, while this read
covers the site boundaries and the limitations they expose.

| Boundary inspected | Finding for the field decision |
| --- | --- |
| `preview/pilot/site_ops.py` and `site_supervisor.py` | An exclusive lease, sealed storage inodes and policy digests are verified before the children run. Restarts check the sealed stores. The listener and site MQTT health checks are loopback-bound. |
| `crates/mqttiumd/src/security.rs` | Standard certificate-chain verification precedes the configured leaf pin. TLS 1.3 uses fresh client authentication. The historic private-identity tests cover the original executable. |
| `preview/pilot/site_backup.py` | Store leases and SQLite backup capture the stopped authorities. Handoff compares source and target signatures and writes the source retirement marker first. It requires access to both directory trees and unchanged sealed policy identity. |
| `preview/pilot/segment_store.py` and `segment_admin.py` | Verified immutable segments and durable frontiers protect deduplication across compaction and restore. A missing or damaged segment suspends that ledger. |
| `preview/pilot/site_health.py` and `operate.py` | Health includes real MQTT exchanges and sampled progression. A disappeared HISTORY session or integrity fault suspends producer admission and requires operator reconciliation. |

No field P0/P1 was reproduced. The deployed LAN and cross-host recovery paths
have not been available for that determination.
[Historical security scope](https://github.com/yoch/mqttium/blob/91e53652a7e4f2b95cabc34476048538e9a64ad7/mqttiumd/docs/pilot/security.md)
· [Open proof and product conditions](https://github.com/yoch/mqttium/blob/91e53652a7e4f2b95cabc34476048538e9a64ad7/mqttiumd/docs/reception/open-conditions.md)
