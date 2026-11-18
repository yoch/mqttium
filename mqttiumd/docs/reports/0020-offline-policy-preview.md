# 0020 — Add a sensor without replacing the mTLS data set

The offline `mqttiumd policy` subcommand uses the normal packaged broker binary.
Its demonstration creates an old sensor and a persistent dashboard with
two pending QoS 1 deliveries and a retained state. It stops the writer,
creates a verified backup, applies the proposed policy, then resumes on the same
file and receives the old deliveries and the new sensor's publication.
An overbroad filter, an unauthorized publication and a claimed foreign
ClientID are refused. The script also serves the original backlog from a
restored copy of the backup.

```sh
cd mqttiumd
tools/run_policy_transition_reception.sh /tmp/mqttiumd-policy-transition
```

The normal receipt uses authenticated MQTT traffic to stage a retained
payload, a published but unacknowledged claim, a queued QoS 1 obligation,
an inbound QoS 2 exchange and an armed Will. It kills the broker with
commitments still in its WAL. The offline backup retains the identical old
authority and business rows. After the transition, Paho receives the
original claim at the same packet identifier in a resumed session without
resubscribing. mqttium publishes for the new sensor. A separate fixture for
the two transaction cuts has both an armed and a pending Will.

The crash cases record a SIGKILL after the fingerprint was staged in
the open transaction and after the commit before the operator reply.
The first case reads the complete old authority, then applies again with a
fresh backup. The second confirms the complete new authority on reissue as
`AlreadyApplied`. The fixture, backups and restored databases are private
copies of the synthetic workload. Receipts identify the commands, exact
executables, policy and business fingerprints, packet metadata, counts and
process outcomes without including key material or message bytes in logs.

The analysis rejects nonmonotone grants, ClientID transfers, authority
inconsistency, a stale plan and an active writer. Scoped controls reach the
four compiled faults, including a fingerprint committed ahead of owner
insertion and a write to a retained record. Ordinary TLS reception and the
durable MQTT campaign suites keep their individual scopes.

Download the normal archive and the `mqttiumd-policy-transition-<SHA>`
receipt from the exact [mqttiumd CI run](https://github.com/yoch/mqttium/actions/workflows/mqttiumd.yml).
The archive includes [the operator guide](../../preview/POLICY.md), both
example configurations, the demo, executable digests and license texts.
The separately recorded extraction uses the normal archive as downloaded.

[Proposed 0036](../decisions/0036-offline-monotone-policy.md) records the plan
format and the two test points. SQLite stays at schema 11 for mTLS and
schema 10 for clear loopback. The MQTT protocol contracts and causal
identities stay as received. Restrictive transitions require a separate
reconciliation policy. The next sensor behavior to receive is retained Will.
