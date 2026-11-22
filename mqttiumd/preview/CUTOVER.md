# Switch one Mosquitto 2.0.21 mTLS listener and return to it

Run these commands from the extracted native package as the service user.
For the local example, prepare fresh short-lived test credentials, a source
configuration and an explicit certificate-to-ClientID mapping. Start Mosquitto
2.0.21 in the foreground. Python 3.11+, OpenSSL 3 and Linux procfs are required.
The checker and launcher use Python's standard library.

```sh
python3 mosquitto_example.py ./site --port 8883
mosquitto -c ./site/mosquitto.conf
```

In another terminal, check the inputs while Mosquitto is serving. Inspect
the effective address, storage, principal names, leaf fingerprints, ClientIDs
and exact/subtree grants in the JSON. Each source directive has a file and
one-based line. The check reports a missing destination parent or occupied
candidate directory before any interruption. It leaves both stores intact.

```sh
python3 mosquitto-config.py check site/mosquitto.conf site/bindings.txt site/candidate > preview.json
python3 -m json.tool preview.json
```

For the initial demo, use Mosquitto's SIGUSR1 save with the PID of the
running demo process and wait for the `mosquitto.db` file before checking.
On subsequent checks the last save may already be present. A running
broker may have admitted work since that save.

Pause publishers and drain outgoing and incoming exchanges before the
switch. Stop the original foreground Mosquitto with Ctrl-C and wait for a
successful shutdown save. Check again with `--after-stop`. This flag is the
operator's confirmation of the stop and of the absence of state transfer.
Inspect `decision.status` and start with the plan marked `ready`.

```sh
python3 mosquitto-config.py check site/mosquitto.conf site/bindings.txt site/candidate --after-stop > checked.json
python3 mosquitto-config.py start checked.json
```

Allow an interruption from source shutdown until candidate startup and
client reconnect. Recreate required subscriptions on mqttiumd. Republish
required retained values from their application source. Preserve the
original MQTT state for rollback.

Use the same two commands before and after the switch. From fresh Mosquitto
2.0.21 client settings, the subscriber prints
`2 0 atelier/capteur-1/handoff same listener qos2`.

```sh
mosquitto_sub -h 127.0.0.1 -p 8883 --tls-version tlsv1.3 --cafile site/pki/ca.crt --cert site/pki/dashboard.crt --key site/pki/dashboard.key -i dashboard -d -q 2 -t atelier/capteur-1/handoff -C 1 -F '%q %r %t %p'
mosquitto_pub -h 127.0.0.1 -p 8883 --tls-version tlsv1.3 --cafile site/pki/ca.crt --cert site/pki/sensor.crt --key site/pki/sensor.key -i capteur-1 -d -q 2 -t atelier/capteur-1/handoff -m 'same listener qos2'
```

Wait for the subscriber's SUBACK 2 before publishing. Both clients complete
PUBREC, PUBREL and PUBCOMP. The packaged reception also subscribes with
mqttium 1.3.0 in MQTT 5 and records its QoS 2 delivery on this listener.

## Return to the original service

Stop the foreground mqttiumd with Ctrl-C, wait for exit, and restart the
original configuration and Mosquitto binary from their original working
directory. Keep both data directories.

```sh
mosquitto -c ./site/mosquitto.conf
```

Mosquitto reopens its own persistence file. During the mqttiumd trial,
positive acknowledgements, pending deliveries, sessions and retained state
belong to the candidate SQLite database. They stay there after rollback.
Subscribers that already received a message keep their application effects.
Plan a drain or reconcile the trial's event IDs at application level before
switching production writers. A deliberate republish can produce duplicates.
To resume the trial's remaining deliveries, restart the matching candidate
with the checked plan and its original data directory. No state transfer is
implemented between brokers.

Keep a coherent pre-upgrade backup and its original executable. Older mqttiumd
releases can reject a database with a QoS 2 subscription at the same schema
number. The secure schema is 13. A matching number alone does not establish
downgrade compatibility. Never point an older executable at the trial store
to implement rollback.

## Read the inventory and decision

With persistence enabled, the checker opens the published `mosquitto.db`
once and takes a bounded, immutable copy in memory. Mosquitto 2.0.21
publishes each save by renaming the completed file. The report includes the
declared broker release, database format, SHA-256 of the saved bytes, file
mtime in Unix nanoseconds and UTC capture time. A clean shutdown marker is
checked for the final plan. The marker describes a saved file and can
outlive a subsequent broker start, so the operator confirms shutdown.
Missing, unreadable, unsupported or damaged files receive distinct statuses
and require investigation before the check can authorize a start.

The supported reader covers Mosquitto persistence format 6 with the
little-endian 64-bit layouts used by the received brokers. It reads at most
64 MiB and 200,000 records. It publishes counts, units and pending counts
for explicitly declared ClientIDs. Payloads, credentials and unknown
ClientIDs are omitted from the report. A stored message may be referenced
by multiple clients, or both by a client and by a retained topic. Count
stored messages once and read queued or active exchange counts separately.

| Decision | Operator action |
|---|---|
| `blocked` | Resolve the named configuration, destination or source inventory obstacle. Exit status 2 is accompanied by JSON. |
| `drain_required` | Keep Mosquitto serving, reconnect the affected clients, complete their acknowledged exchanges and save again. A switch now would strand these references at the source. |
| `recheck_after_stop` | The saved inventory shows no pending exchange. Confirm a clean stop, then check the final snapshot. The preview alone cannot start mqttiumd. |
| `ready` | Review the counts staying at Mosquitto and use this checked plan. Any change to the configuration or saved database requires a new check. |

With persistence disabled, the inventory says `disabled` and leaves counts
unknown. Pause and drain the running service before confirming shutdown.
When starting, the checker compares the new snapshot and validation results
to the plan. A fresh directory is classified as `first_creation`. The
launcher records initialization, then creates the secure schema with no
listener. It durably replaces the initialization marker before it starts
the broker. An interrupted initialization leaves
`.mqttiumd-cutover-initializing` and a blocking decision. Keep that
directory for inspection and choose a separate empty destination if the
verified initialization never served clients.

A directory with the original `.mqttiumd-cutover` marker is treated as
`reopening`. Keep a complete SQLite backup. If the main file is missing
or empty, restore a coherent copy of the candidate database before
rechecking. The native broker opens the existing database without SQLite's
create flag. An original valid database and marker can be opened with the
new package. Generate a new plan after the first start or when changing
packages.

## Supported inputs and observable differences

The bindings file declares `mosquitto_version 2.0.21` and one
`client CN /absolute/client.crt ClientID [ClientID ...]` line per principal.
This first checker accepts one single-CN leaf per principal. The leaf and
server certificate must validate against the configured CA. The server SAN
must cover the listener IP. New certificates require a fresh plan. A change
to grants on an existing candidate store remains subject to the offline
policy transition rules.

| Source | Effective behavior |
|---|---|
| One explicit IPv4 loopback listener, TLS 1.3, required client certificates, anonymous disabled | Reuses the address, port, server certificate and CA. |
| `use_identity_as_username true` | Mosquitto authorizes the authenticated CN. mqttiumd pins the DER SHA-256 and restricts its ClientIDs. A sensor claiming `dashboard` receives MQTT 5 CONNACK `0x87` from mqttiumd. |
| `user` and `topic read/write/readwrite` | A literal `atelier/a` grants only that topic. A literal prefix ending in `/#` grants its root and descendants. |
| Dashboard subscribes to `atelier/#` with read permission only on `atelier/a` and `atelier/capteur-1/#` | Mosquitto 2.0.21 grants QoS 1 and delivers the permitted messages. mqttiumd refuses the broad filter at SUBACK and delivers no messages through it. Subscribe to each allowed branch explicitly. |
| Exact MQTT 5 publish grant | Both brokers deny a child topic. Mosquitto sends PUBACK `0x87`. mqttiumd sends DISCONNECT `0x87`. |
| `persistence true` with explicit location and filename, or `false` | mqttiumd always commits its own MQTT state to SQLite WAL with FULL synchronization. Mosquitto's original database stays separate. |
| `log_type all` | mqttiumd writes structured JSONL to a new private run directory on each start. |

Use explicit absolute file paths and single spaces in these small source
files. Includes, plugins, passwords, dynamic ACL patterns, arbitrary wildcard
grants, multiple listeners, system topics and unrecognized directives block
the check. The initial candidate directory must be fresh, private and
separate from the original store. Its parent must already exist.
The launcher reruns checks and passes the verified TLS/policy inputs to the
broker through sealed Linux memory files. Missing Python or kernel support
for sealing is reported as a blocker in the check.

## Replay the complete switch

With the pinned reference and Python dependencies prepared as in README.md,
run the acceptance from the extracted package.

```sh
.venv/bin/python mqtt311-demo.py ./switch-reception ./reference/mosquitto-2.0.21 VERIFIED_TAR_SHA256 --cutover-only
```

`switch-reception/mqtt311.json` records the actual client arguments, wire
results, both stores on reopening and the hash of the executed broker.
`checked-mosquitto.json` is a favorable plan after a clean stop.
`mosquitto-drain-required.json` shows one stored message shared by one
queued delivery to `dashboard` and one retained topic. The next capture
shows one in-flight reference before PUBACK. After PUBACK both delivery
counts are zero, while the session, subscription, stored message and
retained topic remain. `mosquitto-drained-after-stop.json` records the
decision after shutdown. The original and candidate service logs are
separate. Keep the plans private as they contain local paths and the site's
authorization configuration.

`partial-restore.json` records the missing and zero-byte database refusals
and the original retained QoS 1 publication recovered on the wire after
restoring the database. The reception also cuts initialization after
schema creation and removes the database immediately before native reopen.
