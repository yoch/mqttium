# mqttiumd — MQTT 5 preview for private networks

[Run the TLS 1.3 demonstration and provision two client identities](SECURE.md).
[Add a sensor to an existing mTLS database with plan and apply](POLICY.md).
The historical loopback demonstration is below.

Extract on compatible Linux x86_64 with glibc (see `BUILD.json` and `RUNTIME.txt`).
The normal executable needs neither Rust, Git nor the laboratory. Start on the
trusted local machine:

```sh
./start.sh ./data
```

It listens on `127.0.0.1:1883`; `MQTTIUMD_LISTEN` overrides the address.
`./data/broker.sqlite` survives restarts; every start creates fresh logs.
**Schema10 only:** older databases are refused without modification. Keep an
old installation's data separately; there is no automatic migration or reset.
This launcher uses the plaintext loopback profile. Use the secure guide for mTLS.

## A sensor disappears: online → SIGKILL → offline

Install Python3 and Mosquitto's **client tools**, then run the packaged scenario
in a new directory (the broker must not already use its default port18887):

```sh
./will-demo.sh ./will-demo-data
```

The script waits for the dashboard's real SUBACK and the sensor's successful
CONNACK and PUBACK. It then kills **only the sensor process**, records its PID
and wait status`-9`, and checks that the dashboard receives `offline` in QoS1
with RETAIN0. The durable Will is installed before successful CONNACK. The
sensor's separate retained`online` state remains available to a new dashboard.
A normal DISCONNECT cancels the next sensor's Will. Finally a persistent
subscriber goes offline, a further sensor disappears, and the script restarts
the same broker database before receiving the Will obligation left due.

`will-demo-data/will-demo.json` records source/binary and Mosquitto identities,
actual commands, process statuses, client debug observations, durable counts
and build/execution platforms separately. Other files preserve fresh broker
logs, the client CONNACK/SUBACK/PUBLISH receipts and QoS/RETAIN fields. No SQL
cleanup or fixture preparation is performed. A crash before durable recording
of a normal DISCONNECT cannot recover that volatile cancellation observation.

For a manual run, first launch the broker with `./start.sh ./data`. In a second
terminal, start the dashboard and wait for its `Received SUBACK` debug message:

```sh
mosquitto_sub -V mqttv5 -h 127.0.0.1 -p 1883 -i dashboard -c -x 4294967295 \
  -q 1 -t 'atelier/+/status' -d -F '%t|%p|%q|%r'
```

In a third terminal, keep the public sensor client's stdin open. The recorded
PID is `mosquitto_pub` itself, so the later signal does not kill the broker or
the dashboard:

```sh
mkfifo sensor-input
mosquitto_pub -V mqttv5 -h 127.0.0.1 -p 1883 -i capteur-1 -q 1 -r \
  -t atelier/capteur-1/status -l -d \
  --will-topic atelier/capteur-1/status --will-payload offline --will-qos 1 \
  -D will will-delay-interval 0 < sensor-input &
sensor_pid=$!
exec 3>sensor-input
printf 'online\n' >&3
# Wait for Received CONNACK(0), Received PUBACK and dashboard online.
kill -KILL "$sensor_pid"
wait "$sensor_pid"  # reports the sensor's signal termination
exec 3>&-
```

The dashboard receives`atelier/capteur-1/status|offline|1|0`. A fresh reader
still obtains the retained online state:

```sh
mosquitto_sub -V mqttv5 -h 127.0.0.1 -p 1883 -i fresh-dashboard -q 1 \
  -t atelier/capteur-1/status -C 1 -W 5 -F '%t|%p|%q|%r'
```

Expected:`atelier/capteur-1/status|online|1|1`. Closing a sensor's stdin instead
of killing it lets `mosquitto_pub` send a normal MQTT DISCONNECT and suppresses
its Will. The automated demo checks this separately. DISCONNECT reason0x04,
EOF, RST, confirmed Keep Alive loss and protocol closure after accepted CONNECT
request the Will. Shutdown or broker failure may defer it until the next start.
Delay0 takeover publishes the old connection's Will before establishing its
replacement; the new connection installs its own Will or explicitly none.

The Will profile is **QoS1, optional Retain, delay absent/0, bounded exact topic
and payload**. See the [retained presence demo over mTLS](SECURE.md).
QoS0/2 Will, nonzero Will Delay and every other
Will property (including Will Message Expiry) are explicitly refused. A pending
Will remains durable during insufficient message capacity or excessive fanout;
it is not silently truncated. Its admission can therefore be delayed. This
is a trusted-local-machine preview, not a complete Mosquitto replacement.

## Message Expiry

Install Python3 and Mosquitto's **client tools** for the optional demo, then use
an unused directory:

```sh
./expiry-demo.sh ./expiry-demo-data
```

It publishes a retained temperature with a two-second lifetime before any
subscriber exists, observes its durable admission and physical cleanup, checks
that a new wildcard dashboard receives no expired value, and verifies a new
state without expiry. It then repeats with a broker interruption on the same
file. `expiry-demo-data/expiry-demo.json` identifies the actual restart outcome:

- same recognized clock origin: elapsed BOOTTIME can establish expiry;
- changed or unrecognized origin: old expiring state is **quarantined**, not
  offered as fresh and not claimed to have physically expired. Its references
  remain charged until replacement, clearing or applicable session cleanup.

The managed test environment may report `boottime_namespace_unavailable`.
A new process then has a new private time domain; UTC or an external elapsed
measurement does not silently turn this into recognized continuity. Sessions
have their own retention policy and are not expired by a message's lifetime.

For individual commands on a broker launched with `./start.sh ./data`:

```sh
mosquitto_pub -V mqttv5 -h 127.0.0.1 -p 1883 -i expiring-source -q 1 -r \
  -t atelier/salle1/temperature -m 'temporary-state' \
  -D publish message-expiry-interval 2
# After the lifetime, a new subscriber must not receive temporary-state:
mosquitto_sub -V mqttv5 -h 127.0.0.1 -p 1883 -i expiry-dashboard -q 1 \
  -t 'atelier/+/temperature' -C 1 -W 3 -F '%t|%p|%q|%r'
# No Message Expiry property means an unlimited message lifetime:
mosquitto_pub -V mqttv5 -h 127.0.0.1 -p 1883 -i expiring-source -q 1 -r \
  -t atelier/salle1/temperature -m 'current-state'
```

The bounded empty subscriber wait reports a timeout; that alone is not evidence
of physical cleanup. The automated demo also checks the read-only SQLite
inventory, actual clock events and a succeeding fresh-state delivery.
`./demo.sh ./demo-data` runs both this expiry demonstration and the retained
replacement/clearing guide below, retaining each data file and its new logs.

## Retained state without a lifetime

Install Mosquitto's **client tools** from your distribution. Before starting any
dashboard, publish two retained temperatures (the commands exit their ephemeral
producer sessions after receiving PUBACK):

```sh
mosquitto_pub -V mqttv5 -h 127.0.0.1 -p 1883 -i state-source \
  -q 1 -r -t atelier/salle1/temperature -m 'temperature:1'
mosquitto_pub -V mqttv5 -h 127.0.0.1 -p 1883 -i state-source \
  -q 1 -r -t atelier/salle2/temperature -m 'temperature:2'
```

Stop the broker with Ctrl-C, then run `./start.sh ./data` again. A **new**
dashboard receives both retained values:

```sh
mosquitto_sub -V mqttv5 -h 127.0.0.1 -p 1883 -i dashboard-first \
  -q 1 -t 'atelier/+/temperature' -C 2 -W 5 -F '%t|%p|%q|%r'
```

Expected (order may differ):

```text
atelier/salle1/temperature|temperature:1|1|1
atelier/salle2/temperature|temperature:2|1|1
```

The final two fields are QoS and RETAIN. Replace one state and clear the other
with an **empty retained publication**, then query with another new dashboard:

```sh
mosquitto_pub -V mqttv5 -h 127.0.0.1 -p 1883 -i state-source \
  -q 1 -r -t atelier/salle1/temperature -m 'temperature:3'
mosquitto_pub -V mqttv5 -h 127.0.0.1 -p 1883 -i state-source \
  -q 1 -r -t atelier/salle2/temperature -n
mosquitto_sub -V mqttv5 -h 127.0.0.1 -p 1883 -i dashboard-new \
  -q 1 -t 'atelier/#' -C 1 -W 5 -F '%t|%p|%q|%r'
```

It receives `atelier/salle1/temperature|temperature:3|1|1`. A normal publication
without `-r` leaves the stored value intact. Retained is last-state storage, not
a history archive. Existing deliveries keep their own frozen value through a
replacement or clearing.

For the complete executed sequence, including broker stop/restart, use a **new**
directory (Python3 and the Mosquitto client tools are needed only by this demo):

```sh
./demo.sh ./retained-demo-data
```

It uses localhost port18884 by default (`MQTTIUMD_DEMO_PORT` overrides it), keeps
its SQLite file, command output and comparisons, and never removes a database.

## Subscriptions and profile

MQTT5; explicit ClientID; Clean Start0/1; zero, finite or infinite session expiry;
input QoS0/1/2; output QoS0/1; exact, `+` and `#` filters; No Local; SUBSCRIBE,
UNSUBSCRIBE, PING and Keep Alive. Requests for subscription QoS2 grant1. `a/#`
includes `a`; empty levels matter; leading wildcards exclude `$` topics.

Live overlaps produce one delivery/session at the highest eligible QoS. RAP is
combined across eligible contributions; No Local removes only its own
contribution. Replay always sets RETAIN1. RH0 replays, RH1 replays only when the
literal filter is new at that ordered step, RH2 does not replay. Each eligible
filter occurrence can create its own replay, so overlapping filters or repeated
RH0 filters can intentionally return the same value more than once. Unsubscribe
removes the literal filter, not its overlaps or already admitted deliveries.

Retained QoS0 is deliberately committed to disk, without an MQTT acknowledgement;
client-side send completion does not prove that commit. Ordinary non-retained
QoS0 remains volatile and is dropped for absent destinations. Durable retained QoS0 and QoS1/2
admissions to QoS0 recipients are consumed before one emission; a crash after
consumption can lose the delivery. There is no QoS0 replay after that consumption.

Limits: 128 sessions, 128 subscriptions, 8 filters/request, 16 distinct live
destinations/publication, 8192 receipt slots with live retirement, 128 payload
references, 4MiB shared payload bytes. Retained adds 32 topics and 1MiB current
retained bytes. At most256 candidate replays are considered transactionally;
existing reference limits can refuse a whole subscription update, never truncate
it after success. Packets65536 bytes, topic/ClientID1024, payload32768. Old values
still owed to recipients remain charged. Replacement/clear can release their
own retained reference without destroying such deliveries. Will intentions have a separate
reserve of256 slots and8MiB payload bytes (maximum32768 bytes each). No
per-client historical Will row is kept after cancellation or admission.

This section describes the trusted-local-machine loopback profile.
Will variants outside the profile above, shared subscriptions, Subscription Identifiers, outgoing
QoS2, MQTT3.1.1 and schema migration are unsupported. Finite session expiry uses
a conservative logical clock when time-domain continuity cannot be established;
no UTC jump authorizes deletion. Message Expiry instead preserves the original
raw time domain/tick/interval and blocks fresh delivery on unknown continuity.
Absent expiry differs from zero; 0xffffffff is finite, not infinity. Outgoing
QoS0/1 preserves the property with remaining seconds (whole elapsed seconds
subtracted). A known-started QoS1 exchange can finish after expiry; an ambiguous
claimed transmission after a crash can remain in bounded quarantine until its
session is explicitly ended. This availability limit is not strict cross-domain
MQTT expiry compliance. The first-byte gate checks a pre-write clock sample;
it does not timestamp physical network departure or bound host preemption. It is not a complete Mosquitto replacement.

The separate source and system-reception archives identify exact clients,
platforms, commands, crash witnesses, bounds and validation scope. Success in
this demo is a product system test, not general MQTT certification or a
power-loss guarantee.
