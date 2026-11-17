# mqttiumd — local MQTT 5 retained preview

Extract on compatible Linux x86_64 with glibc (see `BUILD.json` and `RUNTIME.txt`).
The normal executable needs neither Rust, Git nor the laboratory. Run:

```sh
./start.sh ./data
```

It listens on `127.0.0.1:1883`. `MQTTIUMD_LISTEN` overrides the address.
`./data/broker.sqlite` survives every restart; each start creates fresh logs.
Clock recognition and readiness are recorded in `data/logs/run.*/events.jsonl`.
**Schema8 only:** historical databases are refused without modification. Keep
an old installation's data separately; there is no automatic migration or reset.

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
QoS0 remains volatile and is dropped for absent destinations. Durable QoS1/2
admissions to QoS0 recipients are consumed before one emission; a crash after
consumption can lose the delivery. There is no QoS0 replay after that consumption.

Limits: 128 sessions, 128 subscriptions, 8 filters/request, 16 distinct live
destinations/publication, 8192 receipt slots with live retirement, 128 payload
references, 4MiB shared payload bytes. Retained adds 32 topics and 1MiB current
retained bytes. At most256 candidate replays are considered transactionally;
existing reference limits can refuse a whole subscription update, never truncate
it after success. Packets65536 bytes, topic/ClientID1024, payload32768. Old values
still owed to recipients remain charged. Replacement/clear can release their
own retained reference without destroying such deliveries.

This is a trusted-local-machine preview, with **no TLS or authentication**.
Will, Message Expiry, shared subscriptions, Subscription Identifiers, outgoing
QoS2, MQTT3.1.1 and schema migration are unsupported. Finite session expiry uses
a conservative logical clock when time-domain continuity cannot be established;
no UTC jump authorizes deletion. It is not a complete Mosquitto replacement.

The separate source and system-reception archives identify exact clients,
platforms, commands, crash witnesses, bounds and validation scope. Success in
this demo is a product system test, not general MQTT certification or a
power-loss guarantee.
