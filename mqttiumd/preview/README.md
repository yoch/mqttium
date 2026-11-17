# mqttiumd local MQTT 5 preview

Extract the archive on the Linux platform named in `BUILD.json`, then run:

```sh
./start.sh ./data
```

The default listener is `127.0.0.1:1883`. Set `MQTTIUMD_LISTEN` to change it.
The database is preserved in `./data/broker.sqlite`. Every start makes a new
log directory; readiness and clock-origin recognition are in its `events.jsonl`.
A schema refusal stops startup and never replaces the database.

Install the Mosquitto **client tools** using your distribution's package manager.
In a second terminal, start the temperature dashboard:

```sh
mosquitto_sub -V mqttv5 -c -i preview-dashboard \
  -D CONNECT session-expiry-interval 4294967295 \
  -h 127.0.0.1 -p 1883 -q 1 -t 'atelier/+/temperature' -v
```

In a third terminal, start the archive receiver (it prints messages; the broker
is a delivery service, not a permanent publication archive):

```sh
mosquitto_sub -V mqttv5 -c -i preview-archive \
  -D CONNECT session-expiry-interval 4294967295 \
  -h 127.0.0.1 -p 1883 -q 1 -t 'atelier/#' -v
```

After the subscribers report their subscription when run with `-d`, publish:

```sh
mosquitto_pub -V mqttv5 -c -i preview-source \
  -D CONNECT session-expiry-interval 4294967295 \
  -h 127.0.0.1 -p 1883 -q 1 -t atelier/salle1/temperature -m 'temperature:1'
mosquitto_pub -V mqttv5 -c -i preview-source \
  -D CONNECT session-expiry-interval 4294967295 \
  -h 127.0.0.1 -p 1883 -q 2 -t atelier/salle2/temperature -m 'temperature:2'
mosquitto_pub -V mqttv5 -c -i preview-source \
  -D CONNECT session-expiry-interval 4294967295 \
  -h 127.0.0.1 -p 1883 -q 1 -t atelier/salle1/command -m 'command:3'
```

The dashboard receives the two temperatures; the archive receiver also gets
the command. `+` matches one level (including an empty one); `#` matches zero
or more levels. Leading wildcards exclude `$` topics. Overlapping subscriptions
produce one delivery per session with the highest eligible granted QoS, capped
by the publication's QoS and outgoing QoS 1. No Local excludes only that filter's
local contribution. Unsubscribe removes the literal filter, not other overlaps.

Stop the archive receiver, publish another QoS 1 message, stop the broker with
Ctrl-C, then restart `./start.sh ./data` and reconnect that receiver with the
same identity and flags. Its persistent session and pending delivery survive.
Initially QoS 0 publications are volatile and are dropped for offline receivers.
Deliveries at QoS 0 originating from durable QoS 1/2 admissions are consumed
before a single emission; a crash after consumption can lose that delivery.

## Profile and limits

MQTT 5 only, explicit ClientID, Clean Start 0/1, zero/finite/infinite Session
Expiry, non-retained publications, exact and hierarchical filters, No Local,
SUBSCRIBE/UNSUBSCRIBE, PING and Keep Alive. QoS 2 subscription requests grant 1.
Valid RAP and Retain Handling options are accepted and preserved; retained
messages are not supported. A leading `$` rule does not create a `$SYS` service.

128 sessions, 128 subscriptions, 8 filters/request, 16 distinct destinations per
publication, 8192 durable receipt slots with live retirement; packet size 65536,
topic/client ID 1024 bytes, payload 32768 bytes. Exceeding the destination budget
refuses a whole durable admission or explicitly drops a volatile occurrence.
This is a small local preview. Use it on a trusted machine: no TLS or
credentials. Wildcards do not imply shared subscriptions or Subscription
Identifiers. Retained, Will, outgoing QoS 2, MQTT 3.1.1 and format migration are
not supported. Older SQLite schemas are refused, never silently reset.

Finite-session time uses a conservative logical clock. Same-domain BOOTTIME
continuity is used only when positively recognized; otherwise already committed
progress is retained and unproven downtime delays expiry. Do not infer a real
wall-clock expiry guarantee from this preview's fallback environments.

See `BUILD.json`, `RUNTIME.txt`, `SHA256SUMS` and `licenses/` for source identity,
normal-build features, the tested platform and runtime dependencies. No Git,
Rust toolchain or laboratory is needed to run this archive. The separate system
reception records mqttium, Paho and Mosquitto interoperability; it is not a
complete MQTT conformance certificate or a Mosquitto replacement.
