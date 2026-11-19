# mqttiumd MQTT 3.1.1 and MQTT 5 preview

Check the archive SHA-256 against the published digest, extract it and
change into the extracted directory. The archive contains a native Linux
broker. `BUILD.json` gives its architecture, glibc requirements, source
revision and binary SHA-256. The following commands use OpenSSL, Python 3.12
or later and the Mosquitto CLI clients.

Generate a demonstration CA and certificates in a new private directory.
Then run the broker locally with client certificates required.

```sh
python3 demo-pki.py ./demo-pki
MQTTIUMD_LISTEN=127.0.0.1:8883 ./start-tls.sh ./demo-data ./demo-pki
```

In a second terminal, subscribe and wait for the SUBACK shown with `-d`.
Use the default MQTT version in a clean Mosquitto client environment.

```sh
mosquitto_sub -h 127.0.0.1 -p 8883 --tls-version tlsv1.3 \
  --cafile demo-pki/ca.crt --cert demo-pki/dashboard.crt --key demo-pki/dashboard.key \
  -i dashboard -d -q 2 -t atelier/capteur-1/mqtt311/demo -C 1
```

In a third terminal, publish to the matching topic.

```sh
mosquitto_pub -h 127.0.0.1 -p 8883 --tls-version tlsv1.3 \
  --cafile demo-pki/ca.crt --cert demo-pki/sensor.crt --key demo-pki/sensor.key \
  -i capteur-1 -d -q 2 -t atelier/capteur-1/mqtt311/demo -m 'mqttiumd MQTT 3.1.1 QoS 2'
```

The subscriber reports SUBACK 2, receives `mqttiumd MQTT 3.1.1 QoS 2` at
QoS 2, sends PUBREC, receives PUBREL and sends PUBCOMP. Mosquitto 2.0.21 also accepts
`-V mqttv311` to request the same version explicitly. A local
`mosquitto_pub` or `mosquitto_sub` configuration can change defaults.

## Sessions and limits

MQTT 3.1.1 `Clean Session=0` uses a stable ClientID and an indefinitely
persistent session. The Mosquitto clients request it with `-c -i ID`. A
successful SUBACK precedes disconnect. Matching QoS 1 messages admitted
while a persistent subscriber is offline are eligible for delivery after
restart and reconnect. `Clean Session=1` creates a fresh ephemeral session.
Restart with the same data directory and authorized identities to resume.

Both MQTT versions use the same listener, routing and durable SQLite writer.
The 3.1.1 profile accepts incoming and outgoing QoS 0/1/2 PUBLISH and grants QoS 0/1/2
subscriptions. It supports retained
messages, QoS 1 Will, explicit ClientIDs, SUBSCRIBE, UNSUBSCRIBE, PING and
normal DISCONNECT. A QoS 1 publication on a QoS 2 subscription arrives at QoS 1.
For QoS 2, admission and the recipient obligations commit before PUBREC.
A persistent publisher can repeat PUBLISH after a lost PUBREC and repeat
PUBREL after a lost PUBCOMP. Those repetitions finish the same exchange.
A refused publication closes the 3.1.1 connection before PUBACK or PUBREC
when the failure has no wire encoding. Will QoS 0/2 are outside the profile.
The broker requires the configured client
certificate and its authorized ClientID and topic policy. A mixed SUBSCRIBE
reports the authorization and grant separately for each filter.

## Replay the receipt

Build the pinned Mosquitto 2.0.21 reference with a C toolchain and OpenSSL
headers. The same clients and certificates are used for the reference and
mqttiumd. The standalone receipt uses the extracted normal broker, ordinary
clients and an exact-wire witness for restart, QoS 2 replay and capacity.

```sh
sh build-mosquitto-reference.sh ./reference
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python mqtt311-demo.py ./reception ./reference/mosquitto-2.0.21 VERIFIED_TAR_SHA256
```

Replace `VERIFIED_TAR_SHA256` with the digest checked during extraction. The
JSON receipt identifies the executed broker and protocols. It records mixed
SUBACK codes, offline delivery after restart, commit ordering for PUBACK
and PUBREC, the publisher's QoS 2 recovery state, and capacity refusal
before a success ACK. Broker event streams are retained alongside it.
The outgoing crash cases hold PUBREC or PUBCOMP, kill the broker, reopen the
same SQLite file and record the first frame. Use this candidate to reopen
databases that contain outgoing QoS 2 state.
