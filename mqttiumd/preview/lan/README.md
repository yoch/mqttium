# Durable MQTT 5 restart across a private network

This kit runs one normal AArch64 mqttiumd broker and a separate MQTT peer. The
peer subscribes with Paho 2.1.0, disconnects, and publishes with mqttium 1.3.0.
After a broker restart, Paho resumes the session and receives the publication
in manual-acknowledgement mode. The broker records the durable obligation and
the matching claim before signalling the peer to send PUBACK. It checks that
both are gone afterwards. The peer also tests a cross-identity CONNECT
refusal and runs the standard Mosquitto command-line clients.

## Prepare on the broker machine

Use Linux AArch64 with glibc 2.34 or newer, Python 3.12 or newer, OpenSSL, and
128 MiB of temporary free space. Choose a private IPv4 address already
assigned to the broker interface. Use a trusted LAN on which the peer can
reach the broker on TCP 28883 (MQTT) and 28884 (test coordination). Both ports
require TLS 1.3 and a client certificate. The kit makes no changes to the
network configuration. It only creates a dedicated, disposable test site.

Verify the kit TAR against the SHA-256 in the publication record, then
extract it and change into the extracted directory. Set the IP and pick a
unique campaign identifier:

```sh
BROKER_IP=192.168.1.25
CAMPAIGN=lan-restart-01
python3 lan_restart.py prepare --host "$BROKER_IP" --campaign "$CAMPAIGN" --out ./lan-demo
```

Preparation verifies the exact published ARM broker ZIP before executing its
included verifier. The verifier checks the independently pinned TAR digest,
the ELF's native requirements, and the extracted files. Preparation uses the
packaged PKI generator to put the chosen IP in the server certificate and
creates fresh two-day demonstration credentials. Use private directories and
an authenticated file transfer to copy **only** `lan-demo/peer/` to the peer.
It contains the public CA certificate and the two MQTT client certificates
with their private keys. The server key and broker data stay on the broker
machine. The temporary CA signing key is discarded after preparation. No
keys are supplied inside the downloaded kit.

On the peer, install Python 3.11 or newer and `mosquitto_pub` and
`mosquitto_sub` 2.x. Create a virtual environment and install the pinned
clients before starting the trial. Adjust the path to the transferred directory
if needed.

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r peer/requirements.txt
```

## One command per machine

Run this on the broker machine from the kit directory:

```sh
python3 lan-demo/broker/lan_restart.py broker
```

Once the broker prints `Ready`, run this within 60 seconds on the peer:

```sh
.venv/bin/python peer/lan_restart.py peer
```

The topic is `atelier/capteur-1/lan-restart/$CAMPAIGN` and the payload is
`mqttiumd LAN restart $CAMPAIGN`. The two commands use explicit campaign
signals over the mTLS coordination port, including `release_puback`, so
there is no acknowledgement delay to tune. The peer has only its MQTT
observations and those phase signals. It never reads broker storage.

Each role writes a JSON receipt beneath its own `run/`. Collect
`lan-broker.json`, `lan-peer.json` and the two `broker-*.events.jsonl`
streams. Compare the campaign, topic, payload and MQTT Packet Identifier.
The broker receipt records the same SQLite inode and executed binary digest
for both starts. It correlates the observed PUBACK with the claim and
publication IDs read before release. The peer receipt records the granted
SUBACK, mqttium PUBACK, resumed CONNACK, and refusal with CONNACK reason
`0x87` for a valid sensor certificate requesting `dashboard`.

Run the current checker on the folder containing the four collected files.
Pass the private IP of each machine. An ARM64 broker and an x86_64 Linux
peer can use the role receipts from the sealed `01d23635` kit.

```sh
python3 check_receipts.py ./evidence 192.168.1.25 192.168.1.26 --mode field
```

The checker recomputes the PUBACK target from the broker stream and writes
`field.json` with the reported hosts and the correlated exchanges.

The broker command stops its broker when the trial ends. For another trial,
choose a new private site and campaign at preparation. Keep the old receipts
and logs separately from the credentials.

## Standard Mosquitto clients

The peer runs both commands below on the same MQTT listener after the durable
delivery has finished. Run manually from the peer directory against a running
broker with the prepared policy and certificate. Substitute the same
`BROKER_IP`, `CAMPAIGN` and MQTT port as in preparation. The retained QoS 1
publication on a separate topic lets the subscriber start after the publisher.

```sh
MQTT_PORT=28883
TOPIC="atelier/capteur-1/lan-restart/$CAMPAIGN/mosquitto"
mosquitto_pub -V mqttv5 -h "$BROKER_IP" -p "$MQTT_PORT" --tls-version tlsv1.3 \
  --cafile pki/ca.crt --cert pki/sensor.crt --key pki/sensor.key \
  -i capteur-1 -q 1 -r -t "$TOPIC" -m "mosquitto $CAMPAIGN"
mosquitto_sub -V mqttv5 -h "$BROKER_IP" -p "$MQTT_PORT" --tls-version tlsv1.3 \
  --cafile pki/ca.crt --cert pki/dashboard.crt --key pki/dashboard.key \
  -i dashboard-check -q 1 -C 1 -W 15 -v -t "$TOPIC"
```

The peer receipt contains the executed argument vectors and the exact output.

## Hosted ARM64 rehearsal and provenance

From the verified, extracted kit, `bash rehearse.sh ./rehearsal hosted-01`
uses Docker on a native ARM64 host. It runs the two role commands in separate
network and mount namespaces on an internal bridge, with a different
private IP and a different data mount for each role. Provisioning needs
OpenSSL on the host. Image construction needs access to Ubuntu and Python
package repositories. The rehearsal receipt identifies a single-host network
rehearsal. A physical Pi and a second LAN computer require their own receipts.

The kit embeds the unchanged [ARM preview ZIP][arm]. Its SHA-256 is
`0c25d0b4a2c6252aaac01dfdfa3defa84f87c8d51470d0f224e80666f9907d45`.
The broker TAR SHA-256 is
`90335c2ea30fdff16491c784cec018ccbe196a2be82b07a6a9334376e3d4c4c7`.
`KIT-SOURCE` identifies the kit sources separately from the broker and the
original ARM packaging sources. This trial exercises a single broker and a
graceful process restart. Pilot topology, physical storage and endurance have
their own reception.

[arm]: https://github.com/yoch/mqttium/actions/runs/44484552820/artifacts/14509792520
