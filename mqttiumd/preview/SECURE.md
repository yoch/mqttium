# MQTT 5 over TLS 1.3 on a private network

On Linux x86_64, install Python 3, OpenSSL and the Mosquitto client tools for
the demonstration. First verify the archive member checksums, then run

```sh
sha256sum -c SHA256SUMS
python3 retained-will-demo.py ./presence-demo-data
```

The demo creates temporary credentials and starts mqttiumd on loopback.
The sensor publishes retained `online` without a dashboard. After SIGKILL,
mqttiumd commits the retained Will `offline`. A new Mosquitto subscriber
prints `atelier/capteur-1/status|offline|1|1`. The demo kills and restarts
the broker on the same file, and a second new dashboard receives the same
value. The returning sensor publishes `online` and disconnects normally.
A new dashboard then sees `online|1|1`. Use a new demo directory for the
SQLite file, process results and broker events.

`./secure-demo.sh ./secure-demo-data` exercises the non-retained Will and
the authorization refusals with its own database.

## Bring up a private broker

Use a recent OpenSSL and run the generator on a trusted machine.

```sh
./demo-pki.py ./demo-pki --host 127.0.0.1
./start-tls.sh ./secure-data ./demo-pki
```

The generator is for testing. It creates a two-day CA, server certificate and
two different client certificates. The server SAN names the host clients
connect to. Provision production credentials through your own process.
Distribute the CA certificate and each device's own certificate and key to
that device. Keep the CA signing key offline and protect each private key.
The launcher reads only the server key, the server chain, the CA certificates
and the identity configuration. It creates separate logs on every start and
uses the existing database.

In other terminals run

```sh
mosquitto_sub -V mqttv5 -h 127.0.0.1 -p 8883 --tls-version tlsv1.3 \
  --cafile demo-pki/ca.crt --cert demo-pki/dashboard.crt --key demo-pki/dashboard.key \
  -i dashboard -c -x 4294967295 -t 'atelier/+/status' -q 1 -d -F '%t|%p|%q|%r'

mosquitto_pub -V mqttv5 -h 127.0.0.1 -p 8883 --tls-version tlsv1.3 \
  --cafile demo-pki/ca.crt --cert demo-pki/sensor.crt --key demo-pki/sensor.key \
  -i capteur-1 -q 1 -r -d -l -t atelier/capteur-1/status \
  --will-topic atelier/capteur-1/status --will-payload offline --will-qos 1 \
  --will-retain -D will will-delay-interval 0
```

Enter `online` and wait for PUBACK. Kill the publisher process with SIGKILL
to trigger the Will. Sending EOF at its stdin issues a normal MQTT DISCONNECT
and cancels the Will. Use a fresh subscription after the loss to see RETAIN=1;
live delivery follows the subscriber's RAP option. A dashboard certificate using
`-i capteur-1` receives CONNACK `0x87`. An authenticated dashboard subscribing
to `#` receives a SUBACK refusal `0x87` for that filter. The authorized subtree starts at `atelier`.

For an explicitly configured private address, generate credentials for the DNS
name or IP that clients will verify and set `MQTTIUMD_LISTEN` to the chosen
private bind address and port. Use the chosen host in the client commands.
The plain launcher binds only to loopback. Both launchers retain SQLite data.

## Configure identities

Copy `identities.example.json` and replace the example leaf fingerprints.
Use lowercase SHA-256 of the certificate's DER encoding.

```sh
openssl x509 -in demo-pki/sensor.crt -outform DER | sha256sum
```

Each ClientID belongs to one named principal. Its publish and subscribe lists
contain literal MQTT level prefixes. A grant for `atelier/capteur-1` includes
that exact topic and its descendants. A subscription such as
`atelier/capteur-1/+/#` stays inside the grant. `atelier/+/#` and
`atelier/capteur-10/#` fall outside it. Exact levels preserve case, UTF-8,
empty levels and the leading dollar sign. An empty permission list denies that
operation. Optional `publish_exact` and `subscribe_exact` lists grant a single
literal topic each. A mixed SUBSCRIBE returns one result per filter in request
order. Authorized entries consume capacity in order and commit before the
positive SUBACK. A denied entry receives `0x87` in MQTT 5 or `0x80` in
MQTT 3.1.1. Existing authorized filters remain usable on that connection.
A Will requires publish permission on its topic.

Restart with revised certificate pins to rotate a principal's certificate.
Keeping the same name, ClientIDs and topic grants preserves its sessions.
Removing a pin stops new connections using that certificate after restart.
The trusted CA file supplies the trust anchors for client verification and
for checking the server chain at startup. Clients also check the server name.
Use a trustworthy system wall clock for X.509 dates.

This secure profile uses SQLite schema 13. The loopback profile uses schema
12 and a different database. Older schemas, including 10 and 11, are refused
without modification or migration. Use the
[offline policy tool](POLICY.md) to add devices or expand grants while
retaining the stored authority and work. Startup requires the exact committed
authorization policy. This preview offers static
configuration only. Username/password, dynamic ACLs, automatic rotation,
revocation feeds, remote administration and tenant isolation are separate.
The acceptance scope is a bounded private-network preview on Linux x86_64.
