# 0019 — Private MQTT 5 preview with certificate authentication

The first executable path was a Mosquitto sensor and dashboard connected
to the same local broker through TLS 1.3. Both clients verified the server
certificate. The dashboard's certificate was refused when used with the
sensor's ClientID. It received `online` followed by non-retained QoS 1
`offline` after SIGKILL of the actual sensor process. The normal DISCONNECT
case left the previous retained value and produced no additional Will.
The package repeats these observations, exercises a forbidden wildcard and
recovers an offline Will obligation across a same-file restart.

Download `mqttiumd-secure-preview-linux-x86_64` from the successful
[mqttiumd workflow](https://github.com/yoch/mqttium/actions/workflows/mqttiumd.yml)
for the published revision. Unzip the artifact, check the supplied archive
digest, extract the tarball and run

```sh
sha256sum -c SHA256SUMS
./secure-demo.sh ./secure-demo-data
```

The package contains a normal executable, a guide, configuration template,
startup scripts, build metadata and licensing texts. The script creates
disposable keys locally with OpenSSL. Use Python 3 and Mosquitto client tools
for the demo. The broker itself links rustls with ring and bundled SQLite.
The independent extraction receipt records platform and artifact identities.

[Proposed 0035](../decisions/0035-private-mtls-profile.md) bounds the static
authority. It uses verified client leaf pins, immutable ClientID ownership,
topic prefix grants and a durable hash of authorization policy. Schema 11
stores the owner row in the live session's transaction. Schema 10 remains
the historical loopback format. Both profiles reject a mismatched format.
Static rights changes refuse the existing secure file before an epoch opens.
Certificate rotation for the same principal preserves the authorization hash.

The launcher below records the provenance of each result, separate normal
and instrumented binaries, process status, decrypted MQTT observations,
public client configuration, SQLite facts, service distributions, resource
counts and precise compiled controls. The TLS frame probes distinguish
sealing from a positive ciphertext socket write. The initial fragmented
24 KiB probe detected a short ciphertext collection before the correction.
The received test exercises two or more TLS records and holds the completion
report while a PUBACK finishes a started exchange after its expiry.

```sh
cd mqttiumd
tools/run_secure_reception.sh /tmp/mqttiumd-secure-reception
```

The reception uses mqttium 1.3 and Paho 2.1 via their public TLS APIs.
Its normal groups cover absent and invalid certificates, client and server
chain validation, expired leaf, unlisted valid leaf, TLS 1.2 refusal,
same-principal certificate replacement, refused Clean Start and takeover,
forbidden PUBLISH (QoS 0/1/2 with RETAIN), Will and subscription filters,
DISCONNECT 0x87, transport close_notify, persistent recovery after broker
SIGKILL, drained credit and live PING during bounded connection pressure.
The reproducible comparison uses 100 sequential connections per profile
and 24 connections across four concurrent workers. It records separate
Keep Alive observations, CPU ticks, RSS, kernel counts and durable backlog.

The preview is for a private trust domain with a static operator-provisioned
policy and a trustworthy wall clock. Username/password, live credential
reload, remote administration, automated revocation and general tenant
isolation need separate decisions. Durability evidence concerns specific
process crashes and known commits. Full MQTT conformity, storage power loss
and long-lived service targets need their own reception.
