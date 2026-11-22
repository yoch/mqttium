# Retained Will preview

## Run the presence demonstration

Extract the normal Linux x86_64 preview. With Python 3, OpenSSL and the
Mosquitto client tools installed, run the following from the extracted directory.

```sh
sha256sum -c SHA256SUMS
python3 retained-will-demo.py ./presence-demo-data
```

The demo uses a locally generated mTLS policy and a private temporary PKI.
It observes positive CONNACK and PUBACK for `online`, sends SIGKILL to the
sensor, then looks up the durable `offline` value while the sensor's session
and Will intent are gone. Late Mosquitto subscriptions receive
`atelier/capteur-1/status|offline|1|1` on both sides of a broker SIGKILL. The
broker reopens the same SQLite inode. The sensor's return updates the value
to `online`. A normal MQTT DISCONNECT cancels the new intent. The receipt
records the actual binary digest, platform, client commands and process
outcomes. The secure setup commands are in the [package guide](../../preview/SECURE.md).

The initial witness on `7a8b9d3293c4db8c56a2d7ecf2332d2134e517e2` received
CONNACK `0x83` for the same retained-Will CONNECT. The normal base binary
SHA-256 was `9906c92acca73157e66ce064d4c8bd0e18c473fe3aa489a47d6e5adbdd7e750b`.

## Durable behavior

[Proposed0037](../decisions/0037-retained-will.md) specifies kind 8 subtype 7
and kind 18. The old kind 17 and lifecycle encodings remain byte-identical.
Both stores reuse ordinary retained admission and the original intent's
producer ClientID. The retained reference, live obligations, intent removal
and receipt commit together. A reconnect for that ClientID waits while the
old intent remains durable. A QoS 1 retransmission remains possible on a
live delivery already admitted.

SQLite uses schema 12 for loopback TCP and 13 for mTLS. Bring a fresh database
to the preview. Older files are refused before modification. Quotas are
unchanged, including 32 retained topics, 1 MiB retained inventory, 128
shared references, 4 MiB shared payloads, 16 live destinations, 256 Will
intentions, 8 MiB Will payload reserve, 128 sessions and 8,192 receipts.
The contract digest changes from
`sha256:b2af67d0ce3e1b0bd9d21bb8cc059d6177bc8ebe079775166a877f1b6b55b92e` to
`sha256:bf9bbc0a9d74a4a5b72330f60038b2f51189eca58b5784c6c40a54bfe7e22d1a`.

## Reproduce the system reception

From a clean source checkout with the pinned toolchain and client requirements,
run `tools/run_retained_will_reception.sh /tmp/mqttiumd-retained-will`.
The secure-preview CI job passes its verified normal package and hook binary
to the same runner. The receipt index ties each proof to that source and
binary. Each process cut has an independent PID and wait status. A local
`strace` witness is included when available.

| Evidence | Assertion |
|---|---|
| `demo/retained-will-demo.json` | Mosquitto validates absent-dashboard retention after both SIGKILLs, same file recovery and normal disconnect. |
| `normal/receipt.json` | mqttium/Paho both publish and receive, including a late Paho mTLS subscriber. Raw MQTT observes triggers, RAP, RH, No Local, empty clear, old owed payloads, 32/33 retained topics and 16/17 fanout. |
| `normal/mtls` | Topic and ClientID refusals preserve the original attachment. Legitimate takeover commits the old Will. Six same-ClientID transport losses/reconnects report detection, commit, live and late replay timings, counts, file size and RSS. |
| `crashes/receipt.json` | Six SIGKILL cuts and four held or lost results compare operations, recipes, claims, and retained identities. A newer `online` is committed while the prior result is held. Resolving the old result leaves the newer value current. |
| `mutants/receipt.json` | Each compiled mutation is paired with a specific business assertion for the CONNECT bit, zero-target retention, shared references, result loss or deferred CONNECT. |

Loss-detection measurements use the client stimulus and the broker's
observed close timestamp. The retained update and Will admission use one
commit timestamp. Live notification uses the independent socket observation.
Late replay timing spans SUBSCRIBE through received PUBLISH on an already
established TLS connection. Keep Alive detection has a separate protocol
deadline. These short observations do not establish throughput or latency
targets. K1, K2b and K3 keep their fixture-based qualification scope. The new
retained Will tests exercise the product systems.

This preview supports QoS 1 with absent or zero Will Delay, both Retain
values and existing topic ACLs. Will Message Expiry, other Will properties,
nonzero delay and Will QoS 0/2 remain outside the profile. A resource-bound
Will can delay same-ClientID reconnects until capacity returns. TLS and
policy configuration remain static. A subsequent stabilization milestone
can review this PR independently, exercise duration and backup/restore,
measure load and decide whether to start a pilot.
