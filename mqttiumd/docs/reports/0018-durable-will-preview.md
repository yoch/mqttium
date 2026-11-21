# 0018 — Durable zero-delay Will local preview

The normal Linux x86_64 package now demonstrates a sensor disappearing:
`online` retained publication → actual SIGKILL of only the sensor PID →
`offline` non-retained QoS1 message at the dashboard. A new subscriber still
recovers retained `online`; normal DISCONNECT does not publish another Will.

The daemon workflow uploads `mqttiumd-will-preview-linux-x86_64` separately from
`mqttiumd-will-product-<source SHA>`. Download the former from the successful
[daemon workflow](https://github.com/yoch/mqttium/actions/workflows/mqttiumd.yml),
verify its tar checksum, extract it, then run:

```sh
./will-demo.sh /tmp/mqttiumd-sensor-demo
# Or start a localhost broker with a database that survives restarts:
./start.sh ./data
```

The packaged guide contains the executed Mosquitto publisher/dashboard commands.
The demo requires Python3 and Mosquitto client executables; the normal broker
requires neither Python, Git, Cargo nor the laboratory. BUILD.json and the
separate extraction receipt identify construction and execution platforms,
source SHA, normal features, binary/archive hashes and runtime libraries.
SQLite is bundled3.54.1. Schema10 refuses older formats before modification;
there is no migration or database reset.

## Delivered authority

Proposed0034 adds kind8/actions5–6 and canonical kind17, preserving every old
kind1–16 and lifecycle action0–4. A successful known Open installs Armed Will
state before CONNACK. The original opening OperationId identifies the Will;
ClientID is a reusable name, SessionId an incarnation and generation a transport.
Normal End cancels this exact intention; abnormal End makes it Pending before
source cleanup. Startup makes surviving Armed intentions Pending before its
existing ephemeral-session cleanup. Takeover admits the old zero-delay Will
before the replacing Open/Clean Start. The successor installs its own Will or
explicit absence; old facts cannot target it.

WillPublication atomically consumes the exact Pending intention, commits its
frozen canonical routing cut, ordinary publication, target obligations and
accounts, and stores WillPublished. No fictitious inbound Packet Identifier,
exchange or publisher ACK is created. No Local uses saved producer ClientID.
Source deletion cannot delete obligations due to other sessions. Claims/PI,
QoS0 consumption, QoS1 Finish and receipt retirement retain their existing rules.

In a live process, lost/held results resolve with the same OperationId and exact
bytes. After process loss, epoch closure prevents old queued commands executing;
Pending versus consumed durable intent determines whether a new epoch still has
work. A consumed intention is never reconstructed from a historical receipt.
This is one admission per Will occurrence, not a ban on QoS1 wire retransmission.

## Finite reception and scopes

`tools/run_will_reception.sh /tmp/mqttiumd-will-reception` pins a private clean
source, builds the sole normal package, runs its exact normal bytes, builds
separate hooks, retains process/wire/SQL observations, validates event streams,
executes compiled controls and archives the evidence with SHA256SUMS.

| Group | Received behavior |
| --- | --- |
| Public clients | mqttium1.3.0/Paho2.1.0 in both sensor/dashboard directions, SUBACK/CONNACK readiness, real sensor PID/wait-9, offline QoS1/RETAIN0, retained online and normal cancellation |
| Triggers | EOF, RST, Keep Alive, protocol error, DISCONNECT4 publish; DISCONNECT0 cancels; unsupported Will profiles refuse before installation |
| Responsibilities | Source expiry0 deletion, two targets, independent completion, offline obligation and payload survive own broker SIGKILL/restart and release once |
| Lifecycles | Same-incarnation takeover, old physical Close, full128 sessions and129refusal without eviction, Clean Start replacement, finite source active beyond interval then loss/expiry without losing another target's responsibility |
| Capacity/options |17 eligible destinations retain the same Pending Will with no partial fanout; PING progresses; one UNSUBSCRIBE permits16 deliveries. No Local excludes only its contribution, including an eligible overlapping QoS0 contribution |
| Crash cuts | Six separate actual broker SIGKILL/wait-9/same-file runs: Open prepared/committed, Will admission prepared/committed, emitted delivery before PUBACK, Finish committed before result |
| Unknown outcomes | Four genuine dropped/held End/admission result cases resolve the original operation and preserve publication/delivery/claim identities |

The first complete source-matching implementation runs used2fe462b0 with an
explicitly identified test-hooks binary. The delivery launcher repeats these
properties on its own source pin, and all seven normal groups also use the
uninstrumented packaged binary. Each artifact's index, not an older report,
authoritatively names that run's SHA and actual executed bytes. The final CI
artifact must pass before publication is described as received.

Four compiled core controls detect early CONNACK, stale Close reassociation,
changed unknown-operation identity and lost No Local. Two compiled store
controls detect forgotten startup intent and publication after normal cancel.
Each has a passing control, reached patch, successful compilation and exact
business assertion; source/cache restoration is checked. Eleven finite Will
model configurations have their expected outcomes, including two safety passes,
five specified mutant violations and four reachability witnesses. Models, schema
validation, implementation/system tests and historically qualified incoming-QoS2
K1/K2b/K3 campaigns retain separate scopes. No new Will oracle qualification or
power-loss proof is claimed.

## Bounds and limitations

Existing quotas remain128 sessions,8192 receipts,128 message references/4MiB,
16live destinations,8filters and32retained topics/1MiB. A separate reserve holds
at most256 intentions/8MiB of Will payload, with1024-byte topic and32768-byte
payload bounds. Canonical Will-aware Open receipts have a separate8MiB ceiling;
exhaustion writes nothing and returns Unknown, leaving the small Resolve/fence
reserve available. The32MiB raw snapshot limit stays unchanged. The independent
16MiB hex-document reader may refuse maximum-capacity evidence documents.

There is one Will read and one admission flight, bounded fair continuation and
100ms catch-up scan. Full Will storage refuses new installation; temporary
message capacity/fanout pressure keeps Pending state with a coalesced typed
`will_deferred` cause. No historical intention row survives cancellation or
admission. Existing normal diagnostic budgets stay40 fields; Will capabilities
have their own bounded event. Will topic/payload bytes are not product logs.

Only non-retained QoS1 Will with delay absent/zero is supported. Will Message
Expiry and all other Will properties are explicitly refused. Observed but
uncommitted normal cancellation cannot be recovered after a crash. Server
shutdown/failure may defer Wills until restart; no notification before server
SIGKILL or fixed physical wire-departure latency is promised. The preview remains
localhost-only in its guide, without TLS/authentication, not a complete MQTT
broker or a general performance/conformance claim.

Next: a separate secure local deployment profile with TLS/authentication and
measured connection, detection-to-admission, delivery and resource targets.
