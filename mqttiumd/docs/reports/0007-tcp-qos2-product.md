# 0007 — First product TCP receiver: durable incoming QoS 2

This increment adds the `mqttiumd` executable. It starts without a fixture,
creates persistent sessions and receives MQTT 5 QoS 2 publications over real
TCP sockets. Its scope is incoming durability, not subscriber delivery,
complete MQTT 5 conformance or Mosquitto compatibility. QoS 1 is next.

The continuation started from GitHub-confirmed PR #648 head
`d1cd81b46d9554a0b5ff7f73c487db02360be82f`, with main at
`874f298b4de7c3a13006cb1ff767dfb09e4031ac`. The PR remains draft and main is
not merged. Proposed historical records remain proposed.

## Product and authority

```bash
cargo build --locked -p mqttiumd
./target/debug/mqttiumd --db /tmp/mqttiumd.sqlite \
  --listen 127.0.0.1:1883 --events /tmp/mqttiumd.events.jsonl
python -m pip install -r tests/system/requirements.txt
python tests/system/external_paho.py --port 1883 --client-id example-client
tools/run_tcp_reception.sh /tmp/mqttiumd-tcp-reception
```

The independent client actually used is Paho MQTT 2.1.0. Its helper records
its version, command, CONNECT parameters, Session Present and completed QoS 2
callback. Exact ephemeral ports and process receipts belong to the generated
run, rather than being substituted into another run's evidence.

The accepted profile and limits are in [product-tcp.md](../product-tcp.md)
and `contracts/v1/product-profile.json`: MQTT 5, explicit ClientID,
Clean Start 0, infinite Session Expiry, non-retained incoming QoS 2,
no Will/authentication/subscriptions/outgoing delivery. Keep Alive and PING
are applied. Valid unsupported features and malformed/protocol-invalid bytes
have distinct stable causes. Every response respects a validated peer packet
limit; an unknown or too-small limit causes a safe close without an oversized
refusal.

The SQLite worker opens one epoch per broker startup. Its grant constructs
the authority, Outbox and pure Owner. Ordered bounded recovery must finish
before the TCP listener and `ready` observation exist. Existing invalid files
are never replaced. New-session results are correlated to dedicated writer
requests; `WriteFailed` remains unknown, with no blind retry or success
CONNACK. Late confirmation installs durable knowledge with no connection.

The product has no runtime dependency on the lab. Each socket has separate
bounded tasks, input/output queues, absolute packet/write deadlines and
cancellation. SQLite runs only on its exclusive worker. Outbox refusal retains
the original effect identity; closure resolves frame failures separately from
late store commits. Non-reused generations are checked at ingress and again
before core delivery, beyond the core's remembered closed window.

## Discriminating corrections

| Trigger | Correction | Executed detection |
| - | - | - |
| Confirmed new session, close or PING reaches the old owner | Prepared/applied session facts, close input and PING frames | Ten lifecycle tests failed with `ImpossibleInput` before the fix; the full core suite passes afterwards |
| Complete an unrouted publication | Collect metadata only after the last inbound marker and obligation, preserving receipt identity guards | Both stores retained publication metadata in the red tests; unreadable witnesses wrongly admitted a fresh operation; fixed tests compare complete states including reopening |
| Full exchange table precedes another client's release/result | Close only the unaccepted packet's transport; bounded fair retry of nonpacket inputs | Real socket/SQLite reactor regression failed on the starved release, then passed |
| CONNECT and PUBLISH arrive together for a new ClientID | Keep the bounded transport pending until its session confirmation | Real reactor regression failed on premature closure, then passed with CONNACK before PUBREC |
| New PI use arrives before an old PUBCOMP write receipt | Match completion against the latest PUBLISH ingress serial | Red receive-window test lost the new entry; fixed tests include the actual socket/worker receipt race and ninth-PUBLISH refusal |
| Unsupported CONNECT has a tiny Maximum Packet Size | Preserve only structurally validated limit context; flush a fitting refusal through the bounded writer | Codec red test and actual reactor tests cover both fitting refusal and close without oversized output |

The two historical M1 payload-loss control patches are refreshed for the new
metadata-cleanup context. Their mutation and expected verdict/assertion/reason
remain unchanged; the full simulator and SQLite loops must execute them, not
count a patch/build failure as detection.

No positive ACK is emitted for an unapplied packet refused for volatile
capacity. Previously accepted commands remain correlated and may commit.
The runtime does not turn socket closure into certain rejection.

## Finite A–H system reception

`tools/run_tcp_reception.sh` builds normal and explicitly instrumented
products, runs the external client and independent TCP pilot, then builds
isolated premature-ACK and stale-generation mutations. The output includes
source identity, effective features, client observations, durable facts,
process statuses, each run's own system witness, SHA256SUMS and an archive.
The workflow artifact is named `mqttiumd-tcp-product-<published HEAD>`.
Its `index.json.source_head` and binary hashes name the exact received content.

| Criterion | Expected observation and evidence |
| - | - |
| A | New file; external Paho creates one durable session, completes QoS 2 and reconnects with Session Present 1. Admission/release receipts remain; completed publication/payload/obligation/inbound rows do not |
| B | One-byte fragmentation, every split of a bounded example and concatenated packets complete. PINGRESP may legally precede PUBCOMP while release awaits commit. Malformed, oversized and truncated packets refuse; Keep Alive expires after successful PING traffic stops |
| C | Synced `admission.prepared` and `release.prepared` observations identify actual commands inside SQLite transactions. External reads see the old state, no positive ACK is received, then releasing the holds permits PUBREC/PUBCOMP. The premature-ACK mutation fails for `premature_pubrec` |
| D | Actual SIGKILL gives wait status -9 at committed-before-PUBREC. The same file inode restarts, Session Present is true and DUP PUBLISH resumes the original admission before PUBREL/PUBCOMP; no second admission |
| E | Two completed publications reuse PI 7, with distinct publication/admission identities |
| F | Product-created copies pass quick_check but fail startup for invalid session state (`InvalidData`) and a 65,536-byte ClientID (`LimitExceeded`). No readiness/business success/reset; writer reacquired |
| G | Two clients complete; partial reader and real nonreader pressure stay bounded. The actual egress queue fills and retained effect identities reach terminal handling. Eighteen takeovers exceed the core window of eight; replay of the captured old CONNECT is ignored. The generation mutation fails for `stale_generation_forwarded` |
| H | SIGTERM at an acknowledged accepted-command hold releases resources and writer without calling the submitted operation certainly rejected; restart reconciles exactly one admission |

These are system-test receipts, **not qualifying PASS results of the
fixture-based oracle**. No session fixture is manufactured afterwards.
The existing independent oracle, typed-read receipt, K1/K2b/K3 reconstruction
and recomputation, K2a/WAL and model/proof CI gates remain separate. Finite
process tests do not establish power-loss or universal storage guarantees.

## Contracts and remaining work

Proposed record [0022](../decisions/0022-product-tcp-qos2.md) covers the public
session/close/PING types, metadata lifecycle, product bounds and test hooks.
New bounded product profile/event formats are registered with both readers,
with valid/invalid generated examples. The contract digest changes from
`sha256:880b0f581567526780da13b4ea66b29e43d32bb781fbeb64c300f4644e8f8c67` to
`sha256:775794a5e47f5e1d881f0572cdc3e64fa250d46e796e7474d9c5854c102fa5cc`.
The four mandatory checks, normal/instrumented suites, both format readers and
142 oracle tests are rerun; exact commands/results and final SHA checks are
kept with the PR and [continuation journal](../reception/tcp-product-journal.md).

Remaining scope includes QoS 1/PUBACK, outgoing delivery/subscriptions,
expiry/Will/authentication, operational receipt/epoch collection, stronger
shutdown guarantees against hung storage I/O, deployment security and actual
performance targets. The next executable increment is QoS 1 admission before
PUBACK, live Packet Identifier reuse, reconnect behavior under MQTT-4.3.2-5,
external-client restart tests and a premature-PUBACK control. It must not
promise general permanent QoS 1 deduplication after reconnect.
