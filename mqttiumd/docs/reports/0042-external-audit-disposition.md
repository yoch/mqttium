# External audit follow-up on the delivery-sharding branch

This report follows the external audit of `47e1d12` and the outstanding
work in [issue 672](https://github.com/yoch/mqttium/issues/672). Upstream
numbers in the disposition table link to eclipse-mosquitto/mosquitto.
Reports 0031 through 0038 preserve the earlier observations.

The owner accepted six decisions. The corrected broker delivered every expected copy in the stated QoS 0 load and both P1-02 campaigns qualified. The reader throughput comparison on this CPU showed both gains and losses in every measured configuration. Six product decisions and five environment-specific follow-ups have named owners. The final section maps the 32 external conditions, four formerly code-only checks and six migration questions to those results and follow-ups.

## Decision and protocol evidence

| Record | Integration decision | Observable consequence |
| --- | --- | --- |
| 0058 | Accepted | The broker applies Session Expiry Interval on every valid MQTT 5 client DISCONNECT, including 0x80 and 0x83. Reason 0x00 cancels the Will and 0x04 requests its publication. For an armed Will, publication is due when the Will Delay expires or the session ends, whichever comes first, unless the same session reconnects in time. On orderly closure after the TLS handshake, the broker attempts a nonblocking close_notify when no sealed TLS frame is pending. |
| 0059 | Accepted | Session end and the Will transition commit together. SQLite uses a WAL commit, the append log waits for data sync, and volatile state survives restart only through a completed snapshot. TCP EOF and TLS close_notify indicate that the peer closed its write side. Neither acknowledges the DISCONNECT transaction. |
| 0060 | Accepted with amendments | A distinct, eligible PUBREL can pass a waiting MQTT 3.1.1 PUBLISH. If count or byte capacity prevents further processing and only unread remote releases can help, the broker closes that connection. |
| 0061 | Accepted with amendments | The limit of eight inbound QoS 2 exchanges per session is a broker resource policy. MQTT 5 Receive Maximum applies to a network connection. On a resumed connection the sender starts with a new nonzero quota at most equal to the advertised limit. Original QoS 2 exchanges still complete using their identifiers. |
| 0062 | Accepted | Active compatibility tests use Mosquitto 2.1.4. Replay of the frozen preview requests its original 2.0.21 dependency. |
| 0063 | Accepted with amendments | Ordinary live QoS 0 keeps its window. A retained replay due after a granted subscription waits for socket capacity on the same generation, within the existing 16-delivery window. |

The product accepts zero Will Delay. In the differential cases with DISCONNECT 0x80 or 0x83 and Session Expiry reduced to zero, mqttiumd published the Will and Mosquitto 2.1.4 did not. Both ended the session. The matrix also records refusal of a nonzero Will Delay on the current profile.

The differential suite distinguishes two MQTT 5 receive-limit cases. A ninth outstanding PUBLISH on a connection that advertised eight violates Receive Maximum and requires the server to close that connection. mqttiumd sent DISCONNECT 0x93 before closing. Mosquitto sent PUBREC 0x97 and kept the connection open. A new PUBLISH on the resumed connection can legally meet the broker's persistent session quota. Mosquitto's negative PUBREC 0x97 and mqttiumd's DISCONNECT 0x97 are both permitted outcomes for that case. The matrix records the difference as unspecified. [Decision register](../decisions/README.md) · [MQTT 5 §§3.3.4, 4.4 and 4.9](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html) · [MQTT 3.1.1 §4.8](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html) · [TLS 1.3 §6.1](https://www.rfc-editor.org/rfc/rfc8446#section-6.1)

On MQTT 3.1.1 and MQTT 5, the coordinator tests published 18 distinct retained topics, made the wildcard subscriber's SUBACK take the last available slot and held its output full. Another subscriber received all 18 while the first was blocked. Once its socket drained, the first subscriber received SUBACK and the 18 retained PUBLISH packets, each once. The MQTT 5 sharded test used two sessions on the same delivery shard, suspended dequeue on the real output FIFO, and repeated the pressure across disconnect and resumption. Exactly one terminal event named the closed generation. The original subscription and the two wires were individually checked. MQTT 5 Retain Handling 0 and 1 on a new subscription require sending the stored matches. The retained flag, the original generation, and the bounded number of held deliveries all matter here. [Coordinator tests](../../crates/mqttiumd/src/runtime_tests.rs) · [Two-shard test](../../crates/mqttiumd/src/runtime_tests/sharded_retained.rs) · [Retain Handling on subscribe and resubscribe](../../crates/mqttiumd/tests/session_wire.rs)

A second input/output interaction emerged in the volatile K=0 run. With input packets already parked, the I/O thread could write all eight PUBREC yet discard their new coordinator receipts. In another path it checked the available slots once for an entire batch, allowing several receipts into the last slot. The fix delivers old and fresh receipts in order and checks capacity per receipt. Two deterministic tests failed before the change. The restored TCP path completed a pipeline of 64 ordered QoS 1 PUBACKs and the 42-message QoS 2 disconnect/resume scenario on volatile K=0/2/3 and durable K=0. On the distribution executable all 24 MQTT 3.1.1 mutual TLS combinations passed. They covered both stores, K=0/2/3, exactly at and beyond both bounds. Each of the 12 overflow cases checked close_notify on the affected stream, Session Present, release of the eight original exchanges, successful retry with original identifiers and PINGRESP on an independent connection. [Receipt tests](../../crates/mqttiumd/src/net.rs) · [Bounded reader](../decisions/0060-reading-behind-a-waiting-publication.md)

## Reader progress and session isolation

With MQTT 3.1.1, closing every connection that fills a receive window
would interrupt valid publishers. The broker distinguishes a locally
completable exchange from one waiting only for an unread remote PUBREL.
It tracks a positive PUBREC written for the exact occurrence of a packet
identifier. An already observed release can still unblock the connection.
If the next ordinary packet exceeds either the packet count or the
byte budget, and all eight live occurrences require remote input,
the broker closes that peer with `receive_lookahead_capacity`.

The executable handled short pipelines of 12, 20 and 41 QoS 2 publications.
At 42 it returned eight PUBREC, preserved the independent connection and
accepted the original eight PUBREL on persistent-session resumption.
A separate pipeline of 64 QoS 1 publications completed on local PUBACK.
Deterministic tests exercise both prefix budgets, occurrence reuse and a
release already observed by the reader. MQTT 3.1.1 §4.8 covers an internal
buffer full condition preventing successful processing and requires closure
of the affected connection. The 41-message observation applies to a prefix
that also fits the byte budget.

The input lane has its own 32-slot bound. A reader may borrow a batch,
accept its prefix and put back the suffix. Previously the reactor could
refill the vacated places before the suffix returned. Borrowed slots now
remain charged until the coordinator settles the batch. Tests exercise
partial consumption, the returned order and an early exit. With delivery
owners, ACKs for outbound deliveries take their separate reserved lane.
A mixed batch test observes a subscriber ACK at the shard while the
coordinator still holds an incoming PUBLISH and its PUBREL in order.
[Net and runtime witnesses](../../crates/mqttiumd/src/net.rs) · [MQTT 3.1.1 wire tests](../../crates/mqttiumd/tests/reactor_wake.rs)

## DISCONNECT and takeover

The integration owner retained the storage commit boundary in ADR 0059.
The process tests use independent observations of the target session,
a persistent subscriber and a fresh retained Will subscription. A later
marker confirms the subscriptions still work. A successful process
restart with the kernel page cache intact has no bearing on power loss.

| Tested cut | State available to the restarted broker |
| --- | --- |
| SQLite before commit | The old session and its armed Will survive |
| SQLite WAL hook inside the COMMIT call | The transaction has already committed when SQLite invokes the hook |
| SQLite after commit | The new session state survives |
| Log before append | The old state survives |
| Log after a complete write and before `sync_data` | Process restart can read the complete new frame from the kernel page cache |
| Log after successful `sync_data` | The new frame is recovered |
| Volatile before the in-memory change, or after it with the previous snapshot | The previous complete image is recovered |
| Volatile after the next complete snapshot | The new state is recovered |

The individual JSONL observations were captured on instrumented source
`46aa71c8`, built with `--features test-hooks`, executable SHA-256
`d9bb4d6c5c31e0e6f2cc986cad1e620382c917832f015d0234b41a3f1f697921`.
Each of nine cut points ran on TCP and TLS 1.3, at K=0 and K=2, with
DISCONNECT 0x00 and 0x04. All 72 process restarts checked session presence
and Will delivery. Four additional scheduled TLS takeovers paused a
record after three ciphertext bytes. The old connection either drained
the PUBLISH, 0x8e and close_notify or ended with a truncated record.
The replacement received CONNACK and a DUP replay with the durable packet
identifier. These schedules cover the two chosen outcomes. [Executable tests and peer](../../crates/mqttiumd/tests/disconnect_durability/recovery.py)

## Listener profiles, journal rotation and reference

The two mTLS peers use independently verified client certificates and verify
the server. On mqttiumd an unassigned ClientID is refused with CONNACK
0x87 for both Clean Start values. The owner then resumes the original
subscription. On the anonymous loopback listener a new transport can
resume a previously disconnected client by ClientID. Exact and subtree
subscriptions delivered messages on both brokers. A subscription outside
the native grant was refused. Mosquitto accepted a password-file user.
The candidate's configuration checker classified that directive as blocking.

On the same live anonymous connection, an ordinary QoS 1 stream crossed
a 16 MiB audit journal rotation on the final distribution. The client checked
all 7,792 PUBACKs and
received PINGRESP before and after the rotation. Both files contained
complete JSONL records with a contiguous sequence, and the rotation
marker named the preceding last sequence. The first file ended at 63,356
and the second continued through 63,397. This covers normal rotation. The descriptor reuse observed in the upstream installation still requires a deployment trace.
[Reproducible listener run](../../tests/compat/listener_profiles.py)

The reference builder verifies the Mosquitto 2.1.4 source archive and builds
the SQLite persistence plugin. Upstream 3759 and 3760 now exercise a clean
stop and restart with each broker's own persistent files. In 3759 Mosquitto
lost the resumed session and its pending message. In 3760 the plugin
replayed two Wills with distinct identifiers and DUP=0. The candidate
restored the session and delivered one copy in both cases. Independently
specified wire assertions cover the original nine cases and both restarts.
The frozen preview uses its historical pin. Active load tools record the
broker executable and reference version. [Scenarios and classifications](../../tests/compat/MATRIX.md)

Each of K=0, K=2 and K=3 completed the same 137 scenarios against Mosquitto 2.1.4. In each run 105 matched, 15 exercised documented profile refusals, seven exposed a reference nonconformity, and ten had different permitted or unspecified outcomes. The bench classified zero as candidate defects. All 34 applicable independent wire expectations passed in each run. One expectation did not apply.

## Runtime limits

The ready event and the operator guide now derive their capacities from
the implementation. The receipt collector advances up to 512 positions and
collects at most 512 physical receipt or fence rows. Prefix validation
allows 24 MiB per read and 2 MiB per field. The separate page reader
allows 10 MiB per read and 2 MiB per field. The outgoing identifier allocator
checks a 512-bit bitmap before the bounded fallback. On the default
coordinator the last candidate is 32,769. Session expiry can track 4,096
sessions and process four due sessions per turn. Message expiry asks for
one bounded store page of four. The outbound connection window uses the
negotiated Receive Maximum capped at 256. Static checks tie the 224-frame
volatile QoS 0 window and its 32 reserved places to the 256-slot output.
[Current limits and their derivation](../product-tcp.md)

The retained inventory witness admitted 4,096 topics, refused a new
4,097th with 0x97 and verified every original topic and payload through
a wildcard replay followed by a live marker. It ran on K=0 and K=2.
[Retained inventory test](../../crates/mqttiumd/tests/delivery_shards.rs)

## Work arriving during a store read

One of the first 60 paired reader trials stopped after 90 s on candidate `87823a93`, at burst depth 12 and K=2. The broker counted 372 validated PUBLISH packets and 372 receive-credit events. The subscriber was waiting for the next byte of a QoS 0 frame. The coordinator had queued and written 748 frames, and the broker cancelled one outstanding delivery for that subscriber when the harness closed its connections. The broker then exited zero. Its operations journal did not capture the precise interleaving between the subscription's read and the wake-up.

Two deterministic regressions exposed a matching defect in the core shared by the coordinator and delivery shards. A store read could start, another committed obligation could request a new scan, and a work notification could be consumed while the first read was still outstanding. When that older read returned, it erased the pending scan. A second fixture used a newly visible obligation with an identifier lower than the old page's cursor. It checked both a final old page and an old page with a continuation.

The fix applies still-valid items from the old page, keeps one pending scan, and prevents the old page from restoring an invalidated cursor or high-water mark. Notifications already serviced before the read and changes that require no rescan leave an empty session idle. The two tests failed on the old source and passed on the repaired core. The original wire observation is consistent with this sequence. [Page completion](../../crates/mqttiumd-core/src/owner/connections.rs) · [Regression cases](../../crates/mqttiumd-core/src/owner/pubsub.rs)

On the repaired executable, 25 additional independent runs at K=2 each sent 100 bursts at depth 12. All 30,000 PUBLISH, PUBREC, PUBREL, PUBCOMP and ordered subscriber copies were counted. All 2,500 PINGRESP arrived. The broker counted 3,976 visits to the waiting-publication path. Each interrupted trial would now also preserve its client-side protocol counters.

## Formal checks and scope

| Check | Result | Scope |
| --- | --- | --- |
| TLC reader, session quota and volatile QoS 0 window | All 18 configurations produced the declared outcomes. The three positive safety runs reached 147, 995 and 1,230 distinct states. Eight mutants exposed the expected violations and five witnesses reached their designated states. | Finite queues, explicit scheduling and fairness assumptions. The volatile-window model projects ordinary live QoS 0. The retained case uses the executable tests above. |
| Pinned Kani pilot | All 12 harnesses, 41 cover goals and 12 source regions passed. The 12 targeted mutants failed at their designated checks. All 16 harness-runner self-tests passed. | Symbolic execution of the production credit-release path with the recorded toolchain and source digests. |
| LeaseTable::lighten | Eight cover goals, zero failures across 1,377 checks (10 unreachable). Keeping the old charge in the accounting mutant failed the expected invariant. | Production two-entry table, symbolic charges and target generations. The per-session admission and whole-process memory behaviour have separate tests. |

Of 59 source files recorded for these proofs and their executable correspondence, 54 have identical SHA-256 digests on the qualifying source `55cd90f9`. The model and Kani inputs are identical. The five differing files are core and runtime implementation or test files used for correspondence. The executable regression suites run on the final versions of those files.

[Models, assumptions and replay mapping](../../models/flow-pressure/README.md) · [Credit-release proof](../../proofs/credits/PROOF.md) · [Lease reduction harness](../../proofs/lease-lighten/README.md)

## Load methodology

The bench used a single virtualized x86-64 Intel Xeon Platinum 8370C. Five logical CPUs were visible and the cgroup allowed four CPUs of runtime per scheduling period. The timed matrix pinned the broker to CPUs 0–2, the generator to CPU 3 and the intentional CPU competitor to CPU 0. The separate one-hour functional peers used CPU 4. All traffic ran over loopback with volatile storage unless a store is named. Repetition order, input rates, wire identities, executable hashes, scheduler steal and cgroup throttling were recorded per run.

| Role | Code revision | SHA-256 of executable |
| --- | --- | --- |
| mqttiumd distribution after the work-read correction | `76af3a5428e16a53e58a1545944ffa702e3e9a89` | `b45fb46e8087b7a6cc1ca4edbf87857b05c2cf78922be78aedf45f965f95a1b1` |
| Earlier candidate that exposed the work-read stall | `87823a934006a8787bed3d72153889b56b3c8935` | `4c677f429cc437d94b2531af7c42b60562ba935756a11540c23e0650be6ba6eb` |
| Lot 10 comparison executable | `13eb1143` | `1c0543575cefd096d7d06e396c52ba40c76352ea7047da54712d125bfd783188` |
| Mosquitto 2.1.4 broker | Version-checked source archive | `a968b8fa00ac2f10284d59ea21f0835501880380a9f971b70b91fd6706dbceaa` |
| Mosquitto SQLite persistence plugin | Built with the reference broker | `daef935ad1c8afc161bdd8952b5a9e8917f10221bdecdbed0943007d63e34ae9` |

The fixed-load QoS 0 matrix used 16 publications/s for three seconds at fanouts of 1, 16 and 64. Payloads were 256 bytes and 64 KiB, on TCP and mutual TLS. Each combination ran with and without a CPU competitor and with K=0, 2 and 3. Each of the three repetitions rotated the K order, and the second ran the cells in reverse. A distinct final message fenced each subscriber's observations. The harness counts copies in the timed window and after drain, records missing markers, and measures publish-to-copy latency for copies actually seen.

The paired reader comparison exercised 100 bursts of 12 or 20 QoS 2 publications and one independent PINGREQ per burst. It alternated the two executables over five pairs per depth and K. The duration ends when the publisher has its PUBCOMP packets, the subscriber has its ordered copies and the separate peer has its PINGRESP. For each cell the reported change is the median of its completed paired rate ratios. The reference is the already-corrected Lot 10 executable. Its earlier comparison against uncorrected Lot 10 has a different baseline.

[Load driver and invocation](../../tools/runtime_load/README.md) · [Pinned reference build](../../tests/compat/build-reference.sh)

## QoS 0 delivery and paired QoS 2 throughput

The 72 final QoS 0 cells requested 279,936 copies. 279,936 arrived in the three-second windows and 279,936 had arrived after drain. There were 0 duplicates and 5,832 of 5,832 terminal markers arrived. Mean CPU steal over CPUs 0–3 was 3.29%. The cgroup throttled 3 of 7,269 measured periods. The rates below describe this offered load. Delivery capacity at saturation needs a separate load ramp.

Each entry for fanout 64 is the median p99 across the three trials, in milliseconds, with their minimum and maximum in brackets.

| Transport | Payload | Competing CPU load | K=0 p99 [range] | K=2 p99 [range] | K=3 p99 [range] |
| --- | ---: | --- | ---: | ---: | ---: |
| TCP | 256 B | none | 29.95 [4.83–41.11] | 4.43 [3.13–28.91] | 10.94 [3.12–12.99] |
| TCP | 256 B | one competitor | 4.09 [3.92–4.28] | 37.75 [14.46–44.60] | 32.72 [9.35–36.58] |
| TCP | 64 KiB | none | 13.00 [8.80–46.09] | 13.13 [5.80–26.34] | 81.55 [24.04–343.94] |
| TCP | 64 KiB | one competitor | 16.65 [9.70–18.67] | 30.62 [8.22–259.24] | 17.01 [16.95–361.56] |
| mTLS | 256 B | none | 13.54 [8.88–29.39] | 15.42 [4.75–62.82] | 5.18 [3.49–7.52] |
| mTLS | 256 B | one competitor | 6.36 [3.71–12.52] | 18.40 [17.86–24.34] | 11.88 [4.17–35.80] |
| mTLS | 64 KiB | none | 29.76 [13.82–34.73] | 39.96 [36.23–58.27] | 39.61 [36.35–39.76] |
| mTLS | 64 KiB | one competitor | 30.96 [30.07–42.02] | 36.06 [33.23–69.16] | 29.93 [29.65–40.21] |

| QoS 2 burst depth | K | Completed pairs | Lot 10 median exchanges/s | Final median exchanges/s | Paired rate change, median [min, max] | Median p99 PUBCOMP, Lot 10 / final (ms) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 12 | 0 | 5 | 12,368 | 18,843 | +25.3% [-29.0, +52.4] | 3.61 / 1.89 |
| 12 | 2 | 5 | 8,811 | 10,505 | +19.2% [-33.5, +84.4] | 3.62 / 3.27 |
| 12 | 3 | 5 | 12,138 | 12,995 | -3.5% [-29.9, +26.7] | 1.47 / 1.62 |
| 20 | 0 | 5 | 23,117 | 13,563 | -32.3% [-55.2, +16.0] | 1.44 / 5.27 |
| 20 | 2 | 5 | 11,822 | 14,221 | +2.7% [-6.1, +330.7] | 6.15 / 2.09 |
| 20 | 3 | 4 | 13,428 | 9,469 | -24.5% [-43.0, +15.0] | 2.33 / 9.58 |

The corrected executable completed all 30 reader trials. One Lot 10 reference trial, K=3 at depth 20, reached the 90 s driver deadline. The publisher had received 680 PUBREC and 680 PUBCOMP, and the independent PINGRESP for the current burst had arrived. The subscriber had received 676 copies. The driver returned 1 and the broker exited zero after SIGTERM. The table uses the 29 pairs for which both sides completed. In every configuration at least one pair was slower and at least one faster on the corrected executable. No monotonic ordering by K is supported on this CPU. For example, the depth-20 K=2 paired rate ranged from −6.1% to +330.7% despite a median of +2.7%.

The largest individual QoS 0 latency was 407.51 ms, on TCP with 64 KiB payloads, fanout 64, K=3 and no injected competitor. That cell's median p50 was 2.78 ms and its three p99 values ranged from 24.04 to 343.94 ms. The earlier candidate delivered 279,860 of 279,936 copies within its window and all copies after drain. Its matrix and the final matrix ran at different times. Only the reader comparison interleaved executables.

## Capacity, progress and TLS close

| Sessions | QoS 2 PUBREC held simultaneously and then completed | RSS empty / full / after closing (KiB) | Independent progress at full table, median / maximum PINGRESP (ms) |
| ---: | ---: | ---: | ---: |
| 1 | 8 | 7,640 / 7,872 / 7,876 | 0.21 / 0.27 |
| 1,024 | 8,192 | 7,312 / 18,672 / 19,788 | 0.20 / 0.57 |
| 4,096 | 32,768 | 7,428 / 45,808 / 48,464 | 0.28 / 5.93 |

These probes held eight exchanges for each session, sent five PINGREQs on a connected session and completed a new publication after releasing all the exchanges. All three final processes exited zero. On the earlier candidate `87823a93`, the 4,096-session process exited 2 during bounded shutdown. Its terminal event recorded 977 pending connection closes, zero in-flight store commands reported in that field, and lifecycle work outstanding. The one-session and 1,024-session processes exited zero. Repeating the 4,096-session load with the same 0.5 s grace gave 710 session closes before SIGTERM and all 4,096 by process exit, 1.268 s later. A paired run waited another 1.096 s until all 4,096 session closes had been observed before sending SIGTERM. That process exited zero in 0.032 s. Broker shutdown and MQTT workload completion are recorded separately. [Load and shutdown follow-up](https://github.com/yoch/mqttium/issues/683)

In the 50-burst functional checks, depths 1, 8, 12, 20 and 41 completed 50, 400, 600, 1,000 and 2,050 identities in order. The broker counted 0, 0, 76, 192 and 593 waits behind a publication respectively. The maximum-depth run had p99 PUBCOMP 17.65 ms and independent p99 PINGRESP 5.91 ms. These functional probes overlapped integration checks.

Each of three fresh TCP brokers and three fresh mTLS brokers served 50 orderly disconnects. TCP median time from DISCONNECT to peer EOF ranged from 0.08 to 0.53 ms and p99 from 3.57 to 5.84 ms. For TLS the corresponding ranges were 0.15–0.20 ms and 3.83–5.19 ms. Median handshake time, measured separately, was 1.83–2.01 ms. The broker used 0.00–0.03 CPU seconds in the two-second post-close observation intervals. These observations include the scheduling of both endpoints. [Close probe](../../tools/runtime_load/profiles.py)

Six persistent sessions received 256 one-KiB QoS 1 messages per reconnect for each of 20 cycles. The receiver verified 30,720 copies. RSS after a drain and a one-second idle went from 11,888 to 12,940 KiB between first and last cycle. Private dirty pages in the last five cycles ranged from 7,804 to 7,840 KiB. On the intermediate `60dc7771`, an additional 8 MiB retained inventory reached peak sampled RSS 16,344 KiB without snapshots and 29,084 KiB with one-second snapshots. The final file occupied 8,536,064 bytes on overlay. These are bounded observations of the process and allocator. [Memory probe](../../tools/runtime_load/memory.py)

## Reconnect load and event-log integrity

The MQTT 3.1.1 publisher offered 100 sequenced QoS 2 messages per second for one hour. The persistent subscriber disconnected near minute one and reconnected about 35 seconds later. The driver verified Session Present, counted deliveries by identity at PUBREL and checked order and duplication. All runs were on loopback and volatile storage. The two peers and the broker shared CPU 4. The tests overlapped for part of the hour.

| Executable | Publishing duration (s) | PUBCOMP | Unique deliveries | Elapsed offline (s) | PUBCOMP during absence | Complete journal records |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Lot 10 `13eb1143` | 3,599.993 | 360,000 | 360,000 | 35.002 | 3,500 | 3,603 |
| Intermediate `60dc7771` | 3,599.992 | 360,000 | 360,000 | 35.021 | 3,505 | 3,605 |
| Before work-read correction `87823a93` | 3,599.991 | 360,000 | 360,000 | 35.131 | 3,496 | 3,606 |
| Final `76af3a54` | 3,599.993 | 360,000 | 360,000 | 35.006 | 3,519 | 3,606 |

The final run verified all 360,000 scheduled identities, with zero missing, duplicated or out-of-order deliveries. The one-second submission buckets had minimum / median / maximum 65 / 100 / 135. Maximum sender scheduling lag was 605.2 ms. The hourly achieved rate of 100.000/s includes catching up after delays. Each full journal had contiguous sequence numbers, complete JSON lines and a final `stopped` record. The official schema checker accepted the streams. The separate rotation run crossed the 16 MiB threshold. An early development run on `2bbefc04` was deliberately interrupted at 264.39 s and is excluded from the table. The reporting deployment, longer log lifetimes and physical storage failures have named follow-ups. [Reconnect and pacing driver](../../tools/runtime_load/backlog.py) · [Journal check](../../tools/runtime_load/audit_log.py) · [Deployment load](https://github.com/yoch/mqttium/issues/683)

## Contract checks, campaign provenance and archive

The strict product-event reader initially refused records produced by the broker itself. The ready event now describes 50 typed fields, including the two QoS 0 limits. The counter map uses at most 113 decimal u64 tallies. Each of the two TLS capacity pairs has a closed shape. The shutdown count for pending closes accepts 8,192, the limit of two entries for each of 4,096 connections, and rejects 8,193. In a pool of 103,604 genuine events, 49 distinct field shapes from 20 kinds passed this check. Separate bounded-reader controls cover malformed shapes, incorrect types and overflowing counters. ADR 0067 documents this adjustment and remains proposed. The six owner decisions in the register are 0058–0063. [Schema](../../contracts/schema/v1/product-event.schema.json) · [Decision 0067](../decisions/0067-bounded-operational-event-details.md) · [Boundary checks](../../tools/test_product_event_details.py)

The qualifying campaigns, normal workspace suite and Python checks used `55cd90f9f50e6b0268194d64200cfc2968a2faf1`. Its Rust crates tree matches the distributed broker source `76af3a54`. Later commits added wire observations and clarified the QoS 0 compatibility tally, which counted 100 of 100 payloads in order on each broker.

The first instrumented suite on `55cd90f9` exposed a race in a manual reader test. The test and a live reactor shared the same socket and could both reserve the final read credit. An unchanged rerun passed. Revision `56ded031dcf3ea36cc20881b97944e58b345bee7` gives the two manually scheduled tests an unregistered socket and retains their original assertions. This change is confined to the Rust test module. All 29 instrumented network tests passed and each of the two affected tests passed a further 100 times in a separate process per repetition. The evidence archive preserves the original failure, rerun and fixture results. [Reader fixture](../../crates/mqttiumd/src/net.rs)

The qualification used P1-02, seed 1, with sim-store and SQLite in process. Each campaign exercised the lost admission-result injection once, and its verdict was computed by the independent oracle. The campaigns contain the source archive and manifest. A separate rebuild from each archive must reproduce the lab binary exactly. The simulation scope is the state-machine and adapter schedule. The TCP/mTLS process tests above cover actual process termination at selected boundaries. [Qualifying campaign command](../../tools/run_qualifying_campaign.sh)

Each final campaign qualified with PASS. All 12 assertions passed, the named fault hit once, all 18 documents were accepted and recomputation stood. The lab executable matched its independent build on both stores at SHA-256 `29bb21613a4404eca7b49b7855a59cd84bb2f26e9a2ad65eeabe8e511a2dd8d6`. The contract digest printed by `validate` was `cdd2b28f45ffa66c22ff3c556dab4d20396814cb0693fe4f3805c1307b65a300`. These hashes identify the harness and contract separately from the network executable.

The normal workspace reported 1,438 passing tests on `55cd90f9`. On `56ded031` the complete instrumented rerun passed all 334 tests and the normal broker library passed all 218.

The first instrumented run on `56ded031` stopped in the SQLite/mTLS commit-barrier test while its Python peer was polling the live journal. The diagnostic immediately reread the same events successfully. A reader that skips a partial final line and continues iterating can interpret a later suffix as a complete JSON document. The original rejected bytes were unavailable. A deterministic test now exercises an append at that boundary. The peer ends each poll at the first partial line, reopens at the next poll and rejects malformed complete records. The instrumented durability suite passed all 15 tests on `9876f66d` after this fixture change. [Durability peer](../../crates/mqttiumd/tests/disconnect_durability/recovery.py)

On the same final test source the complete workspace passed 1,439 tests and the separate instrumented broker suite passed 335. The project format check, both Clippy configurations and P1-02 validation passed. The instrumented-only feature guard was also checked on the distribution-equivalent source. The Python oracle passed 274 tests, its 405 trace fixtures were current, both schema readers agreed on 661 documents, and all 637 derived corpus files matched. The 65 compatibility harness unit tests passed. On the earlier candidate, running the lab in a second checkout with the same Cargo target invalidated dependencies during the broker doctest. The final verification used exclusive Cargo targets.

From the `mqttiumd/` directory the following checks were run on the final sources. Complete runtime suites used one test thread on the shared host. In earlier concurrent runs of throughput fixtures all MQTT counts were delivered, but the host missed an elapsed-time assertion. The existing time threshold was retained. The independent schema suite also initially caught a historical oracle test that still expected the 33rd closing input to be invalid. Its positive and negative boundary cases now use 8,192 and 8,193. [Oracle shutdown test](../../tests/oracle/tests/test_session_lifecycle.py)

```sh
cargo fmt --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo clippy -p mqttiumd --all-targets --features test-hooks --locked -- -D warnings
tools/check_kill_points.sh
RUST_TEST_THREADS=1 cargo test --workspace --locked
RUST_TEST_THREADS=1 cargo test -p mqttiumd --features test-hooks --locked
cargo run -q -p mqttiumd-lab --locked -- validate --contracts contracts/v1 --scenario tests/scenarios/p1/P1-02.json
python tests/oracle/build_traces.py --check
python -m unittest discover -t tests/oracle -s tests/oracle/tests
python tools/build_corpus.py --check
python tools/check_schemas.py
python tools/test_product_event_details.py
python -m unittest discover -s tests/compat -p 'test_*.py'
python models/check_models.py
python conformance/check.py --check
```

The frozen system receptions belong to the historical executable. A separate control replayed the legal MQTT 5 DISCONNECT 0x80 from `disconnect_outside_profile_reason` with infinite session expiry. The modern executable closed the stream and resumed the session with Session Present. The archived expectation is a server DISCONNECT 0x83. The full historical suite was not run or edited. Upstream ticket 3618 concerns the Python client and retains its existing disposition. [Frozen fixture](../../tests/system/session_reception.py) · [Python-client conformance](../../../docs/conformance.md)

## Disposition by upstream ticket

Upstream links identify the Mosquitto reports. The final column links the MQTTium issue that tracks the result or remaining decision. The protocol and environment in each row delimit the evidence.

### Conditions tied to workload, platform or deployment

| Upstream | Reported condition | Local evidence or remaining work | Responsible role and follow-up |
| --- | --- | --- | --- |
| [#2349](https://github.com/eclipse-mosquitto/mosquitto/issues/2349) | During an hour at 100 QoS 2 messages/s, the subscriber disconnects after one minute and later drains about 3,500 queued messages. | See the reconnect-load section of this report for the targeted local run, achieved rate, duration and reconciliation of admitted and delivered messages. | Performance and persistence · [#683](https://github.com/yoch/mqttium/issues/683) |
| [#3096](https://github.com/eclipse-mosquitto/mosquitto/issues/3096) | A wildcard subscription replays 40,000 retained topics. | The profile admitted 4,096 retained topics, refused the 4,097th and replayed the admitted set on K0 and K2. The reported 40,000-topic replay exceeds this profile. | Capacity and persistence · [#683](https://github.com/yoch/mqttium/issues/683) |
| [#2205](https://github.com/eclipse-mosquitto/mosquitto/issues/2205) | Existing clients disconnect after the civil clock is manually changed. | Broker deadlines use monotonic time. Step the civil clock in both directions on an isolated host and record connection, keep-alive and expiry outcomes. | Operations · [#681](https://github.com/yoch/mqttium/issues/681) |
| [#2682](https://github.com/eclipse-mosquitto/mosquitto/issues/2682) | The broker stops making progress after a clock adjustment. | Step civil time on an isolated host and track an active connection through the adjustment, including message exchange and keep-alive progress. | Operations · [#681](https://github.com/yoch/mqttium/issues/681) |
| [#1558](https://github.com/eclipse-mosquitto/mosquitto/issues/1558) | Saving in-memory data on a Docker volume fails with “Invalid argument”. | Recreate the volume driver and filesystem, capture the save result and check recovery of the preceding valid snapshot. | Storage and operations · [#681](https://github.com/yoch/mqttium/issues/681) |
| [#2469](https://github.com/eclipse-mosquitto/mosquitto/issues/2469) | Physical power loss corrupts a persistent broker database on Windows. | A process restart exercises a different failure boundary. On a Windows storage rig, compare confirmed durable work with what recovers after a physical power interruption. | Storage reliability and Windows · [#682](https://github.com/yoch/mqttium/issues/682) |
| [#3159](https://github.com/eclipse-mosquitto/mosquitto/issues/3159) | A persistent database is corrupt after physical power loss. | On a dedicated rig, record the filesystem and cache policy and reconcile externally recorded acknowledgements with durable exchanges after power is restored. | Storage reliability · [#682](https://github.com/yoch/mqttium/issues/682) |
| [#2898](https://github.com/eclipse-mosquitto/mosquitto/issues/2898) | OpenSSL reports a UI error while loading the deployment’s private key. | Exercise a generated key with equivalent format and protection in the identified provider/UI environment, keeping a supported PEM key as a control. | TLS and operations · [#681](https://github.com/yoch/mqttium/issues/681) |
| [#1616](https://github.com/eclipse-mosquitto/mosquitto/issues/1616) | Publications sent every 20 ms arrive in groups. | The loopback check shows delivery can progress without a timer. Measure send and arrival spacing on a representative link and record its jitter. | Network performance · [#679](https://github.com/yoch/mqttium/issues/679) |
| [#3117](https://github.com/eclipse-mosquitto/mosquitto/issues/3117) | One or more QoS 2 handshake packets arrive up to a minute late. | Local exchanges complete. Record PUBREC, PUBREL and PUBCOMP timings separately under the reporter’s link conditions. | Network performance · [#679](https://github.com/yoch/mqttium/issues/679) |
| [#1912](https://github.com/eclipse-mosquitto/mosquitto/issues/1912) | Split TCP segments coincide with an approximately 40 ms delayed TCP ACK. | Split-frame decoding has local coverage. Capture segment and ACK timings with the representative peer, OS and TCP settings. | Transport and networking · [#679](https://github.com/yoch/mqttium/issues/679) |
| [#2807](https://github.com/eclipse-mosquitto/mosquitto/issues/2807) | A core stays busy after an mTLS client disconnects. | The local TLS close check samples process CPU before and after disconnect. Repeat over a sustained idle interval at the reported connection load. | TLS and performance · [#683](https://github.com/yoch/mqttium/issues/683) |
| [#2813](https://github.com/eclipse-mosquitto/mosquitto/issues/2813) | After a long run, the log ends partway through an event line. | See the event-log integrity section of this report for the targeted local run, observation window, record parsing and termination conditions. | Observability · [#683](https://github.com/yoch/mqttium/issues/683) |
| [#3237](https://github.com/eclipse-mosquitto/mosquitto/issues/3237) | The broker runs out of memory at 500,000 retained topics. | The profile admitted 4,096 retained topics, refused the 4,097th and replayed the admitted set on K0 and K2. The reported 500,000-topic population exceeds the profile. | Capacity and persistence · [#683](https://github.com/yoch/mqttium/issues/683) |
| [#3273](https://github.com/eclipse-mosquitto/mosquitto/issues/3273) | Memory spikes during a periodic save of a large in-memory database. | With 8 MiB of retained data on overlay, the intermediate volatile broker reached a sampled peak RSS of 29,084 KiB with one-second snapshots and 16,344 KiB without. The reported large database, filesystem and storage mode still need deployment evidence. | Storage and performance · [#683](https://github.com/yoch/mqttium/issues/683) |
| [#3110](https://github.com/eclipse-mosquitto/mosquitto/issues/3110) | A Unix-socket client disconnects during a large persistence save. | The current broker lacks a Unix-socket listener and durable mode has no periodic save. A TCP snapshot observation can establish continuity only for that listener and store. | Storage and transport · [#683](https://github.com/yoch/mqttium/issues/683) |
| [#2095](https://github.com/eclipse-mosquitto/mosquitto/issues/2095) | CPU reaches 97–100% and the broker ceases to accept connections. | Measure connection admission and refusal alongside CPU under a comparable sustained load, then check that admission recovers after drain. | Broker performance · [#683](https://github.com/yoch/mqttium/issues/683) |
| [#2101](https://github.com/eclipse-mosquitto/mosquitto/issues/2101) | Sockets in FIN_WAIT coincide with slow connection establishment and message reception. | Collect socket-state counts with connection and receive timings under representative churn and TCP close settings. | Transport and networking · [#679](https://github.com/yoch/mqttium/issues/679) |
| [#2182](https://github.com/eclipse-mosquitto/mosquitto/issues/2182) | CPU and memory both remain high under the reporter’s workload. | Record the client and message mix and measure CPU, memory and queue occupancy through steady load and drain. | Broker performance · [#683](https://github.com/yoch/mqttium/issues/683) |
| [#2267](https://github.com/eclipse-mosquitto/mosquitto/issues/2267) | Virtual memory stays high after six persistent sessions drain. | The local six-session cycle records virtual and resident memory after drain. Longer observation with allocator and live-queue measurements is needed to explain retained space. | Broker performance and persistence · [#683](https://github.com/yoch/mqttium/issues/683) |
| [#2294](https://github.com/eclipse-mosquitto/mosquitto/issues/2294) | Dequeuing messages fails to return memory to the operating system. | Local drain measurements include RSS and private dirty memory. Observe live queue sizes and allocator retention together over a longer idle interval. | Broker performance · [#683](https://github.com/yoch/mqttium/issues/683) |
| [#2455](https://github.com/eclipse-mosquitto/mosquitto/issues/2455) | Broker memory consumption approaches 100% of available memory. | Match the workload and memory limit, trace RSS against live records and queue depth, and record boundary and recovery behaviour. | Broker performance · [#683](https://github.com/yoch/mqttium/issues/683) |
| [#2539](https://github.com/eclipse-mosquitto/mosquitto/issues/2539) | Message latency varies between clients receiving the same workload. | Measure per-recipient delivery distributions with the same publication stream and record link conditions for each client. | Network performance · [#679](https://github.com/yoch/mqttium/issues/679) |
| [#2656](https://github.com/eclipse-mosquitto/mosquitto/issues/2656) | The upstream packet queue is suspected of retaining memory after use. | Measure live enqueue/dequeue counts against allocator and process memory over repeated drains of the corresponding MQTTiumd queue. | Broker performance · [#683](https://github.com/yoch/mqttium/issues/683) |
| [#2830](https://github.com/eclipse-mosquitto/mosquitto/issues/2830) | Memory rises as many clients connect while CPU approaches 100%. | Measure connection churn, CPU, live connection counts and post-drain RSS under representative concurrency. | Broker performance · [#683](https://github.com/yoch/mqttium/issues/683) |
| [#2971](https://github.com/eclipse-mosquitto/mosquitto/issues/2971) | Publications scheduled one second apart arrive 1,110 ms apart. | Record publisher pacing and recipient intervals over a representative network and separate sender jitter from transit and delivery time. | Network performance · [#679](https://github.com/yoch/mqttium/issues/679) |
| [#3008](https://github.com/eclipse-mosquitto/mosquitto/issues/3008) | Mosquitto 2.0.15_1 exhibits high load on FreeBSD 13.2. | Identify the comparable build and workload and measure CPU and throughput on FreeBSD 13.2. | FreeBSD and packaging · [#680](https://github.com/yoch/mqttium/issues/680) |
| [#3641](https://github.com/eclipse-mosquitto/mosquitto/issues/3641) | Mosquitto 2.1.2 exits with a segmentation fault on FreeBSD. | Run the supported workload with recorded FreeBSD and broker build versions and capture process stability and any diagnostic output. | FreeBSD and packaging · [#680](https://github.com/yoch/mqttium/issues/680) |
| [#3499](https://github.com/eclipse-mosquitto/mosquitto/issues/3499) | An intermittent process crash occurs on RHEL 8 VMs with 16 cores and 64 GB, behind HAProxy. | Exercise that topology with multiple broker processes and record the build, concurrency and sustained stability. | Platform and packaging · [#680](https://github.com/yoch/mqttium/issues/680) |
| [#2958](https://github.com/eclipse-mosquitto/mosquitto/issues/2958) | The “v3 client message” and “v3 sub” tests fail on 32-bit ARM. | Run native 32-bit ARM publish and subscribe checks, recording the toolchain, architecture and tested protocol version. | ARM and packaging · [#680](https://github.com/yoch/mqttium/issues/680) |
| [#3409](https://github.com/eclipse-mosquitto/mosquitto/issues/3409) | A Windows binary fails to persist messages with persistence enabled. | Identify a supported Windows build and storage mode, then check acknowledged persistent deliveries across restart. | Windows and storage · [#680](https://github.com/yoch/mqttium/issues/680) |
| [#3076](https://github.com/eclipse-mosquitto/mosquitto/issues/3076) | A systemd service start reaches the unit timeout. | Exercise an actual unit and record its readiness condition, timeout, exit status and journal alongside broker startup. | Service packaging and operations · [#680](https://github.com/yoch/mqttium/issues/680) |

### Runtime checks replayed

| Upstream | Reported condition | Local evidence or remaining work | Responsible role and follow-up |
| --- | --- | --- | --- |
| [#3551](https://github.com/eclipse-mosquitto/mosquitto/issues/3551) | A Clean Start using someone else’s ClientID could erase the original persistent session. | On MQTTiumd, a client with its own valid certificate received CONNACK 0x87 for a ClientID it had no right to use. The original owner resumed with its session intact. | Identity and session maintainers · [#672](https://github.com/yoch/mqttium/issues/672) |
| [#3553](https://github.com/eclipse-mosquitto/mosquitto/issues/3553) | An authenticated principal could resume another principal’s session by reusing its ClientID. | On MQTTiumd, a client with its own valid certificate received CONNACK 0x87 for the unassigned ClientID. The original session remained intact. A normal anonymous reconnect separately demonstrated ClientID reuse on that listener. | Identity and session maintainers · [#672](https://github.com/yoch/mqttium/issues/672) |
| [#1387](https://github.com/eclipse-mosquitto/mosquitto/issues/1387) | A subscriber requests a wildcard outside its permitted topic scope. | Exact and subtree grants allowed the intended subscriptions on both MQTTiumd and Mosquitto. MQTTiumd also refused a filter outside the granted scope. | Authorization and compatibility · [#672](https://github.com/yoch/mqttium/issues/672) |
| [#647](https://github.com/eclipse-mosquitto/mosquitto/issues/647) | Log output appears in a connected client's MQTT stream during descriptor reuse. | On MQTTiumd, the event log rotated at 16 MiB under ordinary MQTT traffic. The same connection continued exchanging MQTT packets across rotation. The upstream kernel descriptor-reuse schedule during churn still needs a trace. | Runtime and observability · [#683](https://github.com/yoch/mqttium/issues/683) |

### Product decisions

| Upstream | Reported condition | Local evidence or remaining work | Responsible role and follow-up |
| --- | --- | --- | --- |
| [#3616](https://github.com/eclipse-mosquitto/mosquitto/issues/3616) | An operator overrides the session expiry interval and the client expects the effective interval in CONNACK. | MQTTiumd has no server-side override or `persistent_client_expiration`. Decide whether expiry may be capped and specify the effective CONNACK property if adopted. | Session protocol · [#673](https://github.com/yoch/mqttium/issues/673) |
| [#2519](https://github.com/eclipse-mosquitto/mosquitto/issues/2519) | An MQTT 3.1 client resumes a persistent session and observes delayed subscription catch-up. | The supported versions are MQTT 3.1.1 and 5.0. Decide the migration path for MQTT 3.1 devices using `MQIsdp`. Legacy catch-up latency remains unmeasured. | Protocol compatibility · [#674](https://github.com/yoch/mqttium/issues/674) |
| [#2538](https://github.com/eclipse-mosquitto/mosquitto/issues/2538) | Mosquitto queue and inflight byte-limit directives behave unexpectedly with 12,000 retained publications. | MQTTiumd lacks `max_queued_bytes` and `max_inflight_bytes`. The reported population exceeds its 4,096-topic ceiling. Decide the byte-limit policy and migration path for larger retained sets. | Capacity and persistence · [#675](https://github.com/yoch/mqttium/issues/675) |
| [#2934](https://github.com/eclipse-mosquitto/mosquitto/issues/2934) | Persistent subscription state appears to change on SIGHUP configuration reload. | SIGHUP terminates the current process. Persistent sessions can resume after restart. Decide whether to introduce live reload and define its effects on connections and sessions. | Runtime and packaging · [#676](https://github.com/yoch/mqttium/issues/676) |
| [#1283](https://github.com/eclipse-mosquitto/mosquitto/issues/1283) | A TLS 1.3 connection defaults to TLS_AES_256_GCM_SHA384 with the reported broker and client. | MQTTiumd uses the TLS library's TLS 1.3 suites and exposes no `ciphers` directive. Decide whether to expose a suite policy and specify how negotiation is checked. | Transport security · [#677](https://github.com/yoch/mqttium/issues/677) |
| [#3718](https://github.com/eclipse-mosquitto/mosquitto/issues/3718) | A shipped image exposes HTTP on all interfaces and raises a software-inventory question. | The broker has no HTTP listener. Decide the intended HTTP scope and reconcile an SBOM and entrypoint/listener inventory against the exact released artifact or image digest. | Release engineering and transport · [#678](https://github.com/yoch/mqttium/issues/678) |
