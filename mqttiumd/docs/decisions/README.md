# Decision records

A record states a context, a decision and its consequences. Its status is
one of:

- **Accepted**: decided with the integration owner. The record says where
  it is applied: in lot L0, or by the lot that will apply it.
- **Proposed**: raised and not decided. It binds nobody.

The specification chapters are never edited. A record that departs from a
chapter names the requirement it amends. An identifier introduced by an
accepted record enters `contracts/v1/requirements.json` with that record as
its source; a test fails when the register and the records drift apart.

| Record | Status | Subject | Applied |
| - | - | - | - |
| [0000](0000-repository-and-launch.md) | Accepted | Repository, layout, language, scope of the launch | L0 |
| [0001](0001-live-indeterminate-path.md) | Accepted with amendments | S0 drives the core through an unknown outcome without a crash | L1, L3, L4 |
| [0002](0002-sqlite-variant-topology.md) | Accepted with amendments | Process topology and evidence of the SQLite variant | L0 in part, L4, L9 |
| [0003](0003-bench-input-audit.md) | Accepted with amendments | Provenance of the broker's inputs; detection of DEM-MU-03 | L2, L3, L4 |
| [0004](0004-contract-revision-status.md) | Accepted with amendments | Revision 1 is strict and provisional, identified by content | L0 |
| [0005](0005-chapter-precedence.md) | Accepted with amendments | Precedence between chapters and resolved differences | L0 |
| [0006](0006-plan-then-apply.md) | Accepted with amendments | Shape of a core transition | L0 in part, L1, L6 |
| [0007](0007-pure-crate-boundary.md) | Accepted with amendments | How the purity of the core is enforced | L0 |
| [0008](0008-early-writer-measurement.md) | Accepted with amendments | An early, non-qualifying measurement of the single writer | L7 |
| [0009](0009-owner-authority.md) | Accepted with amendments | Creation authority for owners and effect boxes; a durable fact is not an authorisation to construct | L1, L2, L4, L7 |
| [0010](0010-credits-interface.md) | Accepted | Credits: the order of the guards of `release_if_current`, `Account::reserve`, unchecked constructors, one record per held lease | L1, L6 |
| [0011](0011-epoch-opening.md) | Accepted | The opening of an epoch: one conditional, idempotent command with an identity; amends 0009 | L1, L2, L3, L4, L7 |
| [0012](0012-reservation-refusals.md) | Accepted | Refusals of a reservation and their wake-ups; `StepOutcome::Exhausted`; a fifth refusal of creation; amends 0006, adds to 0009 | L1, L3, L4 |
| [0013](0013-broker-behaviours-left-open.md) | Accepted with amendments | Twelve behaviours of the broker that the chapters leave open: replays on a live connection, closures, the refusal of an admission and its reason, what happens without a receipt slot | L1, L3, L4 |
| [0014](0014-recovered-facts-order.md) | Accepted | Routes and limits among the recovered facts; the order of an inventory | L1, L2, L3, L7 |
| [0015](0015-scenario-vocabulary-and-fence-point.md) | Accepted | Scenario steps of P1-02b, what a fault holds, the fence point; corrects step 3 of P1-02b in 0001 | L2, L3, L4, L5 |
| [0016](0016-evidence-formats.md) | Accepted | Trace records of the input audit, what every run owes, the perimeter of a run, the store snapshot | L2, L3, L4, L7 |
| [0017](0017-binary-tied-to-its-source.md) | Accepted | `artifact.built_from` in the run manifest: the binary of a run tied to the source it was rebuilt from; what a qualifying result establishes | L3, L4 |
| [0018](0018-permanent-delay.md) | Proposed | Repeated faults, a lag on a repeated `delay_result`, a `DriveUntil` that fires deadlines: a permanent delay stated without fixing the delivery at which the core progresses; completes 0015 | L3, L4 |
| [0019](0019-real-kill-of-the-broker.md) | Accepted | The record `BrokerKilled` of a real kill of the broker, and the member `broker` of the trace header and of the run manifest: where the broker ran | L3, L4 |
| [0020](0020-kill-inside-a-transaction.md) | Accepted | Deterministic transaction points, acknowledged in-flight barriers and identity-based recovery evidence | L3, L4, L7 |
| [0021](0021-typed-bounded-durable-reads.md) | Accepted | Typed bounded durable reads, lifecycle evidence and action termination through the surviving supervisor | L3, L4, L7 |
| [0022](0022-product-tcp-qos2.md) | Accepted with amendments | Product TCP incoming QoS 2, durable session facts, bounded transport and system evidence | Product |
| [0023](0023-incoming-qos1.md) | Accepted | Incoming QoS 1 occurrences and acknowledgement receipts | Product |
| [0024](0024-receive-wire-progress.md) | Accepted | Receive credits follow bounded transport progress | Product |
| [0025](0025-exact-pubsub.md) | Accepted with amendments | Exact persistent subscriptions and outgoing QoS 1; a delivery larger than the peer's Maximum Packet Size is discarded | Product |
| [0026](0026-qos0-volatile-and-consumed.md) | Accepted with amendments | Volatile QoS 0 and consumed durable obligations | Product |
| [0027](0027-live-receipt-retirement.md) | Accepted | Live bounded receipt retirement | Product |
| [0028](0028-outgoing-packet-identifier-reuse.md) | Accepted | Recyclable outgoing Packet Identifiers and immutable PUBACK observations | Product |
| [0029](0029-explicit-session-lifecycle.md) | Accepted with amendments | Explicit bounded zero/infinite session lifecycle, receipted attachments, target-only cancellation and physical cleanup | Product |
| [0030](0030-finite-session-expiry.md) | Accepted | Finite session expiry with a durable temporal anchor | Product |
| [0031](0031-hierarchical-subscriptions.md) | Accepted | Hierarchical subscriptions and per-destination aggregation | Product |
| [0032](0032-durable-retained.md) | Accepted | Topic-owned retained references, atomic frozen replay, ordered RH and live RAP | Product |
| [0033](0033-message-expiry.md) | Accepted with amendments | Original message lifetime, bounded expiry and temporal quarantine | Product/system reception |
| [0034](0034-durable-will.md) | Accepted with amendments | Durable zero-delay QoS1 Will and source-independent intentions | Product and bounded system reception |
| [0035](0035-private-mtls-profile.md) | Accepted with amendments | TLS 1.3, durable ClientID ownership and topic grants | Product |
| [0036](0036-offline-monotone-policy.md) | Accepted | Offline inventory, checked backup and atomic monotone authority transition | Product and SQLite |
| [0037](0037-retained-will.md) | Accepted | Retained zero-delay Will | Product |
| [0038](0038-mqtt311-wire-profile.md) | Accepted | MQTT 3.1.1 wire profile | Product |
| [0039](0039-per-filter-subscribe-authorization.md) | Accepted | Per-filter SUBSCRIBE authorization | Product |
| [0040](0040-mqtt311-incoming-qos2.md) | Accepted | Incoming QoS 2 on MQTT 3.1.1 | Product |
| [0041](0041-outgoing-qos2.md) | Accepted | Durable outgoing QoS 2 | Product |
| [0042](0042-mosquitto-mtls-cutover.md) | Accepted | Checked single-listener Mosquitto cutover | Frozen kit (report 0026) |
| [0043](0043-bounded-reactor-pressure.md) | Accepted with amendments | Bounded reactor progress under transport pressure | Product |
| [0044](0044-reactor-runtime-and-evidence-tiers.md) | Accepted | Target profile of version 0.1, readiness-driven reactor runtime without `async` (departs from ARC-021), two evidence tiers, frozen receptions | Runtime lots |
| [0045](0045-group-commit.md) | Accepted with amendments | Group commit in the SQLite store: up to 64 commands in one transaction, each a savepoint running the code of `apply`; answers after the commit; crash granularity of one group; trait and durable encodings unchanged | Store and store worker; judged by the deterministic store of the lab and, for groups of two formed by the store adapter of the bench, by an oracle campaign on the backend `sqlite-group` |
| [0046](0046-input-backpressure.md) | Accepted with amendments | Input pressure slows a peer instead of disconnecting it: a packet the core cannot take waits in order, a full input lane holds its packet without a deadline, the last places of the lane are kept for packets that release capacity; receipts keep their deadline | Runtime tier |
| [0047](0047-precedence-between-records.md) | Accepted | Precedence between decision records: the later record prevails on the point it states and the earlier one carries an "Amended by" line | All records |
| [0048](0048-runtime-limits.md) | Accepted | Capacities of the runtime and their derivation: the sixteen-connection numbers of eight records describe the bounded profile | Runtime |
| [0049](0049-functional-compatibility.md) | Accepted | Compatibility with the reference broker is functional: the specification prevails over Mosquitto, and the choices where it allows both (PUBACK 0x10 for a publication without subscriber, one copy for overlapping subscriptions, precise refusal codes) | Contract, core, runtime, compatibility bench |
| [0050](0050-acknowledgements-in-order-of-reception.md) | Accepted | The PUBACK and PUBREC of a connection leave in the order of reception of its PUBLISH packets: an answer that would overtake is held in the core and released by the next transition | Core, runtime |
| [0051](0051-core-scaling-indexed-routing-and-credits.md) | Proposed | Core scaling (4,096 sessions, 16,384 subscriptions, 1,024 fanout, 1 MiB payloads), indexed trie routing, aligned exchange credits, SQLite schema 14/15 + `data_version`-guarded caches, and transport coalescing | Contract, core, SQLite store, runtime |
| [0052](0052-volatile-store-and-periodic-snapshot.md) | Proposed | Indexed zero-clone `VolatileStore` (`mqttiumd-store-volatile`) with optional periodic atomic Schema-14/15 SQLite snapshots and explicit `DUR-001`/`DUR-002` readiness reporting | `mqttiumd-store-volatile`, `mqttiumd-store-sqlite`, `mqttiumd` |
| [0053](0053-publish-properties.md) | Proposed | End-to-end MQTT 5 PUBLISH application properties (`0x01`, `0x03`, `0x08`, `0x09`, ordered `0x26`), canonical recipe kind 23, SQLite/Volatile persistence, and MQTT 3.1.1 stripping | Contract, core, stores, runtime |
| [0054](0054-mosquitto-config-subset.md) | Proposed | Native `mosquitto.conf` subset reader (`-c`/`--config`) and static checker (`check-config`) with per-directive CFG-001/002/006/SEC-005 classification | `mqttiumd` |
| [0055](0055-logstore-append-only-wal.md) | Proposed | Append-only sequential `.mlog` durable WAL store (`LogStore`, `--storage-mode log`) with Slice-by-8 CRC32C, torn-tail truncation, fail-closed corruption detection, and automatic Schema-14/15 compaction | `mqttiumd-store-volatile`, `mqttiumd-store-sqlite`, `mqttiumd` |
| [0056](0056-claimed-publication-and-core-optimizations.md) | Proposed | Atomic online admission+claim (`Recipe::ClaimedPublication`, kind 24), `ScopedMap` prefix stripping, Redis-compact runtime/core tables, O(1) `Outbox` control counters, and hash-indexed exact `RouteIndex` | Contract, core, stores, runtime |
| [0057](0057-flatmap-direct-volatile-and-zero-alloc-framing.md) | Proposed | Deterministic open-addressed `FlatMap`, direct synchronous `VolatileStore`, inline `FrameBytes` and the active-peer `ReadyQueue` (Lot 7) | Contract, core, `mqttiumd-store-volatile`, runtime |
| [0058](0058-client-disconnect-reasons-and-tls-close-notify.md) | Proposed | Every legal client DISCONNECT reason code applies its Session Expiry Interval and publishes the Will unless 0x00; Will QoS 3 is malformed; an empty Topic Name is Protocol Error 0x82; close_notify on an orderly TLS close | Runtime |
| [0059](0059-disconnect-durability-boundary.md) | Proposed | The end of a session is durable at its commit and at nothing earlier; the end of the client's stream is not an acknowledgement; the event by storage mode | Runtime |
| [0060](0060-reading-behind-a-waiting-publication.md) | Proposed | The reader goes on behind a publication that waits for a slot of the window: the PUBREL of another exchange goes ahead, every other packet waits behind in order, up to a bound. Open: one session can hold every exchange | Runtime |
| [0061](0061-session-exchange-bound.md) | Proposed | A session holds at most eight inbound QoS 2 exchanges across its connections: at the bound a publication waits while the broker ends one, and the connection is closed when every one awaits a PUBREL. An exchange that awaits its PUBREL holds no payload. The exchange table is sized for the sessions. MQTT 5 is sent DISCONNECT 0x97 on a capacity close | Core, runtime |
| [0062](0062-compatibility-reference-mosquitto-2-1-4.md) | Proposed | The compatibility bench is pinned to Mosquitto 2.1.4, built by `tests/compat/build-reference.sh`; the classification after the replay and nine new scenarios | Runtime (bench) |
| [0063](0063-qos0-paced-per-connection.md) | Proposed | The owner emits QoS 0 by a window of 224 frames per connection, on an output queue of 256, and the next ones as their writes complete; up to 1,024 deliveries wait per connection, the newest is dropped past it; consumed QoS 0 deliveries are paced the same | Core |
| [0064](0064-lot8-direct-dispatch-compact-state-and-wal-safety.md) | Proposed | Direct `Outbox` dispatch and in-place `Draft` stepping, `TargetVec`, consolidated `SessionDeliveries` with O(1) `used_pi_mask`, ownership-moving `VolatileStore::apply_owned` with high-water-mark freshness, `TopicCache`, compact handshake queue reclamation and dense tables, and fail-closed `LogStore::compact()` `wal_desynced` guard (Lot 8) | Contract, core, `mqttiumd-store-volatile`, runtime |
| [0065](0065-lot9-refusals-fairness-and-atomic-fanout.md) | Proposed | Zero-refusal route-mutation serialization under session churn, lossless work wake-ups without 100 ms full-session rescan, `has_expiring` atomic mirror, `OriginOpTable` gap-promotion fix, `Connections`/`Deliveries`/`Shared` encapsulation with `step_checked` index invariants, canonical `Recipe::MultiClaimedPublication` (kind 25) atomic 1:N fan-out, bounded inline pre-claim headroom, and prioritized ACK/round-robin fairness (Lot 9) | Contract, core, stores, lab, runtime |

What each record leaves open is tracked in
[`../reception/open-conditions.md`](../reception/open-conditions.md).
