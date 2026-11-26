# 0066 — Multi-core resident *Delivery Owners* sharding, lock-free inline claim oracle, and drain-first multi-owner group commit (Lot 10)

Status: **Proposed** (25 November 2026). Applied to `mqttiumd-core`, `mqttiumd-store-volatile`, and `mqttiumd`. [Report 0041](../reports/0041-lot10-arm64-delivery-sharding.md) records the ARM64 empirical scaling measurements (`K = 0, 1, 2, 3` vs Mosquitto 2.1.2), per-thread `perf` balance profiles, idle-memory footprint, and full verification ledger.

This record introduces no wire-format changes, no SQLite schema changes (Schema 14/15 unchanged), no `.mlog` record changes, and no new `Recipe` variants (`Recipe` kinds `0..=25` unchanged). `crates/mqttiumd-contract/src/capacity.rs` and all files hashed in `proofs/credits/BODY.sha256` remain byte-for-byte unchanged (`71378725…`), pure crates keep `#![no_std]` + `alloc` and `#![forbid(unsafe_code)]`, the pure-crate boundary suite passes (`17/17`), and the contract digest reported by `mqttiumd-lab validate --contracts contracts/v1` remains unchanged (`sha256:21e96cc81c1f48d59841d8424fee2290102c6782b025222f2d6d2fe72ccf9bb5`).

## Relation to record 0044, and what the code does not meet (added 26 November 2026)

Added by the validation of the lot 10 head (`9dfd568`, report 0043). With
`K = 0`, the default, the runtime is the one of record 0044 (accepted).
With `K >= 1` this record departs from it in three places, and it does
not amend it: the owner decides whether it does.

- 0044 keeps "the single logical owner for version 0.1" and leaves
  partitioned owners (ARC-001) to a later decision. This record brings
  them, as an option.
- 0044 says "no loop polls on a timer for work". A delivery shard waits
  at most `IDLE_TICK` (20 ms), and 1 ms while its outbox holds an effect
  its sink refused, and dispatches again at each wake.
- 0044 keeps "no wait on a full remote queue" from ARC-021. The
  coordinator sends to a shard with a blocking send (`ShardPool::send_to`):
  a shard that does not empty its channel stops the ingress plane.

Statements of this record the code does not meet, kept below as written:

- "Coordinator emits ... without blocking on delivery shards" (Context,
  item 2): see the blocking send above.
- "`SUBACK` before retained replay": the deferral of `NotifyDirty` did
  not cover a read the shard had already started, the one it makes when
  a session attaches. The store could answer it after the commit of the
  SUBSCRIBE that follows the CONNACK, and the shard then queued the
  retained replay before the coordinator queued the SUBACK
  (`tests/delivery_shards.rs` failed about once in 25 runs). Fixed the
  same day: a shard's frames wait for every SUBACK owed to their
  connection (`a_shard_frame_does_not_overtake_the_suback_of_its_connection`).
  MQTT 3.8.4 lets a server send matching PUBLISH packets before the
  SUBACK; this order is the product's, kept equal to that of `K = 0` and
  of the reference broker.
- "`K = 0` preserves the exact Lot 9 path": reports 0040 and 0041 give
  different `K = 0` figures, and `ConnCore` carries the shard route of
  every connection whatever `K`.
- The contract digest it quotes (`21e96cc8...`) is not the one `validate`
  printed at `9dfd568` (`cdd2b28f...`).
- Report 0041, which it cites for its figures, measured a commit 47
  commits older than `9dfd568` with a load generator that could write
  past its buffer (fixed by `675128d`), single runs, against an unpinned
  Mosquitto 2.1.2 holding its state in memory while mqttiumd synchronised
  a log. Its volatile `fanin-32x1-q1` row goes from 0.04 ms at `K = 0` to
  365 ms at `K = 2` (p50); this record does not mention it.

The mode stays experimental until the defects and suspects listed in
`docs/reception/open-conditions.md` are closed and the coordinator and
shard protocol has a model. The inline-claim lease and its barrier have
one since the third part below; the owner decides when the mode leaves
"experimental".

### Corrected since (lot 3 of the validation, first part)

Each by a test committed while it failed, then the fix:

- A shard thread runs its loop under `catch_unwind`; a panic is reported
  to the coordinator as the fault `delivery_shard_panicked` and the broker
  stops, instead of leaving the shard's sessions without deliveries.
- The stop of a shard is a flag, read when its lane is empty, instead of
  a message a full lane could drop. The shard still ends without applying
  acknowledgements it received and had not applied; their deliveries are
  sent again on the next connection.
- A shard releases the reactors' wake-ups before it waits, as
  `Runtime::turn` does; the fairness model reactor-wake assumes now holds
  for every holder.
- A connection the shards cannot attach (no session revision, or a second
  attach of one generation) is closed with a reason that names the cause.
- A shard keeps at most `PENDING_QOS0_LIMIT` (4,096) delegated QoS 0
  deliveries its core cannot take yet, and sheds the next ones with the
  journal entry the core gives a QoS 0 its volatile queue cannot hold.
- The count of claims in flight on a lease never wraps below zero.
- The route of a connection to its shard is boxed: a connection without
  shards pays a pointer for it, not 112 bytes.
- The readiness line ends with `delivery_shards=K experimental`.

Second part, each by a failing test first:

- A shard's claims avoid the packet identifiers the coordinator reserved
  in the lease: at each dirty message the shard gives its core the lease's
  bits, and `Deliveries::free_packet_id` picks beside them. Before, a dirty
  notification sent while an inline claim was pending made the shard
  claim with the reserved identifier (suspect S1).
- A shard publishes no lease while an inline claim committed for the
  session waits to be applied; its identifier was missing from the bitmap
  it published, and the coordinator could reserve it again (suspect B2).
- A claim committed just before the detach of its generation is not
  written to the closed generation and leaves nothing behind (suspect S2,
  a witness: no defect).

Third part, each by a failing test first (report 0046), and checked by
the model `models/inline-lease`:

- A stored target of a committed admission is notified after the
  coordinator's step of the commit, on the store's answer and on a
  resolution alike. Before, the step marked the lease dirty after the
  notification and nothing sent that mark: after one publication admitted
  without an inline claim the coordinator never claimed inline for that
  subscriber again; and a stored publication committed by a resolution
  was never delivered.
- A committed inline claim is held while an inline admission of its
  session has an unknown outcome, and the held claims are handed over in
  order before the session's barrier. Before, the subscriber could
  receive the later publication first (suspect B1).
- A shard does not read a session the coordinator may still owe a
  committed claim: not after a dirty message without sequence, and not
  before the claims that came before a barrier are applied. Before, a
  read during an unknown outcome took the claim's committed row and the
  subscriber received it twice.
- With that fence the first item of the second part is withdrawn: the
  shard's claims no longer look at the coordinator's reserved
  identifiers, which no longer exist when it claims. The second item
  stays.
- A connection without acknowledgement credit reads one packet: one that
  needs no credit goes on, an acknowledgement waits in the task for
  credit and what follows it waits behind it.
- The shard's QoS 0 drop entry names its origin as the coordinator's
  does; a journal entry the shard cannot write stops the broker, as at
  the coordinator; a PUBREL's binding is checked and bounded as there.

**The coordinator's send to a shard stays blocking, for a reason.** A
shard waits only for a bounded time and empties its lane at each pass,
moving what it cannot serve yet into queues that are now bounded by
shedding, not by refusing the lane; a shard that dies is reported (first
item above), and the send then fails instead of waiting. The send can
therefore wait only while a live shard is between two passes. Bounding
the shard by leaving its lane full instead would turn into a cycle: the
coordinator waits for the shard, the shard for the store worker, and the
worker, which takes no request while a reply lane is full, for the
coordinator.

## Context

In Lot 9 (record [0065](0065-lot9-refusals-fairness-and-atomic-fanout.md), report [0040](../reports/0040-lot9-arm64-fairness-fanout-and-sharding.md)), ARM64 `perf` profiling after atomic `1:N` `Recipe::MultiClaimedPublication` showed that the single `Owner` thread consumed **100 % of 1 core** (`65–92 %` of all broker CPU) on high fan-out (`fanout-1x16`) and high-concurrency (`128p`) workloads, while `log-writer` used only `~22–24 %` and the two I/O reactor threads combined used `~8–20 %`. Roughly `70–75 %` of the single Owner thread's CPU on `1:16` fan-out was pure recipient-side work (`EmitPublish`, `ObservedPuback`, `FinishDelivery`, `Deliveries` index maintenance, and `LoadWork` pagination).

Meanwhile, the broker's multi-owner foundations were already built into the store and authority layers:
- `OwnerAuthority::from_grant(grant, owner_count)` grants disjoint `Outbox` instances per `LogicalOwnerId(0..=K)` under the same `(StoreIncarnation, RunEpoch)`.
- Every store (`VolatileStore`, `LogStore`, `SqliteStore`, and `SimStore`) already scopes operation receipts and collection watermarks by `ReceiptOrigin { incarnation, epoch, owner }`.

However, turning resident multi-owner delivery sharding into a zero-regression, linearizable, crash-safe runtime required solving five structural problems:
1. **Multi-owner `OriginOpTable` contention**: `VolatileStore::operations` kept a single hot `OwnerRing`; interleaving `LogicalOwnerId(0)` and `LogicalOwnerId(1..=K)` evicted the ring into `ScopedMap` cold storage on every turn.
2. **Pure-core delegation without leaking threads or atomics into `mqttiumd-core`**: Coordinator (`LogicalOwnerId(0)`) still needed to emit atomic `1:N` `Recipe::ClaimedPublication` / `MultiClaimedPublication` inline without blocking on delivery shards, while preserving `#![no_std]` determinism and exact rollback on `NeedCapacity` and `RejectedCertain`.
3. **Linearizable handoff between inline claims and store catch-up (`LoadWork`)**: A subscriber cannot advertise inline-claim eligibility while Coordinator still has unresolved inline claims or while the shard has not scanned past Coordinator's latest dirty event (attach, detach, unclaimed publish, retained replay, or will).
4. **TCP stream causal ordering (`PUBACK` before `DISCONNECT`, `CONNACK` before `PUBLISH`)**: Direct routing of subscriber `PUBACK`/`PUBREC`/`PUBCOMP` from I/O reactor threads to shard threads must preserve exact MQTT wire ordering against `CONNACK` and `DISCONNECT` handled on the Coordinator.
5. **Multi-owner store backpressure and shard scheduler liveness**: Naive 1:1 round-robin in the exclusive `Writer` starves shard `FinishDelivery` commands (`-1` reference each) behind Coordinator `1:N` admissions (`+N` references each), and draining `ack_inputs` ahead of `StoreResult` can livelock under `ReceiptSlots` pressure.

## Decision

### 1. Opt-in `--delivery-shards K` (`0..=16`, default `0`) and `mqttiumd_delivery_shards`
- Add CLI flag `--delivery-shards K` (`0..=16`, default `0`) and native config directive `mqttiumd_delivery_shards K` (`DirectiveStatus::Equivalent` for `0..=16`, `Blocking` otherwise). Report `delivery_shards` in the `ready` event payload.
- **`K = 0` (default)** preserves the exact Lot 9 single-owner path, including synchronous `DirectWriter` in volatile mode, identical `2,965 B/conn` plain idle RSS, lowest 1:1 latency (`0.03 ms` p50), and maximal 1:1 real-disk `ext4` `fdatasync` coalescing.
- **`K >= 1`** allocates `OwnerAuthority::from_grant(boot.grant, 1 + K)`:
  - `LogicalOwnerId(0)` is the **Coordinator** (ingress, routing, sessions, wills, publication admission, expiry sweeps).
  - `LogicalOwnerId(1..=K)` are `K` resident **`DeliveryShard`** worker threads (`mqtt-deliv-0..K-1`), deterministically partitioning sessions by `session.get() % K`.
  - Each shard owns an independent deterministic `Owner` constructed via `Owner::new_delivery_shard`, its own disjoint `Outbox` and `ReceiptOrigin`, its own SPSC `ShardStoreHandle` to the single exclusive `Writer`, and its own `ShardInbox`.

### 2. Multi-owner `OriginOpTable` hot rings (`mqttiumd-store-volatile`)
- Extend `OriginOpTable<V>` in `crates/mqttiumd-store-volatile/src/tables.rs` to hold up to `MAX_HOT_OWNERS = 32` concurrent hot `OwnerRing<V>` instances indexed by `(StoreIncarnation, RunEpoch, LogicalOwnerId)`.
- Coordinator (`owner 0`) and every `DeliveryShard` (`owner 1..=K`) stay on O(1) direct-indexed ring buffers simultaneously with zero cold-map spillover, while preserving gap-promotion semantics and 10,000-step multi-owner differential equivalence against `BTreeMap`.

### 3. Pure-core `DeliveryShard` hooks and `InlineClaimOracle` (`mqttiumd-core`)
- Add `crates/mqttiumd-core/src/owner/shard.rs` (strict `#![no_std]` + `alloc`):
  - `InlineClaimOracle` trait (`try_reserve`, `mark_ineligible`, `rollback`) passed to `Owner::step_with_oracle` and `Owner::step_and_dispatch_with_oracle`.
  - When `Owner::enable_delegated_deliveries()` is active on the Coordinator:
    - Live QoS 1/2 admissions consult the oracle per target to build `ClaimedPublication` / `MultiClaimedPublication`, rolling back every provisional reservation atomically if the plan is refused (`NeedCapacity` / `Exhausted`) or later receives `StoreOutcome::RejectedCertain`.
    - Committed inline batches are handed out via `take_last_delegated_committed()`; rejected batches via `take_last_delegated_rejected()`; live volatile QoS 0 publications via `take_last_delegated_qos0()`.
    - `pending_delegated_inline_count(session)` tracks in-flight delegated inline admissions per session to fence dirty barriers cleanly.
  - On each `DeliveryShard` `Owner`:
    - `Owner::new_delivery_shard` initializes a recovery-closed delivery owner from Coordinator's `ShardLimits`.
    - `shard_attach_session`, `shard_detach_session`, `shard_retire_session`, `shard_notify_dirty`, `shard_inline_eligibility`, `step_shard_inline_committed`, and `step_shard_qos0` execute delivery state transitions, `Receive Maximum` enforcement, `Maximum Packet Size` discard, QoS 2 `PUBREC -> PUBREL -> PUBCOMP`, and volatile QoS 0 pacing using the exact same core `Deliveries` and `Connections` state machine.
    - `CompactFacts::DeliveryLocal` records explicit `pub_owner` and `claim_owner` so cross-owner deliveries (`owner 0` publication claimed/finished by `owner 1..=K`) remain zero-allocation.

### 4. Lock-free `InlineClaimLease`, direct `ShardIoRoute`, and strict wire ordering (`mqttiumd`)
- **`InlineClaimLease` (`crates/mqttiumd/src/delivery_shard.rs`)**: Cache-line-aligned (`#[repr(align(64))]`) per-session lease holding `dirty_seq`, `acked_dirty_seq`, `eligible_gen`, `in_flight`, `preclaim_cap`, and `used_pi: [AtomicU64; 8]` (512 packet identifiers).
  - Coordinator owns `dirty_seq`; any dirty event (`Attach`, `Detach`, `NotifyDirty`, `mark_ineligible`) clears `eligible_gen = 0` and increments `dirty_seq`. If `pending_delegated_inline_count(session) > 0`, Coordinator defers sending `DirtyBarrier { session, dirty_seq }` until the last pending inline admission resolves (strictly after `InlineCommitted`), ensuring the shard never resets `used_pi` while Coordinator holds an in-flight claim.
  - Shard tracks `scan_started_dirty_seq` on `LoadWork` completion (`next: None`) and only publishes `eligible_gen` and `acked_dirty_seq = seen_dirty_seq` when `scan_started_dirty_seq >= seen_dirty_seq` and `owner.shard_inline_eligibility` confirms `caught_up == true`, zero queued deliveries, and all PIs `<= 512`.
  - While published, Coordinator exclusively sets bits (`fetch_or` + `fetch_add`) and Shard exclusively clears bits (`fetch_and` + `fetch_sub`) upon `last_finished_delivery()`.
- **Direct I/O steering (`crates/mqttiumd/src/net.rs`)**:
  - `Connection::outgoing_handle()` exposes a cloneable `OutgoingHandle` so shards write `PUBLISH` / `PUBREL` frames straight into the subscriber's bounded `ConnCore::outgoing` queue without touching the Coordinator.
  - `Connection::install_shard_route(ShardIoRoute)` steers subscriber `PUBACK`/`PUBREC`/`PUBCOMP` (`first >> 4` in `{4, 5, 7}`) and shard-owned `Written` receipts (`effect.owner == route.owner_id`) directly from I/O reactor threads into the owning shard's `ShardInbox`. Both plain TCP and TLS 1.3 I/O tasks still call `record_heard()` before routing so Keep Alive remains exact.
  - **Strict TCP wire ordering**:
    - `CONNACK` before `PUBLISH`: `install_shard_route` and `CoordToShard::Attach` execute only after `CONNACK` is accepted into `ConnCore::outgoing`.
    - `SUBACK` before retained replay (broker ordering guarantee): for `SubscriptionsChanged` with retained replays, Coordinator records `pending_suback_notify` and defers `CoordToShard::NotifyDirty` until `SUBACK` is accepted into `ConnCore::outgoing` (both single-filter `Sink::try_submit` and multi-filter `serve_requests`).
    - An admitted `DISCONNECT` seals the shared output queue in the same critical section. The writer drains the previously admitted frames and the terminal packet in order. Every handle observes the seal, including delivery shards.
    - Each shard reserves ACK capacity before parsing a TCP or TLS input batch. A routed ACK keeps its slot while awaiting application by the owner. Once capacity is exhausted, the reactor leaves the next input unread and resumes when a slot is returned. Shard-owned writes have a separate bounded reserve for their Started and terminal reports.
    - Detachment waits for the transport to finish publishing its I/O observations, then for the shard to apply the generation's ACK and write reports and resolve the resulting durable QoS transitions. The coordinator retains the peer until it receives the completion. Other sessions can progress while this generation waits for writer capacity or a store reply.
    - A shard can request one close per attached generation. The first reason remains available until the coordinator takes it or the transport closes. A separate slot retains the first fatal error, including while the coordinator is busy.

### 5. Drain-first multi-owner `Writer` and starvation-free shard scheduler (`writer.rs`, `delivery_shard.rs`)
- **Multi-owner SPSC `Writer` (`crates/mqttiumd/src/writer.rs`)**: `Writer::spawn_configured_sharded(..., extra_owners)` allocates `1 + K` independent SPSC request/reply/control lanes (`lanes[0]` Coordinator, `lanes[1..=K]` shards) sharing the single exclusive durable store and coalescing consecutive `Persist` commands across all owners into one group commit (`group = 1024` for `Log` and sharded `Volatile`, `128` for `Sqlite`).
- **Drain-first scheduling**: When `K >= 1`, `Writer` drains shard lanes (`1..=K`, credit-releasing `FinishDelivery`/`ClaimDelivery`/`ConsumeDelivery`) round-robin first up to `group - coord_reserve` (`coord_reserve = (group / 8).clamp(8, 64)`), then drains Coordinator (`lanes[0]`, credit-allocating `AdmitPublication`) up to `group`, then backfills remaining slots from shards. This maintains `1:N` steady-state equilibrium without overflowing store `References` (`0x97 QuotaExceeded`) while never starving publishers.
- **Starvation-free shard lanes & O(1) timer check (`delivery_shard.rs`)**: Separate `store_inputs` (`StoreResult`, `WorkPage`, `MessagesExpired`) and drain in strict credit-releasing order (`control_inputs` -> `terminal_inputs` -> `store_inputs` -> `ack_inputs` -> `inputs`) with per-lane blocking on `NeedCapacity`, immediate `schedule_maintenance()` on `ReceiptSlots` pressure, and an O(1) `next_timer_due: Option<Instant>` fast check avoiding per-turn `BTreeMap` scans in `due_deadlines` and `idle_wait`.
- **Ordered shutdown**: `run_inner` shuts down Coordinator sessions (`Detach` sent to shards), closes/joins sockets, and drains + joins all shard threads (`ShardPool::shutdown`) before outer `run()` sends `Request::Stop` to `Writer`.

## Consequences

- **Zero regression at default `K = 0` (report [0041](../reports/0041-lot10-arm64-delivery-sharding.md))**:
  - Plain TCP idle RSS at 768 connections remains **2,965 B/conn** (byte-identical to Lot 9); mTLS at 400 connections is **12,575 B/conn**.
  - `DirectWriter` volatile latency (`0.03 ms` p50) and real `ext4` 1:1 `log` throughput (`40,787 deliv/s`) are unchanged.
- **Multi-core ARM64 throughput, tail latency, and per-thread balance (`K = 2, 3`, 3 broker cores)**:
  - `fanout-1x16-q1`: `volatile` scales from `171,864 -> 239,920 deliv/s` (**+39.6 %**, `K = 2`, 99.97 % of Mosquitto 2.1.2 `240,000/s`, `p50` `1.49 -> 1.16 ms`); durable `log` scales from `153,544 -> 229,360 deliv/s` (**+49.4 %**, `K = 3`, `p50 = 1.31 ms`, `0` refusals, `0.00 %` loss).
  - `fanout-1x4-q1` (`log`): `143,976 -> 160,000 deliv/s` (**+11.1 %**, 100 % offered rate, `p50` `0.84 -> 0.36 ms`).
  - `fanin-32x1-q1` (`log`, `K = 3`): `p50 / p99` drops from `547.58 / 772.78 ms` to **`0.21 / 8.74 ms`** (**2,600× lower p50**, **88× lower p99**, `0` refusals).
  - `128p QoS 1 @ 120k/s` (`log`, `K = 2`): `54,970 -> 70,728 deliv/s` (**+28.7 %**, `p50` `16.57 -> 13.11 ms`).
  - `32p QoS 0 @ 300k/s` (`volatile`, `K = 2`): `p50` drops **66×** (`308.15 -> 4.65 ms`, `wire` `+18.2 %`).
  - All 8/8 adversarial scenarios achieve **0 refusals and 0.00 % loss** on `K = 2` and `K = 3` in both `volatile` and `log` modes, and ARM64 `perf` confirms balanced multi-core execution (`fanout-1x16-q1` `log` `K = 3`: `log-writer` 23.6 %, three shards 16.7 % / 15.0 % / 14.6 %, Coordinator 10.0 %, two I/O reactors 10.0 % / 10.0 %).
- **Trade-offs and operational guidance**:
  - `K = 2` adds ~7.9 MiB fixed base RSS and ~1.2 KiB/conn (`4,224 B/conn` plain at 768 connections) for the resident shard `Owner` tables and thread stacks, and adds a cross-thread hop on low-concurrency or real-disk 1:1 `fdatasync` workloads where `K = 0` remains faster.
  - Under extreme single-shard saturation (`K = 1` facing 240k deliv/s `1:16` fan-out), Coordinator can still outrun a single shard thread and fill store `References` (`0x97`); operators enabling sharding should set `K >= 2` (or `K = 3` on ≥4-core hosts with heavy durable fan-out).
- **Crash-recovery equivalence**: Multi-owner `SIGKILL` tests confirm exact persistence and cross-mode recovery (`--delivery-shards 2` -> `SIGKILL` -> `--delivery-shards 2` -> `SIGKILL` -> `--delivery-shards 0`) across both `LogStore` and `SqliteStore`.
