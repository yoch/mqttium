//! An early, exploratory measurement of the single writer (record 0008).
//!
//! It runs the real `AdmitPublication` transaction of `SqliteStore` on its
//! real tables, followed by the `ReleaseInbound` that frees its inbound
//! slot, one transaction each, with WAL and `synchronous=FULL` read back.
//! It drives the store directly: no adapter, no core, no evidence, no
//! process control, so that the cost reported is the writer's.
//!
//! What it counts is durable store transactions per second, not MQTT
//! messages of a broker. It qualifies nothing: no threshold, no
//! comparison, no profile. `tools/measure_writer.sh` runs it with the
//! description of the machine; the data it writes is never committed.
//!
//! With `--group N` (record 0045) the same stream of commands, admission
//! then release of each exchange, is committed `N` commands at a time
//! through `SqliteStore::apply_group`: one engine transaction and one
//! synchronisation for each group. `transactions_per_second` then counts
//! the commands made durable, and `commits_per_second` the groups. The
//! default, 1, is the measurement of record 0008 unchanged: `apply`, one
//! transaction for each command.
//!
//! ```text
//! cargo run --release -p mqttiumd-store-sqlite --example measure_writer -- \
//!     --dir <directory on the storage measured> --payload 1024 --fanout 1 \
//!     --ops 2000 --warmup 100 [--group 32] [--memory]
//! ```

use std::path::PathBuf;
use std::time::{Duration, Instant};

use mqttiumd_contract::capacity::{ExchangeSlots, PayloadBytes, ReceiptSlots, References};
use mqttiumd_contract::command::{
    AdmitPublication, Command, Costs, DeliveryCause, FrozenRecipe, PayloadRef, Recipe,
    ReleaseInbound, Source, Target,
};
use mqttiumd_contract::ids::{
    DeliveryId, ExchangeGeneration, ExchangeId, InboundPacketId, LogicalOwnerId, OperationId,
    OperationSeq, PayloadId, PayloadSeq, PublicationId, PublicationSeq, RequestId, RequestSeq,
    Revision, RunEpoch, SessionId, StoreIncarnation,
};
use mqttiumd_contract::outcome::{StoreOutcome, StoreReply};
use mqttiumd_contract::packet::{Qos, SharedBytes};
use mqttiumd_lab::store::conformance::open;
use mqttiumd_lab::store::durable::{
    AdminCommand, AdminDone, Applied, DurableStore, Limits, MemoryStore,
};
use mqttiumd_store_sqlite::{MAX_GROUP, SqliteStore};
use serde_json::{Value, json};

const INCARNATION: StoreIncarnation = StoreIncarnation::new(1);
const OWNER: LogicalOwnerId = LogicalOwnerId::new(0);
const TOPIC: &[u8] = b"measure/writer";

struct Options {
    dir: PathBuf,
    payload: usize,
    fanout: usize,
    ops: u64,
    warmup: u64,
    memory: bool,
    /// Commands committed together (record 0045). 1: `apply`, one
    /// transaction for each command.
    group: usize,
    /// For the experiment of report 0002 only: another
    /// `wal_autocheckpoint`, in pages. The store keeps its default.
    wal_autocheckpoint: Option<i64>,
}

fn options() -> Options {
    let mut options = Options {
        dir: std::env::temp_dir(),
        payload: 1024,
        fanout: 1,
        ops: 2000,
        warmup: 100,
        memory: false,
        group: 1,
        wal_autocheckpoint: None,
    };
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let mut value = || args.next().expect("a value after the flag");
        match flag.as_str() {
            "--dir" => options.dir = PathBuf::from(value()),
            "--payload" => options.payload = value().parse().expect("bytes"),
            "--fanout" => options.fanout = value().parse().expect("targets"),
            "--ops" => options.ops = value().parse().expect("operations"),
            "--warmup" => options.warmup = value().parse().expect("operations"),
            "--memory" => options.memory = true,
            "--group" => options.group = value().parse().expect("commands"),
            "--wal-autocheckpoint" => {
                options.wal_autocheckpoint = Some(value().parse().expect("pages"));
            }
            other => panic!("unknown flag {other}"),
        }
    }
    assert!(
        (1..=MAX_GROUP).contains(&options.group),
        "a group holds 1 to {MAX_GROUP} commands"
    );
    options
}

fn limits(options: &Options) -> Limits {
    let total = options.ops + options.warmup;
    Limits {
        payload_bytes: PayloadBytes::new(total * options.payload as u64 + 1),
        references: References::new(total * options.fanout as u64 + 1),
        exchange_slots: ExchangeSlots::new(4),
        receipt_slots: ReceiptSlots::new(2 * total + 1),
        max_targets_per_admission: options.fanout.max(1),
        max_topic_bytes: 256,
        max_payload_bytes: options.payload.max(1),
        max_sessions: options.fanout + 1,
        max_route_targets: options.fanout.max(1),
    }
}

/// The producer and `fanout` destinations, routed on one topic, in the
/// first opened epoch. Returns the producer, the targets, the route
/// revision and the epoch.
fn prepare<S: DurableStore>(
    store: &mut S,
    fanout: usize,
) -> (SessionId, Vec<SessionId>, Revision, RunEpoch) {
    let mut create = |name: String| match store.admin(AdminCommand::CreateSession {
        client_id: name.into_bytes(),
    }) {
        Ok(AdminDone::SessionCreated { session, .. }) => session,
        other => panic!("no session: {other:?}"),
    };
    let producer = create("producer".to_owned());
    let targets: Vec<SessionId> = (0..fanout).map(|n| create(format!("target-{n}"))).collect();
    let mut route_revision = Revision::new(0);
    for session in &targets {
        match store.admin(AdminCommand::AddRoute {
            topic: TOPIC.to_vec(),
            session: *session,
            qos: Qos::AtLeastOnce,
        }) {
            Ok(AdminDone::RouteAdded { route_revision: r }) => route_revision = r,
            other => panic!("no route: {other:?}"),
        }
    }
    let epoch = open(store);
    (producer, targets, route_revision, epoch)
}

fn admission(
    epoch: RunEpoch,
    route_revision: Revision,
    producer: SessionId,
    targets: &[SessionId],
    n: u64,
    payload: &[u8],
) -> Command {
    let publication = PublicationId::new(INCARNATION, epoch, OWNER, PublicationSeq::new(n));
    let length = PayloadBytes::new(payload.len() as u64);
    let targets: Vec<Target> = targets
        .iter()
        .zip(0u32..)
        .map(|(session, index)| Target {
            session: *session,
            session_revision: Revision::new(1),
            delivery: DeliveryId { publication, index },
            effective_qos: Qos::AtLeastOnce,
            cause: DeliveryCause::LiveExact,
        })
        .collect();
    let recipe = Recipe::AdmitPublication(AdmitPublication {
        operation: OperationId::new(INCARNATION, epoch, OWNER, OperationSeq::new(2 * n)),
        request: RequestId::new(INCARNATION, epoch, OWNER, RequestSeq::new(n)),
        publication,
        source: Source {
            session: producer,
            session_revision: Revision::new(1),
            packet_id: packet(n),
            exchange: ExchangeId {
                session: producer,
                generation: ExchangeGeneration::new(n),
            },
        },
        route_revision,
        qos: Qos::ExactlyOnce,
        retain: false,
        topic: SharedBytes::copy_from(TOPIC, 256).expect("fits"),
        payload: PayloadRef {
            id: PayloadId::new(INCARNATION, epoch, OWNER, PayloadSeq::new(n)),
            length,
        },
        costs: Costs {
            payload_bytes: length,
            references: References::new(targets.len() as u64),
            exchange_slots: ExchangeSlots::new(1),
            receipt_slots: ReceiptSlots::new(1),
        },
        targets: mqttiumd_contract::command::TargetVec::from_slice(&targets, targets.len().max(1))
            .expect("fits"),
    });
    Command {
        recipe: FrozenRecipe::freeze(recipe).expect("well formed"),
        payload: Some(SharedBytes::copy_from(payload, payload.len()).expect("fits")),
    }
}

fn packet(n: u64) -> InboundPacketId {
    InboundPacketId::new(((n - 1) % 65_535 + 1) as u16).expect("non-zero")
}

fn release(epoch: RunEpoch, producer: SessionId, n: u64) -> Command {
    let recipe = Recipe::ReleaseInbound(ReleaseInbound {
        operation: OperationId::new(INCARNATION, epoch, OWNER, OperationSeq::new(2 * n + 1)),
        source_session: producer,
        packet_id: packet(n),
        exchange: ExchangeId {
            session: producer,
            generation: ExchangeGeneration::new(n),
        },
    });
    Command {
        recipe: FrozenRecipe::freeze(recipe).expect("well formed"),
        payload: None,
    }
}

fn committed(store: &mut impl DurableStore, command: &Command) -> Duration {
    let started = Instant::now();
    let applied = store.apply(command);
    let elapsed = started.elapsed();
    assert!(
        matches!(
            applied.reply,
            StoreReply::Outcome(StoreOutcome::Committed(_))
        ),
        "not committed: {:?}",
        applied.reply
    );
    elapsed
}

/// Latencies in microseconds. The two kinds alternate in one window, so
/// a rate is given for the window only, not per kind.
fn summary(mut latencies: Vec<Duration>) -> Value {
    latencies.sort();
    let at = |q: f64| {
        let index = ((latencies.len() as f64 - 1.0) * q).round() as usize;
        latencies[index].as_secs_f64() * 1e6
    };
    let mean = latencies.iter().map(Duration::as_secs_f64).sum::<f64>() / latencies.len() as f64;
    json!({
        "count": latencies.len(),
        "mean_us": mean * 1e6,
        "p50_us": at(0.50),
        "p90_us": at(0.90),
        "p99_us": at(0.99),
        "p999_us": at(0.999),
        "max_us": at(1.0),
    })
}

/// User and system CPU time of this process, in clock ticks, and its peak
/// resident memory, from `/proc` (Linux).
fn usage() -> Value {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let fields: Vec<&str> = stat
        .rsplit_once(')')
        .map(|(_, rest)| rest.split_whitespace().collect())
        .unwrap_or_default();
    let ticks = |index: usize| fields.get(index).and_then(|v| v.parse::<u64>().ok());
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let hwm = status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .map(|v| v.trim().to_owned());
    json!({
        "utime_ticks": ticks(11),
        "stime_ticks": ticks(12),
        "peak_rss": hwm,
    })
}

/// The slowest admissions, in microseconds, with their place in the
/// window, and how many exceed fixed thresholds.
fn tail(latencies: &[Duration]) -> Value {
    let mut ranked: Vec<(usize, Duration)> = latencies.iter().copied().enumerate().collect();
    ranked.sort_by_key(|entry| std::cmp::Reverse(entry.1));
    let over = |micros: u64| {
        latencies
            .iter()
            .filter(|latency| **latency > Duration::from_micros(micros))
            .count()
    };
    json!({
        "slowest": ranked
            .iter()
            .take(10)
            .map(|(index, latency)| json!({"step": index, "us": latency.as_secs_f64() * 1e6}))
            .collect::<Vec<_>>(),
        "over_1ms": over(1_000),
        "over_2ms": over(2_000),
        "over_4ms": over(4_000),
        "over_10ms": over(10_000),
    })
}

fn measure<S: DurableStore>(store: &mut S, options: &Options, wal: Option<&PathBuf>) -> Value {
    let (producer, targets, route_revision, epoch) = prepare(store, options.fanout);
    let payload = vec![0x5A; options.payload];
    for n in 1..=options.warmup {
        committed(
            store,
            &admission(epoch, route_revision, producer, &targets, n, &payload),
        );
        committed(store, &release(epoch, producer, n));
    }
    let before = usage();
    let mut admit = Vec::with_capacity(options.ops as usize);
    let mut free = Vec::with_capacity(options.ops as usize);
    // The largest the WAL file grew, sampled after each step, outside the
    // timed transactions; the sampling is in the window.
    let mut wal_max = 0u64;
    let started = Instant::now();
    for n in options.warmup + 1..=options.warmup + options.ops {
        admit.push(committed(
            store,
            &admission(epoch, route_revision, producer, &targets, n, &payload),
        ));
        free.push(committed(store, &release(epoch, producer, n)));
        if let Some(wal) = wal {
            wal_max = wal_max.max(std::fs::metadata(wal).map_or(0, |m| m.len()));
        }
    }
    let window = started.elapsed();
    let admission_tail = tail(&admit);
    let release_tail = tail(&free);
    let after = usage();
    json!({
        "window_s": window.as_secs_f64(),
        "transactions_per_second": (2 * options.ops) as f64 / window.as_secs_f64(),
        "admit_publication": summary(admit),
        "release_inbound": summary(free),
        "admit_publication_tail": admission_tail,
        "release_inbound_tail": release_tail,
        "wal_max_bytes": wal.map(|_| wal_max),
        "usage_before": before,
        "usage_after": after,
    })
}

/// A store that commits several commands together.
trait GroupWriter: DurableStore {
    fn commit_together(&mut self, commands: &[&Command]) -> Vec<Applied>;
}

impl GroupWriter for SqliteStore {
    fn commit_together(&mut self, commands: &[&Command]) -> Vec<Applied> {
        self.apply_group(commands).expect("within the bound")
    }
}

/// The deterministic store has no medium and no transaction: a group is
/// its commands in order.
impl GroupWriter for MemoryStore {
    fn commit_together(&mut self, commands: &[&Command]) -> Vec<Applied> {
        commands.iter().map(|command| self.apply(command)).collect()
    }
}

/// Commits the commands of `exchanges`, admission then release of each,
/// `group` at a time, and hands the duration of each group to `timed`.
/// The commands are built outside the timed call, as for `apply`.
fn grouped<S: GroupWriter>(
    store: &mut S,
    group: usize,
    exchanges: std::ops::RangeInclusive<u64>,
    build: &dyn Fn(u64) -> [Command; 2],
    mut timed: impl FnMut(Duration),
) {
    let mut pending: Vec<Command> = Vec::with_capacity(group);
    let mut flush = |store: &mut S, pending: &mut Vec<Command>| {
        let members: Vec<&Command> = pending.iter().collect();
        let started = Instant::now();
        let applied = store.commit_together(&members);
        let elapsed = started.elapsed();
        assert_eq!(applied.len(), members.len());
        for reply in &applied {
            assert!(
                matches!(reply.reply, StoreReply::Outcome(StoreOutcome::Committed(_))),
                "not committed: {:?}",
                reply.reply
            );
        }
        pending.clear();
        timed(elapsed);
    };
    for n in exchanges {
        for command in build(n) {
            pending.push(command);
            if pending.len() == group {
                flush(store, &mut pending);
            }
        }
    }
    if !pending.is_empty() {
        flush(store, &mut pending);
    }
}

/// The measurement with `--group N`, N above 1: the same stream of
/// commands as `measure`, committed N at a time.
fn measure_groups<S: GroupWriter>(
    store: &mut S,
    options: &Options,
    wal: Option<&PathBuf>,
) -> Value {
    let (producer, targets, route_revision, epoch) = prepare(store, options.fanout);
    let payload = vec![0x5A; options.payload];
    let build = |n: u64| {
        [
            admission(epoch, route_revision, producer, &targets, n, &payload),
            release(epoch, producer, n),
        ]
    };
    if options.warmup > 0 {
        grouped(store, options.group, 1..=options.warmup, &build, |_| {});
    }
    let before = usage();
    let mut groups = Vec::new();
    // The largest the WAL file grew, sampled after each group, outside the
    // timed commit; the sampling is in the window.
    let mut wal_max = 0u64;
    let started = Instant::now();
    grouped(
        store,
        options.group,
        options.warmup + 1..=options.warmup + options.ops,
        &build,
        |elapsed| {
            groups.push(elapsed);
            if let Some(wal) = wal {
                wal_max = wal_max.max(std::fs::metadata(wal).map_or(0, |m| m.len()));
            }
        },
    );
    let window = started.elapsed();
    let group_tail = tail(&groups);
    let after = usage();
    json!({
        "window_s": window.as_secs_f64(),
        "transactions_per_second": (2 * options.ops) as f64 / window.as_secs_f64(),
        "commits_per_second": groups.len() as f64 / window.as_secs_f64(),
        "group_commit": summary(groups),
        "group_commit_tail": group_tail,
        "wal_max_bytes": wal.map(|_| wal_max),
        "usage_before": before,
        "usage_after": after,
    })
}

/// One transaction for each command, or one for each group.
fn measured<S: GroupWriter>(store: &mut S, options: &Options, wal: Option<&PathBuf>) -> Value {
    if options.group == 1 {
        measure(store, options, wal)
    } else {
        measure_groups(store, options, wal)
    }
}

fn main() {
    let options = options();
    let parameters = json!({
        "payload_bytes": options.payload,
        "fanout": options.fanout,
        "ops": options.ops,
        "warmup": options.warmup,
        "wal_autocheckpoint_set": options.wal_autocheckpoint,
        "group": options.group,
        "one_transaction_per_command": options.group == 1,
    });
    let report = if options.memory {
        let mut store = MemoryStore::create(INCARNATION, limits(&options));
        json!({
            "exploratory": true,
            "qualifies": "nothing",
            "backend": "memory (the deterministic store: no medium, no transaction)",
            "parameters": parameters,
            "result": measured(&mut store, &options, None),
        })
    } else {
        std::fs::create_dir_all(&options.dir).expect("the directory");
        let path = options.dir.join(format!(
            "measure-{}-{}-{}.db",
            std::process::id(),
            options.payload,
            options.fanout
        ));
        let mut store =
            SqliteStore::create(&path, INCARNATION, limits(&options)).expect("a new store");
        if let Some(pages) = options.wal_autocheckpoint {
            store
                .set_wal_autocheckpoint_for_measurement(pages)
                .expect("the setting");
        }
        let settings = store.effective_settings().clone();
        let mut wal = path.clone().into_os_string();
        wal.push("-wal");
        let wal = PathBuf::from(wal);
        let result = measured(&mut store, &options, Some(&wal));
        let size = |suffix: &str| {
            let mut file = path.clone().into_os_string();
            file.push(suffix);
            std::fs::metadata(PathBuf::from(file)).map(|m| m.len()).ok()
        };
        // Before the store is closed: closing checkpoints the WAL away.
        let sizes = json!({"db": size(""), "wal": size("-wal")});
        drop(store);
        for suffix in ["", "-wal", "-shm"] {
            let mut file = path.clone().into_os_string();
            file.push(suffix);
            let _ = std::fs::remove_file(PathBuf::from(file));
        }
        json!({
            "exploratory": true,
            "qualifies": "nothing",
            "backend": "sqlite",
            "settings": {
                "sqlite_version": settings.sqlite_version,
                "compile_options": settings.compile_options,
                "journal_mode": settings.journal_mode,
                "synchronous": settings.synchronous,
                "user_version": settings.user_version,
                "page_size": settings.page_size,
                "wal_autocheckpoint": settings.wal_autocheckpoint,
            },
            "file_bytes_before_close": sizes,
            "parameters": parameters,
            "result": result,
        })
    };
    println!("{}", serde_json::to_string_pretty(&report).expect("JSON"));
}
