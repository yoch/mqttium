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
//! ```text
//! cargo run --release -p mqttiumd-store-sqlite --example measure_writer -- \
//!     --dir <directory on the storage measured> --payload 1024 --fanout 1 \
//!     --ops 2000 --warmup 100 [--memory]
//! ```

use std::path::PathBuf;
use std::time::{Duration, Instant};

use mqttiumd_contract::capacity::{
    BoundedVec, ExchangeSlots, PayloadBytes, ReceiptSlots, References,
};
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
use mqttiumd_lab::store::durable::{AdminCommand, AdminDone, DurableStore, Limits, MemoryStore};
use mqttiumd_store_sqlite::SqliteStore;
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
}

fn options() -> Options {
    let mut options = Options {
        dir: std::env::temp_dir(),
        payload: 1024,
        fanout: 1,
        ops: 2000,
        warmup: 100,
        memory: false,
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
            other => panic!("unknown flag {other}"),
        }
    }
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
        targets: BoundedVec::from_slice(&targets, targets.len().max(1)).expect("fits"),
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

fn measure<S: DurableStore>(store: &mut S, options: &Options) -> Value {
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
    let started = Instant::now();
    for n in options.warmup + 1..=options.warmup + options.ops {
        admit.push(committed(
            store,
            &admission(epoch, route_revision, producer, &targets, n, &payload),
        ));
        free.push(committed(store, &release(epoch, producer, n)));
    }
    let window = started.elapsed();
    let after = usage();
    json!({
        "window_s": window.as_secs_f64(),
        "transactions_per_second": (2 * options.ops) as f64 / window.as_secs_f64(),
        "admit_publication": summary(admit),
        "release_inbound": summary(free),
        "usage_before": before,
        "usage_after": after,
    })
}

fn main() {
    let options = options();
    let parameters = json!({
        "payload_bytes": options.payload,
        "fanout": options.fanout,
        "ops": options.ops,
        "warmup": options.warmup,
        "one_transaction_per_command": true,
    });
    let report = if options.memory {
        let mut store = MemoryStore::create(INCARNATION, limits(&options));
        json!({
            "exploratory": true,
            "qualifies": "nothing",
            "backend": "memory (the deterministic store: no medium, no transaction)",
            "parameters": parameters,
            "result": measure(&mut store, &options),
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
        let settings = store.effective_settings().clone();
        let result = measure(&mut store, &options);
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
