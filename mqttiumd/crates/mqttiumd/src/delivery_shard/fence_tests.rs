use super::*;
use mqttiumd_contract::command::{Recipe, StartupNonce};
use mqttiumd_contract::effects::OwnerAuthority;
use mqttiumd_contract::ids::{
    DeliveryId, OperationSeq, PublicationId, PublicationSeq, RunEpoch, StoreIncarnation,
};
use mqttiumd_contract::outcome::{EpochOpened, EpochOpening, OpeningOutcome, OpeningReply};
use mqttiumd_contract::packet::Qos;
use std::net::{TcpListener, TcpStream};
use std::num::NonZeroUsize;

/// A delivery shard worker built by hand, driven by the test: nothing runs
/// its loop. The ends of its lanes are kept so that none reads as closed.
struct Harness {
    incarnation: StoreIncarnation,
    epoch: RunEpoch,
    worker: DeliveryShardWorker,
    commands: Signalled<CoordToShard>,
    events: std::path::PathBuf,
    _lanes: (
        mpsc::Receiver<Request>,
        mpsc::SyncSender<Response>,
        mpsc::Receiver<ControlRequest>,
        mpsc::SyncSender<ControlResponse>,
    ),
}

fn harness(name: &str) -> Harness {
    let incarnation = StoreIncarnation::new(1);
    let epoch = RunEpoch::new(2);
    let mut opening = EpochOpening::new(incarnation, StartupNonce::new([7; 16]));
    let command = opening.submit(RunEpoch::new(1)).unwrap();
    let _ = opening
        .observe(OpeningReply {
            command,
            outcome: OpeningOutcome::Committed(EpochOpened { incarnation, epoch }),
        })
        .unwrap();
    let nz = |n| NonZeroU32::new(n).unwrap();
    let mut authority = OwnerAuthority::from_grant(opening.hand_over().unwrap(), nz(2)).unwrap();
    let outbox = authority
        .create_outbox(
            LogicalOwnerId::new(1),
            NonZeroUsize::new(64).unwrap(),
            nz(8),
        )
        .unwrap();
    let owner = Owner::new_delivery_shard(
        OwnerConfig {
            max_sessions: nz(2),
            max_routes: nz(2),
            max_exchanges: nz(4),
            session_qos2_exchanges: nz(2),
            account: AccountId::new(1),
            payload_bytes: PayloadBytes::new(4096),
            resolution_delay: LogicalDuration::new(10),
            retained_operations: nz(16),
            remembered_connections: nz(2),
        },
        outbox,
        ShardLimits {
            max_targets: 2,
            max_topic_bytes: 32,
            max_payload_bytes: PayloadBytes::new(1024),
        },
    )
    .unwrap();

    let wake = Wake::new();
    let (store_tx, requests) = crate::wake::signalled(8, &wake);
    let (reply, receiver) = mpsc::sync_channel(8);
    let (control_sender, control) = crate::wake::signalled(8, &wake);
    let (control_reply, control_receiver) = mpsc::sync_channel(8);
    let store = ShardStoreHandle {
        sender: store_tx,
        receiver,
        control_sender,
        control_receiver,
        wake: wake.clone(),
        has_expiring: Arc::default(),
    };
    let (commands, coord_rx) = crate::wake::signalled(8, &wake);
    let path = std::env::temp_dir().join(format!(
        "mqttiumd-shard-{name}-{}.jsonl",
        std::process::id()
    ));
    let events = Events::open(&path).unwrap();
    let worker = DeliveryShardWorker::new(
        owner,
        store,
        coord_rx,
        ShardInbox::new(wake.clone(), 8, 8),
        Arc::new(Notices::new(wake)),
        events,
        Arc::new(crate::session_time::SystemClock::new().unwrap()),
        Limits::BOUNDED,
        Arc::default(),
        Arc::default(),
    );
    Harness {
        incarnation,
        epoch,
        worker,
        commands,
        events: path,
        _lanes: (requests, reply, control, control_reply),
    }
}

#[test]
fn submitted_ack_precedes_detach_even_when_the_writer_withholds_credit() {
    let Harness {
        incarnation,
        epoch,
        mut worker,
        commands,
        events,
        _lanes: (requests, _reply, _control, _control_reply),
    } = harness("ack-fence");
    std::fs::remove_file(&events).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let socket = net::spawn(listener.accept().unwrap().0).unwrap();

    let session = SessionId::new(1);
    let generation = ConnectionGeneration::new(10);
    let key = ConnectionKey {
        session,
        generation,
    };
    let attach = |session, generation| CoordToShard::Attach {
        session,
        revision: Revision::new(1),
        generation,
        receive_maximum: 2,
        maximum_packet_size: 1024,
        version: Version::V5,
        outgoing: socket.outgoing_handle(),
        lease: InlineClaimLease::new(),
        dirty_seq: 1,
    };
    commands.send(attach(session, generation)).unwrap();
    worker.drain_coord_messages().unwrap();
    let source = LogicalOwnerId::new(0);
    let pi = OutboundPacketId::new(1).unwrap();
    let claim = DelegatedClaim {
        delivery: DeliveryId {
            publication: PublicationId::new(incarnation, epoch, source, PublicationSeq::new(1)),
            index: 0,
        },
        target: session,
        target_revision: Revision::new(1),
        qos: Qos::AtLeastOnce,
        claim_operation: OperationId::new(incarnation, epoch, source, OperationSeq::new(1)),
        packet_id: pi,
        generation,
    };
    let bytes = |s| SharedBytes::copy_from(s, 1024).unwrap();
    worker.pending_inline.push_back(PendingInlineItem {
        claim,
        topic: bytes(b"t"),
        payload: bytes(b"p"),
        properties: None,
    });
    worker.drain_pending_inline().unwrap();
    let target = worker
        .owner
        .puback_target(key, pi)
        .expect("the exact delivery was emitted");
    let turn = TurnGrant {
        work_limit: WorkUnits::new(1024),
    };
    assert!(matches!(
        worker.owner.step(
            Input::ObservedPuback {
                connection: key,
                packet_id: pi,
                reason: 0,
                target: Some(target)
            },
            turn
        ),
        StepOutcome::Applied { .. }
    ));
    let is_finish = |effect: &OwnedEffect| matches!(effect.effect(), Effect::Persist {command} if matches!(command.recipe.get(), Recipe::FinishDelivery(f) if f.delivery == claim.delivery));
    assert!(worker.owner.outbox().unwrap().pending().any(is_finish));

    worker.limits.in_flight = 0;
    let completed = Arc::<AtomicBool>::default();
    commands
        .send(CoordToShard::Detach {
            connection: key,
            dirty_seq: 2,
            completed: Arc::clone(&completed),
        })
        .unwrap();
    let other = SessionId::new(2);
    let other_generation = ConnectionGeneration::new(11);
    commands.send(attach(other, other_generation)).unwrap();
    worker.drain_coord_messages().unwrap();
    assert_eq!(worker.owner.connection_of(other), Some(other_generation));
    assert_eq!(
        worker.owner.connection_of(session),
        Some(generation),
        "detached while FinishDelivery was still blocked at the writer"
    );
    assert!(!completed.load(Ordering::Acquire));
    worker.limits.in_flight = 8;
    worker.dispatch().unwrap();
    let Ok(Request::Effect(effect)) = requests.try_recv() else {
        panic!("FinishDelivery was not submitted");
    };
    assert!(is_finish(&effect));
    worker.drain_coord_messages().unwrap();
    assert!(worker.owner.connection_of(session).is_some());
    assert!(!completed.load(Ordering::Acquire));
    let Effect::Persist { command } = effect.effect() else {
        unreachable!()
    };
    let operation = command.recipe.operation();
    assert!(matches!(
        worker.owner.step(
            Input::StoreResult {
                effect: effect.id(),
                operation,
                reply: StoreReply::Outcome(StoreOutcome::Committed(
                    CommitFacts::DeliveryFinished {
                        delivery: claim.delivery,
                        target: session,
                        packet_id: pi,
                        claim: claim.claim_operation,
                    }
                )),
            },
            turn
        ),
        StepOutcome::Applied { .. }
    ));
    worker.drain_coord_messages().unwrap();
    assert_eq!(worker.owner.connection_of(session), None);
    assert!(completed.load(Ordering::Acquire));
    socket.close();
}

/// What the coordinator delegates to a shard whose core cannot take it yet
/// waits in `pending_qos0`. That queue has a bound: beyond it a QoS 0
/// delivery is shed, as the single owner sheds one its volatile queue
/// cannot hold, and the drop is journaled.
#[test]
fn delegated_qos0_waiting_for_the_core_is_bounded() {
    let Harness {
        mut worker,
        commands,
        events,
        _lanes,
        ..
    } = harness("qos0-intake");
    let source = ConnectionKey {
        session: SessionId::new(9),
        generation: ConnectionGeneration::new(90),
    };
    let targets = (0..=PENDING_QOS0_LIMIT as u64)
        .map(|index| DelegatedQos0Target {
            target: SessionId::new(100 + index),
            connection: None,
        })
        .collect();
    commands
        .send(CoordToShard::Qos0Deliver {
            origin: Qos0Origin::Volatile {
                publication: mqttiumd_contract::pubsub::VolatilePublicationId {
                    source,
                    ingress_order: 1,
                },
            },
            topic: SharedBytes::copy_from(b"t", 8).unwrap(),
            payload: SharedBytes::copy_from(b"p", 8).unwrap(),
            expiry: None,
            properties: None,
            targets,
        })
        .unwrap();
    worker.drain_coord_messages().unwrap();
    assert!(
        worker.pending_qos0.len() <= PENDING_QOS0_LIMIT,
        "{} deliveries wait for the core",
        worker.pending_qos0.len()
    );
    let journal = std::fs::read_to_string(&events).unwrap();
    let drops = journal
        .lines()
        .filter(|line| line.contains("\"qos0_dropped\"") && line.contains("capacity_exceeded"))
        .count();
    assert_eq!(drops, 1);
    std::fs::remove_file(&events).unwrap();
}
