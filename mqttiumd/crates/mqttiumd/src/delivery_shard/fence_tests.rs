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

/// The count of claims in flight on a lease never wraps: releasing an
/// identifier the lease does not count, above its bitmap or already
/// released, leaves it as it is.
#[test]
fn a_lease_never_counts_fewer_than_no_claim_in_flight() {
    let lease = InlineClaimLease::new();
    lease.release_pi(OutboundPacketId::new(600).unwrap());
    lease.release_pi(OutboundPacketId::new(3).unwrap());
    assert_eq!(lease.in_flight.load(Ordering::Acquire), 0);
}

/// An inline claim the coordinator committed reaches the shard before the
/// barrier that lets the shard publish the session's lease again. While it
/// waits in `pending_inline` it is not a row of the core yet: the lease the
/// shard publishes must still count its identifier, or the coordinator can
/// reserve that identifier a second time.
#[test]
fn a_published_lease_keeps_the_identifier_of_a_waiting_committed_claim() {
    let Harness {
        incarnation,
        epoch,
        mut worker,
        commands,
        events,
        _lanes: (requests, reply, _control, _control_reply),
    } = harness("lease-publication");
    std::fs::remove_file(&events).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let socket = net::spawn(listener.accept().unwrap().0).unwrap();
    let session = SessionId::new(1);
    let generation = ConnectionGeneration::new(10);
    // As `ShardPool::attach_session` does: the lease is dirty until the
    // shard has scanned past this mark.
    let lease = InlineClaimLease::new();
    let dirty_seq = lease.mark_dirty();
    commands
        .send(CoordToShard::Attach {
            session,
            revision: Revision::new(1),
            generation,
            receive_maximum: 8,
            maximum_packet_size: 1024,
            version: Version::V5,
            outgoing: socket.outgoing_handle(),
            lease: Arc::clone(&lease),
            dirty_seq,
        })
        .unwrap();
    worker.drain_coord_messages().unwrap();
    worker.drain_inputs().unwrap();
    worker.dispatch().unwrap();
    let Ok(Request::Effect(load)) = requests.try_recv() else {
        panic!("the attach read no work");
    };
    assert!(matches!(load.effect(), Effect::LoadWork { .. }));

    // The coordinator's committed inline claim, with identifier 1, is
    // waiting for the shard's core.
    let source = LogicalOwnerId::new(0);
    let pi = OutboundPacketId::new(1).unwrap();
    let bytes = |s| SharedBytes::copy_from(s, 1024).unwrap();
    worker.pending_inline.push_back(PendingInlineItem {
        claim: DelegatedClaim {
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
        },
        topic: bytes(b"t"),
        payload: bytes(b"p"),
        properties: None,
    });

    // The scan ends: the session is caught up and its lease may be published.
    reply
        .send(Response::Work {
            effect: load.id(),
            session,
            result: Ok(mqttiumd_store_api::WorkPage {
                items: BoundedVec::with_capacity(0).unwrap(),
                next: None,
            }),
        })
        .unwrap();
    worker.drain_store_replies().unwrap();
    worker.drain_inputs().unwrap();
    let reserved = lease.try_reserve().map(|(_, id)| id);
    assert_ne!(
        reserved,
        Some(pi),
        "the lease gave away the identifier of a committed claim"
    );
    if let Some(id) = reserved {
        lease.release_pi(id);
    }

    // Once the claim is a row, the lease is published with its identifier.
    worker.drain_pending_inline().unwrap();
    let reserved = lease.try_reserve().map(|(_, id)| id);
    assert!(reserved.is_some(), "the lease was not published");
    assert_ne!(reserved, Some(pi));
    socket.close();
}

/// While the coordinator holds an identifier it reserved in the lease for an
/// inline claim whose admission is pending, a dirty event (`dirty_seq` 0:
/// the barrier waits for the pending claim) does not make the shard read
/// the session: what it claimed then could take that identifier and
/// collide, in the shard, with the coordinator's claim once it commits. The
/// shard reads at the barrier, after the committed claim, and what it
/// claims then takes another identifier.
#[test]
fn a_shard_claim_takes_no_identifier_the_coordinator_reserved() {
    let Harness {
        incarnation,
        epoch,
        mut worker,
        commands,
        events,
        _lanes: (requests, reply, _control, _control_reply),
    } = harness("shard-claim-identifier");
    std::fs::remove_file(&events).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let socket = net::spawn(listener.accept().unwrap().0).unwrap();
    let session = SessionId::new(1);
    let generation = ConnectionGeneration::new(10);
    let lease = InlineClaimLease::new();
    let dirty_seq = lease.mark_dirty();
    commands
        .send(CoordToShard::Attach {
            session,
            revision: Revision::new(1),
            generation,
            receive_maximum: 8,
            maximum_packet_size: 1024,
            version: Version::V5,
            outgoing: socket.outgoing_handle(),
            lease: Arc::clone(&lease),
            dirty_seq,
        })
        .unwrap();
    // A pass of the shard's loop over its lane, up to the read it makes.
    let pass = |worker: &mut DeliveryShardWorker| {
        worker.drain_coord_messages().unwrap();
        worker.drain_pending_inline().unwrap();
        worker.drain_inputs().unwrap();
        worker.dispatch().unwrap();
    };
    let answer = |worker: &mut DeliveryShardWorker,
                  items: Vec<mqttiumd_contract::pubsub::WorkItem>| {
        pass(worker);
        let Some(load) = requests.try_iter().find_map(|request| match request {
            Request::Effect(effect) if matches!(effect.effect(), Effect::LoadWork { .. }) => {
                Some(effect)
            }
            _ => None,
        }) else {
            panic!("no work was read");
        };
        assert!(matches!(load.effect(), Effect::LoadWork { .. }));
        let count = items.len();
        let mut page = BoundedVec::with_capacity(count.max(1)).unwrap();
        for item in items {
            assert!(page.try_push(item).is_ok());
        }
        reply
            .send(Response::Work {
                effect: load.id(),
                session,
                result: Ok(mqttiumd_store_api::WorkPage {
                    items: page,
                    next: None,
                }),
            })
            .unwrap();
        worker.drain_store_replies().unwrap();
        worker.drain_inputs().unwrap();
        worker.dispatch().unwrap();
        requests.try_iter().find_map(|request| match request {
            Request::Effect(effect) => match effect.effect() {
                Effect::Persist { command } => match command.recipe.get() {
                    Recipe::ClaimDelivery(claim) => Some(claim.packet_id),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        })
    };
    assert_eq!(answer(&mut worker, Vec::new()), None);

    // The coordinator reserves an identifier for an inline claim; its
    // admission is pending when a dirty event marks the lease. The shard
    // does not read the session.
    let (claim_generation, reserved) = lease.try_reserve().expect("the lease was published");
    let barrier = lease.mark_dirty();
    commands
        .send(CoordToShard::NotifyDirty {
            session,
            dirty_seq: 0,
            clear_known_through: false,
        })
        .unwrap();
    pass(&mut worker);
    assert!(
        !requests
            .try_iter()
            .any(|request| matches!(request, Request::Effect(effect) if matches!(effect.effect(), Effect::LoadWork { .. }))),
        "the shard read a session the coordinator may still owe a claim"
    );

    // The claim commits: the coordinator sends it, then the barrier.
    let bytes = |s| SharedBytes::copy_from(s, 1024).unwrap();
    let source = LogicalOwnerId::new(0);
    commands
        .send(CoordToShard::InlineCommitted {
            topic: bytes(b"t"),
            payload: bytes(b"c"),
            properties: None,
            claims: vec![DelegatedClaim {
                delivery: DeliveryId {
                    publication: PublicationId::new(
                        incarnation,
                        epoch,
                        source,
                        PublicationSeq::new(4),
                    ),
                    index: 0,
                },
                target: session,
                target_revision: Revision::new(1),
                qos: Qos::AtLeastOnce,
                claim_operation: OperationId::new(incarnation, epoch, source, OperationSeq::new(4)),
                packet_id: reserved,
                generation: claim_generation,
            }],
        })
        .unwrap();
    commands
        .send(CoordToShard::DirtyBarrier {
            session,
            dirty_seq: barrier,
        })
        .unwrap();
    let unclaimed = mqttiumd_contract::pubsub::WorkItem {
        delivery: DeliveryId {
            publication: PublicationId::new(
                incarnation,
                epoch,
                LogicalOwnerId::new(0),
                PublicationSeq::new(5),
            ),
            index: 0,
        },
        target: session,
        target_revision: Revision::new(1),
        qos: Qos::AtLeastOnce,
        cause: DeliveryCause::LiveExact,
        retain: false,
        expiry: None,
        properties: None,
        topic: bytes(b"t"),
        payload: bytes(b"p"),
        claim: None,
    };
    let claimed = answer(&mut worker, vec![unclaimed]).expect("the shard claimed nothing");
    assert_ne!(
        claimed, reserved,
        "the shard took an identifier the coordinator reserved"
    );
    socket.close();
}

/// A barrier comes after every claim the coordinator committed for the
/// session, and the claims that wait in `pending_inline` when the shard
/// reads the barrier are applied before the session is read. Read earlier,
/// the session's store would hold a claim's committed row the shard does
/// not hold yet: it would deliver that row, and the claim again once
/// applied.
#[test]
fn a_session_is_read_only_after_the_claims_that_came_before_its_barrier() {
    let Harness {
        incarnation,
        epoch,
        mut worker,
        commands,
        events,
        _lanes: (requests, reply, _control, _control_reply),
    } = harness("read-after-claims");
    std::fs::remove_file(&events).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let socket = net::spawn(listener.accept().unwrap().0).unwrap();
    let session = SessionId::new(1);
    let generation = ConnectionGeneration::new(10);
    let lease = InlineClaimLease::new();
    let dirty_seq = lease.mark_dirty();
    commands
        .send(CoordToShard::Attach {
            session,
            revision: Revision::new(1),
            generation,
            receive_maximum: 8,
            maximum_packet_size: 1024,
            version: Version::V5,
            outgoing: socket.outgoing_handle(),
            lease: Arc::clone(&lease),
            dirty_seq,
        })
        .unwrap();
    let reads = |requests: &mpsc::Receiver<Request>| {
        requests
            .try_iter()
            .filter_map(|request| match request {
                Request::Effect(effect) if matches!(effect.effect(), Effect::LoadWork { .. }) => {
                    Some(effect.id())
                }
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    worker.drain_coord_messages().unwrap();
    worker.drain_inputs().unwrap();
    worker.dispatch().unwrap();
    let [load] = reads(&requests)[..] else {
        panic!("the attach read no work");
    };
    reply
        .send(Response::Work {
            effect: load,
            session,
            result: Ok(mqttiumd_store_api::WorkPage {
                items: BoundedVec::with_capacity(0).unwrap(),
                next: None,
            }),
        })
        .unwrap();
    worker.drain_store_replies().unwrap();
    worker.drain_inputs().unwrap();
    let (claim_generation, reserved) = lease.try_reserve().expect("the lease was published");

    // The claim commits and the coordinator sends it, then the barrier of a
    // mark made while it was pending. The shard reads both and has not
    // applied the claim yet.
    let barrier = lease.mark_dirty();
    let bytes = |s| SharedBytes::copy_from(s, 1024).unwrap();
    let source = LogicalOwnerId::new(0);
    commands
        .send(CoordToShard::InlineCommitted {
            topic: bytes(b"t"),
            payload: bytes(b"c"),
            properties: None,
            claims: vec![DelegatedClaim {
                delivery: DeliveryId {
                    publication: PublicationId::new(
                        incarnation,
                        epoch,
                        source,
                        PublicationSeq::new(4),
                    ),
                    index: 0,
                },
                target: session,
                target_revision: Revision::new(1),
                qos: Qos::AtLeastOnce,
                claim_operation: OperationId::new(incarnation, epoch, source, OperationSeq::new(4)),
                packet_id: reserved,
                generation: claim_generation,
            }],
        })
        .unwrap();
    commands
        .send(CoordToShard::DirtyBarrier {
            session,
            dirty_seq: barrier,
        })
        .unwrap();
    worker.drain_coord_messages().unwrap();
    worker.drain_inputs().unwrap();
    worker.dispatch().unwrap();
    assert!(
        reads(&requests).is_empty(),
        "the session was read before a claim that came before its barrier"
    );

    // Once the claim is applied, the session is read.
    worker.drain_pending_inline().unwrap();
    worker.drain_inputs().unwrap();
    worker.dispatch().unwrap();
    assert_eq!(
        reads(&requests).len(),
        1,
        "the session was not read after its claims"
    );
    socket.close();
}

/// A session the shard does not read now does not keep the shard busy: the
/// read it waits for is not taken for ready work, so no pass of the shard
/// queues its work again only to find nothing to do.
#[test]
fn a_fenced_session_does_not_keep_its_shard_busy() {
    let Harness {
        incarnation: _,
        epoch: _,
        mut worker,
        commands,
        events,
        _lanes: (requests, reply, _control, _control_reply),
    } = harness("fenced-idle");
    std::fs::remove_file(&events).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let socket = net::spawn(listener.accept().unwrap().0).unwrap();
    let session = SessionId::new(1);
    let generation = ConnectionGeneration::new(10);
    let lease = InlineClaimLease::new();
    let dirty_seq = lease.mark_dirty();
    commands
        .send(CoordToShard::Attach {
            session,
            revision: Revision::new(1),
            generation,
            receive_maximum: 8,
            maximum_packet_size: 1024,
            version: Version::V5,
            outgoing: socket.outgoing_handle(),
            lease: Arc::clone(&lease),
            dirty_seq,
        })
        .unwrap();
    worker.drain_coord_messages().unwrap();
    worker.drain_inputs().unwrap();
    worker.dispatch().unwrap();
    let Some(load) = requests.try_iter().find_map(|request| match request {
        Request::Effect(effect) if matches!(effect.effect(), Effect::LoadWork { .. }) => {
            Some(effect.id())
        }
        _ => None,
    }) else {
        panic!("the attach read no work");
    };
    reply
        .send(Response::Work {
            effect: load,
            session,
            result: Ok(mqttiumd_store_api::WorkPage {
                items: BoundedVec::with_capacity(0).unwrap(),
                next: None,
            }),
        })
        .unwrap();
    worker.drain_store_replies().unwrap();
    worker.drain_inputs().unwrap();
    assert!(lease.try_reserve().is_some(), "the lease was not published");

    // A dirty message while the claim is pending: the session waits for its
    // barrier, and nothing of it is ready work meanwhile.
    let _ = lease.mark_dirty();
    commands
        .send(CoordToShard::NotifyDirty {
            session,
            dirty_seq: 0,
            clear_known_through: true,
        })
        .unwrap();
    worker.drain_coord_messages().unwrap();
    worker.drain_pending_inline().unwrap();
    worker.drain_inputs().unwrap();
    worker.dispatch().unwrap();
    assert!(
        !worker.owner.has_ready_work(session),
        "a fenced session was taken for ready work"
    );
    assert!(
        !worker
            .inputs
            .iter()
            .any(|input| matches!(input, Input::WorkAvailable { session: s } if *s == session)),
        "the shard queued the work of a fenced session again"
    );
    socket.close();
}

/// The shard's sink binds a PUBREL's identifier as the coordinator's does:
/// an identifier already bound is a fault, not a binding silently
/// replaced, and the table of bindings is bounded.
#[test]
fn a_pubrel_binding_is_checked_and_bounded_as_the_coordinator_does() {
    let Harness {
        incarnation,
        epoch,
        mut worker,
        commands,
        events,
        _lanes: (_requests, _reply, _control, _control_reply),
    } = harness("pubrel-binding");
    std::fs::remove_file(&events).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let socket = net::spawn(listener.accept().unwrap().0).unwrap();
    let session = SessionId::new(1);
    let generation = ConnectionGeneration::new(10);
    let lease = InlineClaimLease::new();
    let dirty_seq = lease.mark_dirty();
    commands
        .send(CoordToShard::Attach {
            session,
            revision: Revision::new(1),
            generation,
            receive_maximum: 8,
            maximum_packet_size: 1024,
            version: Version::V5,
            outgoing: socket.outgoing_handle(),
            lease,
            dirty_seq,
        })
        .unwrap();
    worker.drain_coord_messages().unwrap();
    let connection = ConnectionKey {
        session,
        generation,
    };
    let source = LogicalOwnerId::new(1);
    let delivery = DeliveryId {
        publication: PublicationId::new(incarnation, epoch, source, PublicationSeq::new(1)),
        index: 0,
    };
    let claim = OperationId::new(incarnation, epoch, source, OperationSeq::new(1));
    let bound = OutboundPacketId::new(3).unwrap();
    worker.outgoing_bindings.insert(
        (connection, bound),
        PubackTarget {
            delivery,
            claim,
            frame: EffectId {
                epoch,
                owner: source,
                sequence: mqttiumd_contract::ids::EffectSeq::new(1),
            },
        },
    );
    let pubrel = |sequence, packet_id| {
        OwnedEffect::new(
            EffectId {
                epoch,
                owner: source,
                sequence: mqttiumd_contract::ids::EffectSeq::new(sequence),
            },
            Effect::EmitFrame {
                connection,
                frame: OutboundFrame::Pubrel { packet_id },
                publish_order: None,
                pubrel: Some((delivery, claim)),
            },
        )
    };
    let submit = |worker: &mut DeliveryShardWorker, effect| {
        let mut close_notices = Vec::new();
        let mut sink = ShardSink {
            store: &mut worker.store,
            sessions: &mut worker.sessions,
            outgoing_bindings: &mut worker.outgoing_bindings,
            frames: &mut worker.frames,
            inflight: &mut worker.inflight,
            work_reads: &mut worker.work_reads,
            timers: &mut worker.timers,
            next_timer_due: &mut worker.next_timer_due,
            inputs: &mut worker.inputs,
            terminal_inputs: &mut worker.terminal_inputs,
            events: &worker.events,
            limits: worker.limits,
            close_notices: &mut close_notices,
            error: None,
        };
        let result = sink.try_submit(effect);
        (result, sink.error.map(|failure| failure.code))
    };

    // The table is full: a PUBREL of another identifier waits.
    worker.limits.outgoing_bindings = 1;
    let (result, error) = submit(&mut worker, pubrel(2, OutboundPacketId::new(4).unwrap()));
    assert!(
        matches!(
            result,
            SubmitResult::NotSubmitted {
                reason: SubmitBlock::InFlightLimit,
                ..
            }
        ),
        "a PUBREL was bound beyond the table's bound"
    );
    assert_eq!(error, None);

    // An identifier already bound is a fault.
    worker.limits.outgoing_bindings = 8;
    let (result, error) = submit(&mut worker, pubrel(3, bound));
    assert!(
        matches!(result, SubmitResult::NotSubmitted { .. }),
        "a bound identifier was rebound"
    );
    assert_eq!(error, Some("shard_pubrel_binding_conflict"));
    socket.close();
}

/// The coordinator may send a committed inline claim for a generation and,
/// right after, the detach of that generation. The shard reads both in one
/// pass and completes the detach before it applies the claim. The claim is
/// durable: it must not be written to the closed generation, and nothing of
/// it may be left behind in the shard.
#[test]
fn a_claim_committed_just_before_its_detach_is_not_written_to_the_closed_generation() {
    let Harness {
        incarnation,
        epoch,
        mut worker,
        commands,
        events,
        _lanes: (_requests, _reply, _control, _control_reply),
    } = harness("claim-before-detach");
    std::fs::remove_file(&events).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let _client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let socket = net::spawn(listener.accept().unwrap().0).unwrap();
    let session = SessionId::new(1);
    let generation = ConnectionGeneration::new(10);
    let lease = InlineClaimLease::new();
    let dirty_seq = lease.mark_dirty();
    commands
        .send(CoordToShard::Attach {
            session,
            revision: Revision::new(1),
            generation,
            receive_maximum: 8,
            maximum_packet_size: 1024,
            version: Version::V5,
            outgoing: socket.outgoing_handle(),
            lease: Arc::clone(&lease),
            dirty_seq,
        })
        .unwrap();
    worker.drain_coord_messages().unwrap();
    let source = LogicalOwnerId::new(0);
    let bytes = |s| SharedBytes::copy_from(s, 1024).unwrap();
    commands
        .send(CoordToShard::InlineCommitted {
            topic: bytes(b"t"),
            payload: bytes(b"p"),
            properties: None,
            claims: vec![DelegatedClaim {
                delivery: DeliveryId {
                    publication: PublicationId::new(
                        incarnation,
                        epoch,
                        source,
                        PublicationSeq::new(1),
                    ),
                    index: 0,
                },
                target: session,
                target_revision: Revision::new(1),
                qos: Qos::AtLeastOnce,
                claim_operation: OperationId::new(incarnation, epoch, source, OperationSeq::new(1)),
                packet_id: OutboundPacketId::new(1).unwrap(),
                generation,
            }],
        })
        .unwrap();
    let completed = Arc::<AtomicBool>::default();
    commands
        .send(CoordToShard::Detach {
            connection: ConnectionKey {
                session,
                generation,
            },
            dirty_seq: lease.mark_dirty(),
            completed: Arc::clone(&completed),
        })
        .unwrap();
    worker.drain_coord_messages().unwrap();
    worker.drain_pending_inline().unwrap();
    worker.drain_inputs().unwrap();
    worker.dispatch().unwrap();
    worker.drain_coord_messages().unwrap();
    assert!(
        completed.load(Ordering::Acquire),
        "the detach did not complete"
    );
    assert_eq!(
        socket.queued_first_bytes(),
        Vec::<u8>::new(),
        "a frame was queued for the closed generation"
    );
    assert!(
        worker.pending_inline.is_empty(),
        "the claim was left behind"
    );
    socket.close();
}
