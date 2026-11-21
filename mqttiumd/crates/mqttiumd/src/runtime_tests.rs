//! Regressions of the real product reactor and its socket/writer adapters.
use super::*;
use mqttiumd_contract::ids::InboundPacketId;
use mqttiumd_contract::packet::SharedBytes;
use mqttiumd_core::owner::ExchangePhase;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::atomic::AtomicU64;

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "mqttiumd-runtime-test-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir(&path).expect("isolated test directory");
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Build the real worker and fully recover the owner before handing it to a
/// test. Cleanup also runs when a regression assertion panics.
fn with_runtime(max_exchanges: u32, test: impl FnOnce(&mut Runtime<'_>, &Path)) {
    with_runtime_clock(
        max_exchanges,
        Arc::new(crate::session_time::SystemClock::new().unwrap()),
        test,
    );
}
fn with_runtime_clock(
    max_exchanges: u32,
    clock: Arc<dyn SessionClock>,
    test: impl FnOnce(&mut Runtime<'_>, &Path),
) {
    with_runtime_clock_hooks(max_exchanges, clock, Hooks::default(), test);
}
fn with_runtime_clock_hooks(
    max_exchanges: u32,
    clock: Arc<dyn SessionClock>,
    hooks: Hooks,
    test: impl FnOnce(&mut Runtime<'_>, &Path),
) {
    let directory = Directory::new();
    let events = Events::open(&directory.0.join("events.jsonl")).unwrap();
    let cancel = Arc::new(AtomicBool::new(false));
    let mut writer = Writer::spawn_with_clock(
        directory.0.join("store.sqlite"),
        events.clone(),
        hooks.clone(),
        cancel.clone(),
        clock,
    )
    .unwrap();
    let boot = writer
        .boot
        .recv_timeout(Duration::from_secs(3))
        .unwrap()
        .unwrap();
    let mut authority = OwnerAuthority::from_grant(boot.grant, NonZeroU32::MIN).unwrap();
    let outbox = authority
        .create_outbox(
            LogicalOwnerId::new(0),
            NonZeroUsize::new(128).unwrap(),
            nz(65536),
        )
        .unwrap();
    let owner = Owner::new(
        OwnerConfig {
            max_sessions: nz(128),
            max_routes: nz(128),
            max_exchanges: nz(max_exchanges),
            account: AccountId::new(1),
            payload_bytes: PayloadBytes::new(4 * 1024 * 1024),
            resolution_delay: LogicalDuration::new(10000),
            retained_operations: nz(128),
            remembered_connections: nz(8),
        },
        outbox,
    )
    .unwrap();
    let mut runtime = Runtime {
        progress: 0,
        wake: writer.wake.clone(),
        limits: Limits::BOUNDED,
        security: None,
        message_expiry_request: None,
        message_expiry_retry: false,
        message_expiry_retry_attempts: 0,
        message_expiry_observation: None,
        message_expiry_after: None,
        message_expiry_scan: Instant::now(),
        message_expiry_input: None,
        message_last_time: None,
        clock: boot.clock,
        clock_anchor: boot.clock_anchor,
        clock_checkpoint: None,
        last_clock_checkpoint: Instant::now(),
        checkpoint_degraded: false,
        expiry_timers: BTreeMap::new(),
        expiry_inputs: VecDeque::new(),
        last_expiry_inventory: None,
        owner,
        _authority: authority,
        writer: &mut writer,
        events,
        peers: BTreeMap::new(),
        retiring: Vec::new(),
        sessions: boot.sessions,
        session_requests: BTreeMap::new(),
        will_connects: BTreeMap::new(),
        will_reads: None,
        last_will_deferred: None,
        next_session_request: 0,
        generation: ConnectionGeneration::new(0),
        peer_cursor: 0,
        inputs: VecDeque::new(),
        control_inputs: VecDeque::new(),
        closing_inputs: VecDeque::new(),
        terminal_inputs: VecDeque::new(),
        deferred_inputs: VecDeque::new(),
        effects_awaited: false,
        timers: BTreeMap::new(),
        inflight: BTreeMap::new(),
        lifecycle_inflight: BTreeMap::new(),
        last_session_change: None,
        session_cardinality_dirty: false,
        last_session_cardinality: None,
        work_reads: BTreeMap::new(),
        last_work_scan: Instant::now(),
        last_will_scan: Instant::now(),
        maintenance_needed: false,
        #[cfg(feature = "test-hooks")]
        work_cache: BTreeMap::new(),
        #[cfg(feature = "test-hooks")]
        deferred_work: VecDeque::new(),
        #[cfg(feature = "test-hooks")]
        emission_barriers: VecDeque::new(),
        #[cfg(feature = "test-hooks")]
        last_consume_result: None,
        started: Instant::now(),
        #[cfg(feature = "test-hooks")]
        captured: None,
        #[cfg(feature = "test-hooks")]
        hooks,
        #[cfg(feature = "test-hooks")]
        deferred_ack: None,
        #[cfg(feature = "test-hooks")]
        deferred_ack_used: false,
        frames: BTreeMap::new(),
        outgoing_bindings: BTreeMap::new(),
        puback_observation: 0,
        #[cfg(feature = "test-hooks")]
        copied_puback: None,
        #[cfg(feature = "test-hooks")]
        deferred_outgoing_written: None,
        #[cfg(feature = "test-hooks")]
        copied_outgoing_frame: None,
        #[cfg(feature = "test-hooks")]
        last_finish_result: None,
        #[cfg(feature = "test-hooks")]
        copied_session_change: None,
        #[cfg(feature = "test-hooks")]
        copied_session_close: None,
    };
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut cursor = None;
        loop {
            runtime
                .writer
                .sender
                .send(Request::Recovery(cursor))
                .unwrap();
            let Response::Recovery(page) = runtime
                .writer
                .receiver
                .recv_timeout(Duration::from_secs(3))
                .unwrap()
            else {
                panic!("recovery response");
            };
            let page = page.unwrap();
            cursor = page.next;
            runtime
                .queue(Input::Recovery {
                    facts: page.facts,
                    last: cursor.is_none(),
                })
                .unwrap();
            runtime.drain_inputs().unwrap();
            if cursor.is_none() {
                break;
            }
        }
        assert!(!runtime.owner.is_recovering());
        test(&mut runtime, &directory.0);
    }));
    for (_, peer) in std::mem::take(&mut runtime.peers) {
        peer.io.close();
        runtime.retiring.push(peer.io);
    }
    let deadline = Instant::now() + Duration::from_secs(3);
    for mut connection in runtime.retiring {
        while !connection.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            connection.join_finished(),
            "socket cleanup exceeded its bound"
        );
    }
    cancel.store(true, Ordering::Relaxed);
    writer.sender.send(Request::Stop).unwrap();
    while !writer.is_finished() && Instant::now() < deadline {
        while writer.receiver.try_recv().is_ok() {}
        thread::sleep(Duration::from_millis(1));
    }
    writer.join_finished().expect("writer released after test");
    if let Err(payload) = outcome {
        std::panic::resume_unwind(payload);
    }
}

fn peer(runtime: &mut Runtime<'_>) -> (ConnectionGeneration, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    let (socket, _) = listener.accept().unwrap();
    runtime.generation = runtime.generation.checked_next().unwrap();
    let generation = runtime.generation;
    runtime.peers.insert(
        generation,
        Peer {
            io: net::spawn_with_clock(socket, runtime.clock.clone()).unwrap(),
            created: Instant::now(),
            last_packet: Instant::now(),
            connected: false,
            version: None,
            session: None,
            opened_by: None,
            will_managed: false,
            will_disconnect: None,
            connect_expiry: SessionExpiry::OnDisconnect,
            disconnect_expiry: None,
            close_observed_at: None,
            handshake_packet: None,
            pending: false,
            keep_alive: 0,
            max_packet: 268435455,
            ingress: 0,
            receiving: crate::receiving::Window::default(),
            closing: None,
            deferred_publish: None,
            blocked_output: None,
            retire_code: None,
        },
    );
    (generation, client)
}

fn connect_packet(client: &[u8]) -> codec::Connect {
    codec::Connect {
        version: codec::Version::V5,
        will: None,
        client_id: SharedBytes::copy_from(client, BOUNDS.max_client_id).unwrap(),
        clean_start: false,
        expiry: SessionExpiry::Never,
        keep_alive: 10,
        max_packet_size: 65536,
        receive_maximum: 8,
        topic_alias_maximum: 0,
        request_problem_information: true,
    }
}

// The new Will existence barrier is a real writer read preceding Open.
// Service this preflight without fabricating, consuming or weakening the
// authentic lifecycle result that each boundary regression retains.
fn next_control_reply(runtime: &mut Runtime<'_>) -> ControlResponse {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .expect("control reply budget");
        let response = runtime
            .writer
            .control_receiver
            .recv_timeout(remaining)
            .unwrap();
        if matches!(&response, ControlResponse::WillBarrier { .. }) {
            runtime.apply_control_response(response).unwrap();
            runtime.drain_inputs().unwrap();
        } else {
            return response;
        }
    }
}

fn pump(runtime: &mut Runtime<'_>) {
    runtime.writer_results().unwrap();
    runtime.connections().unwrap();
    runtime.discover_work();
    runtime.drain_inputs().unwrap();
    runtime.dispatch().unwrap();
}

fn until(runtime: &mut Runtime<'_>, condition: impl Fn(&Runtime<'_>) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while !condition(runtime) && Instant::now() < deadline {
        pump(runtime);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(
        condition(runtime),
        "runtime failed to make bounded progress: inputs={:?} control={:?} closing={:?} terminal={:?} sessions={} requests={} lifecycle={:?} frames={} timers={} status={:?}",
        runtime.inputs,
        runtime.control_inputs,
        runtime.closing_inputs,
        runtime.terminal_inputs,
        runtime.sessions.len(),
        runtime.session_requests.len(),
        runtime.lifecycle_inflight,
        runtime.frames.len(),
        runtime.timers.len(),
        runtime.owner.session_status()
    );
}

/// Reads the next frame, serving the runtime while it has not arrived: a
/// four-slot output lane is refilled by dispatches, and a wait for output
/// must not depend on how the transport's thread was scheduled.
fn served_frame(runtime: &mut Runtime<'_>, client: &mut TcpStream) -> (u8, Vec<u8>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut first = [0u8; 1];
    client
        .set_read_timeout(Some(Duration::from_millis(2)))
        .unwrap();
    while client.peek(&mut first).is_err() {
        assert!(Instant::now() < deadline, "no frame arrived");
        pump(runtime);
    }
    client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    read_frame(client)
}

fn read_frame(client: &mut impl Read) -> (u8, Vec<u8>) {
    let mut byte = [0];
    client.read_exact(&mut byte).unwrap();
    let first = byte[0];
    let mut remaining = 0usize;
    for digit in 0..4 {
        client.read_exact(&mut byte).unwrap();
        remaining |= usize::from(byte[0] & 127) << (digit * 7);
        if byte[0] & 128 == 0 {
            assert!(remaining <= BOUNDS.max_packet);
            let mut body = vec![0; remaining];
            client.read_exact(&mut body).unwrap();
            return (first, body);
        }
    }
    panic!("malformed server frame");
}

fn publish_body(packet_id: u16) -> Vec<u8> {
    let [high, low] = packet_id.to_be_bytes();
    vec![0, 1, b't', high, low, 0, b'v']
}
#[test]
fn closing_qos0_without_collecting_written_retires_the_exact_attempt() {
    with_runtime(128, |runtime, _| {
        let (generation, mut client) = peer(runtime);
        runtime
            .connect(generation, connect_packet(b"qos0-close"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut client).0, 0x20);
        runtime
            .packet(generation, 0x82, vec![0, 1, 0, 0, 1, b't', 0])
            .unwrap();
        until(runtime, |r| r.inflight.is_empty() && r.inputs.is_empty());
        assert_eq!(read_frame(&mut client).0, 0x90);
        runtime
            .packet(generation, 0x30, vec![0, 1, b't', 0, b'v'])
            .unwrap();
        for _ in 0..8 {
            runtime.drain_inputs().unwrap();
            if runtime.frames.values().any(|f| f.kind == "PUBLISH0") {
                break;
            }
        }
        let effect = *runtime
            .frames
            .iter()
            .find(|(_, f)| f.kind == "PUBLISH0")
            .expect("actual adapter accepted QoS0 attempt")
            .0;
        runtime
            .close(generation, "test_close_before_written")
            .unwrap();
        assert!(
            !runtime.frames.contains_key(&effect),
            "closed transport must not retain a write receipt forever"
        );
        assert!(runtime.terminal_inputs.iter().any(|i|matches!(i,Input::Qos0EmitRefused{effect:e,connection,reason:Qos0DropReason::ConnectionClosed} if *e==effect && connection.generation==generation)),"the owner must receive exact terminal loss, not lose an accepted effect silently");
        runtime.drain_inputs().unwrap();
    });
}

fn event_turn_without_scan(runtime: &mut Runtime<'_>) {
    runtime.writer_results().unwrap();
    runtime.connections().unwrap();
    runtime.drain_inputs().unwrap();
    runtime.dispatch().unwrap();
}

fn until_without_scan(runtime: &mut Runtime<'_>, condition: impl Fn(&Runtime<'_>) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !condition(runtime) && Instant::now() < deadline {
        event_turn_without_scan(runtime);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(
        condition(runtime),
        "ready backlog made no autonomous progress with periodic discovery disabled"
    );
}

fn queued_publications(directory: &Path, generation: ConnectionGeneration) -> usize {
    std::fs::read_to_string(directory.join("events.jsonl"))
        .unwrap()
        .lines()
        .filter(|line| {
            let event: serde_json::Value = serde_json::from_str(line).unwrap();
            event["kind"] == "frame_queued"
                && event["connection"]
                    .as_str()
                    .and_then(|value| value.parse::<u64>().ok())
                    == Some(generation.get())
                && event["details"]["packet_kind"] == "PUBLISH"
        })
        .count()
}

#[test]
fn twelve_offline_deliveries_drain_without_a_discovery_tick_per_message_or_page() {
    with_runtime(128, |runtime, directory| {
        let (first, mut old) = peer(runtime);
        runtime
            .connect(first, connect_packet(b"ready-backlog"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut old).0, 0x20);
        runtime
            .packet(first, 0x82, vec![0, 1, 0, 0, 1, b't', 1])
            .unwrap();
        until(runtime, |r| r.inflight.is_empty() && r.inputs.is_empty());
        assert_eq!(read_frame(&mut old).0, 0x90);
        runtime.close(first, "test_offline").unwrap();
        runtime.drain_inputs().unwrap();
        let (source, mut publisher) = peer(runtime);
        runtime
            .connect(source, connect_packet(b"ready-source"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut publisher).0, 0x20);
        for id in 1..=12 {
            runtime.packet(source, 0x32, publish_body(id)).unwrap();
            until(runtime, |r| r.inputs.is_empty() && r.inflight.is_empty());
            assert_eq!(read_frame(&mut publisher).0, 0x40);
        }
        let before =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert_eq!(before.obligations.len(), 12);
        assert_eq!(before.outbound.len(), 0);
        let (target, mut subscriber) = peer(runtime);
        runtime
            .connect(target, connect_packet(b"ready-backlog"))
            .unwrap();
        // No discover_work or synthetic clock input below: only real store,
        // socket and bounded owner turns may rearm useful work.
        until_without_scan(runtime, |_| queued_publications(directory, target) == 8);
        assert_eq!(read_frame(&mut subscriber).0, 0x20);
        let mut acknowledged = Vec::new();
        for _ in 0..8 {
            let (first, body) = read_frame(&mut subscriber);
            assert_eq!(first, 0x32);
            acknowledged.extend_from_slice(&[0x40, 2, body[3], body[4]]);
        }
        until_without_scan(runtime, |r| {
            r.inputs.is_empty() && r.inflight.is_empty() && r.frames.is_empty()
        });
        let revision = runtime.owner.revision();
        for _ in 0..256 {
            event_turn_without_scan(runtime);
        }
        assert_eq!(
            runtime.owner.revision(),
            revision,
            "a full outgoing window must not rearm a busy loop"
        );
        let (other, mut independent) = peer(runtime);
        runtime
            .connect(other, connect_packet(b"ready-independent"))
            .unwrap();
        until_without_scan(runtime, |r| {
            r.session_requests.is_empty() && r.inputs.is_empty()
        });
        assert_eq!(read_frame(&mut independent).0, 0x20);
        runtime
            .packet(other, 0x82, vec![0, 1, 0, 0, 1, b'u', 1])
            .unwrap();
        until_without_scan(runtime, |r| r.inflight.is_empty() && r.inputs.is_empty());
        assert_eq!(read_frame(&mut independent).0, 0x90);
        let mut separate = publish_body(13);
        separate[2] = b'u';
        runtime.packet(source, 0x32, separate).unwrap();
        until_without_scan(runtime, |_| queued_publications(directory, other) == 1);
        assert_eq!(read_frame(&mut publisher).0, 0x40);
        let (first, body) = read_frame(&mut independent);
        assert_eq!(first, 0x32);
        independent.write_all(&[0x40, 2, body[3], body[4]]).unwrap();
        until_without_scan(runtime, |r| {
            r.inflight.is_empty()
                && r.inputs.is_empty()
                && mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite"))
                    .unwrap()
                    .obligations
                    .len()
                    == 12
        });
        runtime.packet(source, 0xc0, vec![]).unwrap();
        event_turn_without_scan(runtime);
        assert_eq!(
            read_frame(&mut publisher).0,
            0xd0,
            "controls progress beside the full destination"
        );
        subscriber.write_all(&acknowledged).unwrap();
        until_without_scan(runtime, |_| queued_publications(directory, target) == 12);
        for _ in 0..4 {
            let (first, body) = read_frame(&mut subscriber);
            assert_eq!(first, 0x32);
            subscriber.write_all(&[0x40, 2, body[3], body[4]]).unwrap();
        }
        until_without_scan(runtime, |r| {
            r.inputs.is_empty()
                && r.inflight.is_empty()
                && r.work_reads.is_empty()
                && mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite"))
                    .unwrap()
                    .obligations
                    .is_empty()
        });
        let after =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert_eq!(
            after
                .operations
                .values()
                .filter(|op| matches!(
                    op.decision,
                    mqttiumd_store_api::Decision::Committed(
                        CommitFacts::PublicationAdmitted { .. }
                    )
                ))
                .count(),
            13,
            "draining introduces no new admission beyond the independent publication"
        );
    });
}

#[test]
fn empty_ready_work_settles_and_wakes_are_deduplicated() {
    with_runtime(128, |runtime, _| {
        let (generation, mut client) = peer(runtime);
        runtime
            .connect(generation, connect_packet(b"empty-ready"))
            .unwrap();
        until_without_scan(runtime, |r| {
            r.session_requests.is_empty()
                && r.inputs.is_empty()
                && r.work_reads.is_empty()
                && r.frames.is_empty()
        });
        assert_eq!(read_frame(&mut client).0, 0x20);
        let session = runtime.sessions[b"empty-ready".as_slice()].id;
        for _ in 0..1024 {
            runtime.queue_work(session);
        }
        assert_eq!(
            runtime
                .inputs
                .iter()
                .filter(|i| matches!(i,Input::WorkAvailable{session:s} if *s==session))
                .count(),
            1
        );
        until_without_scan(runtime, |r| r.inputs.is_empty() && r.work_reads.is_empty());
        let revision = runtime.owner.revision();
        for _ in 0..256 {
            event_turn_without_scan(runtime);
        }
        assert_eq!(
            runtime.owner.revision(),
            revision,
            "an empty page must not rearm a busy loop"
        );
    });
}

#[test]
fn recipient_puback_preceding_disconnect_is_applied_before_generation_closes() {
    with_runtime(128, |runtime, directory| {
        let (subscriber, mut receiver) = peer(runtime);
        runtime
            .connect(subscriber, connect_packet(b"ordered-subscriber"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut receiver).0, 0x20);
        runtime
            .packet(subscriber, 0x82, vec![0, 1, 0, 0, 1, b't', 1])
            .unwrap();
        until(runtime, |r| r.inflight.is_empty() && r.inputs.is_empty());
        assert_eq!(read_frame(&mut receiver).0, 0x90);
        let (publisher, mut sender) = peer(runtime);
        runtime
            .connect(publisher, connect_packet(b"ordered-publisher"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut sender).0, 0x20);
        runtime.packet(publisher, 0x32, publish_body(1)).unwrap();
        until(runtime, |_| {
            std::fs::read_to_string(directory.join("events.jsonl"))
                .unwrap()
                .lines()
                .any(|line| line.contains("\"frame_queued\"") && line.contains("\"PUBLISH\""))
        });
        let (header, body) = read_frame(&mut receiver);
        assert_eq!(header, 0x32);
        let packet_id = [body[3], body[4]];
        // The actual reader can deliver both packets in one reactor turn.
        // Preserve their order even though close removes the live generation.
        runtime
            .packet(subscriber, 0x40, packet_id.to_vec())
            .unwrap();
        runtime.packet(subscriber, 0xe0, vec![]).unwrap();
        pump(runtime);
        let events = std::fs::read_to_string(directory.join("events.jsonl")).unwrap();
        assert!(
            events
                .lines()
                .any(|line| line.contains("\"command_submitted\"") && line.contains("\"Finish\"")),
            "received PUBACK must submit its durable finish before the following DISCONNECT"
        );
    });
}

#[test]
fn expiring_received_puback_disconnect_preserves_exact_durable_finish() {
    with_runtime(128, |runtime, path| {
        let (subscriber, mut receiver) = peer(runtime);
        runtime
            .connect(subscriber, connect_packet(b"started-subscriber"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut receiver).0, 0x20);
        runtime
            .packet(subscriber, 0x82, vec![0, 1, 0, 0, 1, b't', 1])
            .unwrap();
        until(runtime, |r| r.inflight.is_empty() && r.inputs.is_empty());
        assert_eq!(read_frame(&mut receiver).0, 0x90);
        let session = runtime.sessions[b"started-subscriber".as_slice()].id;
        let connection = ConnectionKey {
            session,
            generation: subscriber,
        };
        let identifier = OutboundPacketId::new(1).unwrap();
        let (publisher, mut sender) = peer(runtime);
        runtime
            .connect(publisher, connect_packet(b"started-publisher"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut sender).0, 0x20);
        // Normal MQTT 5 Message Expiry Interval, long enough to stay fresh.
        runtime
            .packet(
                publisher,
                0x32,
                vec![0, 1, b't', 0, 1, 5, 2, 0, 0, 0, 60, b'v'],
            )
            .unwrap();
        until_without_socket_receipts(runtime, |r| {
            r.outgoing_bindings.contains_key(&(connection, identifier))
        });
        let target = runtime.outgoing_bindings[&(connection, identifier)];
        let (header, body) = read_frame(&mut receiver);
        assert_eq!(header, 0x32);
        assert_eq!(&body[3..5], &[0, 1]);
        assert_eq!(body.last(), Some(&b'v'));
        // Observe authentic writer receipts only to establish the cut. Forward
        // those same values unchanged to the unread reactor lane: no invented
        // FrameResult or StoreReply and no owner knowledge of Started yet.
        let (held_tx, held_rx) = std::sync::mpsc::sync_channel(net::WRITTEN_SLOTS);
        let mut genuine_started = false;
        loop {
            let receipt = runtime.peers[&subscriber]
                .io
                .written
                .recv_timeout(Duration::from_secs(2))
                .unwrap();
            genuine_started |= matches!(&receipt, net::Written::Started { correlation: Some((effect, key)) } if *effect == target.frame && *key == connection);
            let complete = matches!(&receipt, net::Written::Complete { correlation: Some((effect, key)), .. } if *effect == target.frame && *key == connection);
            held_tx.send(receipt).unwrap();
            if complete {
                break;
            }
        }
        assert!(
            genuine_started,
            "actual positive socket write receipt must precede Complete"
        );
        runtime.peers.get_mut(&subscriber).unwrap().io.written = held_rx.into();
        let before =
            mqttiumd_store_sqlite::SqliteStore::inspect(&path.join("store.sqlite")).unwrap();
        assert_eq!(before.outbound[&target.delivery].claim, target.claim);
        assert!(!before.outbound[&target.delivery].started);
        // Valid PUBACK then ordinary MQTT DISCONNECT in stream order; the
        // runtime's decoder entry point freezes the exact registered target.
        runtime
            .packet(subscriber, 0x40, body[3..5].to_vec())
            .unwrap();
        runtime.packet(subscriber, 0xe0, vec![]).unwrap();
        until(runtime, |r| {
            r.inflight.is_empty()
                && r.inputs.is_empty()
                && r.session_requests.is_empty()
                && r.lifecycle_inflight.is_empty()
        });
        let (resumed, mut reconnected) = peer(runtime);
        runtime
            .connect(resumed, connect_packet(b"started-subscriber"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        let (header, connack) = read_frame(&mut reconnected);
        assert_eq!(header, 0x20);
        assert_eq!(connack[0] & 1, 1, "persistent session must actually resume");
        assert_eq!(
            runtime.sessions[b"started-subscriber".as_slice()].id,
            session
        );
        let after =
            mqttiumd_store_sqlite::SqliteStore::inspect(&path.join("store.sqlite")).unwrap();
        assert!(
            after.sessions.contains_key(&session),
            "Finish must not be session deletion"
        );
        let events = std::fs::read_to_string(path.join("events.jsonl")).unwrap();
        let generation_text = subscriber.get().to_string();
        assert!(
            events
                .lines()
                .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                .any(|event| event["kind"] == "connection_closed"
                    && event["connection"].as_str() == Some(generation_text.as_str())
                    && event["details"]["code"] == "client_disconnect"),
            "original generation must close for its ordinary MQTT DISCONNECT"
        );
        let exact_finish = events
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .any(|event| {
                event["kind"] == "core_finish_observed"
                    && event["details"]["delivery"] == delivery_text(target.delivery)
            });
        assert!(
            exact_finish
                && !after.outbound.contains_key(&target.delivery)
                && !after.obligations.contains_key(&target.delivery),
            "received and PUBACKed delivery must have exact durable Finish after DISCONNECT and same-session resume: exact_finish={exact_finish}, remaining_claim={:?}, remaining_obligation={:?}",
            after.outbound.get(&target.delivery),
            after.obligations.get(&target.delivery)
        );
    });
}

#[test]
fn a_deferred_subscription_cannot_be_overtaken_by_its_later_unsubscribe() {
    with_runtime(128, |runtime, directory| {
        let (generation, mut client) = peer(runtime);
        runtime
            .connect(generation, connect_packet(b"ordered-controls"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut client).0, 0x20);
        runtime
            .packet(generation, 0x82, vec![0, 1, 0, 0, 1, b't', 1])
            .unwrap();
        runtime.drain_inputs().unwrap();
        assert_eq!(runtime.inflight.len(), 1);
        runtime
            .packet(generation, 0x82, vec![0, 2, 0, 0, 1, b'u', 1])
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while !runtime
            .inputs
            .iter()
            .any(|i| matches!(i, Input::StoreResult { .. }))
            && Instant::now() < deadline
        {
            runtime.writer_results().unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            runtime
                .inputs
                .iter()
                .any(|i| matches!(i, Input::StoreResult { .. })),
            "the first mutation result is between the two later controls"
        );
        runtime
            .packet(generation, 0xa2, vec![0, 3, 0, 0, 1, b'u'])
            .unwrap();
        until(runtime, |r| r.inputs.is_empty() && r.inflight.is_empty());
        let state =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert!(state.routes.contains_key(b"t".as_slice()));
        assert!(
            !state.routes.contains_key(b"u".as_slice()),
            "SUBSCRIBE u preceding UNSUBSCRIBE u must leave no route, even across ResultSlots deferral"
        );
    });
}

/// MQTT 3.1.1 has no Receive Maximum: a client cannot know the broker's
/// window, so a burst beyond it is pressure. The publications wait for the
/// acknowledgements that free the window; every one is acknowledged and
/// the connection stays open.
#[test]
fn a_3_1_1_burst_beyond_the_receive_window_is_acknowledged_not_refused() {
    with_runtime(128, |runtime, _| {
        let (generation, mut client) = peer(runtime);
        runtime
            .connect(
                generation,
                codec::Connect {
                    version: codec::Version::V311,
                    ..connect_packet(b"legacy-burst")
                },
            )
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut client).0, 0x20);
        const BURST: u16 = 20;
        for id in 1..=BURST {
            let [high, low] = id.to_be_bytes();
            // The transport hands packets over one at a time; a waiting
            // publication keeps the later ones in their lane.
            let deadline = Instant::now() + Duration::from_secs(5);
            while runtime.peers[&generation].deferred_publish.is_some() {
                assert!(
                    runtime.peers[&generation].closing.is_none(),
                    "closed: {:?}",
                    runtime.peers[&generation].closing
                );
                assert!(Instant::now() < deadline, "publication {id} never admitted");
                pump(runtime);
                thread::sleep(Duration::from_millis(1));
            }
            runtime
                .packet(generation, 0x32, vec![0, 1, b't', high, low, b'v'])
                .unwrap();
            assert!(
                runtime
                    .peers
                    .get(&generation)
                    .is_some_and(|peer| peer.closing.is_none()),
                "publication {id} closed the connection: {:?}",
                runtime.peers.get(&generation).and_then(|peer| peer.closing)
            );
        }
        client
            .set_read_timeout(Some(Duration::from_millis(5)))
            .unwrap();
        let mut acknowledged = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut first = [0u8; 1];
        while acknowledged.len() < usize::from(BURST) {
            assert!(
                Instant::now() < deadline,
                "only {acknowledged:?} were acknowledged"
            );
            pump(runtime);
            if client.peek(&mut first).is_ok() {
                client
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                let (kind, body) = read_frame(&mut client);
                assert_eq!(kind, 0x40, "PUBACK expected");
                acknowledged.push(u16::from_be_bytes([body[0], body[1]]));
                client
                    .set_read_timeout(Some(Duration::from_millis(5)))
                    .unwrap();
            }
        }
        assert_eq!(acknowledged, (1..=BURST).collect::<Vec<_>>());
    });
}

/// The one-pass bindings are what a lookup per cached session answered,
/// for attached and detached sessions alike.
#[test]
fn the_bindings_of_a_dispatch_equal_a_lookup_per_session() {
    with_runtime(128, |runtime, _| {
        let per_session = |runtime: &Runtime<'_>| -> BTreeMap<_, _> {
            runtime
                .sessions
                .values()
                .filter_map(|s| runtime.owner.connection_of(s.id).map(|c| (s.id, c)))
                .collect()
        };
        assert!(runtime.bindings().is_empty());
        let (first, mut first_client) = peer(runtime);
        runtime.connect(first, connect_packet(b"bound-1")).unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut first_client).0, 0x20);
        let (second, mut second_client) = peer(runtime);
        runtime.connect(second, connect_packet(b"bound-2")).unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut second_client).0, 0x20);
        assert_eq!(runtime.bindings().len(), 2);
        assert_eq!(runtime.bindings(), per_session(runtime));
        // A persistent session that lost its transport stays cached and
        // has no binding.
        drop(first_client);
        until(runtime, |r| !r.peers.contains_key(&first));
        assert_eq!(runtime.sessions.len(), 2);
        assert_eq!(runtime.bindings(), per_session(runtime));
        assert_eq!(runtime.bindings().len(), 1);
    });
}

#[test]
fn capacity_blocked_publish_does_not_starve_another_clients_release_or_result() {
    with_runtime(1, |runtime, _| {
        let (first, mut first_client) = peer(runtime);
        runtime.connect(first, connect_packet(b"first")).unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut first_client).0, 0x20);
        let first_session = runtime.sessions[b"first".as_slice()].id;
        let packet_id = InboundPacketId::new(7).unwrap();
        runtime.packet(first, 0x34, publish_body(7)).unwrap();
        until(runtime, |r| {
            r.owner.exchange(first_session, packet_id).map(|(_, p)| p)
                == Some(ExchangePhase::AwaitPubrel)
        });
        assert_eq!(read_frame(&mut first_client).0, 0x50);
        let (second, mut second_client) = peer(runtime);
        runtime.connect(second, connect_packet(b"second")).unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut second_client).0, 0x20);
        // This admission cannot fit. The following PUBREL is precisely the
        // transition that frees the resource; neither it nor its store result
        // may wait forever behind the unapplied packet.
        runtime.packet(second, 0x34, publish_body(8)).unwrap();
        runtime.packet(first, 0x62, vec![0, 7]).unwrap();
        runtime.drain_inputs().unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while runtime.owner.exchange(first_session, packet_id).is_some()
            && Instant::now() < deadline
        {
            pump(runtime);
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            runtime.owner.exchange(first_session, packet_id).is_none(),
            "capacity-blocked PUBLISH starved another client's PUBREL/store result"
        );
        assert_eq!(read_frame(&mut first_client).0, 0x70);
    });
}

#[test]
fn concatenated_fresh_connect_and_publish_wait_for_confirmed_session_creation() {
    with_runtime(1, |runtime, _| {
        let (generation, mut client) = peer(runtime);
        // MQTT 5, Clean Start=0, infinite Session Expiry, explicit ClientID.
        // Encode independently of the codec under test, and write both
        // complete control packets in one TCP write.
        let mut bytes = vec![
            0x10, 22, 0, 4, b'M', b'Q', b'T', b'T', 5, 0, 0, 10, 5, 0x11, 0xff, 0xff, 0xff, 0xff,
            0, 4, b'p', b'i', b'p', b'e', 0x34, 7,
        ];
        bytes.extend_from_slice(&publish_body(7));
        client.write_all(&bytes).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while runtime.session_requests.is_empty() && Instant::now() < deadline {
            runtime.connections().unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            !runtime.session_requests.is_empty(),
            "CONNECT reached writer"
        );
        // Deliberately leave the writer response unconsumed while the second
        // TCP packet is available. The pending transport must remain usable;
        // it may not process PUBLISH or send a business success yet.
        let deadline = Instant::now() + Duration::from_millis(50);
        while Instant::now() < deadline {
            runtime.connections().unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            runtime.peers.contains_key(&generation),
            "a concatenated PUBLISH closed the fresh session before its writer reply"
        );
        until(runtime, |r| {
            r.sessions.get(b"pipe".as_slice()).is_some_and(|session| {
                r.owner
                    .exchange(session.id, InboundPacketId::new(7).unwrap())
                    .is_some_and(|(_, phase)| phase == ExchangePhase::AwaitPubrel)
            })
        });
        let (first, body) = read_frame(&mut client);
        assert_eq!(
            first, 0x20,
            "CONNACK precedes the admission acknowledgement"
        );
        assert_eq!(body[0], 0, "a new session has Session Present=0");
        assert_eq!(body[1], 0, "successful CONNECT");
        assert_eq!(read_frame(&mut client).0, 0x50);
    });
}

#[test]
fn closing_transport_reconciles_late_commit_without_acknowledging_after_disconnect() {
    with_runtime(1, |runtime, directory| {
        let (generation, mut client) = peer(runtime);
        runtime
            .connect(generation, connect_packet(b"closing-client"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut client).0, 0x20);
        let session = runtime.sessions[b"closing-client".as_slice()].id;
        let packet_id = InboundPacketId::new(7).unwrap();
        runtime.packet(generation, 0x34, publish_body(7)).unwrap();
        runtime.drain_inputs().unwrap();
        assert_eq!(
            runtime.inflight.len(),
            1,
            "admission reached the real writer"
        );
        assert!(
            runtime
                .owner
                .exchange(session, packet_id)
                .is_some_and(|(_, phase)| phase == ExchangePhase::AdmissionPending)
        );

        // A valid Subscription Identifier is outside this profile. It queues a
        // DISCONNECT while the admitted command's response is still withheld.
        let subscription = vec![0, 9, 2, 0x0b, 1, 0, 1, b't', 1];
        assert_eq!(
            codec::decode(0x82, &subscription, BOUNDS)
                .unwrap_err()
                .kind(),
            codec::ErrorKind::Protocol
        );
        runtime.packet(generation, 0x82, subscription).unwrap();
        assert!(runtime.peers[&generation].closing.is_some());
        assert_eq!(read_frame(&mut client), (0xe0, vec![0xa1, 0]));

        // Reconcile the actual commit without collecting the refusal's socket
        // receipt: this establishes the precise closing-but-not-removed phase.
        let deadline = Instant::now() + Duration::from_secs(1);
        while !runtime.inflight.is_empty() && Instant::now() < deadline {
            runtime.writer_results().unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            runtime.inflight.is_empty(),
            "correlated writer result arrived"
        );
        runtime.drain_inputs().unwrap();
        assert!(runtime.peers[&generation].closing.is_some());
        assert!(
            runtime
                .owner
                .exchange(session, packet_id)
                .is_some_and(|(_, phase)| phase == ExchangePhase::AwaitPubrel)
        );
        let state =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert_eq!(state.inbound.len(), 1, "the late admission remains durable");

        client
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let mut byte = [0];
        match client.read(&mut byte) {
            Ok(0) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            result => panic!(
                "a closing transport emitted MQTT output after DISCONNECT: {result:?}, first byte={:#04x}",
                byte[0]
            ),
        }

        // Suppressing the old socket's frame must not suppress reconciliation.
        // A fresh generation resumes the committed exchange without Persist.
        runtime.connections().unwrap();
        runtime.drain_inputs().unwrap();
        let (resumed, mut resumed_client) = peer(runtime);
        runtime
            .connect(resumed, connect_packet(b"closing-client"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        let (first, body) = read_frame(&mut resumed_client);
        assert_eq!(first, 0x20);
        assert_eq!(body[0], 1, "the durable session is resumed");
        runtime.packet(resumed, 0x3c, publish_body(7)).unwrap();
        runtime.drain_inputs().unwrap();
        assert!(
            runtime.inflight.is_empty(),
            "DUP does not submit a second admission"
        );
        assert_eq!(read_frame(&mut resumed_client).0, 0x50);
        let after =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert_eq!(after.inbound, state.inbound);
        assert_eq!(after.publications, state.publications);
        assert_eq!(
            after.operations.len(),
            state.operations.len() + 1,
            "only the confirmed resume lifecycle adds a receipt"
        );
        for (id, record) in &state.operations {
            assert_eq!(
                after.operations.get(id),
                Some(record),
                "DUP preserves the original exact admission receipt"
            );
        }
    });
}

fn unsupported_connect_with_cap(cap: u32) -> Vec<u8> {
    let mut body = vec![
        0, 4, b'M', b'Q', b'T', b'T', 5, 2, 0, 10, 12, 0x19, 1, 0x11, 0, 0, 0, 1, 0x27,
    ];
    body.extend_from_slice(&cap.to_be_bytes());
    body.extend_from_slice(&[0, 4, b't', b'i', b'n', b'y']);
    body
}

#[test]
fn a_validated_unsupported_connect_flushes_its_bounded_failure_before_close() {
    with_runtime(1, |runtime, _| {
        let (generation, mut client) = peer(runtime);
        runtime
            .packet(generation, 0x10, unsupported_connect_with_cap(10))
            .unwrap();
        until(runtime, |r| !r.peers.contains_key(&generation));
        let (first, body) = read_frame(&mut client);
        assert_eq!(first, 0x20);
        assert_ne!(body[1], 0, "unsupported profile never succeeds");
        assert!(
            runtime.sessions.is_empty(),
            "refused CONNECT never creates a session"
        );
    });
}

#[test]
fn a_refusal_never_exceeds_a_validated_connects_tiny_packet_limit() {
    with_runtime(1, |runtime, _| {
        let (generation, mut client) = peer(runtime);
        runtime
            .packet(generation, 0x10, unsupported_connect_with_cap(3))
            .unwrap();
        until(runtime, |r| !r.peers.contains_key(&generation));
        let mut byte = [0];
        assert_eq!(
            client.read(&mut byte).unwrap(),
            0,
            "five-byte refusal would exceed the peer's three-byte cap"
        );
        assert!(runtime.sessions.is_empty());
    });
}

#[test]
fn a_real_delayed_pubcomp_write_receipt_cannot_release_a_new_packet_identifier_use() {
    with_runtime(8, |runtime, _| {
        let (generation, mut client) = peer(runtime);
        runtime
            .connect(generation, connect_packet(b"receive-window"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut client).0, 0x20);
        let session = runtime.sessions[b"receive-window".as_slice()].id;
        let id = InboundPacketId::new(1).unwrap();
        runtime.packet(generation, 0x34, publish_body(1)).unwrap();
        until(runtime, |r| {
            r.owner
                .exchange(session, id)
                .is_some_and(|(_, p)| p == ExchangePhase::AwaitPubrel)
        });
        assert_eq!(read_frame(&mut client).0, 0x50);
        runtime.packet(generation, 0x62, vec![0, 1]).unwrap();
        until(runtime, |r| r.owner.exchange(session, id).is_none());
        // The socket task writes PUBCOMP, but the reactor has not consumed its
        // receipt. A real peer is already allowed to reuse this identifier.
        assert_eq!(read_frame(&mut client).0, 0x70);
        assert!(runtime.frames.values().any(|p| p.kind == "PUBCOMP"));
        runtime.packet(generation, 0x34, publish_body(1)).unwrap();
        let serial = runtime.peers[&generation].receiving.serial(1).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while runtime.frames.values().any(|p| p.kind == "PUBCOMP") && Instant::now() < deadline {
            runtime.connections().unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(runtime.peers[&generation].receiving.serial(1), Some(serial));
        for next in 2..=8 {
            runtime
                .packet(generation, 0x34, publish_body(next))
                .unwrap();
        }
        runtime.packet(generation, 0x34, publish_body(9)).unwrap();
        assert_eq!(
            runtime.peers[&generation].closing.map(|v| v.0),
            Some("receive_maximum"),
            "the ninth actual PUBLISH is refused despite the late old completion"
        );
    });
}

#[test]
fn a_real_puback_write_receipt_returns_its_receive_credit() {
    with_runtime(8, |runtime, _| {
        let (generation, mut client) = peer(runtime);
        runtime
            .connect(generation, connect_packet(b"qos1-credit"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut client).0, 0x20);
        runtime.packet(generation, 0x32, publish_body(1)).unwrap();
        until(runtime, |r| r.frames.values().any(|p| p.kind == "PUBACK"));
        assert_eq!(read_frame(&mut client).0, 0x40);
        until(runtime, |r| !r.frames.values().any(|p| p.kind == "PUBACK"));
        assert_eq!(
            runtime.peers[&generation].receiving.serial(1),
            None,
            "a terminal PUBACK must return the QoS 1 receive credit"
        );
    });
}

fn seven_qos2(runtime: &mut Runtime<'_>, generation: ConnectionGeneration, client: &mut TcpStream) {
    let session = runtime.sessions[b"flow-new-id".as_slice()].id;
    for id in 1..=7 {
        runtime.packet(generation, 0x34, publish_body(id)).unwrap();
        until(runtime, |r| {
            r.owner
                .exchange(session, InboundPacketId::new(id).unwrap())
                .is_some_and(|(_, phase)| phase == ExchangePhase::AwaitPubrel)
        });
        assert_eq!(read_frame(client), (0x50, id.to_be_bytes().to_vec()));
    }
}

#[test]
fn a_new_identifier_after_received_puback_does_not_exceed_receive_maximum() {
    with_runtime(128, |runtime, _| {
        let (generation, mut client) = peer(runtime);
        runtime
            .connect(generation, connect_packet(b"flow-new-id"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut client).0, 0x20);
        seven_qos2(runtime, generation, &mut client);
        runtime.packet(generation, 0x32, publish_body(8)).unwrap();
        until(runtime, |r| r.frames.values().any(|p| p.kind == "PUBACK"));
        assert_eq!(read_frame(&mut client), (0x40, vec![0, 8]));
        assert!(
            runtime.frames.values().any(|p| p.kind == "PUBACK"),
            "the core's write receipt is still unprocessed"
        );
        let mut bytes = vec![0x32, 7];
        bytes.extend_from_slice(&publish_body(9));
        client.write_all(&bytes).unwrap();
        // The real reader has decoded a new identifier after the peer received
        // the old PUBACK. Deliberately process that input before Written.
        let net::Incoming::Packet {
            first,
            body,
            observed,
        } = runtime.peers[&generation]
            .io
            .incoming
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
        else {
            panic!("network PUBLISH");
        };
        runtime
            .packet_at(generation, first, body, observed)
            .unwrap();
        assert!(
            runtime.peers[&generation].closing.is_none(),
            "received PUBACK already returned a flow credit; the new identifier is legal"
        );
        until(runtime, |r| {
            r.frames
                .values()
                .any(|p| p.kind == "PUBACK" && p.packet_id == Some(9))
        });
        assert_eq!(read_frame(&mut client), (0x40, vec![0, 9]));
    });
}

#[test]
fn a_received_puback_can_precede_publication_of_its_network_progress() {
    with_runtime(128, |runtime, _| {
        let (generation, mut client) = peer(runtime);
        runtime
            .connect(generation, connect_packet(b"flow-new-id"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut client).0, 0x20);
        seven_qos2(runtime, generation, &mut client);
        // Pause only the private test seam between write_one success and the
        // progress lane publication. The real socket write still completes.
        until(runtime, |r| r.frames.is_empty());
        runtime.peers[&generation].io.pause_wire_progress(true);
        runtime.packet(generation, 0x32, publish_body(8)).unwrap();
        until(runtime, |r| r.frames.values().any(|p| p.kind == "PUBACK"));
        assert_eq!(read_frame(&mut client), (0x40, vec![0, 8]));
        let deadline = Instant::now() + Duration::from_secs(1);
        while !runtime.peers[&generation].io.wire_progress_is_waiting() && Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(1));
        }
        assert!(runtime.peers[&generation].io.wire_progress_is_waiting());
        let mut bytes = vec![0x32, 7];
        bytes.extend_from_slice(&publish_body(9));
        client.write_all(&bytes).unwrap();
        let net::Incoming::Packet {
            first,
            body,
            observed,
        } = runtime.peers[&generation]
            .io
            .incoming
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
        else {
            panic!("network PUBLISH");
        };
        runtime
            .packet_at(generation, first, body, observed)
            .unwrap();
        assert!(
            runtime.peers[&generation].closing.is_none(),
            "a preempted completion publisher is not a Receive Maximum violation"
        );
        assert!(
            runtime.peers[&generation].deferred_publish.is_some(),
            "the input remains bounded and unapplied until progress arrives"
        );
        runtime.peers[&generation].io.pause_wire_progress(false);
        until(runtime, |r| {
            r.frames
                .values()
                .any(|p| p.kind == "PUBACK" && p.packet_id == Some(9))
        });
        assert_eq!(read_frame(&mut client), (0x40, vec![0, 9]));
        assert!(runtime.peers[&generation].deferred_publish.is_none());
    });
}

#[test]
fn eight_live_qos2_exchanges_still_refuse_a_genuine_ninth_identifier() {
    with_runtime(128, |runtime, _| {
        let (generation, mut client) = peer(runtime);
        runtime
            .connect(generation, connect_packet(b"flow-new-id"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut client).0, 0x20);
        seven_qos2(runtime, generation, &mut client);
        let session = runtime.sessions[b"flow-new-id".as_slice()].id;
        runtime.packet(generation, 0x34, publish_body(8)).unwrap();
        until(runtime, |r| {
            r.owner
                .exchange(session, InboundPacketId::new(8).unwrap())
                .is_some_and(|(_, phase)| phase == ExchangePhase::AwaitPubrel)
        });
        assert_eq!(read_frame(&mut client), (0x50, vec![0, 8]));
        runtime.packet(generation, 0x32, publish_body(9)).unwrap();
        assert_eq!(
            runtime.peers[&generation].closing.map(|v| v.0),
            Some("receive_maximum")
        );
        assert!(runtime.peers[&generation].deferred_publish.is_none());
        assert_eq!(read_frame(&mut client), (0xe0, vec![0x93, 0]));
    });
}

#[cfg(feature = "test-hooks")]
#[test]
fn a_new_identifier_progresses_while_the_old_core_ack_receipt_is_held() {
    with_runtime(128, |runtime, directory| {
        runtime.hooks.directory = Some(directory.to_path_buf());
        runtime.hooks.pause.push("ack.receipt".to_owned());
        let (generation, mut client) = peer(runtime);
        runtime
            .connect(generation, connect_packet(b"flow-new-id"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut client).0, 0x20);
        seven_qos2(runtime, generation, &mut client);
        runtime.packet(generation, 0x32, publish_body(8)).unwrap();
        until(runtime, |r| r.deferred_ack.is_some());
        assert_eq!(read_frame(&mut client), (0x40, vec![0, 8]));
        let mut bytes = vec![0x32, 7];
        bytes.extend_from_slice(&publish_body(9));
        client.write_all(&bytes).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline
            && runtime
                .peers
                .get(&generation)
                .is_some_and(|p| p.closing.is_none())
            && !runtime
                .frames
                .values()
                .any(|p| p.kind == "PUBACK" && p.packet_id == Some(9))
        {
            pump(runtime);
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            runtime
                .peers
                .get(&generation)
                .is_some_and(|p| p.closing.is_none()),
            "Receive Maximum must use actual wire progress independently of the held core receipt"
        );
        assert_eq!(read_frame(&mut client), (0x40, vec![0, 9]));
        assert!(
            runtime.deferred_ack.is_some(),
            "the old core receipt was not released to make this pass"
        );
    });
}

#[test]
fn a_peer_reuses_qos1_identifier_before_the_old_internal_written_receipt() {
    with_runtime(4, |runtime, _| {
        let (generation, mut client) = peer(runtime);
        runtime
            .connect(generation, connect_packet(b"qos1-reuse-race"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut client).0, 0x20);
        let session = runtime.sessions[b"qos1-reuse-race".as_slice()].id;
        let id = InboundPacketId::new(1).unwrap();
        runtime.packet(generation, 0x32, publish_body(1)).unwrap();
        until(runtime, |r| r.frames.values().any(|p| p.kind == "PUBACK"));
        // This read proves the client can legally reuse PI 1. Deliberately do
        // not poll the socket's internal Written channel before the new input.
        assert_eq!(read_frame(&mut client), (0x40, vec![0, 1]));
        let old = *runtime
            .frames
            .iter()
            .find(|(_, f)| f.kind == "PUBACK")
            .unwrap()
            .0;
        runtime.packet(generation, 0x3a, publish_body(1)).unwrap();
        let serial = runtime.peers[&generation].receiving.serial(1);
        runtime.drain_inputs().unwrap();
        assert_eq!(
            runtime.inflight.len(),
            1,
            "a new internal operation was submitted"
        );
        let new_operation = *runtime.inflight.values().next().unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while runtime.frames.contains_key(&old) && Instant::now() < deadline {
            runtime.connections().unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            !runtime.frames.contains_key(&old),
            "the actual old Written arrived"
        );
        runtime.drain_inputs().unwrap();
        assert_eq!(
            runtime.peers[&generation].receiving.serial(1),
            serial,
            "old receipt cannot return the new PUBLISH receive credit"
        );
        assert_eq!(runtime.inflight.values().next(), Some(&new_operation));
        assert_eq!(
            runtime.owner.exchange(session, id).unwrap().1,
            ExchangePhase::AdmissionPending
        );
        until(runtime, |r| r.frames.values().any(|p| p.kind == "PUBACK"));
        assert_eq!(read_frame(&mut client), (0x40, vec![0, 1]));
    });
}

#[test]
fn a_temporarily_full_output_retries_the_same_durable_publish_without_closing() {
    finish_retained_durable_publish(false);
}

#[test]
fn a_publish_retained_before_enqueue_reports_failure_and_resumes_the_same_claim() {
    finish_retained_durable_publish(true);
}

fn finish_retained_durable_publish(close_before_enqueue: bool) {
    with_runtime(128, |runtime, directory| {
        let (target, mut receiver) = peer(runtime);
        runtime
            .connect(target, connect_packet(b"publish-output-full-target"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut receiver).0, 0x20);
        runtime
            .packet(target, 0x82, vec![0, 1, 0, 0, 1, b't', 1])
            .unwrap();
        until(runtime, |r| {
            r.inflight.is_empty() && r.inputs.is_empty() && r.frames.is_empty()
        });
        assert_eq!(read_frame(&mut receiver).0, 0x90);

        let (source, mut publisher) = peer(runtime);
        runtime
            .connect(source, connect_packet(b"publish-output-full-source"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut publisher).0, 0x20);
        let (blocked, queued) = std::sync::mpsc::sync_channel(net::OUTPUT_SLOTS);
        for _ in 0..net::OUTPUT_SLOTS {
            blocked
                .try_send(net::Write {
                    expiry: None,
                    correlation: None,
                    bytes: vec![0xd0, 0],
                })
                .unwrap();
        }
        let original = std::mem::replace(
            &mut runtime.peers.get_mut(&target).unwrap().io.outgoing,
            blocked.into(),
        );
        runtime.packet(source, 0x32, publish_body(1)).unwrap();
        until(runtime, |r| {
            !r.peers.contains_key(&target)||r.owner.outbox().unwrap().pending().any(|effect|matches!(effect.effect(),Effect::EmitPublish{connection,..}if connection.generation==target))
        });
        assert!(
            runtime.peers.contains_key(&target),
            "temporary_publish_queue_full_closed_generation"
        );
        assert!(runtime.peers[&target].closing.is_none());
        let retained:Vec<_>=runtime.owner.outbox().unwrap().pending().filter(|effect|matches!(effect.effect(),Effect::EmitPublish{connection,..}if connection.generation==target)).map(|effect|(effect.id(),effect.effect().clone())).collect();
        assert_eq!(retained.len(), 1);
        let (effect, publication) = &retained[0];
        let Effect::EmitPublish {
            connection,
            delivery,
            packet_id,
            claim,
            ..
        } = publication
        else {
            panic!("actual committed outgoing claim");
        };
        let path = directory.join("store.sqlite");
        let before = mqttiumd_store_sqlite::SqliteStore::inspect(&path).unwrap();
        assert_eq!(before.outbound[delivery].claim, *claim);
        assert!(
            !runtime
                .outgoing_bindings
                .contains_key(&(*connection, *packet_id)),
            "full_publish_left_a_provisional_binding"
        );
        assert!(!runtime.frames.contains_key(effect));

        runtime.dispatch().unwrap();
        assert!(
            runtime.peers.contains_key(&target),
            "repeated_publish_pressure_closed_generation"
        );
        let pending = runtime
            .owner
            .outbox()
            .unwrap()
            .pending()
            .find(|pending| pending.id() == *effect)
            .unwrap();
        assert_eq!(
            pending.effect(),
            publication,
            "publish_retry_changed_the_owned_effect"
        );
        assert!(
            !runtime
                .outgoing_bindings
                .contains_key(&(*connection, *packet_id))
        );
        let blocked_state = mqttiumd_store_sqlite::SqliteStore::inspect(&path).unwrap();
        assert_eq!(blocked_state.outbound, before.outbound);
        assert_eq!(blocked_state.obligations, before.obligations);
        let (target, mut receiver, active) = if close_before_enqueue {
            // Leave the substitute queue full until the old network task dies.
            receiver.shutdown(std::net::Shutdown::Both).unwrap();
            until(runtime, |r| {
                !r.peers.contains_key(&target)
                    && r.owner
                        .outbox()
                        .unwrap()
                        .pending()
                        .all(|pending| pending.id() != *effect)
            });
            assert_eq!(queued.try_iter().count(), net::OUTPUT_SLOTS);
            drop(original);
            let persisted = mqttiumd_store_sqlite::SqliteStore::inspect(&path).unwrap();
            assert_eq!(persisted.outbound, before.outbound);
            assert_eq!(persisted.obligations, before.obligations);

            let retained_generation = target.get().to_string();
            let retained_effect = effect_text(*effect);
            let events = std::fs::read_to_string(directory.join("events.jsonl")).unwrap();
            let events = events
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .filter(|event| {
                    event["connection"].as_str() == Some(retained_generation.as_str())
                        && event["details"]["effect"].as_str() == Some(retained_effect.as_str())
                })
                .collect::<Vec<_>>();
            assert_eq!(
                events
                    .iter()
                    .map(|event| event["kind"].as_str().unwrap())
                    .collect::<Vec<_>>(),
                ["effect_retained", "frame_failed"],
                "retained_publish_terminal_before_enqueue"
            );
            assert!(
                events
                    .iter()
                    .all(|event| event["details"]["packet_kind"] == "PUBLISH")
            );
            assert_eq!(events[0]["details"]["code"], "output_queue_full");
            assert_eq!(events[1]["details"]["code"], "unusable_generation");

            let (resumed, mut client) = peer(runtime);
            runtime
                .connect(resumed, connect_packet(b"publish-output-full-target"))
                .unwrap();
            until(runtime, |r| r.session_requests.is_empty());
            let (first, body) = read_frame(&mut client);
            assert_eq!((first, &body[..2]), (0x20, &[1, 0][..]));
            assert_eq!(runtime.peers[&resumed].session, Some(connection.session));
            (
                resumed,
                client,
                ConnectionKey {
                    generation: resumed,
                    session: connection.session,
                },
            )
        } else {
            assert_eq!(queued.try_iter().count(), net::OUTPUT_SLOTS);
            runtime.peers.get_mut(&target).unwrap().io.outgoing = original;
            runtime.dispatch().unwrap();
            (target, receiver, *connection)
        };
        until(runtime, |r| {
            r.outgoing_bindings.contains_key(&(active, *packet_id))
        });
        assert!(
            runtime
                .owner
                .outbox()
                .unwrap()
                .pending()
                .all(|pending| pending.id() != *effect)
        );
        let expected = runtime.outgoing_bindings[&(active, *packet_id)];
        assert_eq!((expected.delivery, expected.claim), (*delivery, *claim));
        assert_eq!(expected.frame == *effect, !close_before_enqueue);
        let (first, body) = read_frame(&mut receiver);
        assert_eq!(first & !0x08, 0x32);
        assert_eq!(&body[3..5], &packet_id.get().to_be_bytes());
        assert_eq!(body.last(), Some(&b'v'));
        assert_eq!(read_frame(&mut publisher).0, 0x40);
        let ack = dequeue_puback(runtime, target, &mut receiver, packet_id.get());
        assert!(matches!(&ack,Input::ObservedPuback{target:Some(target),..}if *target==expected));
        runtime.queue(ack).unwrap();
        until(runtime, |r| r.inflight.is_empty() && r.inputs.is_empty());
        let final_state = mqttiumd_store_sqlite::SqliteStore::inspect(&path).unwrap();
        assert!(final_state.outbound.is_empty());
        assert!(final_state.obligations.is_empty());
        assert!(runtime.peers.contains_key(&target));
        let events = std::fs::read_to_string(directory.join("events.jsonl")).unwrap();
        assert!(
            events
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .any(|event| event["kind"] == "effect_retained"
                    && event["details"]["packet_kind"] == "PUBLISH"
                    && event["details"]["effect"] == effect_text(*effect))
        );
    });
}

#[test]
fn a_slow_transport_holds_its_acks_while_an_independent_exchange_progresses() {
    for reverse in [false, true] {
        with_runtime(128, |runtime, _| {
            let first = peer(runtime);
            let second = peer(runtime);
            let ((slow, mut receiver_a), (independent, mut receiver_b)) = if reverse {
                (second, first)
            } else {
                (first, second)
            };
            for (id, name, client) in [
                (slow, b"slow".as_slice(), &mut receiver_a),
                (independent, b"independent".as_slice(), &mut receiver_b),
            ] {
                runtime.connect(id, connect_packet(name)).unwrap();
                until(runtime, |r| {
                    r.session_requests.is_empty() && r.frames.is_empty()
                });
                assert_eq!(read_frame(client).0, 0x20);
            }
            runtime
                .packet(independent, 0x82, vec![0, 1, 0, 0, 1, b't', 1])
                .unwrap();
            until(runtime, |r| {
                r.frames.values().all(|frame| frame.kind != "SUBACK")
                    && r.inflight.is_empty()
                    && r.inputs.is_empty()
            });
            assert_eq!(read_frame(&mut receiver_b).0, 0x90);

            let (blocked, held) = std::sync::mpsc::sync_channel(net::OUTPUT_SLOTS);
            for _ in 0..net::OUTPUT_SLOTS {
                blocked
                    .try_send(net::Write {
                        expiry: None,
                        correlation: None,
                        bytes: vec![0xd0, 0],
                    })
                    .unwrap();
            }
            let original = std::mem::replace(
                &mut runtime.peers.get_mut(&slow).unwrap().io.outgoing,
                blocked.into(),
            );
            for mid in [1, 2] {
                let mut body = publish_body(mid);
                body[2] = b'a';
                runtime.packet(slow, 0x32, body).unwrap();
            }
            let pending = |r: &Runtime<'_>| {
                r.owner
                    .outbox()
                    .unwrap()
                    .pending()
                    .filter_map(|effect| match effect.effect() {
                        Effect::EmitFrame {
                            connection,
                            frame: OutboundFrame::Puback { packet_id, .. },
                            ..
                        } if connection.generation == slow => Some((effect.id(), packet_id.get())),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            };
            until(runtime, |r| {
                !r.peers.contains_key(&slow) || pending(r).len() == 2
            });
            assert!(
                runtime.peers.contains_key(&slow),
                "temporary_ack_backpressure_closed_connection"
            );
            let before = pending(runtime);
            assert_eq!(
                before.iter().map(|(_, mid)| *mid).collect::<Vec<_>>(),
                [1, 2]
            );
            let (incoming, held_packet) = std::sync::mpsc::sync_channel(net::INPUT_SLOTS);
            incoming
                .try_send(net::Incoming::Packet {
                    first: 0xc0,
                    body: vec![],
                    observed: Ok(runtime.message_now().unwrap()),
                })
                .unwrap();
            let original_input = std::mem::replace(
                &mut runtime.peers.get_mut(&slow).unwrap().io.incoming,
                held_packet.into(),
            );
            let ingress = runtime.peers[&slow].ingress;

            runtime.packet(independent, 0x32, publish_body(7)).unwrap();
            until(runtime, |r| {
                r.frames.values().any(|frame| {
                    frame.connection.generation == independent && frame.kind == "PUBLISH"
                })
            });
            let frames = [read_frame(&mut receiver_b), read_frame(&mut receiver_b)];
            assert!(
                frames
                    .iter()
                    .any(|(first, body)| *first == 0x40 && body[..2] == [0, 7])
            );
            assert!(frames.iter().any(|(first, body)| *first == 0x32
                && body[..3] == [0, 1, b't']
                && body.last() == Some(&b'v')));
            assert_eq!(
                pending(runtime),
                before,
                "slow connection must still own both ordered frames"
            );
            assert_eq!(
                runtime.peers[&slow].ingress, ingress,
                "a blocked output must stop further input for that peer"
            );
            assert_eq!(held.try_iter().count(), net::OUTPUT_SLOTS);
            runtime.peers.get_mut(&slow).unwrap().io.outgoing = original;
            runtime.dispatch().unwrap();
            for mid in [1, 2] {
                let (first, body) = read_frame(&mut receiver_a);
                assert_eq!((first, &body[..2]), (0x40, &[0, mid][..]));
            }
            assert!(pending(runtime).is_empty());
            runtime.connections().unwrap();
            runtime.drain_inputs().unwrap();
            assert_eq!(runtime.peers[&slow].ingress, ingress + 1);
            assert_eq!(read_frame(&mut receiver_a), (0xd0, vec![]));
            runtime.peers.get_mut(&slow).unwrap().io.incoming = original_input;
        });
    }
}

fn fill_ordinary_inputs(runtime: &mut Runtime<'_>, generation: ConnectionGeneration) {
    let count = (0..=INPUTS)
        .take_while(|order| {
            runtime
                .queue(Input::Packet {
                    connection: generation,
                    ingress_order: 100 + *order as u64,
                    packet: InboundPacket::Pingreq,
                })
                .is_ok()
        })
        .count();
    assert!(count > 0 && count <= INPUTS);
}

/// Every limit builds a runtime with room for at least one connection and
/// no more than the store has sessions.
#[test]
fn limits_are_brought_within_their_bounds() {
    assert_eq!(Limits::new(0), Limits::new(1));
    assert_eq!(Limits::new(usize::MAX), Limits::DEFAULT);
    assert_eq!(Limits::DEFAULT.connections, Limits::MAX_CONNECTIONS);
    assert_eq!(Limits::new(16), Limits::BOUNDED);
    assert!(Limits::new(1).effect_box >= 1);
}

/// A connection with Keep Alive 1 whose input lane the core does not take.
fn held_keep_alive_peer(
    runtime: &mut Runtime<'_>,
    name: &[u8],
) -> (ConnectionGeneration, TcpStream) {
    let (generation, mut client) = peer(runtime);
    runtime
        .connect(
            generation,
            codec::Connect {
                keep_alive: 1,
                ..connect_packet(name)
            },
        )
        .unwrap();
    until(runtime, |r| {
        r.session_requests.is_empty() && r.frames.is_empty()
    });
    assert_eq!(read_frame(&mut client).0, 0x20);
    fill_ordinary_inputs(runtime, generation);
    (generation, client)
}

/// Keep Alive runs from the last complete packet the broker received, not
/// from the moment the core takes it. A packet the broker holds does not
/// stop the clock: a peer silent for one and a half Keep Alive after it is
/// closed, however long its last packet waits (independent audit of
/// 8e4303d, record 0046).
#[test]
fn keep_alive_expires_a_silent_peer_although_its_last_packet_is_still_held() {
    with_runtime(128, |runtime, _| {
        let (generation, mut client) = held_keep_alive_peer(runtime, b"silent-keep-alive");
        client.write_all(&[0xc0, 0]).unwrap();
        thread::sleep(Duration::from_millis(1700));
        runtime.connections().unwrap();
        let closing = runtime.peers.get(&generation).map(|peer| peer.closing);
        assert!(
            !matches!(closing, Some(None)),
            "a peer silent for 1.7 s with Keep Alive 1 is still open"
        );
        if let Some(Some((code, _))) = closing {
            assert_eq!(code, "keep_alive");
        }
    });
}

/// The same held lane, with a peer that keeps sending in time: the broker
/// has heard it, so its Keep Alive does not expire although the core has
/// taken none of its packets.
#[test]
fn keep_alive_does_not_expire_a_peer_the_broker_keeps_hearing() {
    with_runtime(128, |runtime, _| {
        let (generation, mut client) = held_keep_alive_peer(runtime, b"heard-keep-alive");
        // Five packets fit the lane and the one the task holds.
        for _ in 0..5 {
            client.write_all(&[0xc0, 0]).unwrap();
            thread::sleep(Duration::from_millis(400));
        }
        let ingress = runtime.peers[&generation].ingress;
        runtime.connections().unwrap();
        assert!(
            runtime
                .peers
                .get(&generation)
                .is_some_and(|peer| peer.closing.is_none()),
            "a peer heard 0.4 s ago was closed: {:?}",
            runtime.peers.get(&generation).and_then(|peer| peer.closing)
        );
        assert_eq!(
            runtime.peers[&generation].ingress, ingress,
            "the full lane took a packet"
        );
    });
}

#[test]
fn a_full_input_lane_leaves_the_next_socket_packet_in_its_bounded_channel() {
    with_runtime(128, |runtime, _| {
        let (generation, mut client) = peer(runtime);
        runtime
            .connect(generation, connect_packet(b"pressure-input"))
            .unwrap();
        until(runtime, |r| {
            r.session_requests.is_empty() && r.frames.is_empty()
        });
        assert_eq!(read_frame(&mut client).0, 0x20);
        let (producer, consumer) = std::sync::mpsc::sync_channel(net::INPUT_SLOTS);
        producer
            .try_send(net::Incoming::Packet {
                first: 0xc0,
                body: vec![],
                observed: Ok(runtime.message_now().unwrap()),
            })
            .unwrap();
        let original = std::mem::replace(
            &mut runtime.peers.get_mut(&generation).unwrap().io.incoming,
            consumer.into(),
        );
        fill_ordinary_inputs(runtime, generation);
        let ingress = runtime.peers[&generation].ingress;
        let result = runtime.connections();
        assert!(
            result.is_ok(),
            "full_input_consumed_socket_packet: {result:?}"
        );
        assert_eq!(runtime.peers[&generation].ingress, ingress);
        runtime.inputs.pop_front().unwrap();
        runtime.inputs.pop_front().unwrap();
        runtime.connections().unwrap();
        assert_eq!(runtime.peers[&generation].ingress, ingress + 1);
        runtime.connections().unwrap();
        assert_eq!(runtime.peers[&generation].ingress, ingress + 1);
        runtime.peers.get_mut(&generation).unwrap().io.incoming = original;
    });
}

#[test]
fn a_full_ordinary_lane_still_collects_an_authentic_writer_reply() {
    with_runtime(128, |runtime, _| {
        let (generation, mut client) = peer(runtime);
        runtime
            .connect(generation, connect_packet(b"pressure-result"))
            .unwrap();
        until(runtime, |r| {
            r.session_requests.is_empty()
                && r.frames.is_empty()
                && r.work_reads.is_empty()
                && r.inputs.is_empty()
        });
        assert_eq!(read_frame(&mut client).0, 0x20);
        runtime.packet(generation, 0x32, publish_body(1)).unwrap();
        runtime.drain_inputs().unwrap();
        let response = runtime
            .writer
            .receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        assert!(matches!(
            &response,
            Response::Store {
                reply: StoreReply::Outcome(StoreOutcome::Committed(
                    CommitFacts::PublicationAdmitted { .. }
                )),
                ..
            }
        ));
        let (producer, consumer) = std::sync::mpsc::sync_channel(1);
        producer.try_send(response).unwrap();
        let original = std::mem::replace(&mut runtime.writer.receiver, consumer);
        fill_ordinary_inputs(runtime, generation);
        runtime.writer_results().unwrap();
        runtime.writer.receiver = original;
        assert!(
            runtime.inflight.is_empty(),
            "full_ordinary_input_delayed_already_committed_writer_reply"
        );
    });
}

#[test]
fn a_saturated_four_slot_output_retains_the_committed_puback_until_terminal_failure() {
    with_runtime(4, |runtime, directory| {
        let (generation, mut client) = peer(runtime);
        runtime
            .connect(generation, connect_packet(b"qos1-output-full"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut client).0, 0x20);
        until(runtime, |r| r.frames.is_empty());
        runtime.packet(generation, 0x32, publish_body(1)).unwrap();
        runtime.drain_inputs().unwrap();
        assert_eq!(runtime.inflight.len(), 1);
        // Hold the connection adapter at the receiver side of its real bounded
        // channel. This isolates Sink's PUBACK queue refusal; the separate G
        // system run establishes saturation by an actual TCP nonreader.
        let (blocked, receiver) = std::sync::mpsc::sync_channel(net::OUTPUT_SLOTS);
        for _ in 0..net::OUTPUT_SLOTS {
            blocked
                .try_send(net::Write {
                    expiry: None,
                    correlation: None,
                    bytes: vec![0xd0, 0],
                })
                .unwrap();
        }
        let original = std::mem::replace(
            &mut runtime.peers.get_mut(&generation).unwrap().io.outgoing,
            blocked.into(),
        );
        let deadline = Instant::now() + Duration::from_secs(1);
        while !runtime.inflight.is_empty() && Instant::now() < deadline {
            runtime.writer_results().unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        assert!(runtime.inflight.is_empty());
        runtime.drain_inputs().unwrap();
        let retained: Vec<_> = runtime
            .owner
            .outbox()
            .unwrap()
            .pending()
            .filter(|e| {
                matches!(
                    e.effect(),
                    Effect::EmitFrame {
                        frame: OutboundFrame::Puback { .. },
                        ..
                    }
                )
            })
            .map(OwnedEffect::id)
            .collect();
        assert_eq!(
            retained.len(),
            1,
            "the full queue retains the same PUBACK effect"
        );
        assert!(runtime.peers.contains_key(&generation));
        runtime.close(generation, "test_write_terminal").unwrap();
        assert!(!runtime.peers.contains_key(&generation));
        runtime.drain_inputs().unwrap();
        runtime.dispatch().unwrap();
        assert!(runtime.owner.outbox().unwrap().is_empty());
        let events: Vec<serde_json::Value> =
            std::fs::read_to_string(directory.join("events.jsonl"))
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
        let effect = effect_text(retained[0]);
        assert!(events.iter().any(|e| e["kind"] == "effect_retained"
            && e["details"]["effect"] == effect
            && e["details"]["packet_kind"] == "PUBACK"));
        assert!(
            events
                .iter()
                .any(|e| e["kind"] == "frame_failed" && e["details"]["effect"] == effect)
        );
        let state =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert_eq!(
            state.operations.len(),
            2,
            "the durable Open and accepted admission both survive transport failure"
        );
        assert!(state.inbound.is_empty() && state.publications.is_empty());
        drop((original, receiver));
    });
}

fn connect_then_subscribe(client_id: &[u8]) -> Vec<u8> {
    let mut body = vec![
        0, 4, b'M', b'Q', b'T', b'T', 5, 0, 0, 10, 5, 0x11, 255, 255, 255, 255,
    ];
    body.extend_from_slice(&u16::try_from(client_id.len()).unwrap().to_be_bytes());
    body.extend_from_slice(client_id);
    let mut packets = vec![0x10, u8::try_from(body.len()).unwrap()];
    packets.extend_from_slice(&body);
    // Valid MQTT 5 Subscription Identifier, still outside this profile.
    packets.extend_from_slice(&[0x82, 9, 0, 9, 2, 0x0b, 1, 0, 1, b't', 1]);
    packets
}

fn concatenated_subscription_waits_for_connack(known: bool) {
    with_runtime(1, |runtime, _| {
        let client_id = b"handshake";
        if known {
            let (previous, mut previous_client) = peer(runtime);
            runtime
                .connect(previous, connect_packet(client_id))
                .unwrap();
            until(runtime, |r| r.session_requests.is_empty());
            assert_eq!(read_frame(&mut previous_client).0, 0x20);
            runtime.close(previous, "test_disconnect").unwrap();
            runtime.drain_inputs().unwrap();
        }
        let (generation, mut client) = peer(runtime);
        client
            .write_all(&connect_then_subscribe(client_id))
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while !runtime.inputs.iter().any(|input| matches!(input, Input::OpenSessionWillAt { connection, .. } | Input::OpenSession { connection, .. } | Input::OpenSessionAt {connection,..} if *connection == generation))
            && Instant::now() < deadline
        {
            runtime.writer_results().unwrap();
            runtime.connections().unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        assert!(runtime.inputs.iter().any(|input| matches!(input, Input::OpenSessionWillAt { connection, .. } | Input::OpenSession { connection, .. } | Input::OpenSessionAt {connection,..} if *connection == generation)), "every fresh or known CONNECT reaches the correlated owner lifecycle path");
        assert!(runtime.peers[&generation].pending);
        assert!(
            !runtime.peers[&generation].connected,
            "neither a cached name nor submitted Open proves readiness"
        );
        // Hold owner input application while polling the actual transport.
        // A ready SQLite result alone must not permit a DISCONNECT to overtake
        // the core-authorized CONNACK in this connection's write FIFO.
        let poll_deadline = Instant::now() + Duration::from_millis(50);
        while Instant::now() < poll_deadline {
            runtime.connections().unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        client
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let mut first = [0];
        match client.read(&mut first) {
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            result => panic!(
                "server emitted output before queued CONNECT was applied (known={known}): {result:?}, first byte={:#04x}",
                first[0]
            ),
        }
        runtime.drain_inputs().unwrap();
        until(runtime, |r| !r.peers.contains_key(&generation));
        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let (kind, body) = read_frame(&mut client);
        assert_eq!(kind, 0x20, "CONNACK must be the first server packet");
        assert_eq!(body[0], u8::from(known), "accurate Session Present");
        assert_eq!(body[1], 0);
        // Subscription Identifiers were announced as unavailable (0xA1).
        assert_eq!(read_frame(&mut client), (0xe0, vec![0xa1, 0]));
    });
}

#[test]
fn a_framed_publish_waiting_for_connack_keeps_its_reader_expiry_origin() {
    use crate::session_time::testing::FakeClock;
    let sample = |tick| SessionClockReading {
        domain: [83; 32],
        tick,
    };
    let clock = Arc::new(FakeClock::new(sample(100)));
    with_runtime_clock(128, clock.clone(), |runtime, directory| {
        let (generation, mut client) = peer(runtime);
        let mut frames = connect_then_subscribe(b"expiry-reader-handshake");
        frames.truncate(2 + usize::from(frames[1]));
        frames.extend_from_slice(&[0x31, 10, 0, 1, b't', 5, 2, 0, 0, 0, 1, b'v']);
        client.write_all(&frames).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while runtime.peers[&generation].handshake_packet.is_none() {
            runtime.connections().unwrap();
            assert!(
                Instant::now() < deadline,
                "reader never framed the held PUBLISH"
            );
            thread::sleep(Duration::from_millis(1));
        }
        assert!(runtime.peers[&generation].pending);
        assert!(!runtime.peers[&generation].connected);
        clock.set(sample(2_000_000_100));
        runtime.drain_inputs().unwrap();
        until(runtime, |r| {
            r.session_requests.is_empty()
                && r.peers[&generation].handshake_packet.is_none()
                && r.inputs.is_empty()
                && r.inflight.is_empty()
        });
        assert_eq!(read_frame(&mut client).0, 0x20);
        let state =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert!(
            state.retained.is_empty(),
            "reader_queue_delay_reset_message_expiry"
        );
        assert!(state.publications.is_empty());
        assert!(state.payloads.is_empty());
        assert_eq!(state.accounts.references.get(), 0);
        let events = std::fs::read_to_string(directory.join("events.jsonl")).unwrap();
        let ingress = events
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .find(|event| event["kind"] == "message_expiry_received")
            .unwrap();
        assert_eq!(
            ingress["details"]["tick"], "100",
            "queued_expiry_origin_was_refreshed"
        );
        assert_eq!(ingress["details"]["authority"], "reader_frame_completion");
    });
}

#[test]
fn a_regressed_reader_sample_is_not_reclassified_as_message_expiration() {
    use crate::session_time::testing::FakeClock;
    let sample = |tick| SessionClockReading {
        domain: [87; 32],
        tick,
    };
    let clock = Arc::new(FakeClock::new(sample(10_000_000_000)));
    with_runtime_clock(128, clock.clone(), |runtime, directory| {
        let (generation, mut client) = peer(runtime);
        let mut connect = connect_then_subscribe(b"expiry-reader-regression");
        connect.truncate(2 + usize::from(connect[1]));
        client.write_all(&connect).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while !runtime.peers[&generation].pending {
            runtime.connections().unwrap();
            assert!(Instant::now() < deadline, "reader did not frame CONNECT");
            thread::sleep(Duration::from_millis(1));
        }
        clock.set(sample(5_000_000_000));
        client
            .write_all(&[0x31, 10, 0, 1, b't', 5, 2, 0, 0, 0, 1, b'v'])
            .unwrap();
        while runtime.peers[&generation].handshake_packet.is_none() {
            runtime.connections().unwrap();
            assert!(
                Instant::now() < deadline,
                "reader did not frame regressed PUBLISH"
            );
            thread::sleep(Duration::from_millis(1));
        }
        clock.set(sample(12_000_000_000));
        until(runtime, |r| r.peers[&generation].connected);
        assert_eq!(read_frame(&mut client).0, 0x20);
        let error = runtime
            .connections()
            .expect_err("regressed_reader_sample_became_message_expiration");
        assert_eq!(error.code, "session_clock_regressed");
        let state =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert!(state.publications.is_empty());
        assert!(state.retained.is_empty());
    });
}

#[test]
fn a_frozen_reader_clock_error_or_domain_change_cannot_refresh_a_timed_publish() {
    use crate::session_time::{TimeError, testing::FakeClock};
    for reader_error in [true, false] {
        let sample = |tick| SessionClockReading {
            domain: [84; 32],
            tick,
        };
        let clock = Arc::new(FakeClock::new(sample(100)));
        with_runtime_clock(128, clock.clone(), |runtime, directory| {
            let (generation, mut client) = peer(runtime);
            runtime
                .connect(generation, connect_packet(b"expiry-reader-error"))
                .unwrap();
            until(runtime, |r| r.session_requests.is_empty());
            assert_eq!(read_frame(&mut client).0, 0x20);
            if reader_error {
                clock.fail(TimeError::ClockUnavailable);
            }
            client
                .write_all(&[0x31, 10, 0, 1, b't', 5, 2, 0, 0, 0, 1, b'v'])
                .unwrap();
            let net::Incoming::Packet {
                first,
                body,
                observed,
            } = runtime.peers[&generation]
                .io
                .incoming
                .recv_timeout(Duration::from_secs(1))
                .unwrap()
            else {
                panic!("actual framed PUBLISH");
            };
            assert_eq!(observed.is_err(), reader_error);
            if reader_error {
                clock.set(sample(2_000_000_100));
            } else {
                clock.set(SessionClockReading {
                    domain: [85; 32],
                    tick: 2_000_000_100,
                });
            }
            let error = runtime
                .packet_at(generation, first, body, observed)
                .expect_err("uncertain_reader_observation_was_refreshed");
            assert_eq!(
                error.code,
                if reader_error {
                    "session_clock_unavailable"
                } else {
                    "session_clock_domain_changed"
                }
            );
            let state =
                mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite"))
                    .unwrap();
            assert!(
                state.retained.is_empty(),
                "uncertain_reader_observation_created_fresh_retained"
            );
            assert!(state.publications.is_empty());
        });
    }
}

#[test]
fn an_untimed_packet_does_not_acquire_a_reader_clock_dependency() {
    use crate::session_time::{TimeError, testing::FakeClock};
    let sample = |tick| SessionClockReading {
        domain: [86; 32],
        tick,
    };
    let clock = Arc::new(FakeClock::new(sample(100)));
    with_runtime_clock(128, clock.clone(), |runtime, _| {
        let (generation, mut client) = peer(runtime);
        runtime
            .connect(generation, connect_packet(b"untimed-reader-error"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut client).0, 0x20);
        clock.fail(TimeError::ClockUnavailable);
        client.write_all(&[0xc0, 0]).unwrap();
        let net::Incoming::Packet {
            first,
            body,
            observed,
        } = runtime.peers[&generation]
            .io
            .incoming
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
        else {
            panic!("actual PINGREQ");
        };
        assert_eq!(observed, Err(TimeError::ClockUnavailable));
        clock.set(sample(200));
        runtime
            .packet_at(generation, first, body, observed)
            .unwrap();
        runtime.drain_inputs().unwrap();
        assert_eq!(read_frame(&mut client), (0xd0, vec![]));
    });
}

#[test]
fn fresh_connect_pipeline_cannot_send_disconnect_before_core_connack() {
    concatenated_subscription_waits_for_connack(false);
}

#[test]
fn known_connect_pipeline_cannot_send_disconnect_before_core_connack() {
    concatenated_subscription_waits_for_connack(true);
}

#[test]
fn terminal_admission_results_retire_deadlines_after_the_owner_applies_them() {
    with_runtime(8, |runtime, _| {
        let (generation, mut client) = peer(runtime);
        runtime
            .connect(generation, connect_packet(b"receipt-timer-prune"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut client).0, 0x20);
        runtime.packet(generation, 0x32, publish_body(1)).unwrap();
        until(runtime, |r| r.frames.values().any(|p| p.kind == "PUBACK"));
        assert_eq!(read_frame(&mut client).0, 0x40);
        until(runtime, |r| r.inflight.is_empty() && r.inputs.is_empty());
        assert!(
            runtime.timers.is_empty(),
            "a consumed terminal admission must not retain a dead resolution timer until its ten-second deadline"
        );
    });
}

#[test]
fn collection_reply_keeps_reserved_owner_input_capacity_under_business_pressure() {
    with_runtime(8, |runtime, _| {
        runtime.queue(Input::MaintainReceipts).unwrap();
        runtime.drain_inputs().unwrap();
        assert!(!runtime.owner.collection_ready());
        // Fill the regular retained-input budget while the actual SQLite
        // registration is in flight. These bounded no-op work hints are valid
        // owner inputs, so neither a fault nor a closed transport is involved.
        for _ in 0..ORDINARY_INPUTS {
            runtime.deferred_inputs.push_back(Input::WorkAvailable {
                session: SessionId::new(999),
            });
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut result = Ok(());
        while !runtime.owner.collection_ready() && Instant::now() < deadline {
            result = runtime.writer_results();
            if result.is_err() {
                break;
            }
            runtime.drain_inputs().unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            result.is_ok(),
            "a real reserved collector response must not be rejected by the full regular input budget: {:?}",
            result.err()
        );
        assert!(
            runtime.owner.collection_ready(),
            "the exact durable registration response must reach and enable the owner under business pressure"
        );
    });
}

fn finish_startup_writer(writer: &mut Writer) {
    let deadline = Instant::now() + Duration::from_secs(3);
    if !writer.is_finished() {
        let _ = writer.sender.send(Request::Stop);
    }
    while !writer.is_finished() && Instant::now() < deadline {
        while writer.receiver.try_recv().is_ok() {}
        while writer.control_receiver.try_recv().is_ok() {}
        thread::sleep(Duration::from_millis(1));
    }
    writer
        .join_finished()
        .expect("cancelled startup releases its writer within the cleanup budget");
}

#[test]
fn cancelled_startup_never_announces_ready_and_releases_its_opened_epoch() {
    let directory = Directory::new();
    let path = directory.0.join("store.sqlite");
    let event_path = directory.0.join("events.jsonl");
    let events = Events::open(&event_path).unwrap();
    let cancel = Arc::new(AtomicBool::new(true));
    let mut writer = Writer::spawn(
        path.clone(),
        events.clone(),
        Hooks::default(),
        cancel.clone(),
    )
    .unwrap();
    let result = run_inner(
        &"127.0.0.1:0".parse().unwrap(),
        Hooks::default(),
        &events,
        cancel,
        &mut writer,
        None,
        Limits::BOUNDED,
    );
    finish_startup_writer(&mut writer);
    let store = mqttiumd_store_sqlite::SqliteStore::open(&path)
        .expect("the startup writer is no longer alive");
    let state = mqttiumd_store_api::DurableStore::snapshot(&store).unwrap();
    assert_eq!(
        state.ledger.last_opened().get(),
        1,
        "opening an epoch was already accepted and is not called rejected by cancellation"
    );
    assert!(state.sessions.is_empty() && state.operations.is_empty() && state.inbound.is_empty());
    assert!(
        matches!(
            result,
            Err(Failure {
                code: "startup_cancelled",
                read: None
            })
        ),
        "an already cancelled startup must terminate as cancelled before declaring readiness"
    );
    assert!(
        !std::fs::read_to_string(event_path)
            .unwrap()
            .lines()
            .any(
                |line| serde_json::from_str::<serde_json::Value>(line).unwrap()["kind"] == "ready"
            ),
        "a stop request before startup cannot publish a transient Ready observation"
    );
}

#[test]
fn established_typed_startup_failure_takes_precedence_over_cancellation() {
    let directory = Directory::new();
    let path = directory.0.join("store.sqlite");
    std::fs::write(&path, []).unwrap();
    let events = Events::open(&directory.0.join("events.jsonl")).unwrap();
    let cancel = Arc::new(AtomicBool::new(true));
    let mut writer = Writer::spawn(
        path.clone(),
        events.clone(),
        Hooks::default(),
        cancel.clone(),
    )
    .unwrap();
    let result = run_inner(
        &"127.0.0.1:0".parse().unwrap(),
        Hooks::default(),
        &events,
        cancel,
        &mut writer,
        None,
        Limits::BOUNDED,
    );
    finish_startup_writer(&mut writer);
    assert!(
        matches!(
            result,
            Err(Failure {
                code: "durable_read",
                read: Some((ReadError::InvalidData, ReadPhase::Open))
            })
        ),
        "a typed failure already established by bootstrap must not be flattened into startup cancellation"
    );
    assert!(
        !matches!(
            mqttiumd_store_sqlite::SqliteStore::open(&path),
            Err(mqttiumd_store_sqlite::OpenError::WriterAlive)
        ),
        "even failed bootstrap releases writer authority"
    );
}

#[test]
#[cfg(feature = "test-hooks")]
fn cancellation_during_unacknowledged_registration_never_announces_ready() {
    let directory = Directory::new();
    let path = directory.0.join("store.sqlite");
    let event_path = directory.0.join("events.jsonl");
    let hook_path = directory.0.join("hooks");
    std::fs::create_dir(&hook_path).unwrap();
    std::fs::write(hook_path.join("drop-next-close-result"), []).unwrap();
    let hooks = Hooks {
        directory: Some(hook_path),
        pause: Vec::new(),
    };
    let events = Events::open(&event_path).unwrap();
    let cancel = Arc::new(AtomicBool::new(false));
    let mut writer =
        Writer::spawn(path.clone(), events.clone(), hooks.clone(), cancel.clone()).unwrap();
    let (result, cancelled_at) = thread::scope(|scope| {
        let watcher = scope.spawn(|| {
            let deadline = Instant::now() + Duration::from_secs(3);
            let mut hit = false;
            while Instant::now() < deadline {
                if std::fs::read_to_string(&event_path)
                    .unwrap()
                    .contains("\"kind\":\"receipt_collection_result_dropped\"")
                {
                    hit = true;
                    break;
                }
                thread::sleep(Duration::from_millis(1));
            }
            let at = Instant::now();
            cancel.store(true, Ordering::Relaxed);
            hit.then_some(at)
        });
        let result = run_inner(
            &"127.0.0.1:0".parse().unwrap(),
            hooks,
            &events,
            cancel.clone(),
            &mut writer,
            None,
            Limits::BOUNDED,
        );
        (result, watcher.join().unwrap())
    });
    finish_startup_writer(&mut writer);
    assert!(
        cancelled_at.is_some(),
        "the actual durable empty-prefix reply must be lost before cancellation is injected"
    );
    assert!(
        cancelled_at.unwrap().elapsed() < Duration::from_secs(2),
        "cancellation must not wait for the ten-second owner retry or thirty-second registration budget"
    );
    assert!(matches!(
        result,
        Err(Failure {
            code: "startup_cancelled",
            read: None
        })
    ));
    let text = std::fs::read_to_string(event_path).unwrap();
    assert!(
        !text.lines().any(
            |line| serde_json::from_str::<serde_json::Value>(line).unwrap()["kind"] == "ready"
        )
    );
    let store = mqttiumd_store_sqlite::SqliteStore::open(&path).unwrap();
    let state = mqttiumd_store_api::DurableStore::snapshot(&store).unwrap();
    assert_eq!(
        state.receipt_frontiers.len(),
        1,
        "the accepted registration remains a durable fact even though its owner acknowledgement was lost"
    );
    assert!(state.operations.is_empty() && state.sessions.is_empty());
}

#[test]
fn four_blocked_ordinary_deadlines_cannot_occupy_the_collectors_reserved_reply_lane() {
    with_runtime(8, |runtime, path| {
        runtime.queue(Input::MaintainReceipts).unwrap();
        until(runtime, |r| r.owner.collection_ready());
        let (generation, mut client) = peer(runtime);
        runtime
            .connect(generation, connect_packet(b"deadline-lane"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut client).0, 0x20);
        let known_open_span = runtime.owner.receipt_status().unwrap().1;
        assert_eq!(
            known_open_span, 1,
            "the confirmed Open owns one consumed receipt before publications"
        );
        for _ in known_open_span..63 {
            runtime.packet(generation, 0x32, publish_body(1)).unwrap();
            until(runtime, |r| r.frames.values().any(|f| f.kind == "PUBACK"));
            assert_eq!(read_frame(&mut client).0, 0x40);
            until(runtime, |r| !r.frames.values().any(|f| f.kind == "PUBACK"));
        }
        runtime.packet(generation, 0x32, publish_body(1)).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while !runtime
            .owner
            .receipt_status()
            .is_some_and(|(_, span, holes)| span == 64 && holes == 0)
            && Instant::now() < deadline
        {
            event_turn_without_scan(runtime);
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            runtime
                .owner
                .receipt_status()
                .is_some_and(|(_, s, h)| s == 64 && h == 0)
        );
        runtime.drain_inputs().unwrap(); // submit the real closure, leave its reply unread
        assert_eq!(runtime.timers.len(), 1);
        let collector_token = *runtime.timers.keys().next().unwrap();
        assert_eq!(read_frame(&mut client).0, 0x40);
        runtime.connections().unwrap();
        runtime.drain_inputs().unwrap();
        for id in 2..=5 {
            runtime.packet(generation, 0x32, publish_body(id)).unwrap();
            runtime.drain_inputs().unwrap();
        }
        let ordinary: Vec<_> = runtime
            .timers
            .keys()
            .copied()
            .filter(|t| *t != collector_token)
            .collect();
        assert_eq!(
            ordinary.len(),
            4,
            "four actual submitted admissions retain their own unresolved deadlines"
        );
        for order in 0..126 {
            assert!(matches!(
                runtime.owner.step(
                    Input::Packet {
                        connection: generation,
                        ingress_order: 1000 + order,
                        packet: InboundPacket::Pingreq
                    },
                    TurnGrant {
                        work_limit: WorkUnits::new(65536)
                    }
                ),
                StepOutcome::Applied { .. }
            ));
        }
        assert_eq!(runtime.owner.outbox().unwrap().len(), 126);
        assert!(
            matches!(runtime.owner.step(Input::Deadline {token:ordinary[0],now:LogicalTime::new(10000)},TurnGrant {work_limit:WorkUnits::new(65536)}),StepOutcome::NeedCapacity {need,..} if need.resource==Resource::EffectSlots),
            "ordinary retries demonstrably need the two business slots reserved for maintenance"
        );
        for token in ordinary {
            runtime.timers.insert(token, Instant::now());
        }
        runtime.due_deadlines().unwrap();
        while runtime.ordinary_room() != 0 {
            runtime.inputs.push_back(Input::WorkAvailable {
                session: SessionId::new(999),
            });
        }
        let committed_by = Instant::now() + Duration::from_secs(2);
        while !mqttiumd_store_sqlite::SqliteStore::inspect(&path.join("store.sqlite"))
            .unwrap()
            .receipt_frontiers
            .values()
            .any(|f| f.through.get() == 64)
            && Instant::now() < committed_by
        {
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            mqttiumd_store_sqlite::SqliteStore::inspect(&path.join("store.sqlite"))
                .unwrap()
                .receipt_frontiers
                .values()
                .any(|f| f.through.get() == 64),
            "the precise closure must really commit before the input-pressure check"
        );
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut result = Ok(());
        while !runtime
            .control_inputs
            .iter()
            .any(|i| matches!(i, Input::ReceiptsCollected { .. }))
            && Instant::now() < deadline
        {
            result = runtime.writer_results();
            if result.is_err() {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        // The independent reader establishes the collection actually committed,
        // so a missing collector reply cannot be attributed to unvisited SQL.
        let durable =
            mqttiumd_store_sqlite::SqliteStore::inspect(&path.join("store.sqlite")).unwrap();
        assert!(
            durable
                .receipt_frontiers
                .values()
                .any(|f| f.through.get() == 64)
        );
        assert!(
            result.is_ok(),
            "ordinary command deadlines cannot consume maintenance reply capacity: {:?}",
            result.err()
        );
        let reply = runtime
            .control_inputs
            .pop_front()
            .expect("actual collector reply kept its reserved input");
        assert!(matches!(reply, Input::ReceiptsCollected { .. }));
        assert!(matches!(
            runtime.owner.step(
                reply,
                TurnGrant {
                    work_limit: WorkUnits::new(65536)
                }
            ),
            StepOutcome::Applied { .. }
        ));
        assert_eq!(runtime.owner.receipt_status().unwrap().0.through.get(), 64);
    });
}

#[test]
fn a_full_maintenance_input_lane_leaves_the_real_reply_in_reserved_transport() {
    with_runtime(8, |runtime, path| {
        runtime.queue(Input::MaintainReceipts).unwrap();
        runtime.drain_inputs().unwrap();
        for _ in 0..crate::writer::CONTROL_QUEUE {
            runtime.queue(Input::MaintainReceipts).unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while mqttiumd_store_sqlite::SqliteStore::inspect(&path.join("store.sqlite"))
            .unwrap()
            .receipt_frontiers
            .is_empty()
            && Instant::now() < deadline
        {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            mqttiumd_store_sqlite::SqliteStore::inspect(&path.join("store.sqlite"))
                .unwrap()
                .receipt_frontiers
                .len(),
            1,
            "the actual registration committed while its transport reply remains pending"
        );
        // Two ordinary read markers establish transport order: even when the
        // worker was waiting on its ordinary receiver, it checks the reserved
        // request before processing marker two, and flushes the reserved reply
        // before publishing that marker's ordinary response. No elapsed-time
        // assumption establishes that the collector reached its reply channel.
        for _ in 0..2 {
            runtime.writer.sender.send(Request::Recovery(None)).unwrap();
        }
        for _ in 0..2 {
            assert!(matches!(
                runtime.writer.receiver.recv_timeout(Duration::from_secs(2)),
                Ok(Response::Recovery(Ok(_)))
            ));
        }
        let result = runtime.writer_results();
        assert!(
            result.is_ok(),
            "a full reserved input lane must backpressure its reply receiver, not consume then lose the reply: {:?}",
            result.err()
        );
        assert_eq!(runtime.control_inputs.len(), crate::writer::CONTROL_QUEUE);
        assert!(!runtime.owner.collection_ready());
        runtime.drain_inputs().unwrap();
        until(runtime, |r| r.owner.collection_ready());
    });
}

/// Capture one real reader-channel PUBACK after the normal decoder queued it,
/// retaining its complete input rather than manufacturing a target association.
fn dequeue_puback(
    runtime: &mut Runtime<'_>,
    generation: ConnectionGeneration,
    client: &mut TcpStream,
    packet_id: u16,
) -> Input {
    let [high, low] = packet_id.to_be_bytes();
    client.write_all(&[0x40, 2, high, low]).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        runtime.connections().unwrap();
        if let Some(index)=runtime.inputs.iter().position(|input|matches!(input,Input::ObservedPuback {connection,packet_id:observed,..} if connection.generation==generation && observed.get()==packet_id)) {
            return runtime.inputs.remove(index).unwrap();
        }
        assert!(
            Instant::now() < deadline,
            "the real reader/decode boundary did not observe the requested PUBACK"
        );
        thread::sleep(Duration::from_millis(1));
    }
}
fn duplicate_puback_input(input: &Input) -> Input {
    match input {
        Input::ObservedPuback {
            connection,
            packet_id,
            reason,
            target,
        } => Input::ObservedPuback {
            connection: *connection,
            packet_id: *packet_id,
            reason: *reason,
            target: *target,
        },
        _ => panic!("only a real queued PUBACK observation may be copied"),
    }
}
fn until_publish_was_accepted(
    runtime: &mut Runtime<'_>,
    path: &Path,
    generation: ConnectionGeneration,
) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        pump(runtime);
        let events = std::fs::read_to_string(path.join("events.jsonl")).unwrap();
        if events
            .lines()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .any(|event| {
                event["kind"] == "frame_queued"
                    && event["connection"]
                        .as_str()
                        .and_then(|text| text.parse::<u64>().ok())
                        == Some(generation.get())
                    && event["details"]["packet_kind"] == "PUBLISH"
            })
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the actual claim and outgoing adapter acceptance were not reached"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn a_no_target_puback_observed_before_the_first_claim_cannot_bind_to_it_later() {
    with_runtime(8, |runtime, path| {
        let (subscriber, mut receiver) = peer(runtime);
        runtime
            .connect(subscriber, connect_packet(b"no-target-subscriber"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut receiver).0, 0x20);
        runtime
            .packet(subscriber, 0x82, vec![0, 1, 0, 0, 1, b't', 1])
            .unwrap();
        until(runtime, |r| r.inflight.is_empty() && r.inputs.is_empty());
        assert_eq!(read_frame(&mut receiver).0, 0x90);
        assert!(
            mqttiumd_store_sqlite::SqliteStore::inspect(&path.join("store.sqlite"))
                .unwrap()
                .outbound
                .is_empty()
        );
        let original = dequeue_puback(runtime, subscriber, &mut receiver, 1);
        let stale_copy = duplicate_puback_input(&original);
        runtime.queue(original).unwrap();
        runtime.drain_inputs().unwrap();
        let (publisher, mut source) = peer(runtime);
        runtime
            .connect(publisher, connect_packet(b"no-target-publisher"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut source).0, 0x20);
        runtime.packet(publisher, 0x32, publish_body(1)).unwrap();
        until_publish_was_accepted(runtime, path, subscriber);
        let (first, body) = read_frame(&mut receiver);
        assert_eq!(first, 0x32);
        assert_eq!(
            &body[3..5],
            &[0, 1],
            "the first real outgoing claim owns Packet Identifier1"
        );
        let before =
            mqttiumd_store_sqlite::SqliteStore::inspect(&path.join("store.sqlite")).unwrap();
        assert_eq!(before.outbound.len(), 1);
        runtime.queue(stale_copy).unwrap();
        runtime.drain_inputs().unwrap();
        until(runtime, |r| r.inflight.is_empty() && r.inputs.is_empty());
        let after =
            mqttiumd_store_sqlite::SqliteStore::inspect(&path.join("store.sqlite")).unwrap();
        assert_eq!(
            after.outbound, before.outbound,
            "no_target_puback_rebound: an already observed no-target PUBACK cannot acquire the later claim"
        );
        assert_eq!(after.obligations, before.obligations);
        let own_ack = dequeue_puback(runtime, subscriber, &mut receiver, 1);
        runtime.queue(own_ack).unwrap();
        runtime.drain_inputs().unwrap();
        until(runtime, |r| r.inflight.is_empty() && r.inputs.is_empty());
        assert!(
            mqttiumd_store_sqlite::SqliteStore::inspect(&path.join("store.sqlite"))
                .unwrap()
                .outbound
                .is_empty()
        );
    });
}

#[test]
fn stale_puback_during_takeover_with_a_blocked_close_cannot_stop_other_connections() {
    fn until_phase(
        runtime: &mut Runtime<'_>,
        path: &Path,
        phase: &str,
        condition: impl Fn(&Runtime<'_>) -> bool,
    ) {
        let deadline = Instant::now() + Duration::from_secs(1);
        while !condition(runtime) && Instant::now() < deadline {
            pump(runtime);
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            condition(runtime),
            "takeover phase {phase} failed to make bounded progress: session_requests={}, inflight={:?}, inputs={:?}, deferred={:?}, control={:?}, outbox={:?}, peers={:?}, latest_events={:?}",
            runtime.session_requests.len(),
            runtime.inflight,
            runtime.inputs,
            runtime.deferred_inputs,
            runtime.control_inputs,
            runtime
                .owner
                .outbox()
                .unwrap()
                .pending()
                .collect::<Vec<_>>(),
            runtime
                .peers
                .iter()
                .map(|(generation, peer)| (
                    generation,
                    peer.session,
                    peer.connected,
                    peer.pending,
                    peer.closing
                ))
                .collect::<Vec<_>>(),
            std::fs::read_to_string(path.join("events.jsonl"))
                .unwrap()
                .lines()
                .rev()
                .take(12)
                .collect::<Vec<_>>()
        );
    }

    with_runtime(128, |runtime, path| {
        let (old_generation, mut old) = peer(runtime);
        runtime
            .connect(old_generation, connect_packet(b"takeover-boundary"))
            .unwrap();
        until_phase(runtime, path, "establish old transport", |r| {
            r.session_requests.is_empty()
        });
        assert_eq!(read_frame(&mut old).0, 0x20);
        let session = runtime.sessions[b"takeover-boundary".as_slice()].id;
        let (first, mut first_client) = peer(runtime);
        runtime
            .connect(first, connect_packet(b"takeover-pressure-a"))
            .unwrap();
        until_phase(runtime, path, "establish pressure peer A", |r| {
            r.session_requests.is_empty()
        });
        assert_eq!(read_frame(&mut first_client).0, 0x20);
        let (second, mut second_client) = peer(runtime);
        runtime
            .connect(second, connect_packet(b"takeover-pressure-b"))
            .unwrap();
        until_phase(runtime, path, "establish pressure peer B", |r| {
            r.session_requests.is_empty()
        });
        assert_eq!(read_frame(&mut second_client).0, 0x20);
        let (third, mut third_client) = peer(runtime);
        runtime
            .connect(third, connect_packet(b"takeover-pressure-c"))
            .unwrap();
        until_phase(runtime, path, "establish pressure peer C", |r| {
            r.session_requests.is_empty()
        });
        assert_eq!(read_frame(&mut third_client).0, 0x20);
        for identifier in 1..=8 {
            runtime
                .packet(first, 0x32, publish_body(identifier))
                .unwrap();
            runtime.drain_inputs().unwrap();
            runtime
                .packet(second, 0x32, publish_body(identifier))
                .unwrap();
            runtime.drain_inputs().unwrap();
        }
        assert_eq!(
            runtime.inflight.len(),
            IN_FLIGHT,
            "sixteen actual submissions block the ordinary effect head until their correlated replies are collected"
        );
        runtime.packet(third, 0x32, publish_body(1)).unwrap();
        runtime.drain_inputs().unwrap();
        let blocked_persist = runtime
            .owner
            .outbox()
            .unwrap()
            .pending()
            .next()
            .expect("the seventeenth admission waits for a real store slot")
            .id();
        assert!(matches!(
            runtime.owner.outbox().unwrap().pending().next().unwrap().effect(),
            Effect::Persist { command } if matches!(command.recipe.get(), Recipe::AdmitPublication(admission) if admission.source.session == runtime.sessions[b"takeover-pressure-c".as_slice()].id)
        ));
        // Observe a real old-generation PUBACK before the takeover cut, then
        // keep a copy of the already correlated fact for a later replay.
        let observed_before = runtime.puback_observation;
        old.write_all(&[0x40, 2, 0, 1]).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while runtime.puback_observation == observed_before && Instant::now() < deadline {
            runtime.connections().unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            runtime.puback_observation > observed_before,
            "real reader reached old-generation PUBACK"
        );
        let stale = runtime
            .inputs
            .iter()
            .find_map(|input| match input {
                Input::ObservedPuback {
                    connection,
                    packet_id,
                    reason,
                    target,
                } if connection.generation == old_generation => {
                    Some((*connection, *packet_id, *reason, *target))
                }
                _ => None,
            })
            .expect("old observation is correlated before either owner input queue");
        assert_eq!(stale.0.session, session);
        assert_eq!(stale.3, None);
        let (new_generation, mut new) = peer(runtime);
        runtime
            .connect(new_generation, connect_packet(b"takeover-boundary"))
            .unwrap();
        runtime.drain_inputs().unwrap();
        assert_ne!(
            runtime.owner.connection_of(session),
            Some(new_generation),
            "new authority waits for the durable Open"
        );
        assert_eq!(
            runtime.peers[&old_generation].session,
            Some(session),
            "Peer keeps its original confirmed durable identity"
        );
        assert!(
            runtime.peers[&old_generation].closing.is_some(),
            "reserved Close reaches the old socket even behind sixteen held ordinary operations"
        );
        assert_eq!(
            read_frame(&mut old),
            (0xe0, vec![0x8e, 0]),
            "takeover control is actually written before new Open can commit"
        );
        assert!(
            runtime.lifecycle_inflight.is_empty(),
            "Open Persist is fenced until the actual old transport shutdown, not merely a queued close intent"
        );
        // Relay one actual admission reply, retaining the real worker's other
        // replies. This distinguishes takeover/control progress from global
        // quiescence while preserving the adapter's original correlations.
        let (released, receiver) = std::sync::mpsc::sync_channel(IN_FLIGHT * 2);
        let worker_receiver = std::mem::replace(&mut runtime.writer.receiver, receiver);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            until_phase(
                runtime,
                path,
                "reserved Open after actual takeover shutdown",
                |r| {
                    !r.peers.contains_key(&old_generation)
                        && r.owner.connection_of(session) == Some(new_generation)
                        && r.session_requests.is_empty()
                },
            );
            assert_eq!(
                runtime.inflight.len(),
                IN_FLIGHT,
                "session lifecycle did not consume an ordinary writer slot"
            );
            assert_eq!(
                runtime
                    .owner
                    .outbox()
                    .unwrap()
                    .pending()
                    .next()
                    .unwrap()
                    .id(),
                blocked_persist,
                "the same ordinary effect remains retained with its original identity"
            );
            assert!(
                runtime.peers[&new_generation].pending,
                "CONNACK remains behind the older ordinary Persist until that exact operation can be submitted"
            );
            runtime
                .queue(Input::ObservedPuback {
                    connection: stale.0,
                    packet_id: stale.1,
                    reason: stale.2,
                    target: stale.3,
                })
                .unwrap();
            runtime.drain_inputs().unwrap();
            assert_eq!(
                runtime.owner.connection_of(session),
                Some(new_generation),
                "old observed fact cannot stop or rebind the replacement"
            );

            let mut admission_released = false;
            for _ in 0..=IN_FLIGHT {
                let response = worker_receiver
                    .recv_timeout(Duration::from_secs(3))
                    .expect("the real worker completes an accepted admission");
                let admission = matches!(
                    &response,
                    Response::Store {
                        reply: StoreReply::Outcome(StoreOutcome::Committed(
                            CommitFacts::PublicationAdmitted { .. }
                        )),
                        ..
                    }
                );
                assert!(admission || matches!(&response, Response::Work { .. }));
                if let Response::Store {
                    effect, operation, ..
                } = &response
                {
                    assert_eq!(runtime.inflight.get(effect), Some(operation));
                }
                released.send(response).unwrap();
                if admission {
                    admission_released = true;
                    break;
                }
            }
            assert!(
                admission_released,
                "bounded relay must reach the first real admission reply"
            );
            until_phase(
                runtime,
                path,
                "one ordinary reply releases only the retained Persist and CONNACK",
                |r| {
                    !r.peers.contains_key(&old_generation)
                        && r.peers.get(&new_generation).is_some_and(|p| !p.pending)
                },
            );
            assert_eq!(runtime.inflight.len(), IN_FLIGHT);
            assert_eq!(read_frame(&mut new).0, 0x20);
            runtime.packet(new_generation, 0xc0, vec![]).unwrap();
            runtime.packet(third, 0xc0, vec![]).unwrap();
            runtime.drain_inputs().unwrap();
            assert_eq!(
                read_frame(&mut new).0,
                0xd0,
                "the replacement answers PING before the remaining store replies are released"
            );
            assert_eq!(
                read_frame(&mut third_client).0,
                0xd0,
                "an independent connection answers PING while its admission remains inflight"
            );
            assert_eq!(
                runtime.inflight.len(),
                IN_FLIGHT,
                "control progress must not depend on collecting the held store replies"
            );
        }));
        // Restore the real adapter even if an assertion panics. Afterwards,
        // require every held effect's own correlated reply to be collected;
        // each wait measures one operation's progress, not total quiescence.
        runtime.writer.receiver = worker_receiver;
        if let Err(payload) = outcome {
            std::panic::resume_unwind(payload);
        }
        let pending = runtime.inflight.keys().copied().collect::<Vec<_>>();
        for effect in pending {
            until_phase(runtime, path, "collect held correlated admission", |r| {
                !r.inflight.contains_key(&effect)
            });
        }
        assert!(runtime.inflight.is_empty());
    });
}

fn turn_without_socket_receipts(runtime: &mut Runtime<'_>) {
    runtime.writer_results().unwrap();
    runtime.drain_inputs().unwrap();
    runtime.dispatch().unwrap();
}
fn until_without_socket_receipts(
    runtime: &mut Runtime<'_>,
    condition: impl Fn(&Runtime<'_>) -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !condition(runtime) && Instant::now() < deadline {
        turn_without_socket_receipts(runtime);
        thread::sleep(Duration::from_millis(1));
    }
    assert!(
        condition(runtime),
        "the real durable/owner pipeline did not progress while socket receipts were deliberately unread"
    );
}

#[test]
fn observed_puback_copy_and_actual_old_callbacks_cannot_finish_a_reused_identifier() {
    with_runtime(8, |runtime, path| {
        let (subscriber, mut receiver) = peer(runtime);
        runtime
            .connect(subscriber, connect_packet(b"copy-subscriber"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut receiver).0, 0x20);
        runtime
            .packet(subscriber, 0x82, vec![0, 1, 0, 0, 1, b't', 1])
            .unwrap();
        until(runtime, |r| r.inflight.is_empty() && r.inputs.is_empty());
        assert_eq!(read_frame(&mut receiver).0, 0x90);
        let session = runtime.sessions[b"copy-subscriber".as_slice()].id;
        let connection = ConnectionKey {
            session,
            generation: subscriber,
        };
        let identifier = OutboundPacketId::new(1).unwrap();
        let (publisher, mut source) = peer(runtime);
        runtime
            .connect(publisher, connect_packet(b"copy-publisher"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut source).0, 0x20);
        // The actual network writer completes A's bytes, then waits before it
        // can publish either WireProgress or Written. The peer can still ACK.
        runtime.peers[&subscriber].io.pause_wire_progress(true);
        runtime.packet(publisher, 0x32, publish_body(1)).unwrap();
        until_without_socket_receipts(runtime, |r| {
            r.outgoing_bindings.contains_key(&(connection, identifier))
        });
        let first_target = runtime.outgoing_bindings[&(connection, identifier)];
        let (first, body) = read_frame(&mut receiver);
        assert_eq!(first, 0x32);
        assert_eq!(&body[3..5], &[0, 1]);
        let original = dequeue_puback(runtime, subscriber, &mut receiver, 1);
        assert!(
            matches!(&original,Input::ObservedPuback {target:Some(target),..} if *target==first_target)
        );
        let old_observation = duplicate_puback_input(&original);
        runtime.queue(original).unwrap();
        runtime.drain_inputs().unwrap();
        let mut old_finish = None;
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            runtime.writer_results().unwrap();
            for input in &runtime.inputs {
                if let Input::StoreResult {
                    effect,
                    operation,
                    reply:
                        reply @ StoreReply::Outcome(StoreOutcome::Committed(
                            CommitFacts::DeliveryFinished { delivery, .. },
                        )),
                } = input
                    && *delivery == first_target.delivery
                {
                    old_finish = Some((*effect, *operation, *reply));
                }
            }
            runtime.drain_inputs().unwrap();
            runtime.dispatch().unwrap();
            if runtime.inflight.is_empty() && runtime.inputs.is_empty() && old_finish.is_some() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "A's exact known Finish was not applied"
            );
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            runtime.frames.contains_key(&first_target.frame),
            "legitimate A PUBACK must finish before the local Written callback can even be collected"
        );
        assert!(
            mqttiumd_store_sqlite::SqliteStore::inspect(&path.join("store.sqlite"))
                .unwrap()
                .outbound
                .is_empty()
        );
        runtime.packet(publisher, 0x32, publish_body(2)).unwrap();
        until_without_socket_receipts(runtime, |r| {
            r.outgoing_bindings
                .get(&(connection, identifier))
                .is_some_and(|target| *target != first_target)
        });
        let second_target = runtime.outgoing_bindings[&(connection, identifier)];
        assert_ne!(second_target.delivery, first_target.delivery);
        assert_ne!(second_target.claim, first_target.claim);
        assert_ne!(second_target.frame, first_target.frame);
        runtime.peers[&subscriber].io.pause_wire_progress(false);
        let (first, body) = read_frame(&mut receiver);
        assert_eq!(
            first, 0x32,
            "B is a fresh occurrence, not a DUP retransmission of A"
        );
        assert_eq!(
            &body[3..5],
            &[0, 1],
            "known Finish makes the smallest free identifier reusable"
        );
        let before =
            mqttiumd_store_sqlite::SqliteStore::inspect(&path.join("store.sqlite")).unwrap();
        runtime.queue(old_observation).unwrap();
        runtime.drain_inputs().unwrap();
        let (effect, operation, reply) = old_finish.unwrap();
        runtime
            .queue(Input::StoreResult {
                effect,
                operation,
                reply,
            })
            .unwrap();
        runtime.drain_inputs().unwrap();
        until_without_socket_receipts(runtime, |r| r.inflight.is_empty() && r.inputs.is_empty());
        let after =
            mqttiumd_store_sqlite::SqliteStore::inspect(&path.join("store.sqlite")).unwrap();
        assert_eq!(
            after.outbound, before.outbound,
            "stale_puback_observation_rebound: A's already-associated copy must not finish B"
        );
        assert_eq!(after.obligations, before.obligations);
        assert_eq!(
            runtime.owner.puback_target(connection, identifier),
            Some(second_target)
        );
        // Collect A's real delayed Written callback with B already occupying1.
        runtime.connections().unwrap();
        let delayed_b=runtime.inputs.iter().position(|input|matches!(input,Input::FrameResult {effect,..} if *effect==second_target.frame)).map(|index|runtime.inputs.remove(index).unwrap());
        runtime.drain_inputs().unwrap();
        assert_eq!(
            runtime.owner.puback_target(connection, identifier),
            Some(second_target)
        );
        assert_eq!(
            mqttiumd_store_sqlite::SqliteStore::inspect(&path.join("store.sqlite"))
                .unwrap()
                .outbound,
            before.outbound,
            "old socket callbacks release no new obligation or identifier"
        );
        let own_ack = dequeue_puback(runtime, subscriber, &mut receiver, 1);
        assert!(
            matches!(&own_ack,Input::ObservedPuback {target:Some(target),..} if *target==second_target)
        );
        runtime.queue(own_ack).unwrap();
        runtime.drain_inputs().unwrap();
        until_without_socket_receipts(runtime, |r| r.inflight.is_empty() && r.inputs.is_empty());
        if let Some(callback) = delayed_b {
            runtime.queue(callback).unwrap();
            runtime.drain_inputs().unwrap();
        }
        let final_state =
            mqttiumd_store_sqlite::SqliteStore::inspect(&path.join("store.sqlite")).unwrap();
        assert!(
            final_state.outbound.is_empty()
                && final_state.obligations.is_empty()
                && final_state.payloads.is_empty()
        );
        assert!(runtime.outgoing_bindings.is_empty());
    });
}

#[test]
fn ephemeral_sessions_return_owner_cache_and_durable_capacity() {
    with_runtime(128, |runtime, directory| {
        let mut previous = 0;
        for cycle in 0..6 {
            let (generation, mut client) = peer(runtime);
            let mut connect = connect_packet(format!("ephemeral-{cycle}").as_bytes());
            connect.expiry = SessionExpiry::OnDisconnect;
            runtime.connect(generation, connect).unwrap();
            until(runtime, |r| r.session_requests.is_empty());
            let (kind, body) = read_frame(&mut client);
            assert_eq!(kind, 0x20);
            assert_eq!(&body[..2], &[0, 0]);
            let session = runtime.peers[&generation].session.unwrap();
            assert!(session.get() > previous, "SessionId must never recycle");
            previous = session.get();
            runtime.packet(generation, 0x32, publish_body(1)).unwrap();
            until(runtime, |r| r.inflight.is_empty() && r.inputs.is_empty());
            assert_eq!(read_frame(&mut client).0, 0x40);
            runtime.packet(generation, 0xe0, vec![]).unwrap();
            until(runtime, |r| {
                let status = r.owner.session_status();
                r.sessions.is_empty()
                    && status.sessions == 0
                    && status.bindings == 0
                    && status.inbound == 0
                    && status.outgoing == 0
                    && status.retired_flights == 0
                    && !status.lifecycle_pending
                    && r.closing_inputs.is_empty()
                    && r.lifecycle_inflight.is_empty()
                    && r.work_reads.is_empty()
                    && r.frames.is_empty()
                    && r.timers.is_empty()
            });
            let durable =
                mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite"))
                    .unwrap();
            assert!(
                durable.sessions.is_empty(),
                "terminal session history must be physically reclaimed"
            );
            assert!(durable.routes.is_empty() && durable.inbound.is_empty());
            assert!(durable.obligations.is_empty() && durable.publications.is_empty());
            assert!(runtime.owner.session_status().closed_generations <= 8);
        }
    });
}

#[test]
fn zero_expiry_reconnect_first_resumes_then_ends_the_existing_incarnation() {
    with_runtime(128, |runtime, directory| {
        let name = b"resume-then-end";
        let (old, mut old_client) = peer(runtime);
        runtime.connect(old, connect_packet(name)).unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut old_client).0, 0x20);
        let original = runtime.peers[&old].session.unwrap();
        runtime
            .packet(old, 0x82, vec![0, 1, 0, 0, 1, b't', 1])
            .unwrap();
        until(runtime, |r| r.inflight.is_empty() && r.inputs.is_empty());
        assert_eq!(read_frame(&mut old_client).0, 0x90);
        runtime.close(old, "test_offline").unwrap();
        runtime.drain_inputs().unwrap();
        let (current, mut client) = peer(runtime);
        let mut connect = connect_packet(name);
        connect.expiry = SessionExpiry::OnDisconnect;
        runtime.connect(current, connect).unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        let (kind, body) = read_frame(&mut client);
        assert_eq!(kind, 0x20);
        assert_eq!(&body[..2], &[1, 0]);
        assert_eq!(runtime.peers[&current].session, Some(original));
        assert_eq!(
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite"))
                .unwrap()
                .routes
                .len(),
            1,
            "expiry0 resumes existing state before its future closure"
        );
        runtime.packet(current, 0xe0, vec![]).unwrap();
        until(runtime, |r| {
            r.sessions.is_empty()
                && !r.owner.session_status().lifecycle_pending
                && r.closing_inputs.is_empty()
        });
        let durable =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert!(durable.sessions.is_empty() && durable.routes.is_empty());
        let (next, mut new_client) = peer(runtime);
        runtime.connect(next, connect_packet(name)).unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(&read_frame(&mut new_client).1[..2], &[0, 0]);
        assert!(runtime.peers[&next].session.unwrap().get() > original.get());
    });
}

fn relay_control_reply(runtime: &mut Runtime<'_>, response: ControlResponse) {
    let (sender, receiver) = std::sync::mpsc::sync_channel(crate::writer::CONTROL_QUEUE);
    let original = std::mem::replace(&mut runtime.writer.control_receiver, receiver);
    sender.send(response).unwrap();
    let result = runtime.writer_results();
    runtime.writer.control_receiver = original;
    result.unwrap();
}

#[test]
fn a_real_duplicated_message_expiry_reply_cannot_reapply_removed_deliveries() {
    use crate::session_time::testing::FakeClock;
    let sample = |tick| SessionClockReading {
        domain: [77; 32],
        tick,
    };
    let clock = Arc::new(FakeClock::new(sample(100)));
    with_runtime_clock(128, clock.clone(), |runtime, directory| {
        let (subscriber, mut receiver) = peer(runtime);
        runtime
            .connect(subscriber, connect_packet(b"expiry-copy-target"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut receiver).0, 0x20);
        runtime
            .packet(subscriber, 0x82, vec![0, 1, 0, 0, 1, b't', 1])
            .unwrap();
        until(runtime, |r| r.inflight.is_empty() && r.inputs.is_empty());
        assert_eq!(read_frame(&mut receiver).0, 0x90);
        runtime.close(subscriber, "test_offline").unwrap();
        until(runtime, |r| r.closing_inputs.is_empty());

        let (publisher, mut sender) = peer(runtime);
        runtime
            .connect(publisher, connect_packet(b"expiry-copy-source"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut sender).0, 0x20);
        runtime
            .packet(
                publisher,
                0x32,
                vec![0, 1, b't', 0, 1, 5, 2, 0, 0, 0, 1, b'v'],
            )
            .unwrap();
        until(runtime, |r| r.inflight.is_empty() && r.inputs.is_empty());
        assert_eq!(read_frame(&mut sender).0, 0x40);
        let path = directory.join("store.sqlite");
        let queued = mqttiumd_store_sqlite::SqliteStore::inspect(&path).unwrap();
        assert_eq!(queued.obligations.len(), 1);
        let original = *queued.obligations.keys().next().unwrap();

        clock.set(sample(1_000_000_100));
        runtime.message_expiry_scan = Instant::now() - Duration::from_millis(101);
        runtime.expire_messages().unwrap();
        let response = next_control_reply(runtime);
        let ControlResponse::MessageExpiry {
            result: Ok(sweep), ..
        } = &response
        else {
            panic!("real committed Message Expiry response");
        };
        assert_eq!(sweep.removed.len(), 1);
        assert_eq!(sweep.removed.as_slice()[0].delivery, original);
        let duplicate = response.clone();
        relay_control_reply(runtime, response);
        runtime.drain_inputs().unwrap();
        assert!(runtime.message_expiry_input.is_none());
        let expired = mqttiumd_store_sqlite::SqliteStore::inspect(&path).unwrap();
        assert!(expired.obligations.is_empty());
        assert_eq!(expired.accounts.references.get(), 0);

        runtime
            .packet(
                publisher,
                0x32,
                vec![0, 1, b't', 0, 2, 5, 2, 0, 0, 0, 30, b'w'],
            )
            .unwrap();
        until(runtime, |r| r.inflight.is_empty() && r.inputs.is_empty());
        assert_eq!(read_frame(&mut sender).0, 0x40);
        let fresh = mqttiumd_store_sqlite::SqliteStore::inspect(&path).unwrap();
        assert_eq!(fresh.obligations.len(), 1);
        assert_ne!(*fresh.obligations.keys().next().unwrap(), original);
        relay_control_reply(runtime, duplicate);
        assert!(
            runtime.message_expiry_input.is_none(),
            "duplicate_message_expiry_reply_reapplied_removed_ids"
        );
        let after = mqttiumd_store_sqlite::SqliteStore::inspect(&path).unwrap();
        assert_eq!(
            after.accounts, fresh.accounts,
            "duplicate_expiry_reply_changed_credits"
        );
        assert_eq!(after.obligations, fresh.obligations);
        assert_eq!(after.publications, fresh.publications);
        assert_eq!(after.payloads, fresh.payloads);
    });
}

#[test]
fn a_stopped_clock_cannot_reuse_a_completed_message_expiry_header() {
    use crate::session_time::testing::FakeClock;
    let sample = |tick| SessionClockReading {
        domain: [78; 32],
        tick,
    };
    let clock = Arc::new(FakeClock::new(sample(100)));
    with_runtime_clock(128, clock.clone(), |runtime, directory| {
        runtime.message_expiry_scan = Instant::now() - Duration::from_millis(101);
        runtime.expire_messages().unwrap();
        let first_header = runtime.message_expiry_request.unwrap();
        let first = next_control_reply(runtime);
        let duplicate = first.clone();
        runtime.apply_control_response(first).unwrap();
        runtime.message_expiry_scan = Instant::now() - Duration::from_millis(101);
        runtime.expire_messages().unwrap();
        assert!(
            runtime.message_expiry_request.is_none(),
            "message_expiry_header_reused_with_stopped_clock"
        );

        clock.set(sample(101));
        runtime.message_expiry_scan = Instant::now() - Duration::from_millis(101);
        runtime.expire_messages().unwrap();
        let next_header = runtime.message_expiry_request.unwrap();
        assert_ne!(next_header, first_header);
        runtime.apply_control_response(duplicate).unwrap();
        assert_eq!(
            runtime.message_expiry_request,
            Some(next_header),
            "stale_expiry_reply_retargeted_to_new_scan"
        );
        let next = next_control_reply(runtime);
        runtime.apply_control_response(next).unwrap();
        assert!(runtime.message_expiry_request.is_none());
        let events = std::fs::read_to_string(directory.join("events.jsonl")).unwrap();
        assert!(
            events
                .lines()
                .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                .any(|event| event["kind"] == "message_expiry_result_ignored"
                    && event["details"]["code"] == "stale_known")
        );
    });
}

#[test]
fn all_pages_of_a_real_message_expiry_scan_keep_the_first_observation() {
    use crate::session_time::testing::FakeClock;
    let sample = |tick| SessionClockReading {
        domain: [81; 32],
        tick,
    };
    let clock = Arc::new(FakeClock::new(sample(100)));
    with_runtime_clock(128, clock.clone(), |runtime, directory| {
        let (publisher, mut sender) = peer(runtime);
        runtime
            .connect(publisher, connect_packet(b"expiry-pages"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut sender).0, 0x20);
        for suffix in b'0'..=b'4' {
            runtime
                .packet(
                    publisher,
                    0x31,
                    vec![0, 2, b't', suffix, 5, 2, 0, 0, 0, 1, b'v'],
                )
                .unwrap();
            until(runtime, |r| r.inflight.is_empty() && r.inputs.is_empty());
        }
        let path = directory.join("store.sqlite");
        assert_eq!(
            mqttiumd_store_sqlite::SqliteStore::inspect(&path)
                .unwrap()
                .retained
                .len(),
            5
        );
        clock.set(sample(1_000_000_100));
        runtime.message_expiry_scan = Instant::now() - Duration::from_millis(101);
        runtime.expire_messages().unwrap();
        let first_header = runtime.message_expiry_request.unwrap();
        let first = next_control_reply(runtime);
        assert!(
            matches!(&first,ControlResponse::MessageExpiry{result:Ok(sweep),..} if sweep.retained_removed==4&&sweep.next.is_some())
        );
        runtime.apply_control_response(first).unwrap();
        clock.set(sample(2_000_000_100));
        runtime.expire_messages().unwrap();
        let next_header = runtime.message_expiry_request.unwrap();
        assert_eq!(
            next_header.0, first_header.0,
            "message_expiry_page_changed_frozen_time"
        );
        assert_ne!(next_header.1, first_header.1);
        let second = next_control_reply(runtime);
        assert!(
            matches!(&second,ControlResponse::MessageExpiry{result:Ok(sweep),..} if sweep.retained_removed==1)
        );
        runtime.apply_control_response(second).unwrap();
        assert!(
            mqttiumd_store_sqlite::SqliteStore::inspect(&path)
                .unwrap()
                .retained
                .is_empty()
        );
    });
}

#[cfg(feature = "test-hooks")]
#[test]
fn the_real_writer_duplicate_expiry_hook_is_applied_once() {
    use crate::session_time::testing::FakeClock;
    let controls = Directory::new();
    std::fs::write(
        controls.0.join("duplicate-next-message-expire-result"),
        b"duplicate",
    )
    .unwrap();
    let clock = Arc::new(FakeClock::new(SessionClockReading {
        domain: [82; 32],
        tick: 100,
    }));
    with_runtime_clock_hooks(
        128,
        clock,
        Hooks {
            directory: Some(controls.0.clone()),
            pause: vec![],
        },
        |runtime, directory| {
            runtime.message_expiry_scan = Instant::now() - Duration::from_millis(101);
            runtime.expire_messages().unwrap();
            let first = next_control_reply(runtime);
            let duplicate = next_control_reply(runtime);
            assert!(
                matches!((&first,&duplicate),(ControlResponse::MessageExpiry{result:Ok(a),..},ControlResponse::MessageExpiry{result:Ok(b),..}) if Arc::ptr_eq(a,b)),
                "duplicate_hook_did_not_share_the_authentic_result"
            );
            runtime.apply_control_response(first).unwrap();
            runtime.apply_control_response(duplicate).unwrap();
            assert!(runtime.message_expiry_request.is_none());
            assert!(runtime.message_expiry_input.is_none());
            let events = std::fs::read_to_string(directory.join("events.jsonl")).unwrap();
            assert!(
                events
                    .lines()
                    .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                    .any(|event| event["kind"] == "message_expiry_result_duplicated")
            );
        },
    );
}

#[cfg(feature = "test-hooks")]
#[test]
fn a_lost_real_expiry_reply_retries_its_frozen_header_and_observes_empty_success() {
    use crate::session_time::testing::FakeClock;
    let controls = Directory::new();
    let drop = controls.0.join("drop-next-message-expire-result");
    std::fs::write(&drop, b"drop").unwrap();
    let sample = |tick| SessionClockReading {
        domain: [79; 32],
        tick,
    };
    let clock = Arc::new(FakeClock::new(sample(100)));
    with_runtime_clock_hooks(
        128,
        clock.clone(),
        Hooks {
            directory: Some(controls.0.clone()),
            pause: vec![],
        },
        |runtime, directory| {
            runtime.message_expiry_scan = Instant::now() - Duration::from_millis(101);
            runtime.expire_messages().unwrap();
            let frozen = runtime.message_expiry_request.unwrap();
            let unknown = next_control_reply(runtime);
            assert!(
                matches!(&unknown,ControlResponse::MessageExpiryUnknown{now,after} if (*now,*after)==frozen)
            );
            let stale_unknown = unknown.clone();
            runtime.apply_control_response(unknown).unwrap();
            assert!(runtime.message_expiry_retry);
            clock.set(sample(1_000_000_100));
            runtime.expire_messages().unwrap();
            assert_eq!(runtime.message_expiry_request, Some(frozen));
            let reply = next_control_reply(runtime);
            assert!(
                matches!(&reply,ControlResponse::MessageExpiry{now,after,result:Ok(sweep)} if (*now,*after)==frozen&&sweep.removed.is_empty())
            );
            runtime.apply_control_response(reply).unwrap();
            runtime.apply_control_response(stale_unknown).unwrap();
            assert!(runtime.message_expiry_request.is_none());
            assert!(!runtime.message_expiry_retry);
            assert_eq!(runtime.message_expiry_retry_attempts, 0);
            let events = std::fs::read_to_string(directory.join("events.jsonl")).unwrap();
            let events: Vec<serde_json::Value> = events
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            let retry = events
                .iter()
                .find(|event| event["kind"] == "message_expiry_retry")
                .unwrap();
            let applied = events
                .iter()
                .find(|event| event["kind"] == "message_expiry_retry_applied")
                .expect("empty_retry_success_not_observed");
            for field in ["domain", "tick", "after"] {
                assert_eq!(
                    retry["details"][field], applied["details"][field],
                    "retry_header_changed"
                );
            }
            assert_eq!(applied["details"]["tick"], "100");
            assert_eq!(applied["details"]["after"], "");
            assert_eq!(applied["details"]["removed"], 0);
            assert!(
                events
                    .iter()
                    .any(|event| event["kind"] == "message_expiry_result_ignored"
                        && event["details"]["code"] == "stale_unknown")
            );
        },
    );
}

#[cfg(feature = "test-hooks")]
#[test]
fn repeated_real_expiry_reply_loss_stops_at_the_finite_retry_budget() {
    use crate::session_time::testing::FakeClock;
    let controls = Directory::new();
    let clock = Arc::new(FakeClock::new(SessionClockReading {
        domain: [80; 32],
        tick: 100,
    }));
    with_runtime_clock_hooks(
        128,
        clock,
        Hooks {
            directory: Some(controls.0.clone()),
            pause: vec![],
        },
        |runtime, directory| {
            runtime.message_expiry_scan = Instant::now() - Duration::from_millis(101);
            let mut frozen = None;
            for attempt in 0..=MESSAGE_EXPIRY_RETRIES {
                std::fs::write(controls.0.join("drop-next-message-expire-result"), b"drop")
                    .unwrap();
                runtime.expire_messages().unwrap();
                let header = runtime.message_expiry_request.unwrap();
                assert_eq!(*frozen.get_or_insert(header), header);
                let unknown = next_control_reply(runtime);
                assert!(
                    matches!(&unknown,ControlResponse::MessageExpiryUnknown{now,after} if (*now,*after)==header)
                );
                runtime.apply_control_response(unknown).unwrap();
                assert_eq!(runtime.message_expiry_retry_attempts, attempt);
            }
            assert_eq!(
                runtime.expire_messages().unwrap_err().code,
                "message_expiry_retry_exhausted"
            );
            let events = std::fs::read_to_string(directory.join("events.jsonl")).unwrap();
            assert_eq!(
                events
                    .lines()
                    .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                    .filter(|event| event["kind"] == "message_expiry_retry")
                    .count(),
                usize::from(MESSAGE_EXPIRY_RETRIES)
            );
        },
    );
}

#[test]
fn disconnect_zero_applies_open_already_relayed_during_its_own_drain() {
    with_runtime(128, |runtime, directory| {
        let (generation, _client) = peer(runtime);
        runtime
            .connect(generation, connect_packet(b"draining-end"))
            .unwrap();
        runtime.drain_inputs().unwrap();
        let response = next_control_reply(runtime);
        assert!(
            matches!(
                &response,
                ControlResponse::Lifecycle {
                    reply: StoreReply::Outcome(StoreOutcome::Committed(
                        CommitFacts::SessionLifecycle(SessionChange::Opened { .. })
                    )),
                    ..
                }
            ),
            "the writer must really commit Open"
        );
        relay_control_reply(runtime, response);
        assert!(runtime.peers[&generation].session.is_none());
        assert!(!runtime.control_inputs.is_empty());
        // No drain between the authentic reply and this DISCONNECT. Its own
        // drain installs the attachment while the transport is still present.
        runtime
            .packet(generation, 0xe0, vec![0, 5, 0x11, 0, 0, 0, 0])
            .unwrap();
        until(runtime, |r| {
            !r.owner.session_status().lifecycle_pending
                && r.lifecycle_inflight.is_empty()
                && r.closing_inputs.is_empty()
        });
        let state =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert!(
            state.sessions.is_empty(),
            "disconnect_zero_during_open_drain_retained_durable_session"
        );
        assert!(runtime.sessions.is_empty());
        assert_eq!(runtime.owner.session_status().sessions, 0);
        let (next, mut client) = peer(runtime);
        runtime
            .connect(next, connect_packet(b"draining-end"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(&read_frame(&mut client).1[..2], &[0, 0]);
    });
}

#[test]
fn disconnect_zero_before_connack_ends_the_exact_committed_infinite_open() {
    with_runtime(128, |runtime, directory| {
        let (generation, mut client) = peer(runtime);
        runtime
            .connect(generation, connect_packet(b"early-end"))
            .unwrap();
        runtime.drain_inputs().unwrap();
        let response = next_control_reply(runtime);
        let ControlResponse::Lifecycle {
            operation,
            reply:
                StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
                    SessionChange::Opened {
                        session,
                        expiry: SessionExpiry::Never,
                        ..
                    },
                ))),
            ..
        } = response.clone()
        else {
            panic!("actual Open commit, not a fabricated result");
        };
        assert!(runtime.peers[&generation].pending);
        assert!(
            runtime.sessions.is_empty(),
            "unknown Open cannot announce readiness"
        );
        // The pending reader must still observe an explicit end before CONNACK.
        client
            .write_all(&[0xe0, 7, 0, 5, 0x11, 0, 0, 0, 0])
            .unwrap();
        until(runtime, |r| !r.peers.contains_key(&generation));
        assert!(
            runtime
                .session_requests
                .values()
                .any(|pending| pending.operation == Some(operation)
                    && pending
                        .close_intent
                        .is_some_and(|i| i.expiry == SessionExpiry::OnDisconnect)),
            "late exact Open result retains the observed DISCONNECT policy"
        );
        relay_control_reply(runtime, response.clone());
        runtime.drain_inputs().unwrap();
        until(runtime, |r| {
            !r.owner.session_status().lifecycle_pending
                && r.lifecycle_inflight.is_empty()
                && r.closing_inputs.is_empty()
        });
        assert!(
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite"))
                .unwrap()
                .sessions
                .is_empty(),
            "early_disconnect_zero_did_not_end_known_open"
        );
        let (replacement, mut next) = peer(runtime);
        runtime
            .connect(replacement, connect_packet(b"early-end"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(&read_frame(&mut next).1[..2], &[0, 0]);
        let current = runtime.peers[&replacement].session.unwrap();
        assert_ne!(current, session);
        relay_control_reply(runtime, response);
        runtime.drain_inputs().unwrap();
        assert_eq!(
            runtime.peers[&replacement].session,
            Some(current),
            "late authentic old Open response cannot rebind a reused ClientID"
        );
    });
}

#[test]
fn closing_an_ephemeral_transport_keeps_accepted_frame_facts_under_input_pressure() {
    with_runtime(128, |runtime, directory| {
        let (generation, mut client) = peer(runtime);
        let mut connect = connect_packet(b"close-pressure");
        connect.expiry = SessionExpiry::OnDisconnect;
        runtime.connect(generation, connect).unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut client).0, 0x20);
        until(runtime, |r| r.frames.is_empty() && r.inputs.is_empty());
        runtime.packet(generation, 0x32, publish_body(1)).unwrap();
        until(runtime, |r| {
            r.frames.values().any(|frame| frame.kind == "PUBACK")
        });
        assert_eq!(read_frame(&mut client).0, 0x40);
        for order in 0..ORDINARY_INPUTS {
            runtime
                .queue(Input::Packet {
                    connection: generation,
                    ingress_order: 100 + order as u64,
                    packet: InboundPacket::Pingreq,
                })
                .unwrap();
        }
        assert_eq!(
            runtime.inputs.len(),
            ORDINARY_INPUTS,
            "actual configured ordinary input budget is full"
        );
        let close = runtime.close(generation, "test_input_pressure");
        assert!(
            close.is_ok(),
            "session_close_lost_accepted_frame_fact_under_input_pressure: {close:?}"
        );
        until(runtime, |r| {
            r.sessions.is_empty()
                && !r.owner.session_status().lifecycle_pending
                && r.closing_inputs.is_empty()
                && r.frames.is_empty()
        });
        let durable =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert!(durable.sessions.is_empty() && durable.publications.is_empty());
    });
}

#[test]
#[cfg(feature = "test-hooks")]
fn copied_lifecycle_effect_and_actual_old_close_cannot_rebind_a_replaced_client_id() {
    with_runtime(128, |runtime, directory| {
        let hook_dir = directory.join("session-hooks");
        std::fs::create_dir(&hook_dir).unwrap();
        runtime.hooks.directory = Some(hook_dir.clone());
        std::fs::write(hook_dir.join("capture-next-session-change"), []).unwrap();
        let (old, mut old_client) = peer(runtime);
        runtime
            .connect(old, connect_packet(b"copied-lifecycle"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut old_client).0, 0x20);
        let copied = runtime
            .copied_session_change
            .as_ref()
            .expect("captured from actual SessionChanged sink effect");
        let old_sid = runtime.peers[&old].session.unwrap();
        assert_eq!(copied.connection, Some(old));
        std::fs::write(hook_dir.join("capture-next-session-close"), []).unwrap();
        runtime.close(old, "test_end_old_transport").unwrap();
        runtime.drain_inputs().unwrap();
        let close = runtime
            .copied_session_close
            .expect("captured from actual transport close");
        assert_eq!((close.session, close.generation), (old_sid, old));
        let (current, mut client) = peer(runtime);
        let mut connect = connect_packet(b"copied-lifecycle");
        connect.clean_start = true;
        runtime.connect(current, connect).unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(&read_frame(&mut client).1[..2], &[0, 0]);
        let current_sid = runtime.peers[&current].session.unwrap();
        let current_token = runtime.peers[&current].opened_by;
        assert_ne!(current_sid, old_sid);
        std::fs::write(hook_dir.join("replay-session-change"), []).unwrap();
        std::fs::write(hook_dir.join("replay-session-close"), []).unwrap();
        runtime.controls().unwrap();
        runtime.drain_inputs().unwrap();
        assert_eq!(
            runtime.sessions[b"copied-lifecycle".as_slice()].id,
            current_sid,
            "stale_session_change_rebound"
        );
        assert_eq!(runtime.peers[&current].session, Some(current_sid));
        assert_eq!(runtime.peers[&current].opened_by, current_token);
        assert_eq!(
            runtime.owner.connection_of(current_sid),
            Some(current),
            "old_close_ended_new_incarnation"
        );
        runtime.packet(current, 0xc0, vec![]).unwrap();
        runtime.drain_inputs().unwrap();
        assert_eq!(read_frame(&mut client).0, 0xd0);
        let text = std::fs::read_to_string(directory.join("events.jsonl")).unwrap();
        for kind in [
            "session_change_copied",
            "session_change_replayed",
            "session_change_ignored",
            "session_close_copied",
            "session_close_replayed",
        ] {
            assert!(
                text.lines().any(
                    |line| serde_json::from_str::<serde_json::Value>(line).unwrap()["kind"] == kind
                ),
                "actual hook {kind} reached"
            );
        }
    });
}

#[test]
fn real_accept_reserves_both_future_close_facts_behind_a_held_lifecycle() {
    with_runtime(128, |runtime, _| {
        let mut confirmed = Vec::new();
        let mut clients = Vec::new();
        for index in 0..CONNECTIONS {
            let (generation, mut client) = peer(runtime);
            let mut connect = connect_packet(format!("close-reserve-{index}").as_bytes());
            connect.expiry = SessionExpiry::OnDisconnect;
            runtime.connect(generation, connect).unwrap();
            until(runtime, |r| r.session_requests.is_empty());
            assert_eq!(read_frame(&mut client).0, 0x20);
            confirmed.push(generation);
            clients.push(client);
        }
        until(runtime, |r| {
            r.frames.is_empty() && r.inputs.is_empty() && r.work_reads.is_empty()
        });
        runtime.close(confirmed[0], "test_hold_close").unwrap();
        runtime.drain_inputs().unwrap();
        let committed = next_control_reply(runtime);
        assert!(
            matches!(
                committed,
                ControlResponse::Lifecycle {
                    reply: StoreReply::Outcome(StoreOutcome::Committed(
                        CommitFacts::SessionLifecycle(SessionChange::Closed { .. })
                    )),
                    ..
                }
            ),
            "hold an authentic committed lifecycle result"
        );
        for generation in &confirmed[1..8] {
            runtime.close(*generation, "test_closing_backlog").unwrap();
        }
        assert_eq!(runtime.closing_inputs.len(), 14);
        let deadline = Instant::now() + Duration::from_secs(1);
        while !runtime.retiring.is_empty() && Instant::now() < deadline {
            runtime.connections().unwrap();
            thread::sleep(Duration::from_millis(1));
        }
        assert!(runtime.retiring.is_empty());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let mut accepted = 0;
        for _ in 0..CONNECTIONS {
            let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            let (stream, _) = listener.accept().unwrap();
            if runtime.accept(stream).unwrap() {
                accepted += 1;
                clients.push(client);
            }
            assert!(
                runtime.closing_inputs.len() + 2 * runtime.peers.len() <= CLOSING_INPUTS,
                "session_close_reservation_overcommitted: queued={} peers={} reserve={}",
                runtime.closing_inputs.len(),
                runtime.peers.len(),
                CLOSING_INPUTS
            );
        }
        assert_eq!(
            accepted, 1,
            "new transport consumes its conservative two-fact close reservation"
        );
        let independent = confirmed[8];
        runtime.packet(independent, 0xc0, vec![]).unwrap();
        runtime.drain_inputs().unwrap();
        assert_eq!(
            read_frame(&mut clients[8]).0,
            0xd0,
            "independent control progresses while new acceptance is bounded"
        );
        relay_control_reply(runtime, committed);
        until(runtime, |r| {
            r.closing_inputs.is_empty() && !r.owner.session_status().lifecycle_pending
        });
        drop(clients);
    });
}

#[test]
fn accepted_frames_reserve_their_terminal_fact_before_reactor_backpressure() {
    with_runtime(128, |runtime, _| {
        let mut generations = Vec::new();
        let mut clients = Vec::new();
        for index in 0..CONNECTIONS {
            let (generation, mut client) = peer(runtime);
            runtime
                .connect(
                    generation,
                    connect_packet(format!("frame-reserve-{index}").as_bytes()),
                )
                .unwrap();
            until(runtime, |r| r.session_requests.is_empty());
            assert_eq!(read_frame(&mut client).0, 0x20);
            generations.push(generation);
            clients.push(client);
        }
        until(runtime, |r| {
            r.frames.is_empty() && r.inputs.is_empty() && r.work_reads.is_empty()
        });
        let mut retained = false;
        // Drive the configured reservation to saturation, including both start
        // and terminal facts reserved for expiring frames.
        for _ in 0..(2 * TERMINAL_INPUTS / CONNECTIONS + 2) {
            for generation in &generations {
                for _ in 0..3 {
                    runtime.packet(*generation, 0xc0, vec![]).unwrap();
                }
            }
            runtime.drain_inputs().unwrap();
            assert!(
                runtime.terminal_inputs.len() + 2 * runtime.frames.len() <= TERMINAL_INPUTS,
                "terminal_fact_reservation_overcommitted: facts={} accepted_frames={} reserve={}",
                runtime.terminal_inputs.len(),
                runtime.frames.len(),
                TERMINAL_INPUTS
            );
            if runtime.owner.outbox().unwrap().pending().any(|effect| {
                matches!(
                    effect.effect(),
                    Effect::EmitFrame {
                        frame: OutboundFrame::Pingresp,
                        ..
                    }
                )
            }) {
                retained = true;
                break;
            }
            let deadline = Instant::now() + Duration::from_secs(1);
            while !runtime.frames.is_empty() && Instant::now() < deadline {
                runtime.connections().unwrap();
                thread::sleep(Duration::from_millis(1));
            }
            assert!(
                runtime.frames.is_empty(),
                "every prior frame had an actual local write completion before the next pass"
            );
        }
        assert!(
            retained,
            "finite genuine PING traffic reached the terminal reservation, retaining effects in the owner"
        );
        until(runtime, |r| {
            r.inputs.is_empty()
                && r.terminal_inputs.is_empty()
                && r.frames.is_empty()
                && r.owner.outbox().unwrap().is_empty()
        });
        assert_eq!(
            runtime.peers.len(),
            CONNECTIONS,
            "bounded write-fact backpressure did not close independent peers"
        );
        drop(clients);
    });
}

#[test]
fn graceful_shutdown_finishes_zero_sessions_and_preserves_persistent_state() {
    with_runtime(128, |runtime, directory| {
        let (zero, mut ephemeral) = peer(runtime);
        let mut connect = connect_packet(b"shutdown-zero");
        connect.expiry = SessionExpiry::OnDisconnect;
        runtime.connect(zero, connect).unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut ephemeral).0, 0x20);
        let (persistent, mut kept) = peer(runtime);
        runtime
            .connect(persistent, connect_packet(b"shutdown-persistent"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut kept).0, 0x20);
        runtime
            .packet(persistent, 0x82, vec![0, 1, 0, 0, 1, b't', 1])
            .unwrap();
        until(runtime, |r| r.inputs.is_empty() && r.inflight.is_empty());
        assert_eq!(read_frame(&mut kept).0, 0x90);
        let persistent_sid = runtime.peers[&persistent].session.unwrap();
        runtime.shutdown_sessions(Duration::from_secs(2)).unwrap();
        let durable =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert_eq!(
            durable.sessions.len(),
            1,
            "normal_stop_must_finish_zero_session_before_writer_release"
        );
        assert!(durable.sessions.contains_key(&persistent_sid));
        assert_eq!(
            durable.routes.len(),
            1,
            "persistent responsibilities survive graceful shutdown"
        );
        assert!(runtime.peers.is_empty());
        assert_eq!(runtime.owner.session_status().bindings, 0);
        assert_eq!(runtime.sessions.len(), 1);
        let mut byte = [0];
        assert_eq!(ephemeral.read(&mut byte).unwrap(), 0);
        assert_eq!(kept.read(&mut byte).unwrap(), 0);
    });
}

#[test]
fn bounded_shutdown_reports_an_authentic_unknown_open_without_fabricating_rejection() {
    with_runtime(128, |runtime, directory| {
        let (generation, _client) = peer(runtime);
        runtime
            .connect(generation, connect_packet(b"shutdown-unknown"))
            .unwrap();
        runtime.drain_inputs().unwrap();
        let actual = next_control_reply(runtime);
        let ControlResponse::Lifecycle {
            operation,
            reply:
                StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
                    SessionChange::Opened { session, .. },
                ))),
            ..
        } = actual.clone()
        else {
            panic!("actual committed Open result");
        };
        let start = Instant::now();
        let stopped = runtime.shutdown_sessions(Duration::from_millis(30));
        assert!(matches!(stopped,Err(ref failure) if failure.code=="shutdown_incomplete"));
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "shutdown must retain a finite budget"
        );
        assert_eq!(
            runtime.owner.lifecycle_dispatch().map(|(op, _)| op),
            Some(operation),
            "unknown lifecycle keeps its exact operation identity"
        );
        assert!(
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite"))
                .unwrap()
                .sessions
                .contains_key(&session),
            "unknown result is not proof the committed session is absent"
        );
        let text = std::fs::read_to_string(directory.join("events.jsonl")).unwrap();
        assert!(text.lines().any(|line| {
            let event: serde_json::Value = serde_json::from_str(line).unwrap();
            event["kind"] == "shutdown_incomplete"
                && event["details"]["code"] == "pending_or_unknown"
        }));
        relay_control_reply(runtime, actual);
        runtime.drain_inputs().unwrap();
        assert_eq!(
            runtime.owner.session_status().bindings,
            0,
            "late result may establish durability, never a ghost transport"
        );
    });
}

#[test]
#[cfg(feature = "test-hooks")]
fn an_actual_held_work_page_cannot_repopulate_a_retired_session_cache() {
    with_runtime(128, |runtime, directory| {
        let hooks = directory.join("held-page-hooks");
        std::fs::create_dir(&hooks).unwrap();
        runtime.hooks.directory = Some(hooks.clone());
        let (old, mut subscriber) = peer(runtime);
        runtime
            .connect(old, connect_packet(b"old-page-target"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut subscriber).0, 0x20);
        runtime
            .packet(old, 0x82, vec![0, 1, 0, 0, 1, b't', 1])
            .unwrap();
        until(runtime, |r| {
            r.inputs.is_empty() && r.inflight.is_empty() && r.work_reads.is_empty()
        });
        assert_eq!(read_frame(&mut subscriber).0, 0x90);
        let old_sid = runtime.peers[&old].session.unwrap();
        std::fs::write(hooks.join("hold-work-pages"), []).unwrap();
        let (source, mut publisher) = peer(runtime);
        runtime
            .connect(source, connect_packet(b"old-page-source"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut publisher).0, 0x20);
        runtime.packet(source, 0x32, publish_body(1)).unwrap();
        until(runtime, |r| {
            r.deferred_work.iter().any(|response| matches!(response,Response::Work { session,result:Ok(page),.. } if *session==old_sid&&!page.items.is_empty()))
        });
        assert_eq!(read_frame(&mut publisher).0, 0x40);
        let before =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert_eq!(before.obligations.len(), 1);
        assert!(
            before.outbound.is_empty(),
            "actual useful page was held before any claim"
        );
        runtime
            .packet(old, 0xe0, vec![0, 5, 0x11, 0, 0, 0, 0])
            .unwrap();
        until(runtime, |r| {
            !r.sessions.contains_key(b"old-page-target".as_slice())
                && !r.owner.session_status().lifecycle_pending
        });
        let (current, mut current_client) = peer(runtime);
        let mut connect = connect_packet(b"old-page-target");
        connect.clean_start = true;
        runtime.connect(current, connect).unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(&read_frame(&mut current_client).1[..2], &[0, 0]);
        let current_sid = runtime.peers[&current].session.unwrap();
        assert_ne!(current_sid, old_sid);
        std::fs::remove_file(hooks.join("hold-work-pages")).unwrap();
        runtime.controls().unwrap();
        runtime.drain_inputs().unwrap();
        assert!(
            runtime
                .work_cache
                .values()
                .all(|item| item.target != old_sid),
            "retired_work_page_repopulated_cache"
        );
        assert!(
            runtime
                .work_reads
                .values()
                .all(|session| *session != old_sid),
            "the real accepted read correlation is consumed exactly once"
        );
        assert_eq!(runtime.owner.connection_of(current_sid), Some(current));
        runtime.packet(current, 0xc0, vec![]).unwrap();
        runtime.drain_inputs().unwrap();
        assert_eq!(read_frame(&mut current_client).0, 0xd0);
    });
}

#[test]
fn finite_expiry_uses_exact_clock_boundary_and_never_expires_an_active_transport() {
    use crate::session_time::testing::FakeClock;
    let sample = |tick| SessionClockReading {
        domain: [42; 32],
        tick,
    };
    let clock = Arc::new(FakeClock::new(sample(100)));
    with_runtime_clock(128, clock.clone(), |runtime, directory| {
        let (generation, mut client) = peer(runtime);
        let mut connect = connect_packet(b"finite-boundary");
        connect.expiry = SessionExpiry::After(1);
        runtime.connect(generation, connect).unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(&read_frame(&mut client).1[..2], &[0, 0]);
        clock.set(sample(2_000_000_100));
        runtime.expire_sessions().unwrap();
        assert!(runtime.expiry_timers.is_empty(), "active_session_expired");
        runtime.close(generation, "test_lost").unwrap();
        until(runtime, |r| {
            !r.owner.session_status().lifecycle_pending
                && r.closing_inputs.is_empty()
                && r.lifecycle_inflight.is_empty()
        });
        let timer = runtime.expiry_timers.values().next().unwrap();
        assert_eq!(timer.detached.deadline, Some(3_000_000_000));
        clock.set(sample(3_000_000_099));
        runtime.expire_sessions().unwrap();
        assert!(runtime.expiry_inputs.is_empty(), "expiry_before_deadline");
        clock.set(sample(3_000_000_100));
        runtime.expire_sessions().unwrap();
        until(runtime, |r| {
            r.sessions.is_empty() && r.lifecycle_inflight.is_empty()
        });
        assert!(runtime.expiry_timers.is_empty());
        assert!(
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite"))
                .unwrap()
                .sessions
                .is_empty()
        );
    });
}

#[test]
fn due_connect_cannot_resume_while_the_expiration_timer_is_deliberately_unpolled() {
    use crate::session_time::testing::FakeClock;
    let sample = |tick| SessionClockReading {
        domain: [43; 32],
        tick,
    };
    let clock = Arc::new(FakeClock::new(sample(0)));
    with_runtime_clock(128, clock.clone(), |runtime, _| {
        let (old, mut old_client) = peer(runtime);
        let mut connect = connect_packet(b"late-connect");
        connect.expiry = SessionExpiry::After(1);
        runtime.connect(old, connect.clone()).unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        read_frame(&mut old_client);
        let original = runtime.peers[&old].session.unwrap();
        runtime.close(old, "test_lost").unwrap();
        until(runtime, |r| {
            !r.owner.session_status().lifecycle_pending
                && r.closing_inputs.is_empty()
                && r.lifecycle_inflight.is_empty()
        });
        assert_eq!(runtime.expiry_timers.len(), 1);
        clock.set(sample(1_000_000_000));
        let (current, mut client) = peer(runtime);
        runtime.connect(current, connect).unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(
            &read_frame(&mut client).1[..2],
            &[0, 0],
            "late_open_resumed_without_timer"
        );
        assert_ne!(runtime.peers[&current].session, Some(original));
        assert!(runtime.expiry_timers.is_empty());
    });
}

#[test]
fn early_finite_disconnect_keeps_observed_time_when_open_result_is_already_relayed() {
    use crate::session_time::testing::FakeClock;
    let sample = |tick| SessionClockReading {
        domain: [44; 32],
        tick,
    };
    let clock = Arc::new(FakeClock::new(sample(0)));
    with_runtime_clock(128, clock.clone(), |runtime, directory| {
        let (generation, _client) = peer(runtime);
        runtime
            .connect(generation, connect_packet(b"finite-early-end"))
            .unwrap();
        runtime.drain_inputs().unwrap();
        let response = next_control_reply(runtime);
        relay_control_reply(runtime, response);
        clock.set(sample(2_000_000_000));
        runtime
            .packet(generation, 0xe0, vec![0, 5, 0x11, 0, 0, 0, 1])
            .unwrap();
        clock.set(sample(9_000_000_000));
        until(runtime, |r| {
            !r.owner.session_status().lifecycle_pending
                && r.closing_inputs.is_empty()
                && r.lifecycle_inflight.is_empty()
        });
        let state =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        let detached = state.sessions.values().next().unwrap().detached.unwrap();
        assert_eq!(
            detached.deadline,
            Some(3_000_000_000),
            "detach_deadline_rearmed_after_observation"
        );
        runtime.expire_sessions().unwrap();
        until(runtime, |r| r.sessions.is_empty());
    });
}

#[test]
fn blocked_expiration_keeps_its_reserved_lane_when_ordinary_inputs_are_full() {
    use crate::session_time::testing::FakeClock;
    let sample = |tick| SessionClockReading {
        domain: [45; 32],
        tick,
    };
    let clock = Arc::new(FakeClock::new(sample(0)));
    with_runtime_clock(128, clock.clone(), |runtime, _| {
        let (old, mut client) = peer(runtime);
        let mut connect = connect_packet(b"lane-finite");
        connect.expiry = SessionExpiry::After(1);
        runtime.connect(old, connect).unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        read_frame(&mut client);
        let session = runtime.peers[&old].session.unwrap();
        runtime.close(old, "test_lost").unwrap();
        until(runtime, |r| {
            !r.owner.session_status().lifecycle_pending
                && r.closing_inputs.is_empty()
                && r.lifecycle_inflight.is_empty()
        });
        let (opening, _other) = peer(runtime);
        runtime
            .connect(opening, connect_packet(b"held-real-open"))
            .unwrap();
        runtime.drain_inputs().unwrap();
        // A real committed result exists; retain it outside the owner.
        let held = next_control_reply(runtime);
        assert!(matches!(
            &held,
            ControlResponse::Lifecycle {
                reply: StoreReply::Outcome(StoreOutcome::Committed(_)),
                ..
            }
        ));
        clock.set(sample(1_000_000_000));
        runtime.expire_sessions().unwrap();
        assert_eq!(runtime.expiry_inputs.len(), 1);
        for _ in 0..ORDINARY_INPUTS {
            runtime.queue(Input::WorkAvailable { session }).unwrap();
        }
        runtime.drain_inputs().unwrap();
        assert_eq!(
            runtime.expiry_inputs.len(),
            1,
            "blocked_expiry_escaped_its_reserved_lane"
        );
        assert!(
            !runtime
                .inputs
                .iter()
                .any(|i| matches!(i, Input::ExpireSession { .. })),
            "blocked_expiry_escaped_its_reserved_lane"
        );
        relay_control_reply(runtime, held);
        until(runtime, |r| !r.sessions.values().any(|s| s.id == session));
    });
}

#[test]
fn certain_detach_failure_is_observable_instead_of_losing_offline_expiration() {
    with_runtime(128, |runtime, directory| {
        let (generation, mut client) = peer(runtime);
        let mut connect = connect_packet(b"certain-no-detach");
        connect.expiry = SessionExpiry::After(1);
        runtime.connect(generation, connect).unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        read_frame(&mut client);
        let cached = runtime.sessions[b"certain-no-detach".as_slice()].clone();
        let input = Input::DetachSession {
            connection: ConnectionKey {
                session: cached.id,
                generation,
            },
            opened_by: cached.opened_by,
            expiry: cached.expiry,
            observed_at: runtime.session_now().unwrap(),
        };
        assert!(matches!(
            runtime.owner.step(
                input,
                TurnGrant {
                    work_limit: WorkUnits::new(65536)
                }
            ),
            StepOutcome::Applied { .. }
        ));
        let (effect, operation) = runtime
            .owner
            .outbox()
            .unwrap()
            .pending()
            .find_map(|pending| match pending.effect() {
                Effect::Persist { command } => Some((pending.id(), command.recipe.operation())),
                _ => None,
            })
            .unwrap();
        // A real Resolve reaches the writer before the withheld Persist.
        // Its durable fence proves no effect; no fabricated store result.
        runtime
            .writer
            .control_sender
            .send(ControlRequest::Lifecycle(OwnedEffect::new(
                effect,
                Effect::ResolveAndFence { operation },
            )))
            .unwrap();
        let response = next_control_reply(runtime);
        let ControlResponse::Lifecycle { reply, .. } = response else {
            panic!("actual writer fence result")
        };
        assert!(matches!(
            reply,
            StoreReply::Outcome(StoreOutcome::RejectedCertain(_))
        ));
        runtime
            .queue(Input::StoreResult {
                effect,
                operation,
                reply,
            })
            .unwrap();
        let failure = runtime
            .drain_inputs()
            .expect_err("certain_detach_failure_silently_lost_expiry");
        assert_eq!(failure.code, "session_detach_rejected");
        assert!(!runtime.owner.lifecycle_operation_is_live(operation));
        let durable =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert_eq!(durable.sessions.len(), 1);
        assert!(durable.sessions[&cached.id].detached.is_none());
    });
}

#[test]
fn coalesced_clock_cas_reply_does_not_hide_an_uncheckpointed_interval() {
    use crate::session_time::testing::FakeClock;
    let sample = |tick| SessionClockReading {
        domain: [46; 32],
        tick,
    };
    let clock = Arc::new(FakeClock::new(sample(0)));
    with_runtime_clock(128, clock.clone(), |runtime, directory| {
        let (generation, mut client) = peer(runtime);
        let mut connect = connect_packet(b"coalesced-clock");
        connect.expiry = SessionExpiry::After(100);
        runtime.connect(generation, connect).unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        read_frame(&mut client);
        clock.set(sample(2_000_000_000));
        runtime.close(generation, "test_lost").unwrap();
        until(runtime, |r| {
            !r.owner.session_status().lifecycle_pending
                && r.closing_inputs.is_empty()
                && r.lifecycle_inflight.is_empty()
        });
        assert_eq!(
            runtime.clock_anchor.logical, 0,
            "adapter still has the earlier valid projection"
        );
        clock.set(sample(5_000_000_000));
        runtime.last_clock_checkpoint = Instant::now() - Duration::from_secs(31);
        runtime.expire_sessions().unwrap();
        runtime.expire_sessions().unwrap();
        let response = next_control_reply(runtime);
        assert!(
            matches!(&response,ControlResponse::ClockCheckpoint {result:Ok(anchor),..} if anchor.logical==2_000_000_000)
        );
        relay_control_reply(runtime, response);
        assert!(
            runtime.last_clock_checkpoint.elapsed() >= Duration::from_secs(30),
            "stale_clock_cas_reply_hid_uncommitted_progress"
        );
        assert!(runtime.clock_checkpoint.is_none());
        runtime.expire_sessions().unwrap();
        let response = next_control_reply(runtime);
        assert!(
            matches!(&response,ControlResponse::ClockCheckpoint {result:Ok(anchor),..} if anchor.logical==5_000_000_000)
        );
        relay_control_reply(runtime, response);
        assert!(runtime.last_clock_checkpoint.elapsed() < Duration::from_secs(1));
        assert!(!runtime.checkpoint_degraded);
        let state =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert_eq!(state.session_clock.unwrap().logical, 5_000_000_000);
        assert_eq!(runtime.expiry_timers.len(), 1);
    });
}

#[test]
fn simultaneous_expiration_is_batched_while_an_independent_connection_pings() {
    use crate::session_time::testing::FakeClock;
    let sample = |tick| SessionClockReading {
        domain: [47; 32],
        tick,
    };
    let clock = Arc::new(FakeClock::new(sample(0)));
    with_runtime_clock(128, clock.clone(), |runtime, directory| {
        let (witness, mut client) = peer(runtime);
        runtime
            .connect(witness, connect_packet(b"infinite-witness"))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        read_frame(&mut client);
        let retained = runtime.peers[&witness].session.unwrap();
        for n in 0..8 {
            let (generation, mut finite_client) = peer(runtime);
            let mut connect = connect_packet(format!("simultaneous-{n}").as_bytes());
            connect.expiry = SessionExpiry::After(1);
            runtime.connect(generation, connect).unwrap();
            until(runtime, |r| r.session_requests.is_empty());
            read_frame(&mut finite_client);
            runtime.close(generation, "test_lost").unwrap();
            until(runtime, |r| {
                !r.owner.session_status().lifecycle_pending
                    && r.closing_inputs.is_empty()
                    && r.lifecycle_inflight.is_empty()
            });
        }
        assert_eq!(runtime.expiry_timers.len(), 8);
        clock.set(sample(1_000_000_000));
        runtime.expire_sessions().unwrap();
        assert_eq!(runtime.expiry_inputs.len(), 4, "expiry_turn_exceeded_four");
        runtime.expire_sessions().unwrap();
        assert_eq!(runtime.expiry_inputs.len(), 8);
        runtime.expire_sessions().unwrap();
        assert_eq!(runtime.expiry_inputs.len(), 8, "duplicate_expiry_wakeups");
        assert_eq!(
            runtime.sessions.len(),
            9,
            "capacity_released_before_known_expiry"
        );
        runtime.packet(witness, 0xc0, vec![]).unwrap();
        runtime.drain_inputs().unwrap();
        assert_eq!(
            read_frame(&mut client),
            (0xd0, vec![]),
            "expiry_batch_starved_control"
        );
        until(runtime, |r| {
            r.sessions.len() == 1 && r.expiry_inputs.is_empty() && r.lifecycle_inflight.is_empty()
        });
        assert!(runtime.expiry_timers.is_empty());
        assert_eq!(runtime.peers[&witness].session, Some(retained));
        let state =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert_eq!(state.sessions.len(), 1);
        assert!(state.sessions.contains_key(&retained));
    });
}

fn will_connect(name: &[u8], expiry: SessionExpiry) -> codec::Connect {
    let mut connect = connect_packet(name);
    connect.expiry = expiry;
    connect.will = Some(mqttiumd_contract::will::WillSpec {
        retain: false,
        topic: SharedBytes::copy_from(b"atelier/sensor/status", 1024).unwrap(),
        payload: SharedBytes::copy_from(b"offline-private", 32768).unwrap(),
    });
    connect
}

#[test]
fn normal_disconnect_during_real_will_open_result_drain_cancels_exact_intention() {
    with_runtime(128, |runtime, directory| {
        let (generation, _client) = peer(runtime);
        runtime
            .connect(
                generation,
                will_connect(b"will-early-normal", SessionExpiry::Never),
            )
            .unwrap();
        let actual = next_control_reply(runtime);
        assert!(matches!(
            actual,
            ControlResponse::Lifecycle {
                reply: StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::SessionLifecycle(
                    SessionChange::Opened { .. }
                ))),
                ..
            }
        ));
        let before =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert_eq!(before.wills.len(), 1, "will_not_durable_before_open_result");
        relay_control_reply(runtime, actual);
        // No intervening drain: this is the authentic CONNECT-result race.
        runtime.packet(generation, 0xe0, vec![]).unwrap();
        until(runtime, |r| {
            !r.owner.session_status().lifecycle_pending
                && r.lifecycle_inflight.is_empty()
                && r.closing_inputs.is_empty()
        });
        let after =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert!(
            after.wills.is_empty(),
            "normal_disconnect_did_not_cancel_exact_will"
        );
        assert_eq!(
            after.sessions.len(),
            1,
            "normal_disconnect_erased_infinite_session"
        );
        assert!(
            after.operations.values().all(|row| !matches!(
                row.decision,
                mqttiumd_store_api::Decision::Committed(CommitFacts::WillPublished { .. })
            )),
            "will_admitted_after_normal_disconnect"
        );
        let events = std::fs::read_to_string(directory.join("events.jsonl")).unwrap();
        assert!(
            !events.contains("offline-private"),
            "will_bytes_in_product_diagnostics"
        );
        let detached = events
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .find(|event| {
                event["kind"] == "session_changed" && event["details"]["action"] == "Detached"
            })
            .expect("real durable Will End diagnostic");
        assert!(
            detached["details"]["detached_by"].is_string(),
            "will_detachment_identity_not_bounded_scalar"
        );
        assert!(
            detached["details"].get("deadline").is_none(),
            "infinite_will_detachment_invented_deadline_field"
        );
    });
}

#[test]
fn disconnect_four_before_real_open_reply_survives_source_cleanup_and_admits_once() {
    with_runtime(128, |runtime, directory| {
        let (generation, _client) = peer(runtime);
        runtime
            .connect(
                generation,
                will_connect(b"will-early-publish", SessionExpiry::OnDisconnect),
            )
            .unwrap();
        let actual = next_control_reply(runtime);
        let ControlResponse::Lifecycle {
            operation: opening, ..
        } = actual
        else {
            panic!("authentic Will Open reply")
        };
        // The writer committed, but neither owner nor transport has its result.
        runtime.packet(generation, 0xe0, vec![4, 0]).unwrap();
        assert!(!runtime.peers.contains_key(&generation));
        relay_control_reply(runtime, actual);
        until(runtime, |r| {
            r.sessions.is_empty()
                && !r.owner.session_status().lifecycle_pending
                && r.closing_inputs.is_empty()
        });
        runtime.queue(Input::DiscoverWills).unwrap();
        runtime.drain_inputs().unwrap();
        until(runtime, |r| {
            !r.owner.will_status().reading
                && !r.owner.will_status().admission_pending
                && r.will_reads.is_none()
                && r.owner.outbox().unwrap().is_empty()
        });
        let after =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert!(
            after.sessions.is_empty() && after.wills.is_empty(),
            "early_explicit_will_lost_after_source_cleanup"
        );
        let wills: Vec<_> = after
            .operations
            .values()
            .filter_map(|row| match row.decision {
                mqttiumd_store_api::Decision::Committed(CommitFacts::WillPublished {
                    will,
                    publication,
                }) => Some((will, publication)),
                _ => None,
            })
            .collect();
        assert_eq!(wills.len(), 1, "closure_created_wrong_will_admission_count");
        assert_eq!(wills[0].0, opening, "closure_rebound_will_occurrence");
    });
}

#[test]
fn takeover_waits_for_old_will_admission_and_old_close_cannot_publish_new_will() {
    with_runtime(128, |runtime, directory| {
        let (old, mut old_client) = peer(runtime);
        runtime
            .connect(old, will_connect(b"will-takeover", SessionExpiry::Never))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut old_client).0, 0x20);
        let old_open = runtime.peers[&old].opened_by.unwrap();
        let (new, mut new_client) = peer(runtime);
        runtime
            .connect(new, will_connect(b"will-takeover", SessionExpiry::Never))
            .unwrap();
        until(runtime, |r| r.session_requests.is_empty());
        assert_eq!(read_frame(&mut new_client).0, 0x20);
        let new_open = runtime.peers[&new].opened_by.unwrap();
        assert_ne!(old_open, new_open);
        runtime.close(old, "delayed_old_close").unwrap();
        runtime.drain_inputs().unwrap();
        let state =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert_eq!(state.wills.len(), 1, "old_close_changed_new_will");
        assert_eq!(
            state.wills[&new_open].state,
            mqttiumd_contract::will::WillState::Armed
        );
        let admitted = state
            .operations
            .iter()
            .find_map(|(id, row)| match row.decision {
                mqttiumd_store_api::Decision::Committed(CommitFacts::WillPublished {
                    will,
                    ..
                }) if will == old_open => Some(*id),
                _ => None,
            })
            .expect("old Will admitted before replacing Open");
        assert!(
            admitted.sequence() < new_open.sequence(),
            "new_open_preceded_zero_delay_old_will_admission"
        );
        runtime.packet(new, 0xe0, vec![]).unwrap();
        until(runtime, |r| {
            !r.owner.session_status().lifecycle_pending && r.closing_inputs.is_empty()
        });
        let state =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert!(state.wills.is_empty(), "new_normal_close_failed_to_cancel");
    });
}

#[test]
fn overlapping_same_client_will_connects_leave_the_replacement_connected() {
    with_runtime(128, |runtime, directory| {
        let (first, mut first_client) = peer(runtime);
        let (replacement, mut replacement_client) = peer(runtime);
        runtime
            .connect(first, will_connect(b"overlap-will", SessionExpiry::Never))
            .unwrap();
        runtime
            .connect(
                replacement,
                will_connect(b"overlap-will", SessionExpiry::Never),
            )
            .unwrap();
        // Both authentic SQLite barrier reads precede either Open submission.
        let one = runtime
            .writer
            .control_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        let two = runtime
            .writer
            .control_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        assert!(matches!(
            &one,
            ControlResponse::WillBarrier {
                result: Ok(false),
                ..
            }
        ));
        assert!(matches!(
            &two,
            ControlResponse::WillBarrier {
                result: Ok(false),
                ..
            }
        ));
        runtime.apply_control_response(one).unwrap();
        runtime.drain_inputs().unwrap();
        runtime.apply_control_response(two).unwrap();
        runtime.drain_inputs().unwrap();
        until(runtime, |r| {
            r.session_requests.is_empty()
                && r.lifecycle_inflight.is_empty()
                && !r.owner.session_status().lifecycle_pending
                && r.closing_inputs.is_empty()
        });
        let initial = read_frame(&mut first_client);
        assert!(
            matches!(initial.0, 0x20 | 0xe0),
            "first transport receives CONNACK or takeover DISCONNECT"
        );
        assert!(
            !runtime.peers.contains_key(&first),
            "takeover must close the first generation"
        );
        let reply = read_frame(&mut replacement_client);
        let state =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert_eq!(reply.0, 0x20);
        assert_eq!(
            reply.1[1], 0,
            "replacement refused after overlapping real barriers; durable wills={:?}",
            state.wills
        );
        assert!(
            runtime.peers.get(&replacement).is_some_and(|p| p.connected),
            "replacement transport lost"
        );
    });
}

/// Use the production TLS task with locally generated, verified demo credentials.
fn tls_test_peer(
    runtime: &mut Runtime<'_>,
    directory: &Path,
) -> (
    ConnectionGeneration,
    rustls::StreamOwned<rustls::ClientConnection, TcpStream>,
) {
    let pki = directory.join("pki");
    let status = std::process::Command::new("python3")
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../preview/demo-pki.py"))
        .arg(&pki)
        .output()
        .expect("local demo credential generator");
    assert!(
        status.status.success(),
        "local TLS fixture generation failed"
    );
    let security = Security::load(&TlsFiles {
        cert: pki.join("server.crt"),
        key: pki.join("server.key"),
        client_ca: pki.join("ca.crt"),
        identities: pki.join("identities.json"),
    })
    .unwrap();
    let certs = |name: &str| {
        rustls_pemfile::certs(&mut std::io::Cursor::new(
            std::fs::read(pki.join(name)).unwrap(),
        ))
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    };
    let mut roots = rustls::RootCertStore::empty();
    for certificate in certs("ca.crt") {
        roots.add(certificate).unwrap();
    }
    let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(
        std::fs::read(pki.join("sensor.key")).unwrap(),
    ))
    .unwrap()
    .unwrap();
    let config = rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_root_certificates(roots)
        .with_client_auth_cert(certs("sensor.crt"), key)
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let socket = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let (server, _) = listener.accept().unwrap();
    let io = net::spawn_tls(
        server,
        security.tls,
        security.policy,
        runtime.clock.clone(),
        runtime.wake.clone(),
        #[cfg(feature = "test-hooks")]
        (runtime.hooks.clone(), runtime.events.clone()),
    )
    .unwrap();
    let (generation, previous_client) = peer(runtime);
    drop(previous_client);
    let previous = std::mem::replace(&mut runtime.peers.get_mut(&generation).unwrap().io, io);
    previous.close();
    runtime.retiring.push(previous);
    let client =
        rustls::ClientConnection::new(Arc::new(config), "127.0.0.1".try_into().unwrap()).unwrap();
    let mut client = rustls::StreamOwned::new(client, socket);
    while client.conn.is_handshaking() {
        client.conn.complete_io(&mut client.sock).unwrap();
    }
    (generation, client)
}

#[test]
fn tls_terminal_writer_cannot_discard_an_already_decoded_normal_disconnect() {
    with_runtime(128, |runtime, directory| {
        let (generation, mut client) = tls_test_peer(runtime, directory);
        runtime
            .connect(
                generation,
                will_connect(b"tls-ordered-normal", SessionExpiry::Never),
            )
            .unwrap();
        until(runtime, |r| {
            r.session_requests.is_empty() && r.frames.is_empty()
        });
        assert_eq!(read_frame(&mut client).0, 0x20);
        client.write_all(&[0xc0, 0].repeat(5)).unwrap();
        client.write_all(&[0xe0, 0]).unwrap();
        client.flush().unwrap();
        // Retain the exact six facts decoded by the real TLS task. No fact or
        // writer result is manufactured. Requeue through a bounded lane so the
        // reactor's four-input turn leaves the normal DISCONNECT for next turn.
        let mut facts = Vec::new();
        for _ in 0..6 {
            facts.push(
                runtime.peers[&generation]
                    .io
                    .incoming
                    .recv_timeout(Duration::from_secs(2))
                    .unwrap(),
            );
        }
        assert!(matches!(
            facts.last(),
            Some(net::Incoming::Packet { first: 0xe0, .. })
        ));
        client.sock.shutdown(std::net::Shutdown::Both).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while !runtime.peers[&generation].io.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        assert!(
            runtime.peers[&generation].io.is_finished(),
            "real TLS task did not terminate"
        );
        let (sender, lane) = std::sync::mpsc::sync_channel(net::INPUT_SLOTS);
        let tail = facts.split_off(net::INPUT_SLOTS);
        for fact in facts {
            sender.send(fact).unwrap();
        }
        runtime.peers.get_mut(&generation).unwrap().io.incoming = lane.into();
        let forwarding = thread::spawn(move || {
            for fact in tail {
                if sender.send(fact).is_err() {
                    break;
                }
            }
        });
        until(runtime, |r| {
            !r.peers.contains_key(&generation)
                && r.closing_inputs.is_empty()
                && !r.owner.session_status().lifecycle_pending
                && r.lifecycle_inflight.is_empty()
        });
        forwarding.join().unwrap();
        let state =
            mqttiumd_store_sqlite::SqliteStore::inspect(&directory.join("store.sqlite")).unwrap();
        assert!(
            state.wills.is_empty(),
            "a received normal DISCONNECT must cancel the exact durable Will even when the TLS writer has already terminated"
        );
        assert!(
            state.operations.values().all(|row| !matches!(
                row.decision,
                mqttiumd_store_api::Decision::Committed(CommitFacts::WillPublished { .. })
            )),
            "normal TLS disconnect admitted an unauthorized Will occurrence"
        );
    });
}

#[test]
fn blocked_output_and_full_input_preserve_independent_work_and_reconnect_credit() {
    for reverse in [false, true] {
        with_runtime(128, |runtime, directory| {
            let first = peer(runtime);
            let second = peer(runtime);
            let ((slow, mut receiver_a), (publisher, mut source)) = if reverse {
                (second, first)
            } else {
                (first, second)
            };
            let (subscriber, mut receiver_b) = peer(runtime);
            let slow_name = b"pressure-slow";
            for (generation, name, socket) in [
                (slow, slow_name.as_slice(), &mut receiver_a),
                (publisher, b"pressure-source".as_slice(), &mut source),
                (
                    subscriber,
                    b"pressure-subscriber".as_slice(),
                    &mut receiver_b,
                ),
            ] {
                let mut connect = connect_packet(name);
                if generation == slow {
                    connect.receive_maximum = 1;
                }
                runtime.connect(generation, connect).unwrap();
                until(runtime, |state| {
                    state.session_requests.is_empty() && state.frames.is_empty()
                });
                assert_eq!(read_frame(socket).0, 0x20);
            }
            for (generation, topic, socket) in [
                (slow, b'a', &mut receiver_a),
                (subscriber, b't', &mut receiver_b),
            ] {
                runtime
                    .packet(generation, 0x82, vec![0, 1, 0, 0, 1, topic, 1])
                    .unwrap();
                until(runtime, |state| {
                    state.inflight.is_empty() && state.inputs.is_empty()
                });
                assert_eq!(read_frame(socket).0, 0x90);
            }

            let session = runtime.peers[&slow].session.unwrap();
            let old_connection = ConnectionKey {
                generation: slow,
                session,
            };
            let first_id = OutboundPacketId::new(1).unwrap();
            let sqlite = directory.join("store.sqlite");
            let mut initial = publish_body(6);
            initial[2] = b'a';
            runtime.packet(publisher, 0x32, initial).unwrap();
            until_without_socket_receipts(runtime, |state| {
                state
                    .outgoing_bindings
                    .contains_key(&(old_connection, first_id))
            });
            let old_target = runtime.outgoing_bindings[&(old_connection, first_id)];
            let (first, body) = read_frame(&mut receiver_a);
            assert_eq!(first, 0x32);
            assert_eq!(body, [0, 1, b'a', 0, 1, 0, b'v']);
            assert_eq!(read_frame(&mut source), (0x40, vec![0, 6]));

            // Take the real socket receipt before the reactor can see it.
            let wire = &runtime.peers[&slow].io.written;
            let old_completion = loop {
                match wire.recv_timeout(Duration::from_secs(1)).unwrap() {
                    net::Written::Complete {
                        correlation: Some((effect, connection)),
                        bytes,
                    } if effect == old_target.frame && connection == old_connection => {
                        break bytes;
                    }
                    net::Written::Started { .. } | net::Written::Complete { .. } => {}
                    other => panic!("unexpected old write observation: {other:?}"),
                }
            };
            assert!(runtime.frames.contains_key(&old_target.frame));

            let (blocked, held) = std::sync::mpsc::sync_channel(net::OUTPUT_SLOTS);
            for _ in 0..net::OUTPUT_SLOTS {
                blocked
                    .try_send(net::Write {
                        expiry: None,
                        correlation: None,
                        bytes: vec![0xd0, 0],
                    })
                    .unwrap();
            }
            let _old_sender = std::mem::replace(
                &mut runtime.peers.get_mut(&slow).unwrap().io.outgoing,
                blocked.into(),
            );
            for id in [1, 2] {
                let mut body = publish_body(id);
                body[2] = b'x';
                runtime.packet(slow, 0x32, body).unwrap();
            }
            let held_acks = |r: &Runtime<'_>| {
                r.owner
                    .outbox()
                    .unwrap()
                    .pending()
                    .filter_map(|effect| match effect.effect() {
                        Effect::EmitFrame {
                            connection,
                            frame: OutboundFrame::Puback { packet_id, .. },
                            ..
                        } if connection.generation == slow => Some((effect.id(), packet_id.get())),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            };
            until(runtime, |state| {
                held_acks(state).len() == 2
                    && state.inflight.is_empty()
                    && state.work_reads.is_empty()
                    && state.inputs.is_empty()
            });
            let pending = held_acks(runtime);
            assert_eq!(
                pending.iter().map(|(_, id)| *id).collect::<Vec<_>>(),
                [1, 2]
            );
            for _ in 0..3 {
                runtime.dispatch().unwrap();
                assert_eq!(held_acks(runtime), pending);
            }

            runtime.packet(publisher, 0x32, publish_body(7)).unwrap();
            runtime.drain_inputs().unwrap();
            let reply = runtime
                .writer
                .receiver
                .recv_timeout(Duration::from_secs(1))
                .unwrap();
            let Response::Store {
                effect,
                reply:
                    StoreReply::Outcome(StoreOutcome::Committed(CommitFacts::PublicationAdmitted {
                        ..
                    })),
                ..
            } = &reply
            else {
                panic!("expected the real committed B admission");
            };
            let committed_effect = *effect;
            assert!(runtime.inflight.contains_key(&committed_effect));
            let (replay, ready) = std::sync::mpsc::sync_channel(1);
            replay.try_send(reply).unwrap();
            let writer = std::mem::replace(&mut runtime.writer.receiver, ready);
            fill_ordinary_inputs(runtime, publisher);
            assert_eq!(runtime.ordinary_room(), 0);
            assert_eq!(runtime.inputs.len(), ORDINARY_INPUTS);
            let progress_started = Instant::now();
            runtime.writer_results().unwrap();
            runtime.writer.receiver = writer;
            assert!(!runtime.inflight.contains_key(&committed_effect));
            assert_eq!(runtime.ordinary_room(), 0);
            assert_eq!(runtime.writer_input_count(), 1);
            until(runtime, |state| {
                state
                    .outgoing_bindings
                    .keys()
                    .any(|(key, _)| key.generation == subscriber)
            });
            // The order and content of the output are what is asserted;
            // how many frames the passes above had already queued is not.
            for _ in 0..ORDINARY_INPUTS {
                assert_eq!(served_frame(runtime, &mut source), (0xd0, vec![]));
            }
            assert_eq!(served_frame(runtime, &mut source), (0x40, vec![0, 7]));
            let (first, body) = served_frame(runtime, &mut receiver_b);
            assert_eq!((first, body[2], body[6]), (0x32, b't', b'v'));
            let b_id = u16::from_be_bytes([body[3], body[4]]);
            let independent_micros = progress_started.elapsed().as_micros();
            assert_eq!(held_acks(runtime), pending);
            let b_ack = dequeue_puback(runtime, subscriber, &mut receiver_b, b_id);
            runtime.queue(b_ack).unwrap();
            until(runtime, |state| {
                state.inputs.is_empty() && state.inflight.is_empty()
            });

            let before = mqttiumd_store_sqlite::SqliteStore::inspect(&sqlite).unwrap();
            assert_eq!((before.obligations.len(), before.outbound.len()), (1, 1));
            assert_eq!(
                before.outbound[&old_target.delivery].claim,
                old_target.claim
            );
            receiver_a.shutdown(std::net::Shutdown::Both).unwrap();
            until(runtime, |state| {
                !state.peers.contains_key(&slow) && state.closing_inputs.is_empty()
            });
            assert_eq!(held.try_iter().count(), net::OUTPUT_SLOTS);
            let offline = mqttiumd_store_sqlite::SqliteStore::inspect(&sqlite).unwrap();
            assert_eq!(offline.outbound, before.outbound);
            assert_eq!(offline.obligations, before.obligations);

            let (resumed, mut receiver) = peer(runtime);
            let mut reconnect = connect_packet(slow_name);
            reconnect.receive_maximum = 1;
            runtime.connect(resumed, reconnect).unwrap();
            until(runtime, |state| state.session_requests.is_empty());
            let (kind, connack) = read_frame(&mut receiver);
            assert_eq!((kind, &connack[..2]), (0x20, &[1, 0][..]));
            assert_eq!(runtime.peers[&resumed].session, Some(session));
            let current = ConnectionKey {
                generation: resumed,
                session,
            };
            until(runtime, |state| {
                state.outgoing_bindings.contains_key(&(current, first_id))
            });
            assert_eq!(
                read_frame(&mut receiver),
                (0x3a, vec![0, 1, b'a', 0, 1, 0, b'v'])
            );
            let new_target = runtime.outgoing_bindings[&(current, first_id)];
            assert_eq!(
                (old_target.delivery, old_target.claim),
                (new_target.delivery, new_target.claim)
            );
            assert_ne!(old_target.frame, new_target.frame);

            let mut next = publish_body(8);
            next[2] = b'a';
            next[6] = b'w';
            runtime.packet(publisher, 0x32, next).unwrap();
            until(runtime, |state| {
                state.inflight.is_empty() && state.inputs.is_empty()
            });
            assert_eq!(read_frame(&mut source), (0x40, vec![0, 8]));
            let waiting = mqttiumd_store_sqlite::SqliteStore::inspect(&sqlite).unwrap();
            assert_eq!((waiting.obligations.len(), waiting.outbound.len()), (2, 1));
            runtime
                .written_complete(old_target.frame, old_connection, old_completion)
                .unwrap();
            runtime
                .queue(Input::FrameResult {
                    effect: old_target.frame,
                    connection: old_connection,
                    progress: FrameProgress::Written {
                        confirmed: old_completion as u64,
                        total: old_completion as u64,
                    },
                })
                .unwrap();
            runtime.packet(resumed, 0xc0, vec![]).unwrap();
            runtime.drain_inputs().unwrap();
            assert_eq!(read_frame(&mut receiver), (0xd0, vec![]));
            assert_eq!(
                runtime.owner.puback_target(current, first_id),
                Some(new_target)
            );
            let after_late = mqttiumd_store_sqlite::SqliteStore::inspect(&sqlite).unwrap();
            assert_eq!(after_late.outbound, waiting.outbound);
            assert_eq!(after_late.obligations, waiting.obligations);

            let genuine = dequeue_puback(runtime, resumed, &mut receiver, 1);
            runtime.queue(genuine).unwrap();
            until(runtime, |state| {
                state
                    .owner
                    .puback_target(current, first_id)
                    .is_some_and(|target| target != new_target)
            });
            assert_eq!(
                read_frame(&mut receiver),
                (0x32, vec![0, 1, b'a', 0, 1, 0, b'w'])
            );
            let final_ack = dequeue_puback(runtime, resumed, &mut receiver, 1);
            runtime.queue(final_ack).unwrap();
            until(runtime, |state| {
                state.inflight.is_empty()
                    && state.inputs.is_empty()
                    && state.owner.puback_target(current, first_id).is_none()
            });
            let final_state = mqttiumd_store_sqlite::SqliteStore::inspect(&sqlite).unwrap();
            assert!(final_state.outbound.is_empty() && final_state.obligations.is_empty());
            eprintln!(
                "progression witness {}",
                json!({
                    "reversed": reverse, "slow_generation": slow.get(), "publisher_generation": publisher.get(),
                    "resumed_generation": resumed.get(), "ordinary_input_count": ORDINARY_INPUTS,
                    "actual_writer_reply": effect_text(committed_effect),
                    "held_frames": pending.iter().map(|(effect, mid)| (effect_text(*effect), *mid)).collect::<Vec<_>>(),
                    "repeated_dispatch_passes": 3, "independent_subscriber_micros": independent_micros,
                    "independent_publisher_puback": 7, "independent_subscriber_packet_id": b_id,
                    "session_present": true, "replay_dup": true, "packet_id": 1,
                    "original_frame": effect_text(old_target.frame), "resumed_frame": effect_text(new_target.frame),
                    "late_written_bytes": old_completion,
                    "outgoing_claims_before_and_after_old_callback": [waiting.outbound.len(), after_late.outbound.len()],
                    "obligations_before_and_after_old_callback": [waiting.obligations.len(), after_late.obligations.len()],
                    "final_claims": final_state.outbound.len(), "final_obligations": final_state.obligations.len(),
                })
            );
        });
    }
}
