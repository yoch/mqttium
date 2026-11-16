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
    let directory = Directory::new();
    let events = Events::open(&directory.0.join("events.jsonl")).unwrap();
    let cancel = Arc::new(AtomicBool::new(false));
    let mut writer = Writer::spawn(
        directory.0.join("store.sqlite"),
        events.clone(),
        Hooks::default(),
        cancel.clone(),
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
            max_routes: nz(1),
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
        owner,
        _authority: authority,
        writer: &mut writer,
        events,
        peers: BTreeMap::new(),
        retiring: Vec::new(),
        sessions: boot.sessions,
        session_requests: BTreeMap::new(),
        next_session_request: 0,
        generation: ConnectionGeneration::new(0),
        inputs: VecDeque::new(),
        timers: BTreeMap::new(),
        inflight: BTreeMap::new(),
        started: Instant::now(),
        #[cfg(feature = "test-hooks")]
        captured: None,
        #[cfg(feature = "test-hooks")]
        hooks: Hooks::default(),
        #[cfg(feature = "test-hooks")]
        deferred_ack: None,
        #[cfg(feature = "test-hooks")]
        deferred_ack_used: false,
        frames: BTreeMap::new(),
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
            io: net::spawn(socket).unwrap(),
            created: Instant::now(),
            last_packet: Instant::now(),
            connected: false,
            pending: false,
            keep_alive: 0,
            max_packet: 268435455,
            ingress: 0,
            receiving: crate::receiving::Window::default(),
            closing: None,
        },
    );
    (generation, client)
}

fn connect_packet(client: &[u8]) -> codec::Connect {
    codec::Connect {
        client_id: SharedBytes::copy_from(client, BOUNDS.max_client_id).unwrap(),
        keep_alive: 10,
        max_packet_size: 65536,
        receive_maximum: 8,
        topic_alias_maximum: 0,
        request_problem_information: true,
    }
}

fn pump(runtime: &mut Runtime<'_>) {
    runtime.writer_results().unwrap();
    runtime.connections().unwrap();
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
        "runtime failed to make bounded progress"
    );
}

fn read_frame(client: &mut TcpStream) -> (u8, Vec<u8>) {
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

        // A valid SUBSCRIBE is outside this product profile. It queues a
        // DISCONNECT while the admitted command's response is still withheld.
        let subscription = vec![0, 9, 0, 0, 1, b't', 2];
        assert_eq!(
            codec::decode(0x82, &subscription, BOUNDS)
                .unwrap_err()
                .kind(),
            codec::ErrorKind::Unsupported
        );
        runtime.packet(generation, 0x82, subscription).unwrap();
        assert!(runtime.peers[&generation].closing.is_some());
        assert_eq!(read_frame(&mut client), (0xe0, vec![0x83, 0]));

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
        runtime.drain_inputs().unwrap();
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
        assert_eq!(after.operations, state.operations);
    });
}

fn unsupported_connect_with_cap(cap: u32) -> Vec<u8> {
    let mut body = vec![
        0, 4, b'M', b'Q', b'T', b'T', 5, 2, 0, 10, 10, 0x11, 255, 255, 255, 255, 0x27,
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
        let net::Incoming::Packet { first, body } = runtime.peers[&generation]
            .io
            .incoming
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
        else {
            panic!("network PUBLISH");
        };
        runtime.packet(generation, first, body).unwrap();
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
                    correlation: None,
                    bytes: vec![0xd0, 0],
                })
                .unwrap();
        }
        let original = std::mem::replace(
            &mut runtime.peers.get_mut(&generation).unwrap().io.outgoing,
            blocked,
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
            1,
            "transport failure does not cancel the admission"
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
    // Valid MQTT 5 SUBSCRIBE, outside this product's documented profile.
    packets.extend_from_slice(&[0x82, 7, 0, 9, 0, 0, 1, b't', 2]);
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
        if known {
            // The cached path must also defer the second TCP packet while
            // ConnectResume remains queued and unapplied.
            while !runtime.inputs.iter().any(|input| {
                matches!(input, Input::Packet { connection, packet: InboundPacket::ConnectResume { .. }, .. }
                    if *connection == generation)
            }) && Instant::now() < deadline
            {
                runtime.connections().unwrap();
                thread::sleep(Duration::from_millis(1));
            }
            assert!(runtime.inputs.iter().any(|input| {
                matches!(input, Input::Packet { connection, packet: InboundPacket::ConnectResume { .. }, .. }
                    if *connection == generation)
            }), "known CONNECT reached the owner input queue");
        } else {
            while runtime.session_requests.is_empty() && Instant::now() < deadline {
                runtime.connections().unwrap();
                thread::sleep(Duration::from_millis(1));
            }
            assert!(
                !runtime.session_requests.is_empty(),
                "fresh CONNECT reached writer"
            );
            while !runtime.session_requests.is_empty() && Instant::now() < deadline {
                runtime.writer_results().unwrap();
                thread::sleep(Duration::from_millis(1));
            }
            assert!(
                runtime.session_requests.is_empty(),
                "session result is correlated"
            );
            assert!(
                runtime.inputs.iter().any(|input| {
                    matches!(input, Input::SessionEstablished { connection: Some(found), .. }
                    if *found == generation)
                }),
                "confirmed creation remains queued before owner application"
            );
        }
        let session = runtime.sessions[client_id.as_slice()].id;
        assert_eq!(runtime.owner.connection_of(session), None);
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
        assert_eq!(read_frame(&mut client), (0xe0, vec![0x83, 0]));
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
