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
