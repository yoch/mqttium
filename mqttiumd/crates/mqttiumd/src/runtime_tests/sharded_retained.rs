use super::*;

fn subscribe(runtime: &mut Runtime<'_>, generation: ConnectionGeneration, id: u8) {
    runtime
        .packet(generation, 0x82, vec![0, id, 0, 0, 1, b't', 0])
        .unwrap();
}

fn subscribe_with_held_output(runtime: &mut Runtime<'_>, generation: ConnectionGeneration, id: u8) {
    runtime.peers[&generation].io.hold_output(true);
    // Only SUBACK fits after these PINGRESPs. Keep the real socket FIFO
    // stopped until the shard has actually attempted its retained replay.
    for _ in 1..net::OUTPUT_SLOTS {
        runtime.packet(generation, 0xc0, Vec::new()).unwrap();
        runtime.drain_inputs().unwrap();
    }
    subscribe(runtime, generation, id);
    let session = runtime.peers[&generation].session.unwrap();
    let index = runtime.shards.as_ref().unwrap().shard_index(session);
    let owner = LogicalOwnerId::new(index as u64 + 1);
    until(runtime, |r| {
        r.peers[&generation].io.held_output_refusal() == Some((0x31, owner))
    });
}

fn live_marker(
    runtime: &mut Runtime<'_>,
    source: ConnectionGeneration,
    publisher: &mut TcpStream,
    id: u8,
    byte: u8,
) -> (u8, Vec<u8>) {
    let mut packet = publish_body(u16::from(id));
    packet[6] = byte;
    runtime.packet(source, 0x32, packet).unwrap();
    assert_eq!(served_frame(runtime, publisher), (0x40, vec![0, id]));
    (0x30, vec![0, 1, b't', 0, byte])
}

/// The store may answer a read of the shard after the commit of a
/// subscription whose SUBACK the coordinator has not queued yet: the read a
/// shard makes when a session attaches, or any later one, travels on a lane
/// of its own. No frame of the shard enters the connection's queue before
/// that SUBACK, as no frame of the single owner does.
#[test]
fn a_shard_frame_does_not_overtake_the_suback_of_its_connection() {
    with_runtime_layout(
        128,
        TestWriter::Sharded(NonZeroUsize::new(2).unwrap()),
        Hooks::default(),
        |runtime, directory| {
            let (source, mut publisher) = connected(runtime, b"fence-source");
            let (reader, mut client) = connected(runtime, b"fence-reader");
            runtime.packet(source, 0x33, publish_body(1)).unwrap();
            assert_eq!(
                served_frame(runtime, &mut publisher),
                (0x40, vec![0, 1, 0x10])
            );
            let session = runtime.peers[&reader].session.unwrap();
            let index = runtime.shards.as_ref().unwrap().shard_index(session);
            let owner = LogicalOwnerId::new(index as u64 + 1);
            runtime.peers[&reader].io.hold_output(true);

            // The coordinator reads the SUBSCRIBE and submits its command,
            // and does not look at the result.
            subscribe(runtime, reader, 1);
            runtime.drain_inputs().unwrap();
            runtime.dispatch().unwrap();
            let committed = || {
                std::fs::read_to_string(directory.join("events.jsonl"))
                    .unwrap()
                    .lines()
                    .any(|line| {
                        line.contains("\"store_result\"") && line.contains("\"Subscriptions\"")
                    })
            };
            let deadline = Instant::now() + Duration::from_secs(5);
            while !committed() {
                assert!(
                    Instant::now() < deadline,
                    "the subscription was not committed"
                );
                thread::sleep(Duration::from_millis(1));
            }

            // The shard reads the session's work after the commit. Its
            // replay is either queued, which is the defect, or held back.
            runtime
                .shards
                .as_mut()
                .unwrap()
                .notify_dirty(session, false, 0);
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let queued = runtime.peers[&reader].io.queued_first_bytes();
                assert!(
                    queued.is_empty(),
                    "a frame of the shard entered the queue before the SUBACK: {queued:x?}"
                );
                if runtime.peers[&reader].io.held_output_refusal() == Some((0x31, owner)) {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "the shard did not try its replay"
                );
                thread::sleep(Duration::from_millis(1));
            }

            // The coordinator queues the SUBACK; the replay follows it.
            until(runtime, |r| {
                r.peers[&reader].io.queued_first_bytes().first() == Some(&0x90)
            });
            runtime.peers[&reader].io.hold_output(false);
            assert_eq!(served_frame(runtime, &mut client), (0x90, vec![0, 1, 0, 0]));
            assert_eq!(
                served_frame(runtime, &mut client),
                (0x31, vec![0, 1, b't', 0, b'v'])
            );
        },
    );
}

#[test]
fn sharded_retained_qos0_waits_for_its_subscriber_and_stays_with_its_generation() {
    with_runtime_layout(
        128,
        TestWriter::Sharded(NonZeroUsize::new(2).unwrap()),
        Hooks::default(),
        |runtime, directory| {
            let (slow, mut held) = connected(runtime, b"retained-slow");
            let (source, mut publisher) = connected(runtime, b"retained-source");
            let (fast, mut witness) = connected(runtime, b"retained-witness");
            let slow_session = runtime.peers[&slow].session.unwrap();
            let shards = runtime.shards.as_ref().unwrap();
            let index =
                |generation| shards.shard_index(runtime.peers[&generation].session.unwrap());
            assert_eq!(
                index(slow),
                index(fast),
                "both readers must share one worker"
            );
            assert_ne!(index(slow), index(source));
            runtime.packet(source, 0x33, publish_body(1)).unwrap();
            assert_eq!(
                served_frame(runtime, &mut publisher),
                (0x40, vec![0, 1, 0x10])
            );
            subscribe_with_held_output(runtime, slow, 1);
            subscribe(runtime, fast, 1);
            let suback = (0x90, vec![0, 1, 0, 0]);
            let retained = (0x31, vec![0, 1, b't', 0, b'v']);
            assert_eq!(served_frame(runtime, &mut witness), suback);
            assert_eq!(served_frame(runtime, &mut witness), retained);
            assert_eq!(
                qos0_drops(directory),
                0,
                "a due replay was discarded on the full socket"
            );

            runtime.peers[&slow].io.hold_output(false);
            for _ in 1..net::OUTPUT_SLOTS {
                assert_eq!(served_frame(runtime, &mut held), (0xd0, vec![]));
            }
            assert_eq!(served_frame(runtime, &mut held), suback);
            assert_eq!(served_frame(runtime, &mut held), retained);
            let marker = live_marker(runtime, source, &mut publisher, 2, b'y');
            assert_eq!(served_frame(runtime, &mut held), marker);
            assert_eq!(served_frame(runtime, &mut witness), marker);

            // Close during a second due replay. The replacement connection
            // resumes the subscription and its first delivery must be live.
            subscribe_with_held_output(runtime, slow, 2);
            held.shutdown(std::net::Shutdown::Both).unwrap();
            until(runtime, |r| {
                !r.peers.contains_key(&slow)
                    && r.closing_inputs.is_empty()
                    && r.lifecycle_inflight.is_empty()
                    && !r.owner.session_status().lifecycle_pending
            });
            let (new, mut resumed) = peer(runtime);
            runtime
                .connect(new, connect_packet(b"retained-slow"))
                .unwrap();
            let (header, body) = served_frame(runtime, &mut resumed);
            assert_eq!((header, &body[..2]), (0x20, &[1, 0][..]));
            let marker = live_marker(runtime, source, &mut publisher, 3, b'z');
            assert_eq!(served_frame(runtime, &mut resumed), marker);
            assert_eq!(served_frame(runtime, &mut witness), marker);
            // The old, consumed replay has one terminal disposition. The
            // successful replay and the witness produce no refusal.
            until(runtime, |_| qos0_drops(directory) == 1);
            let events = std::fs::read_to_string(directory.join("events.jsonl")).unwrap();
            let drops: Vec<serde_json::Value> = events
                .split_inclusive('\n')
                .filter(|line| line.ends_with('\n') && line.contains("\"qos0_dropped\""))
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert_eq!(drops.len(), 1);
            let details = &drops[0]["details"];
            assert_eq!(details["target"], slow_session.get().to_string());
            // The entry the coordinator writes for a QoS 0 drop: its origin
            // named in full.
            assert!(
                details["origin"]
                    .as_str()
                    .is_some_and(|origin| origin.starts_with("consumed/")),
                "{details}"
            );
            assert!(details.get("origin_kind").is_none(), "{details}");
            assert_eq!(details["reason"], "connection_closed");
        },
    );
}
