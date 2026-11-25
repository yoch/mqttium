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
            assert_eq!(details["origin_kind"], "consumed");
            assert_eq!(details["reason"], "connection_closed");
        },
    );
}
