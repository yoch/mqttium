//! The inline-claim lease of a sharded session, against the runtime: what
//! the subscriber receives, and whether the coordinator may claim inline
//! again. Model: `models/inline-lease`.
use super::*;

fn sharded(hooks: Hooks, test: impl FnOnce(&mut Runtime<'_>, &Path)) {
    with_runtime_layout(
        128,
        TestWriter::Sharded(NonZeroUsize::new(2).unwrap()),
        hooks,
        test,
    );
}

/// Serves the runtime until the lease of `session` lets the coordinator
/// reserve, or `within` has passed; tells which.
fn eligible_within(runtime: &mut Runtime<'_>, session: SessionId, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    loop {
        if runtime.shards.as_ref().unwrap().lease_is_eligible(session) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        pump(runtime);
        thread::sleep(Duration::from_millis(1));
    }
}

/// A subscriber of `t` at QoS 1, whose lease the coordinator may reserve
/// from, and a publisher.
fn subscribed_pair(
    runtime: &mut Runtime<'_>,
) -> (SessionId, TcpStream, ConnectionGeneration, TcpStream) {
    let (subscriber, mut subscribed) = connected(runtime, b"lease-subscriber");
    let (publisher, publishing) = connected(runtime, b"lease-publisher");
    runtime
        .packet(subscriber, 0x82, vec![0, 1, 0, 0, 1, b't', 1])
        .unwrap();
    assert_eq!(
        served_frame(runtime, &mut subscribed),
        (0x90, vec![0, 1, 0, 1])
    );
    let session = runtime.peers[&subscriber].session.unwrap();
    assert!(
        eligible_within(runtime, session, Duration::from_secs(5)),
        "the shard never published the lease of the subscriber"
    );
    (session, subscribed, publisher, publishing)
}

/// A PUBLISH of `t` at QoS 1 with one byte of payload.
fn publication(packet_id: u8, payload: u8) -> Vec<u8> {
    vec![0, 1, b't', 0, packet_id, 0, payload]
}

/// What the subscriber reads, as (first byte, payload byte), each PUBLISH
/// acknowledged, while `serve` turns the runtime; until `done` holds of
/// the frames read, the connection ends, or `within` has passed.
fn read_acknowledging(
    runtime: &mut Runtime<'_>,
    subscribed: &mut TcpStream,
    within: Duration,
    serve: fn(&mut Runtime<'_>),
    done: impl Fn(&[(u8, u8)]) -> bool,
) -> Vec<(u8, u8)> {
    let deadline = Instant::now() + within;
    let mut read = Vec::new();
    let mut first = [0u8; 1];
    while !done(&read) && Instant::now() < deadline {
        serve(runtime);
        subscribed
            .set_read_timeout(Some(Duration::from_millis(2)))
            .unwrap();
        match subscribed.peek(&mut first) {
            Ok(0) => break,
            Ok(_) => {
                subscribed
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                let (kind, body) = read_frame(subscribed);
                read.push((kind, *body.last().unwrap_or(&0)));
                if kind & 0xf0 == 0x30 {
                    subscribed.write_all(&[0x40, 2, body[3], body[4]]).unwrap();
                }
            }
            Err(_) => {}
        }
    }
    read
}

#[cfg(feature = "test-hooks")]
fn turn(runtime: &mut Runtime<'_>) {
    runtime.turn().unwrap();
}

#[cfg(feature = "test-hooks")]
fn resolved(directory: &Path) -> bool {
    std::fs::read_to_string(directory.join("events.jsonl"))
        .unwrap()
        .lines()
        .any(|line| line.contains("\"store_resolved\""))
}

/// The payloads of the PUBLISH packets read.
fn payloads(read: &[(u8, u8)]) -> Vec<u8> {
    read.iter()
        .filter(|(kind, _)| kind & 0xf0 == 0x30)
        .map(|&(_, payload)| payload)
        .collect()
}

/// A publication admitted without an inline claim leaves the lease
/// eligible again once its delivery is done: the step of its commit marks
/// the lease dirty after the notification sent when the store answered,
/// and that mark must reach the shard. Here Receive Maximum 8 gives the
/// lease room for 16 claims, and the 17th publication is stored.
#[test]
fn a_lease_is_eligible_again_after_a_publication_admitted_without_claim() {
    sharded(Hooks::default(), |runtime, _| {
        let (session, mut subscribed, publisher, mut publishing) = subscribed_pair(runtime);
        for id in 1..=17u8 {
            runtime
                .packet(publisher, 0x32, publication(id, id))
                .unwrap();
            assert_eq!(served_frame(runtime, &mut publishing), (0x40, vec![0, id]));
        }
        let read = read_acknowledging(
            runtime,
            &mut subscribed,
            Duration::from_secs(5),
            pump,
            |read| read.len() == 17,
        );
        assert_eq!(payloads(&read), (1..=17).collect::<Vec<_>>());
        assert!(
            eligible_within(runtime, session, Duration::from_secs(2)),
            "the lease stays ineligible once a publication was admitted without a claim"
        );
    });
}

/// A publication stored for the subscriber, whose admission answer is
/// unknown, is delivered once the resolution commits it. The resolution
/// names no target: the coordinator notifies the shard after the step.
#[cfg(feature = "test-hooks")]
#[test]
fn a_stored_publication_committed_by_a_resolution_is_delivered() {
    let controls = Directory::new();
    let hold = controls.0.join("hold-admission-result");
    let hooks = Hooks {
        directory: Some(controls.0.clone()),
        pause: vec![],
    };
    sharded(hooks, |runtime, directory| {
        let (_, mut subscribed, publisher, _publishing) = subscribed_pair(runtime);
        std::fs::write(&hold, []).unwrap();
        // A retained publication is always stored for its subscribers.
        runtime
            .packet(publisher, 0x33, publication(7, b'R'))
            .unwrap();
        let read = read_acknowledging(
            runtime,
            &mut subscribed,
            Duration::from_millis(400),
            pump,
            |read| !read.is_empty(),
        );
        assert!(
            read.is_empty(),
            "delivered before its admission was known: {read:?}"
        );
        std::fs::remove_file(&hold).unwrap();
        let read = read_acknowledging(
            runtime,
            &mut subscribed,
            Duration::from_secs(25),
            turn,
            |read| !read.is_empty(),
        );
        assert!(resolved(directory), "the admission was never resolved");
        assert_eq!(
            payloads(&read),
            b"R",
            "the publication committed by the resolution was not delivered"
        );
    });
}

/// The subscriber receives the publications of one publisher in the order
/// of their admission, also when the answer to the first is unknown while
/// the second, claimed inline, commits.
#[cfg(feature = "test-hooks")]
#[test]
fn an_inline_claim_does_not_overtake_one_whose_outcome_is_unknown() {
    let controls = Directory::new();
    let hold = controls.0.join("hold-admission-result");
    let hooks = Hooks {
        directory: Some(controls.0.clone()),
        pause: vec![],
    };
    sharded(hooks, |runtime, directory| {
        let (_, mut subscribed, publisher, _publishing) = subscribed_pair(runtime);
        std::fs::write(&hold, []).unwrap();
        runtime
            .packet(publisher, 0x32, publication(7, b'A'))
            .unwrap();
        runtime
            .packet(publisher, 0x32, publication(8, b'B'))
            .unwrap();
        let mut read = read_acknowledging(
            runtime,
            &mut subscribed,
            Duration::from_millis(400),
            pump,
            |read| read.len() == 2,
        );
        std::fs::remove_file(&hold).unwrap();
        read.extend(read_acknowledging(
            runtime,
            &mut subscribed,
            Duration::from_secs(25),
            turn,
            |later| read.len() + later.len() == 2,
        ));
        assert!(resolved(directory), "the admission was never resolved");
        assert_eq!(payloads(&read), b"AB", "the order of admission");
    });
}

/// A publication is sent once on a connection. While the answer to an
/// inline claim is unknown, a stored publication to the same subscriber
/// asks the shard to read the session: the read must not take the claim's
/// committed row, which the subscriber would acknowledge before the claim
/// arrives at the resolution and is inserted again.
#[cfg(feature = "test-hooks")]
#[test]
fn a_read_of_the_store_does_not_deliver_a_claim_the_coordinator_still_owes() {
    let controls = Directory::new();
    let hold = controls.0.join("hold-admission-result");
    let hooks = Hooks {
        directory: Some(controls.0.clone()),
        pause: vec![],
    };
    sharded(hooks, |runtime, directory| {
        let (_, mut subscribed, publisher, _publishing) = subscribed_pair(runtime);
        std::fs::write(&hold, []).unwrap();
        runtime
            .packet(publisher, 0x32, publication(7, b'A'))
            .unwrap();
        runtime
            .packet(publisher, 0x33, publication(8, b'B'))
            .unwrap();
        let mut read = read_acknowledging(
            runtime,
            &mut subscribed,
            Duration::from_millis(400),
            pump,
            |_| false,
        );
        std::fs::remove_file(&hold).unwrap();
        let deadline = Instant::now() + Duration::from_secs(25);
        while !resolved(directory) && Instant::now() < deadline {
            read.extend(read_acknowledging(
                runtime,
                &mut subscribed,
                Duration::from_millis(100),
                turn,
                |_| false,
            ));
        }
        assert!(resolved(directory), "the admission was never resolved");
        read.extend(read_acknowledging(
            runtime,
            &mut subscribed,
            Duration::from_secs(1),
            turn,
            |_| false,
        ));
        assert_eq!(
            payloads(&read),
            b"AB",
            "each publication once, in order: {read:x?}"
        );
        assert!(
            read.iter().all(|(kind, _)| kind & 0xf0 == 0x30),
            "the subscriber was sent something else: {read:x?}"
        );
        assert!(
            read.iter().all(|(kind, _)| kind & 0x08 == 0),
            "a first delivery marked as a repeat: {read:x?}"
        );
    });
}
