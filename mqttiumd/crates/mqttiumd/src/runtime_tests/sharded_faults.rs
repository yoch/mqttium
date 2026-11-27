//! What a fault of a delivery shard leaves for the coordinator.
use super::*;
use crate::delivery_shard::ShardTest;
use std::sync::mpsc;

fn sharded(test: impl FnOnce(&mut Runtime<'_>, &Path)) {
    with_runtime_layout(
        128,
        TestWriter::Sharded(NonZeroUsize::new(2).unwrap()),
        Hooks::default(),
        test,
    );
}

/// The coordinator's next notice after `deadline`, or the failure a shard
/// reported.
fn reported_failure(runtime: &mut Runtime<'_>, within: Duration) -> Option<Failure> {
    let deadline = Instant::now() + within;
    let mut notices = Vec::new();
    while Instant::now() < deadline {
        if let Err(failure) = runtime.shards.as_mut().unwrap().drain_notices(&mut notices) {
            return Some(failure);
        }
        thread::sleep(Duration::from_millis(1));
    }
    None
}

/// A shard that panics stops serving its sessions. The coordinator learns
/// it at once, as it learns any fault of a shard, and the broker stops,
/// rather than leaving the sessions of that shard without deliveries until
/// it happens to write to the shard again.
#[test]
fn a_shard_that_panics_is_reported_to_the_coordinator() {
    sharded(|runtime, _| {
        runtime
            .shards
            .as_mut()
            .unwrap()
            .send_for_test(0, ShardTest::Panic);
        let failure = reported_failure(runtime, Duration::from_secs(5))
            .expect("the panic of a shard was never reported");
        assert_eq!(failure.code, "delivery_shard_panicked");
        let shards = runtime.shards.take().unwrap();
        let stopped = shards.shutdown(Duration::from_secs(3));
        assert_eq!(
            stopped.err().map(|failure| failure.code),
            Some("delivery_shard_panicked")
        );
    });
}

/// The stop of a shard is not lost when its lane is full: the shard ends
/// once it has read what was sent before the stop, within the budget.
#[test]
fn a_shard_whose_lane_is_full_still_stops() {
    sharded(|runtime, _| {
        let shards = runtime.shards.as_mut().unwrap();
        let (release, paused) = mpsc::channel();
        shards.send_for_test(0, ShardTest::Pause(paused));
        let mut queued = 0usize;
        while shards.try_send_for_test(0, ShardTest::Nothing) {
            queued += 1;
        }
        assert!(queued > 0, "the lane took nothing");
        let shards = runtime.shards.take().unwrap();
        // The shard reads its lane again once the stop was asked for.
        let releaser = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            let _ = release.send(());
        });
        let stopped = shards.shutdown(Duration::from_secs(3));
        releaser.join().unwrap();
        assert!(
            stopped.is_ok(),
            "a stop asked while the lane was full: {:?}",
            stopped.err().map(|failure| failure.code)
        );
    });
}

/// A shard with nothing to do waits, at most `IDLE_TICK`, without holding
/// the reactors' wake-ups: a hold kept through the wait suppresses the
/// kick of a producer that holds nothing, such as a reactor thread giving
/// back acknowledgement credit, until some holder releases or the reactor's
/// own poll times out. Test builds make a shard panic where it would wait
/// while holding, and a panic is reported as a fault.
#[test]
fn an_idle_shard_waits_without_holding_the_reactors_wake_ups() {
    sharded(|runtime, _| {
        let fault = reported_failure(runtime, Duration::from_millis(200));
        assert!(
            fault.is_none(),
            "an idle shard failed: {:?}",
            fault.map(|failure| failure.code)
        );
    });
}

/// Why `generation` is closing, or was closed: a peer whose transport has
/// finished and whose shard has detached leaves the table at once, and its
/// close is then read from the journal.
fn closing_code(
    runtime: &Runtime<'_>,
    directory: &Path,
    generation: ConnectionGeneration,
) -> Option<String> {
    if let Some(peer) = runtime.peers.get(&generation) {
        return peer.closing().map(|(code, _)| code.to_owned());
    }
    let journal = std::fs::read_to_string(directory.join("events.jsonl")).unwrap();
    journal
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|event| {
            event["kind"] == "connection_closed"
                && event["connection"]
                    .as_str()
                    .and_then(|text| text.parse::<u64>().ok())
                    == Some(generation.get())
        })
        .find_map(|event| event["details"]["code"].as_str().map(str::to_owned))
}

/// A connection whose session revision the coordinator does not know
/// cannot be attached to its shard. It is closed and says why, rather than
/// left open with no shard to deliver to it.
#[test]
fn a_connection_whose_revision_is_unknown_is_closed_at_attach() {
    sharded(|runtime, directory| {
        let (generation, _client) = connected(runtime, b"attach-unknown");
        let session = runtime.peers[&generation].session.unwrap();
        runtime.session_revisions.remove(&session);
        let _ = runtime.attach_ready_shards(&[ConnectionKey {
            session,
            generation,
        }]);
        assert_eq!(
            closing_code(runtime, directory, generation).as_deref(),
            Some("shard_attach_without_revision")
        );
    });
}

/// One generation is attached once. A second attach would keep the first
/// route of the connection and send the shard a second `Attach`; the
/// connection is closed instead.
#[test]
fn a_connection_attached_twice_is_closed() {
    sharded(|runtime, directory| {
        let (generation, _client) = connected(runtime, b"attach-twice");
        let session = runtime.peers[&generation].session.unwrap();
        let _ = runtime.attach_ready_shards(&[ConnectionKey {
            session,
            generation,
        }]);
        assert_eq!(
            closing_code(runtime, directory, generation).as_deref(),
            Some("shard_attached_twice")
        );
    });
}
