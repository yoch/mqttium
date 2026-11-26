//! What a fault of a delivery shard leaves for the coordinator.
use super::*;
use crate::delivery_shard::CoordToShard;

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
            .send_for_test(0, CoordToShard::Panic);
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
