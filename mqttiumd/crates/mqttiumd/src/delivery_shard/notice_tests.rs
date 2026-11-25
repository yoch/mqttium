use super::*;

/// Produce all notices without running the coordinator until the final drain.
fn while_coordinator_is_busy(generations: &[u64], notices: Vec<ShardToCoord>) -> Vec<ShardToCoord> {
    let (tx, rx) = mpsc::sync_channel(generations.len());
    let wake = Wake::new();
    for notice in notices {
        publish_notice(&tx, &wake, notice);
    }
    assert!(wake.take());
    rx.try_iter().collect()
}

fn close(generation: u64, code: &'static str) -> ShardToCoord {
    ShardToCoord::CloseConnection {
        generation: ConnectionGeneration::new(generation),
        code,
        disconnect_reason: Some(0x82),
    }
}

#[test]
fn busy_coordinator_receives_first_close_of_each_live_generation() {
    let delivered = while_coordinator_is_busy(
        &[1, 2],
        vec![close(1, "first"), close(1, "duplicate"), close(2, "other")],
    );
    let closes: Vec<_> = delivered
        .iter()
        .filter_map(|notice| match notice {
            ShardToCoord::CloseConnection {
                generation,
                code,
                disconnect_reason,
            } => Some((generation.get(), *code, *disconnect_reason)),
            _ => None,
        })
        .collect();
    assert_eq!(closes, [(1, "first", Some(0x82)), (2, "other", Some(0x82))]);
}

#[test]
fn fatal_survives_saturated_close_lane() {
    let delivered = while_coordinator_is_busy(
        &[1],
        vec![
            close(1, "first"),
            ShardToCoord::Fatal(Failure::plain("shard_core_fault")),
        ],
    );
    assert!(delivered.iter().any(|notice| matches!(notice, ShardToCoord::Fatal(failure) if failure.code == "shard_core_fault")));
    assert!(delivered.iter().any(|notice| matches!(notice, ShardToCoord::CloseConnection { generation, .. } if generation.get() == 1)));
}
