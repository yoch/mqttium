use super::*;

/// Produce all notices without running the coordinator until the final drain.
fn while_coordinator_is_busy(generations: &[u64], notices: Vec<ShardToCoord>) -> Vec<ShardToCoord> {
    let wake = Wake::new();
    let inbox = Notices::new(wake.clone());
    for &generation in generations {
        inbox.attach(ConnectionGeneration::new(generation));
    }
    for notice in notices {
        inbox.publish(notice);
    }
    assert!(wake.take());
    let mut delivered = Vec::new();
    if let Some(failure) = inbox.drain_into(&mut delivered) {
        delivered.push(ShardToCoord::Fatal(failure));
    }
    delivered
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

#[test]
fn stale_close_cannot_reach_a_reconnected_session() {
    let inbox = Notices::new(Wake::new());
    let first = ConnectionGeneration::new(3);
    let next = ConnectionGeneration::new(4);
    inbox.attach(first);
    inbox.publish(close(3, "already_closed"));
    inbox.detach(first);
    inbox.attach(next);
    inbox.publish(close(3, "late"));
    inbox.publish(close(4, "current"));
    let mut delivered = Vec::new();
    assert!(inbox.drain_into(&mut delivered).is_none());
    assert!(
        matches!(delivered.as_slice(), [ShardToCoord::CloseConnection { generation, code: "current", .. }] if *generation == next)
    );
    delivered.clear();
    inbox.publish(close(4, "duplicate_after_drain"));
    assert!(inbox.drain_into(&mut delivered).is_none());
    assert!(delivered.is_empty());
}
