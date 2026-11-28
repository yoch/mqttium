# Sharded ordered shutdown and panic reporting

`MShardStop` models ordered shutdown (`ShardPool::shutdown`, atomic
`ShardClient::stop`, `DeliveryShardWorker::drain_coord_messages`,
`DeliveryShardWorker::run_loop`) and panic reporting (`catch_unwind` ->
`Notices::publish(ShardToCoord::Fatal)`) for `--delivery-shards K >= 1`.

## Questions

- Can a full `CoordToShard` lane cause a shutdown stop signal to be lost?
  (`StopNeverLost`)
- When a shard exits cleanly, has it drained every message sent before `stop`
  and finished every in-flight store command? (`CleanExitDrainedAll`)
- Does the coordinator stop the store writer strictly after all shards exit?
  (`WriterStopsAfterShard`)
- Is every shard panic published to the coordinator's dedicated fatal slot?
  (`PanicAlwaysReported`)

## Configurations

| Configuration | Mutation | Expected |
| - | - | - |
| `safety` | `None`, `LaneCap = 2`, `MaxSent = 2`, `AllowPanic = TRUE` | all safety invariants hold |
| `mut-lane-stop` | `LaneStop` (sends stop inside bounded lane) | `StopNeverLost` violated |
| `mut-drop-inflight` | `DropInflight` (exits without draining lane/store) | `CleanExitDrainedAll` violated |
| `mut-silent-panic` | `SilentPanic` (panic does not publish `Fatal`) | `PanicAlwaysReported` violated |

## Running

From `mqttiumd/`:

```bash
python models/check_models.py
python ../tools/formal/run_tlc.py --models-dir models/shard-stop
```

## Correspondence

| Model action | Code | Replay |
| - | - | - |
| `CoordSend` | `ShardPool::send_to` | `a_shard_whose_lane_is_full_still_stops` |
| `CoordRequestStop` | `ShardPool::shutdown` (`shard.stop.store(true)`) | `a_shard_whose_lane_is_full_still_stops` |
| `ShardRecvMsg` | `DeliveryShardWorker::drain_coord_messages` | `a_shard_whose_lane_is_full_still_stops` |
| `ShardObserveStop` | `DeliveryShardWorker::drain_coord_messages` (empty lane + atomic `stop`) | `a_shard_whose_lane_is_full_still_stops` |
| `StoreFinishOne` | `DeliveryShardWorker::drain_store_replies` | `a_shard_whose_lane_is_full_still_stops` |
| `ShardExitOk` | `DeliveryShardWorker::run_loop` stopping check | `a_shard_whose_lane_is_full_still_stops` |
| `ShardPanic` | `catch_unwind` -> `Notices::publish(ShardToCoord::Fatal)` | `a_shard_that_panics_is_reported_to_the_coordinator`, `fatal_survives_saturated_close_lane` |
| `CoordStopWriter` | `runtime/start.rs` sending `Request::Stop` after `ShardPool::shutdown` | `a_shard_whose_lane_is_full_still_stops` |
| `Done` | quiescent terminal state | `a_shard_whose_lane_is_full_still_stops` |
