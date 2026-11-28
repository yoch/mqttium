# Sharded generation closure and detach fence

`MShardDetach` models the closing-connection fence (`ConnCore::closing`, D7)
and generation detach fence (`Runtime::finish_close`,
`DeliveryShardWorker::finish_detaches` with O(1) `terminal_by_conn`, D9, and
`Owner::shard_ack_progress`) for `--delivery-shards K >= 1`.

## Questions

- Can a delivery shard enqueue any frame onto a generation after the
  coordinator marks it closing? (`NoWriteAfterClosing`)
- Does the O(1) `terminal_by_conn` index match the exact number of queued
  terminal inputs for the generation? (`TerminalIndexExact`)
- Can a generation complete detach while I/O ACKs/written receipts, terminal
  inputs, or `FinishDelivery`/`PreparePubrel` commands remain unsettled?
  (`NoDetachWithPendingWork`)
- Can a replacement generation attach before the previous generation completes
  detach? (`NoOverlapWithNextGen`, `TerminalConsistent`)

## Configurations

| Configuration | Mutation | Expected |
| - | - | - |
| `safety` | `None` | all safety invariants hold |
| `mut-write-while-closing` | `WriteWhileClosing` | `NoWriteAfterClosing` violated |
| `mut-ignore-terminal` | `IgnoreTerminal` | `NoDetachWithPendingWork` violated |
| `mut-ignore-finish-inflight` | `IgnoreFinishInFlight` | `NoDetachWithPendingWork` violated |

## Running

From `mqttiumd/`:

```bash
python models/check_models.py
python ../tools/formal/run_tlc.py --models-dir models/shard-detach
```

## Correspondence

| Model action | Code | Replay |
| - | - | - |
| `CoordMarkClosing` | `Peer::set_closing` / `Runtime::close` -> `Connection::mark_closing` | `shard_sink_refuses_writes_to_closing_connection_and_oversize_publish` |
| `IoFinish` | I/O task publishes final shard events and exits | `submitted_ack_precedes_detach_even_when_the_writer_withholds_credit` |
| `CoordSendDetach` | `Runtime::finish_close` -> `ShardPool::detach_session` | `submitted_ack_precedes_detach_even_when_the_writer_withholds_credit` |
| `ShardTryWrite` | `ShardSink::try_submit` + `ConnCore::try_send` closing guard | `shard_sink_refuses_writes_to_closing_connection_and_oversize_publish` |
| `ShardDrainIoAck` | `DeliveryShardWorker::drain_io_events` -> `ObservedPuback` | `submitted_ack_precedes_detach_even_when_the_writer_withholds_credit` |
| `ShardDrainIoWritten` | `DeliveryShardWorker::handle_written` -> `push_terminal_back` | `finish_detaches_tracks_terminal_inputs_per_connection_and_finishes_in_order` |
| `ShardStepTerminal` | `DeliveryShardWorker::drain_inputs` -> `pop_terminal_front` | `finish_detaches_tracks_terminal_inputs_per_connection_and_finishes_in_order` |
| `ShardFinishCommit` | `DeliveryShardWorker::drain_store_replies` + `Owner::step` | `submitted_ack_precedes_detach_even_when_the_writer_withholds_credit` |
| `ShardFinishDetach` | `DeliveryShardWorker::finish_detaches` setting `completed` | `finish_detaches_tracks_terminal_inputs_per_connection_and_finishes_in_order` |
| `CoordAttachGen2` | `ShardPool::attach_session` after peer removal | `submitted_ack_precedes_detach_even_when_the_writer_withholds_credit` |
| `Done` | quiescent state with gen 2 attached | `submitted_ack_precedes_detach_even_when_the_writer_withholds_credit` |
