# Sharded direct I/O ACK and write-receipt routing

`MShardAckRoute` models direct I/O routing (`ShardAckBudget`, `ShardIoRoute`,
`ConnCore::plan_read`, `ConnCore::park_unpaid_ack`,
`ConnCore::push_incoming_batch`, `ShardIoRoute::reserve_write`, and
`ShardIoRoute::written_event` in `crates/mqttiumd/src/net.rs`) for TCP and TLS
connections routed to a delivery shard (`--delivery-shards K >= 1`).

## Questions

- Are ACK credits and write-receipt slots conserved across socket reads,
  `ShardInbox`, worker retry queues, `Written::Started`, and terminal `Written`
  completions? (`BudgetBounded`)
- Can a `DISCONNECT` ever overtake an earlier `PUBACK`/`PUBREC`/`PUBCOMP` that
  was parked for lack of ACK credit? (`DisconnectNeverOvertakesAck`)
- Can a non-ACK packet (such as `PINGREQ`) still progress when `ShardAckBudget`
  is at zero? (`NeverReadNonAckAtZeroCredit` violated in
  `reach-nonack-zero-credit`)
- Once the connection drains, have all packets reached their exact owner and
  returned all credits? (`TerminalConsistent`)

## Configurations

| Configuration | Mutation | Expected |
| - | - | - |
| `safety` | `None`, `AckCap = 1`, `WriteCap = 1`, `MaxWrites = 2` | all safety invariants hold |
| `reach-nonack-zero-credit` | `None` | `NeverReadNonAckAtZeroCredit` violated |
| `mut-no-park` | `NoPark` (continues reading behind unpaid ACK) | `DisconnectNeverOvertakesAck` violated |
| `mut-early-credit` | `EarlyCredit` (frees ACK credit at inbox drain) | `BudgetBounded` violated |
| `mut-double-write-free` | `DoubleWriteFree` (frees `WriteSlot` on `Started`) | `BudgetBounded` violated |

## Running

From `mqttiumd/`:

```bash
python models/check_models.py
python ../tools/formal/run_tlc.py --models-dir models/shard-ack-route
```

## Correspondence

| Model action | Code | Replay |
| - | - | - |
| `PromoteUnpaid` | `ConnCore::plan_read` releasing `StalledState.unpaid_ack` | `a_shard_connection_without_ack_credit_still_reads_what_needs_none` |
| `ReadWithCredit` | `ConnCore::plan_read` + `push_incoming_batch` with `remaining > 0` | `sharded_ack_backpressure_preserves_the_unread_suffix_and_write_receipts` |
| `ReadWithoutCredit` | `ConnCore::plan_read` + `park_unpaid_ack` at `remaining == 0` (TCP & TLS) | `tls_shard_connection_without_ack_credit_still_reads_what_needs_none` |
| `ShardDrainInbox` | `DeliveryShardWorker::drain_io_events` | `sharded_ack_backpressure_preserves_the_unread_suffix_and_write_receipts` |
| `ShardStepAck` | `DeliveryShardWorker::drain_inputs` dropping `ShardAckCredit` | `sharded_ack_backpressure_preserves_the_unread_suffix_and_write_receipts` |
| `AdmitWrite` | `ShardIoRoute::reserve_write` | `shard_socket_close_reports_only_admitted_unfinished_writes_once` |
| `EmitWriteStarted` | `ShardIoRoute::written_event(Written::Started)` | `shard_socket_close_reports_only_admitted_unfinished_writes_once` |
| `EmitWriteTerminal` | `ShardIoRoute::written_event(Written::Complete/Failed)` | `shard_socket_close_reports_only_admitted_unfinished_writes_once` |
| `ShardConsumeStarted` | `DeliveryShardWorker::handle_written(Started)` | `shard_socket_close_reports_only_admitted_unfinished_writes_once` |
| `ShardConsumeTerminal` | `DeliveryShardWorker::handle_written(Complete/Failed)` dropping `WriteSlot` | `shard_socket_close_reports_only_admitted_unfinished_writes_once` |
| `Done` | quiescent terminal state | `sharded_ack_backpressure_preserves_the_unread_suffix_and_write_receipts` |
