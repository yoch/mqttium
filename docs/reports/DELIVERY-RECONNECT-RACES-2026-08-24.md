# Delivery × reconnect lifecycle races — 2026-08-24

| | |
| --- | --- |
| Date | 2026-08-24 |
| Base | `a336f83` (`1.0.0rc9`) |
| Scope | Application delivery, stream generations, reconnect, explicit connect/disconnect, callbacks, manual ACK, byte accounting |
| Method | Deterministic asyncio barriers and in-process brokers; no security fuzzing |
| Tests | `tests/unit/test_delivery_reconnect_races.py` |

This campaign treats the stream-generation contract from #343 and #348 as given.
It composes that contract with slower consumers, callback re-entry, manual ACK,
and explicit connect during an automatic reconnect gap. MQTT broker redelivery
and durable-session restart replay are out of scope except where a local runtime
path duplicated or dropped a message without a broker round-trip.

## Starting contract

- Automatic reconnect keeps the current `messages()` generation. A suspended
  `anext()` must resume on the replacement transport.
- A terminal disconnect ends the generation. A later explicit `connect()`,
  `connect_unix()`, or `connect_ws()` starts a new one. Old iterators stay
  terminal.
- `ApplicationDelivery` owns queues, byte reservations, and the callback worker.
  It does not own transports. `AsyncClient` decides when to `close()`,
  `reopen()`, or `reset_stream()`.
- `MessageDeliveryError` is locally fatal and suppresses automatic reconnect.
  That is a documented delivery-timeout contract, not a silent stream death.

## Adversarial matrix

| Interleaving | Oracle | Result |
| --- | --- | --- |
| Iterator suspended + transient reconnect + PUBLISH immediately after CONNACK | Silent stream death; cross-generation leak | Held. Same iterator receives the post-reconnect message. |
| Iterator suspended + explicit disconnect then connect | Old iterator must not consume the new generation | Held. `StopAsyncIteration` then a fresh `messages()` iterator. |
| `close()` then `reset_stream()` before the waiter is scheduled | Old waiter must not take the new queue | Held. Generation check after wake is sufficient on the asyncio thread. |
| `message_delivery="both"`, callback releases while iterator is still queued | `pending_delivery_bytes` divergence; double release | Held. Shared token stays charged until the iterator also releases. |
| Callback raises during reconnect | Stream death; worker leak | Held. Exception handler records the error; later messages still run. |
| Callback raises `CancelledError` with later jobs already queued | Worker dies; queued jobs stall | **Bug.** Worker treated self-cancellation as task cancellation. |
| Final shutdown with accounted messages and no consumer | Delivery-reference leak | Held as designed. Bytes remain until `_reset_message_stream()` / next explicit connect. An active iterator still drains before seeing `closed`. |
| Final shutdown after disconnect; late broker PUBLISH | Delivery after shutdown | Held. Closed iterator stays terminal. |
| Reader parked on delivery-byte waiters + `disconnect()` | Deadlocked waiters | Held. Cancelling the reader unblocks `reserve_slow`. Abandoned accounted rows still need reset. |
| Manual ACK outstanding, reconnect gap | Incorrect ACK ownership | Held. `ack()` raises `NotConnectedError`; the stream stays open. |
| Manual ACK outstanding, automatic reconnect, clean session | Stale mid reused as ownership | Held. `ack(old)` is `ProtocolError`; a new inbound mid 4 is a new exchange. Local clean reconnect is not MQTT redelivery. |
| Repeated reconnect success/failure with one suspended consumer | Silent termination | Held. |
| Queue full, consumer resumes, transport already closed | Reconnect suppressed by delivery timeout | Held when the consumer drains inside `delivery_timeout`. A stuck consumer still hits `MessageDeliveryError`, which is terminal by contract. |
| Cancel `anext()` during reconnect | Generation dies | Not a local bug. Cancelling `anext()` closes that async generator. The generation survives; `messages()` binds a new iterator. |
| `aclose()` during reconnect | Cross-generation leak | Held after cancelling the in-flight `anext()`. Python forbids `aclose()` on a running generator. |
| `on_disconnect` calls `disconnect()` | Reconnect keeps running; stream stays open | Held. `_intentional_disconnect` stops the retry loop. |
| `on_disconnect` calls `connect()` | Writer already running; reconnect loop tears down the new connection | **Bug.** Explicit connect did not stop the leftover writer or cancel the retry loop, and did not start a new generation. |
| Explicit `connect()` during reconnect sleep | Same as above; old iterator consumes the new connection | **Bug.** Same writer/generation path. |
| `on_connect` calls `disconnect()` | Deadlock / callback-worker leak | **Bug.** `shutdown_callbacks()` joined and cancelled the worker that was running `on_connect`. |
| Both-mode reset while the callback is in flight | Double release of a shared token | Held. Iterator reset drops one reference; the callback drop is ignored once `remaining` hits 0. |

## Interleavings in detail

### Suspended iterator and automatic reconnect

The reader does not `close()` the delivery stream when `_will_reconnect()` is
true. `reopen()` only clears `closed`. The waiter stays on the same
`message_ready` event. A PUBLISH on the replacement transport wakes it. Failed
factory attempts in the same retry loop keep that generation as well.

### Explicit stream reset

`reset_stream()` requires `closed`. `close()` sets the current `message_ready`,
so a waiter already in `wait()` resumes, then sees either `closed` or a
generation mismatch. There is no `await` between the generation check and
`get_nowait()`, so a same-thread race cannot steal from the replacement queue.

### Both-mode accounting

One `_SharedDeliveryReservation` is charged once and released twice. A blocked
iterator does not prevent the callback from finishing. `pending_delivery_bytes`
stays at the logical size until the iterator also consumes. Reset of a closed
stream releases the iterator reference only.

### Callback `CancelledError`

`_callback_worker` re-raised every `CancelledError`. A user callback that raises
it completed `task_done()` for that job, then the worker exited. Jobs already in
the queue had no worker until the next `ensure_callback_worker()` on a later
accept. `join()` waited forever. The AUTH handler already distinguished
self-cancellation from task cancellation; the message worker now does the same.

### Final shutdown and byte accounting

`close()` does not drain iterator accounting: an active iterator is supposed to
yield remaining messages and `release_nowait` each token. With no consumer,
bytes stay until the next explicit connect resets the stream. That is not a
cross-connection leak. `disconnect()` while the reader is in `reserve_slow`
cancels the reader, so the waiter does not deadlock.

### Manual ACK and transport loss

`ProtocolEngine.ack()` requires `CONNECTED`. A gap therefore yields
`NotConnectedError` without dropping the in-memory inbound row. After a clean
automatic reconnect the broker session is absent, inbound state is discarded,
and `ack(old_message)` is `ProtocolError`. A later inbound packet may reuse mid
4; that is a new exchange, not stolen ownership. Restart redelivery of
`delivered=True` rows is a durable-session path (`_recovered_mids`) and was not
treated as a local duplicate of the live generation.

### Queue saturation and reconnect

The reader applies MESSAGE effects inline, so a full iterator queue stalls
`read()`. Peer EOF sits unread until the put completes or `delivery_timeout`
fires. If the consumer drains in time, the reader sees EOF and reconnects. If
not, `MessageDeliveryError` is terminal. That timeout is the documented slow-
consumer bound, not a reconnect policy bug.

### Callback re-entry

`on_connect` / `on_message` / `on_publish` run on the callback worker.
`disconnect()` used to `join()` and cancel that worker, which deadlocked when
the current job was the callback.

`on_disconnect` runs on the reader after `notify_transport_closed()`. An
explicit `connect()` from that callback is a new application connection: it
must stop the writer kept alive for automatic reconnect, start a new stream
generation, and prevent the reader's stale `will_reconnect` snapshot from
launching `_reconnect_loop()`. `disconnect()` must not hold `_lifecycle_lock`
while joining the reader, or that `connect()` waits for the lock the joiner
holds. After the old reader finishes, `_force_close` must not stop a writer
the callback already started.

## Production fixes

1. **Callback worker** (`src/mqttium/api/_delivery.py`). Self-`CancelledError`
   is reported and the loop continues. `shutdown_callbacks()` from inside the
   worker sets `_callback_stop` instead of joining/cancelling itself.
2. **Explicit connect** (`AsyncClient._prepare_explicit_connect`). When not
   already connected, cancel automatic reconnect, `force_close(preserve_reconnect=True)`
   to stop a leftover writer, and `close()` + `reset_stream()` when replacing a
   previous connection or closed stream. First connect of a fresh client still
   does not bump the generation, so a consumer started before `connect()` stays
   valid.
3. **Reader takeover**. After `on_disconnect`, skip callback shutdown and
   reconnect scheduling when the application already disconnected or started a
   replacement reader.
4. **Disconnect join**. Flush the DISCONNECT packet under `_lifecycle_lock`,
   then join the reader outside it. If `_force_close` observes a new
   `_reader_task` after joining the old one, it returns without tearing down
   the replacement.

## Validation

```bash
python -m pytest -q tests/unit
```

1387 tests passed, including `tests/unit/test_delivery_reconnect_races.py` and
the earlier stream-generation and reconnect-iterator files.

## Limitations

- In-process brokers, not Mosquitto. Broker session-present redelivery remains a
  protocol/session property and is covered by inbound replay tests.
- `delivery_timeout` while the reader is parked on a full queue still suppresses
  reconnect. That is the existing MessageDeliveryError contract.
- Two `messages()` iterators on one generation still share one queue. That was
  not in scope.
- No performance claim. These paths are lifecycle, not the publish hot path.
