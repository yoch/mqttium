# Runtime fuzzer finding: terminal EOF leaked the keepalive task

- Date: 2026-08-24
- Baseline: `6e1c7b7`
- Target: initial deterministic `AsyncClient` runtime schedule fuzzer

## Finding

A terminal broker EOF let the reader and writer finish but left the
connection-scoped keepalive task alive. With keepalive disabled, that task
remained parked in its one-second sleep indefinitely. With automatic reconnect,
a replacement connection could overwrite `_keepalive_task`, losing the only
reference to the old task.

The runtime fuzzer found this with the sequence:

```text
0 app.connect
1 checkpoint.wire CONNECT
2 broker.connack
3 checkpoint.connected
4 broker.eof
5 checkpoint.terminal
6 invariant failure: connection-scoped task survived terminal teardown
```

The owner snapshot showed `state=DISCONNECTED`, `reader=false`, `writer=false`,
and `keepalive=true`. This was a liveness/resource ownership violation, not an
expected timeout.

## Minimal reproduction

`test_terminal_broker_eof_stops_connection_keepalive_task` connects the real
`AsyncClient` to the existing packet-aware `_Broker`, captures the live
keepalive task, injects EOF, joins the reader, and requires the captured task to
be done and the client reference cleared. The test failed on the baseline.

## Correction

The reader's existing `finally` teardown now cancels and joins the keepalive
task for its connection epoch immediately after invalidating that epoch. It
clears `_keepalive_task` only if the reference still names the task it joined.
No writer, codec, engine, delivery, or ordinary keepalive hot path changed.

## Validation

```text
test_terminal_broker_eof_stops_connection_keepalive_task: pass
runtime reference campaign, seeds 1..100, 24-step bound: 0 failures
runtime mutation qualification: 4/4 bug classes detected
unit and project suites: 1425 passed
```

The runtime target did not report another production finding in the first 100
reference seeds.
