# 0059 — Where the end of a session becomes durable

Status: **Accepted** (25 November 2026, by the integration owner). It states an existing behaviour of
`mqttiumd` (runtime tier, ADR 0044) and changes no code path. It answers the
point of an external audit of `47e1d12` that upstream Mosquitto ticket 3761
raised: a session the client had ended was resumed after the process was
interrupted. [Report 0032](../reports/0032-disconnect-durability-boundary.md)
holds the evidence.

## Context

A client DISCONNECT has no acknowledgement in MQTT. On receiving one the
runtime closes the transport, then queues the end of the session, and the
writer commits it afterwards (`Runtime::close`, `finish_close`). The client
therefore reads the end of its stream before the end of its session is
durable.

Records 0034 and 0029, `docs/product-tcp.md` and `models/will/README.md`
already say that a normal cancellation is authoritative when its
transaction commits. None said what a client may conclude from the end of
its stream, none named the event per storage mode, and no test killed the
executable with clients connected.

## Decision

### The promise

1. **An answer follows its commit.** Unchanged, and stated by records 0045
   and 0055: an answer that acknowledges a stored fact leaves after the
   commit of that fact, in the modes whose store is durable.
2. **The end of a session is durable at its commit, and at nothing
   earlier.** The commit is one transaction on the control lane of the
   writer: the session and its subscriptions are erased or detached, and a
   Will is cancelled or made pending, together.
3. **The end of the stream is not an acknowledgement.** A client that has
   sent DISCONNECT and reads the end of its stream, or close_notify, knows
   that the broker received its packet. It does not know that the end of
   its session survives the loss of the broker process.

### The event, by storage mode

| `--storage-mode` | Readiness string | The end of a session is durable when |
| - | - | - |
| `durable` (default) | `per_commit_fsync_wal_full` | the SQLite `COMMIT` of the lifecycle command returns (WAL, `synchronous=FULL`) |
| `log` | `per_commit_fdatasync_append_log` | the `fdatasync` of the record returns |
| `volatile` with a snapshot interval | `periodic_atomic_snapshot` | the next atomic snapshot is written; until then a restart returns to the previous one |
| `volatile` without one | `none_in_memory_only` | never |

The runtime writes `session_changed` (`action` `Closed` or `Detached`) once
the store has answered: an operator who needs the fact reads that event.
Record 0055 named the guarantee of the log mode `per_commit_fdatasync_wal`
until it was corrected; the string the executable prints is the one of this
table.

### What a loss of the process before the commit does

- A session opened with a non-zero Session Expiry Interval and ended with
  interval 0 is resumed, with its subscriptions, by the next CONNECT of its
  ClientID: Session Present 1.
- A Will that a normal DISCONNECT cancelled is still armed. The restart
  makes it pending and publishes it (record 0034).
- A session whose durable policy is already zero is not concerned: the next
  epoch erases it whatever was committed (record 0029).

After the commit neither happens.

### Considered and not taken: close the transport after the commit

The runtime could keep the socket of a client that said DISCONNECT open
until the end of its session has committed, so that the end of the stream
would mean a durable end. It is not done.

- It serves only a client that waits for the broker to close. Most clients
  close their own socket after DISCONNECT, and MQTT gives the end of the
  stream no meaning.
- Ends of session are one flight at a time and one synchronous commit each.
  Under a mass disconnection the sockets, and their places among the
  connections the broker accepts, would be held for the whole queue.
- It changes the close path of the runtime, where the takeover, the frames
  in flight and the TLS shutdown already meet (record 0058).
- It gives nothing in volatile mode.

The window is measured, not assumed: see the report.

## Consequences

- A client that must know its session ended asks again: a CONNECT with Clean
  Start 0 answers Session Present 0 once the end is durable.
- A deployment for which a Will published after a normal disconnection is
  not acceptable must treat the loss of the broker process as able to
  publish the Wills of the clients that were disconnecting at that instant.
- `crates/mqttiumd/tests/disconnect_durability.rs` pins the boundary on the
  real executable: a kill inside the transaction, a kill after its commit,
  with and without a Will, and the three storage modes.

## Process cuts and their limits

The executable tests in `crates/mqttiumd/tests/disconnect_durability.rs`
and its companion peer exercise the commit boundary. The SQLite
instrumentation can stop inside the COMMIT call at the WAL hook. At that
point the transaction has already committed. The before-commit and
after-return stops distinguish it from the other two observations.

The instrumented log writer exposes stops before append, after writing a
whole frame but before `sync_data`, and after `sync_data` returns.
Restart after a process kill can read a frame already in the kernel page
cache. In volatile mode the recovery evidence identifies the previous
or the next complete atomic snapshot. The peer checks session presence
and the Will independently, including DISCONNECT 0x04. The store hooks
are excluded from ordinary builds.

A power cut, a torn sector and loss of the kernel page cache require a
disposable storage target. [Physical power-loss follow-up](https://github.com/yoch/mqttium/issues/682)
