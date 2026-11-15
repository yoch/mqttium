# Process topology of the runner (lot L4)

Realisation plan of accepted record 0002; changes nothing in it. Lot L7
reviews the SQLite part. No line of the control protocol or of a kill is
written before this plan is read.

## Processes

| Process | Holds | Origins it writes |
| - | - | - |
| Supervisor: `mqttiumd-lab run` | The scheduler (seed, choice log), the peers, the collector (the one writer of `trace.jsonl`), the inspector, the kill and the restart | `bench-supervisor`, `peer` |
| Child: `mqttiumd-lab child` | The core, the store adapter with the injection layer, the timer adapter, the transport | `broker-core`, `store-adapter`, `store-evidence`, `fault-injector`, `dispatcher`, `timer`, `transport` |

The child is the same binary as the supervisor; the supervisor checks
its digest before it starts it. The child never reads the scenario. It
receives the path of the data set the fixture installed, the seed, the
rank of its start-up and the faults already translated to the terms of
the store adapter (point, action, command kind, source as a session and
a Packet Identifier, occurrence): no binding, no identity.

## Channels

Actions, supervisor to child: one pipe, frames of at most 64 KiB with a
big-endian `u32` length, one action outstanding at a time.

| Action | Carries |
| - | - |
| `Start` | The rank of the start-up |
| `Choose` | An index in the list of eligible choices the child published last, and the digest of that list. No identity goes down |
| `Connect` | A connection generation, assigned by the supervisor, and the client |
| `Bytes` | A connection generation, the bytes a peer sent, and the incarnation and sequence of the `PeerPacketSent` record that states them |
| `Recover` | Nothing: the recovery of the process just started |
| `Settle` | Nothing: the adapters move what waits in the box |
| `FireDeadline` | Nothing: the armed deadline due first |
| `ReleaseHeld`, `DeliverHeld`, `LeaveBarrier` | The index of a fault in the list the child was started with |
| `Crash` | Nothing: the simulated crash of the store adapter (sim-store only) |
| `Inspect` | Nothing: the durable state of the store, read without the writer (sim-store only) |
| `Stop` | Nothing |
| `Acknowledge` | The emission number up to which, included, the supervisor read every evidence frame |

Evidence, child to supervisor: one file descriptor per origin of the
child, and one control descriptor (eligible choices, end of action,
refusal of an action, the stop of a run). The origin of a record is the
descriptor it arrives on; a record that claims another is refused, as
the collector does today.

Order of the evidence. The child puts in the frame header, never in the
record, an emission number that increases without gap within one child
incarnation. The supervisor reads every descriptor until the end of the
action, orders the batch by that number, and refuses a gap, a duplicate
or a number already seen. The number is a claim of the child about the
order of its own emissions. It is bounded by the local order of each
origin, which the collector checks, and by the causal parents, which the
oracle checks; the origin of a record is never taken from it.

Window: at most 64 records not yet acknowledged. A full window makes
the child wait, reading acknowledgements. While an action is
outstanding the supervisor keeps reading and acknowledging, so neither
side blocks on a write the other is waiting to read.

Decisive records: a record of store truth (every `store-evidence`
record, present or future) or a `FaultHit`. The child goes past the
point a decisive record describes only once that record is
acknowledged.

The frame size and the window are working values; the run manifest
records them as effective parameters.

## Wire format

Written and tested in `crates/mqttiumd-lab/src/bench/protocol.rs` before
any child exists; nothing there starts or stops a process.

- A frame is a big-endian `u32` length and its body, of 1 to 65 536
  bytes. A length is checked from the header alone, before anything is
  allocated; an empty frame, a longer one or a stream that ends inside a
  frame is refused.
- An action or a report is a tag byte and its fields, integers
  big-endian, byte strings and texts with a `u32` length first, the
  choice digest as 32 bytes. One encoding, no optional field: an unknown
  tag, a field cut short, a text that is not UTF-8 or a byte left over is
  refused. A list of choices whose count the frame cannot hold is refused
  before it is allocated.
- An evidence frame is the emission number (`u64`) and the record as
  the child wrote it, never empty. Its origin is the descriptor.
- Reports on the control descriptor: `Eligible` (the descriptions of the
  choices and their digest), `EndOfAction` (the last emission number of
  the action, or none), `Refused` (the action did nothing), `Stopped`
  (the run cannot go on in the child), `Started` (the epoch, the
  elementary choices of the start-up, the settings the engine read
  back), `State` (whether an effect waits, whether the adapter stands at
  a barrier, the deadlines armed, the hits and the items each scenario
  fault holds), `Written` (the bytes the transport wrote to a client and
  the record that states it), `Crashed` (the clients whose connection was
  open) and `Finished` (the events no record carries).
- The choice digest is the SHA-256 of the number of choices, then of
  each description with its length first, in list order. The child
  refuses a `Choose` whose digest is not that of the list it published
  last, or whose index is outside it.
- At the end of an action the supervisor orders the frames of every
  descriptor by emission number and refuses the batch on a duplicate, a
  number of an earlier action of the incarnation, a missing number, or a
  last number other than the one `EndOfAction` states. A new incarnation
  numbers from 0.
- The child emits at most 64 frames not acknowledged, goes past a
  decisive record only once it is acknowledged, and is killable only
  when every frame is acknowledged. An acknowledgement of a frame not
  emitted, or older than one already received, is refused.

The descriptors of the child: actions on its standard input, reports on
its standard output, and one Unix socket per origin of the child
(`broker-core`, `dispatcher`, `fault-injector`, `store-adapter`,
`store-evidence`, `timer`, `transport`), which the supervisor binds in a
directory it creates with mode 0700 and accepts one connection on each;
standard error is diagnostics, never evidence. The lab forbids unsafe
code, so a descriptor cannot be placed at a number in the child: the
socket the supervisor accepted is the channel, and the origin of a
record is the socket it arrived on. The supervisor reads each descriptor
on its own reader, so that a full descriptor never waits for another to
be read (`tests/bench_protocol.rs` plays 200 records through a window of
64 on socket pairs, with no process).

Implemented in `crates/mqttiumd-lab/src/bench/child.rs`
(`run --backend sqlite --process child`; the default stays in-process).
The child relays each record its own collector wrote; the collector of
the supervisor takes it with `accept`, checks the origin against the
socket and the sequence against the origin, and numbers it in the
trace. At the end of each action the child waits until every frame is
acknowledged before it reads the next action, so that between two
actions it is drained. Every read of the supervisor has a deadline (30
seconds): a child that writes nothing stops the run. The settings the
child's engine read back are compared by the supervisor to those the
manifest records. The child computes its settings and its faults from
the campaign directory, the sessions and the faults still armed that the
supervisor passes on its command line. A crash is still simulated in this
step: the child drops the adapter and the connection of the writer,
reports, and exits on its own; the supervisor waits for its exit with a
status of success before it starts the next child on the same file.

## Causal parents of the supervisor and the peers

> The supervisor names what it had read when it acted: the last record
> of each origin of the process under test and the last record of the
> peer; a peer names the last supervisor record it acted on and what it
> observed on its connection; neither names a record it had not read.

One record per origin of the child, not the last record of the child:
the order of arrival between two origins is not a causality. A peer sees
bytes only; it never names an internal record of the child, and what it
observed it names through its own `PeerPacketObserved`. The header and
the footer name nothing. The collector adds these parents; the in-process
runner writes them today, the child protocol will keep them.

## Failure model

Stated as a limit of S0, not as a property of the product, in this plan
and in `failure_model`:

> Kill of the process at a declared point, after the decisive record is
> acknowledged. The kernel and its page cache survive; nothing is shown
> about fsync, power loss or torn writes.

A kill of the process does not exercise `fsync`: what the process wrote
is in the page cache, which survives it. "Never inside a transaction" is
a restriction of the bench (a kill is allowed only in the states below),
not a property of the store.

| Backend | Crash |
| - | - |
| `sim-store` | No real kill. The crash stays simulated inside the child: the store adapter is dropped and the store reopened from its durable state, as the runner does today, labelled as simulated |
| `sqlite` | Real kill of the child by the supervisor (`SIGKILL`), with lot L7 |

A kill is allowed in two states only: the child waits for an action,
with every record acknowledged and the window empty; or the child stands
at a declared point (a barrier, a fault that holds) after the decisive
record that describes it was acknowledged. No inspector is open at a
kill. A commit is reported as `StoreCommitStable` only after a `COMMIT`
that succeeded; an error of `COMMIT` is an unknown outcome, never a
certain rejection. The effective `journal_mode` and `synchronous`, read
back, go into the manifest (DEM-011).

## Crash and restart

`CrashBroker`: the supervisor checks that the child is in an allowed
state, kills it, waits for its exit, and writes `BrokerCrashed` and a
`PeerConnectionClosed` per open connection. It then reads every
descriptor of the child to its end. A record that arrives after an
accepted kill is refused: it can only describe what was not allowed.
The supervisor counts these records and writes itself a record that
states their number and their origins, so that the refusal is in the
trace and not only in a counter; the footer then declares the trace
incomplete.

Between the kill and the restart nothing opens, copies, moves or deletes
the database or its `-wal` and `-shm` files: recovering the WAL belongs
to the restarted child. A proof drawn from a copy of the database
without its `-wal` is false.

`RestartBroker` starts a new child with the next rank. On SQLite the
child, in this order: reads `user_version` before any write; sets
`journal_mode` and `synchronous`, reads them back, and refuses to go on
when they differ; reads back the engine version and the options, which
the supervisor compares to the manifest; runs `PRAGMA quick_check`;
decodes the meta row. The bench then starts the core: the incarnation is
the one of the fixture, the recovery pages are read, and the new epoch
opens (`EpochOpening`). One writer only: `busy_timeout = 0` and
`BEGIN IMMEDIATE`, so that a second writer is refused instead of
waiting. Its origins start a new incarnation, and the supervisor writes
`BrokerRecovered`.

## Inspection

The inspector reads without the writer. On SQLite it uses
`SqliteStore::inspect(path) -> Result<DurableState, OpenError>` of lot L7: the database
opened with `mode=ro`, never `immutable=1`; one read transaction; the
connection closed at once; never a checkpoint. No inspector is open at a
kill, and between a kill and the restart an inspection is refused on
SQLite. The in-process runner already reads this way, and states it in
`backend_settings` of `campaign.json`. On `sim-store` the child answers
an inspection action with the durable state of its store, read without
the writer.

## Determinism

The scheduler stays in the supervisor. One action is one elementary
choice; `max_steps` counts them on both backends. The choice log holds,
per action, the index chosen and the digest of the list it was chosen
from. The same seed gives the same trace bytes; no process identifier
enters a record.

## The footer

`complete` is true only when the collector refused nothing, every child
ended with `Stop` or was killed in an allowed state, and nothing arrived
after an accepted kill.

## Order of work

1. The protocol and the child on `sim-store`: `Stop` and the simulated
   crash only. The six campaigns give the same verdicts; the traces are
   compared to the in-process runner, record by record.
2. The real kill on SQLite, with lot L7.
