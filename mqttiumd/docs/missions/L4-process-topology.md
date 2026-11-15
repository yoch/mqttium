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
| `Connect` | A connection generation, assigned by the supervisor |
| `Bytes` | A connection generation and the bytes a peer sent |
| `FireDeadline`, `ReleaseHeld`, `DeliverHeld`, `LeaveBarrier` | The index of a fault in the list the child was started with |
| `Crash` | Nothing: the simulated crash of the store adapter (sim-store only) |
| `Inspect` | Nothing: the durable state of the store, read without the writer (sim-store only) |
| `Stop` | Nothing |

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

## Failure model

Stated as a limit of S0, not as a property of the product, in this plan
and in `failure_model`:

> Kill of the process at a declared point, after the decisive record is
> acknowledged; never in the middle of a transaction; no power loss, no
> torn write.

| Backend | Crash |
| - | - |
| `sim-store` | No real kill. The crash stays simulated inside the child: the store adapter is dropped and the store reopened from its durable state, as the runner does today, labelled as simulated |
| `sqlite` | Real kill of the child by the supervisor (`SIGKILL`), with lot L7 |

A kill is allowed in two states only: the child waits for an action,
with every record acknowledged and the window empty; or the child stands
at a declared point (a barrier, a fault that holds) after the decisive
record that describes it was acknowledged. A commit is reported as
`StoreCommitStable` only once the SQLite writer has returned it; the
effective `journal_mode` and `synchronous`, read back, go into the
manifest (DEM-011).

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

Nothing opens the data set between the kill and the restart: recovering
the WAL belongs to the restarted child. `RestartBroker` starts a new
child with the next rank; its origins start a new incarnation, it opens
a new epoch, and the supervisor writes `BrokerRecovered`.

## Inspection

The inspector reads without the writer. On SQLite it opens the data set
read-only while the child is alive, reads one snapshot, and holds no read
transaction open beyond it, so that it never blocks the checkpoint of
the child's WAL (lot L7 confirms). Between a kill and the restart an
inspection is refused on SQLite. On `sim-store` the child answers an
inspection action with the durable state of its store, read without the
writer.

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
