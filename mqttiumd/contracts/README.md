# Contracts, revision 1

Revision 1 is strict and provisional: unknown members, formats and versions
are errors today, and the formats may still change by decision record until
the S0 reception (ADR 0004).

A provisional revision has no stable name, so it is identified by exact
content. The contract digest is the SHA-256 of one `<sha256>  <path>` line
per JSON file of `v1/` and `schema/v1/`, in path order. `mqttiumd-lab
validate` prints it, a manifest records it, and a qualifying result names
it. The examples illustrate the contract and are not part of the digest.

## Layout

| Path | Content |
| - | - |
| `schema/v1/*.schema.json` | JSON Schema 2020-12. No `$id`; references are file-relative and never fetched |
| `schema/v1/store-snapshot.schema.json` | The durable state of a store at one checkpoint, as `InspectDurableState` writes it |
| `v1/fault-points.json` | Contractual injection points, what each carries (a command or a result) and the actions it offers |
| `v1/profiles.json` | Profiles and their perimeter |
| `v1/assertions.json` | Assertion names, their meaning and the requirements they serve |
| `v1/requirements.json` | Every identifier the specification defines, each with its source: a chapter or an accepted decision record |
| `v1/p1.catalog.json` | Vocabulary of the transition registry: machines, state classes, event classes, guard dimensions, effects |
| `v1/p1.transitions.json` | Transition registry of P1. Status `complete`: every guard point of every cell has exactly one rule |
| `examples/v1/index.json` | Every example, and for each invalid one the code and pointer it must be refused with |

## Rules every v1 document follows

- It declares `format` and `schema_version`. Objects are closed; tagged
  unions carry a constant discriminator.
- A 64-bit quantity is a canonical decimal string: no sign, no leading zero,
  no whitespace, no line break, at most `18446744073709551615`.
- A number literal is a small integer. `7.0`, `7e0`, `-0` and integers
  beyond 64 bits are refused while parsing; zero is written `0`.
- A Packet Identifier is an integer from 1 to 65535.
- Duplicate keys are refused while parsing.
- Paths are relative to the `mqttiumd/` root and stay below it. Symbolic
  links are followed; the resolved file must be a regular file below the
  resolved root. A document is at most 16 MiB. Evidence cited by digest,
  such as a trace, has no size bound: it is hashed as a stream.
- A document refers to another only as the table of references allows
  (scenario to fixture, registry to catalogue, qualifying result to scenario
  and manifest). The format of the referenced document is checked before it
  is followed; the table is acyclic.
- Payloads are lower-case hexadecimal bytes. Nothing is Unicode-normalised.

## Validation

Validation runs in a fixed order and stops at the first stage that refuses:
strict parsing, known format and version, JSON Schema, semantic pass. The
semantic pass checks numeric range, references to catalogues and files,
binding definitions, the causal order of steps, verdicts, cited evidence,
and the coverage of a transition registry. A report keeps at most 64
diagnostics and 64 notes and counts the others exactly; a truncated report
still refuses.

### Held commands, held results and deadlines

Three steps let a scenario order a race without a crash (ADR 0001):

| Step | What the bench does |
| - | - |
| `FireDeadline` | Drives the broker, within `max_steps`, until no effect waits in an effect box; then lets the timer adapter deliver the armed deadline that is due first, and drives until the core has consumed it. Two deadlines due at the same logical time fire in the order they were armed. If no deadline is armed the step fails and the run is inconclusive: it is never a no-op. Logical time advances in no other way: a deadline never fires by itself between two steps |
| `ReleaseHeldCommand` | Lets the command that the named fault holds enter the writer |
| `DeliverHeldResult` | Hands the result that the named fault delays to the core, and drives, within `max_steps`, until the core has consumed it |

**What a fault holds.** The action `hold_command` holds a command and
`delay_result` holds a result. Such a fault holds one item in the life of a
scenario:

- it holds it once a `DriveUntil` step has waited for its hit, and a
  scenario waits for it once, so its `expected_hits` is at most 1;
- what is held is handed over at most once, by the step of its kind;
- a crash of the broker destroys it, and the fault holds nothing again. A
  fault that no step waited for before a crash may still be reached, and
  waited for, after the restart;
- a held command or result that no step hands over stays held to the end
  of the run (QABS-C keeps its command held).

A scenario does not wait for a fault whose `expected_hits` is 0.

**What a point carries.** Each injection point `carries` a command or a
result, and offers only actions that act on it: `hold_command` on a
command; `drop_result`, `delay_result` and `duplicate_result` on a result;
`refuse_fence` on the fence of a resolution, which is a command of the
writer; `barrier` on either. On a point that carries a result, `match.result_of`
selects the result of a `Persist` or of a `ResolveAndFence` about a
matching operation; without it, every result about a matching operation
counts, in the order they become ready.

**Where a fence waits.** A resolution enters the writer and reads what is
established for its operation. A receipt, a fence or a retired identity
answers it. An absence brings it to the point `store.fence.transit`, the
`occurrence` being counted over the resolutions of the operation that read
an absence. There:

- `hold_command` sets the fence aside until `ReleaseHeldCommand`. The
  read of the absence is not a decision: the fence is written only if the
  operation is still absent when the held fence enters the writer. If the
  command committed meanwhile (QABS-A) the resolution answers `Committed`
  and writes no fence; otherwise it fences (QABS-B), and the command
  released afterwards is refused without effect;
- `refuse_fence` makes the write of this one fence impossible: the
  resolution answers `PendingOrUnknown` (QABS-C), nothing else changes and
  the store stays available. The trace records it as `StoreFenceRefused`,
  so that a refused fence is never inferred from a missing
  `StoreFenceStable`.

A held command does not by itself prevent a fence: a store fences whenever
it can write the fence.

**What selects a resolution.** A store that reads an absence does not
know the command of the operation: neither its kind nor its publication.
At `store.fence.transit`, `match.command_kind` and
`match.publication_binding` name the resolved operation by its command as
the bench knows it, from the fault that selected that command earlier in
the scenario (`store.command.transit` in QABS-A, B and C). The runner of
the bench maps the selection to the operation; the store is handed only
the identity. No semantic rule yet refuses a scenario whose fence fault
names a command no earlier fault selected.

**One fault per item.** Two faults of a scenario never select the same
command or result. Two selections on one point, for one command kind, with
compatible publication bindings and compatible `result_of`, are refused
when they name the same occurrence, and also when only one of them names
`result_of` or only one of them names `publication_binding`: they would
count occurrences over different sets, and which fault acts would depend
on the store.

These steps give the broker nothing it could not know (ADR 0003): a fired
deadline carries the token the core armed, a released command is the one
the adapter accepted, a delivered result answers an effect still in flight.

### Perimeter

A trace header declares the `perimeter` of its run: `full`, or
`core-without-bytes` when the core is driven with validated packets and no
transport or peer observes anything. A scenario may require a perimeter;
absent, it requires `full`.

A run is judged only against a scenario of its own perimeter: for any
other, every assertion is out of scope, and a qualifying result that holds
an out-of-scope assertion is refused. A run without bytes therefore never
passes a full scenario. It has its own scenario, which declares
`core-without-bytes` and lists the assertions that perimeter can decide;
an assertion that needs what the peers observed stays out of scope there.

### Trace records and the provenance of broker inputs

Every kind of record is stated by one origin only. The audit of ADR 0003
reads the trace as chains, from what happened outside the core to what the
core consumed:

| Input of the core | Record of the core | What must precede it |
| - | - | - |
| Packet | `PacketDelivered` | `ClientBytesReceived` of the transport whose bytes are exactly that packet, itself caused by the `PeerPacketSent` of the peer with the same bytes |
| Store result | `StoreResultDelivered` | `CommandSubmitted` or `ResolveSubmitted` of the store adapter with the same effect and operation, not answered before |
| Deadline | `DeadlineDelivered` | `DeadlineArmed` of the timer with the same token, not delivered before |
| Recovery page | `RecoveryPageDelivered` | `RecoveryPageRead` of the store adapter with the same content |

A recovery page lists its facts. Besides the epoch, the sessions, the
inbound exchanges and the operations, an inventory holds what the core
needs to prepare an admission (record 0014): `Limits`, `RouteTable` and
one `Route` per target of a topic. Every quantity is a decimal string;
the two 32-bit bounds of an admission (`max_targets_per_admission`,
`max_topic_bytes`) refer to the `u32` primitive, in a trace, a snapshot
and a fixture alike, and the semantic pass refuses a value above
4294967295 (`u32-out-of-range`). The facts of a page follow the order
of an inventory, which is the rule of `RecoveryOrder::admit`: `Epoch`,
`Limits`, `RouteTable`, `Route`, `Session`, `InboundExchange`,
`Operation`, ranks never decreasing; the first three at most once
(`duplicate-name`); routes sorted by the bytes of their topic, equal
topics accepted (`rows-out-of-order` for a rank or a topic out of place).
A reader of documents sees one page. That the pages of one recovery form
one inventory, and that a route has its table before it, is checked by
the oracle on the whole trace.

`FrameEligible` carries the reason of an acknowledgement. A certain
rejection in `StoreResultReady` and `StoreResultDelivered` names its
`reject_reason`, and no other outcome has one. The reason a refusing
PUBREC carries is a function of it, stated once by the transition
catalogue as the `refusal_reasons` of the effect `EmitPubrecRefusal`:
`QuotaExceeded` for `CapacityExceeded`, `ImplementationSpecificError`
(0x83) for `Fenced`, `PreconditionFailed` and `EpochClosed` (record 0013,
line 10). A test holds that table to `AckReason::refusing` of the
contract crate, and the assertion
`refusal_carries_the_reason_of_its_rejection` holds a run to it: the
rejection a refusing frame answers is the latest certain rejection among
its causes, delivered to the same incarnation of the core.

`InputRefused` is what the dispatcher records when it refuses a result
that answers no effect in flight. `StoreCommitStable`, `StoreFenceStable`,
`StoreFenceRefused` and `StoreResultReady` are hidden truth of the store: they judge a run and
are never an input. `FaultHit` names the operation and effect it stopped,
when there is one, and `HeldItemReleased` the fault whose command or
result a step handed over.

A record says what its emitter claims. Checking a trace as a whole, and
the assertion `broker_inputs_have_provenance`, is the oracle's work.

### Store snapshots

`InspectDurableState` writes one document of the format
`mqttiumd.store-snapshot` per checkpoint, and records it in the trace as
`DurableStateInspected`, cited by path, digest and size. A snapshot is
read from the store independently of the broker; it is evidence for the
oracle and never an input of the broker.

| Table | Rows, in the order of their key |
| - | - |
| `meta` | Store incarnation; the last opened epoch (every earlier one is closed) and the attempt that opened it; the revision frontier; the next session |
| `sessions` | SessionId, ClientID bytes, revision, state. A deleted session stays, terminal |
| `route_table` | Its revision, and each exact topic with its target sessions and their QoS |
| `operations` | Receipts: OperationId, the exact canonical bytes of the recipe, the terminal decision with its content (commit facts, or the reason of a certain rejection) |
| `fences` | Identities fenced without a recipe: definitively not executable |
| `payloads` | PayloadId, length, digest, count of live obligations that refer to it |
| `publications` | PublicationId, the admission that created it, source session, topic, QoS, retain, PayloadId |
| `obligations` | DeliveryId (`publication_id`, `index`), target session and its revision, cause, effective QoS, phase |
| `inbound_exchanges` | ExchangeId, Packet Identifier, publication, phase; a released exchange has no row |
| `account` | Usage and limits of the one account of S0 |

Every table is present, even empty: an absent table is not an empty one.
What is volatile (a command in transit, a transaction in preparation, a
result not yet delivered, the control state of an opening) is not in a
snapshot; the trace shows it.

The revision of the route table is what the store writes; the oracle
never computes it. The store advances it at each `AddRoute`, so the
fixture of P1, whose one route has two targets, is at revision 2 in the
examples, the recipes and the traces of the corpus.

Rows are in the order of their key, numbers numerically, identities
component by component and bytes as bytes, so that two stores in the same
state write the same bytes. What a key member is comes from the schema,
never from its value: a topic whose bytes are all decimal digits is bytes,
and an empty topic is a key like another. A snapshot shows what the store holds, sound or not. The
reader checks its form only: 64-bit ranges, no row listed twice under its
key (`duplicate-name`) and rows in order (`rows-out-of-order`). An
obligation without its publication, or two admissions for one exchange, is
a finding the oracle asserts on, not a malformed document.

### Registry coverage

For each machine, state class and event class, the guard domain is the
product of the guard dimensions that the catalogue attaches to the event
class. Every point of that domain must be covered by exactly one rule. Two
rules on one point are an overlap: there is no implicit priority. No rule on
a point is a hole: an error in a `complete` registry, a listed note in a
`seed` one. A seed registry cannot support a PASS.

Coverage is a product of dimensions, so it is sized before it is
enumerated. A cell may have at most 4 096 guard points and a registry
262 144 over all its cells; the sizes are computed with checked arithmetic
and a wider registry is refused (`coverage-budget-exceeded`) before any
point exists. The catalogue schema bounds its lists as well.

### Results

A result is `provisional` or `qualifying`.

- A **provisional** result is a development document. It never carries
  PASS: a run whose checks all pass without the evidence below is
  INCONCLUSIVE.
- A **qualifying** result is offered as evidence. It names its scenario,
  manifest and trace by path, digest and size, and the contract digest. The
  files must exist with those digests, the contracts must be the ones in
  use, its faults must be exactly those of the scenario, its assertions
  exactly those of the scenario and those every run owes, and none of them
  may be out of scope.

Each entry of the assertion catalogue says whether `every_run` owes it.
`trace_complete` and `broker_inputs_have_provenance` are owed by every
run: a scenario cannot opt out of them by not listing them, and P1-02,
which is the text of the specification and lists only the first, owes
both (ADR 0003).

For both, the verdict must equal the one the assertions and injections
determine: FAIL if any assertion failed; otherwise INCONCLUSIVE if any is
inconclusive, an injection was not reached as announced, or nothing was
evaluated; otherwise PASS if all passed; otherwise OUT_OF_SCOPE.

This checks that a result is complete and consistent with what it cites,
not that its assertions are true. Recomputing them is the oracle's work:
see `../docs/reception/open-conditions.md`.

### Manifests

A manifest keeps apart `artifact`, what is certain about the binary that
ran (its digest and the build parameters captured when it was compiled),
from `checkout` and `tools_at_execution`, what was observed around it when
it ran. It does not claim that the binary was built from that checkout.

`modifications_sha256` is the digest of one `<content>  <path>` line per
path that differs from the revision or is untracked, in path order, where
the content is the SHA-256 of a regular file, `link:<target>` for a
symbolic link and `deleted` for a missing path. The list is defined by
paths: rename detection, external diff drivers and text conversions are
off, so a renamed file is one path gone and one path new, and the
user-level exclude file is ignored. What git itself considers modified
still applies: the `.gitignore` files of the tree, the exclude file of the
clone, and the file-mode and line-ending settings of the repository. The
contract files are listed once each, in path order, which is the order
their digest is defined by.

## Example corpus

The examples and their index are derived: `../tools/build_corpus.py` writes
them, each invalid example being one edit of a valid document, and
`requirements.json` with them. Edit the tool, not the files; CI fails when
they are not up to date. Schemas, catalogues, the registry, the fixture and
scenario P1-02 are sources and are edited directly.

An invalid example must be refused **for its declared reason**: every
diagnostic carries the declared code and one sits at the declared pointer.
An example refused for another reason fails the suite, and so does a code
that no example exercises. Any other reader of these formats, starting with
the independent oracle, is expected to agree with `index.json`.

### Second schema reader

`../tools/check_schemas.py` reads the same schema files with the Python
`jsonschema` package, at the versions pinned in `../tools/requirements.txt`,
and must agree with `index.json` on everything a schema reader can decide,
including the pointer of each schema violation and the strict-parsing
codes. CI runs it.

It is not the independent oracle of lot L3: it reads no trace and no store
and repeats no semantic rule. A second reader of the semantic rules is an
open condition. Its strict parsing matches the Rust reader on the cases the
corpus holds, including `NaN`, a lone surrogate escape, an integer beyond
64 bits and the lexeme `-0`.
