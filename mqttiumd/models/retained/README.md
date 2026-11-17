# Retained ownership and frozen replay

`MRetainedOwnership` is a small finite specification model for Proposed
[ADR 0032](../../docs/decisions/0032-durable-retained.md). It checks the distinction
between a current retained reference and already admitted recipient references,
frozen replay versions, producer disappearance and replay-command retirement.
Existing models and configurations are unchanged. This is not a Rust/SQLite
refinement, a real crash experiment or an independent-oracle qualification.

```bash
# Repository root, with the pinned tools:
python tools/formal/run_tlc.py --models-dir mqttiumd/models/retained --list
python tools/formal/run_tlc.py --models-dir mqttiumd/models/retained
cd mqttiumd
python models/check_models.py
python -m unittest discover -s models -p 'test_*.py'
```

## Finite workload and ownership

There is one exact topic and two immutable value/payload versions, V1 and V2.
The version numbers are labels, not concrete PublicationSeq values. Initially,
the topic retains V1 and an independently persistent recipient B is still owed
a live V1 delivery. V1 therefore has two references: one retained reference and
one recipient reference. The producing session still exists, but owns neither
of those references merely by being their source.

`Replace` admits V2 and changes the topic's one retained reference atomically.
It adds V2's reference and removes only the old retained reference. If V1 is
still owed to B or a replay recipient, its payload remains alive and charged.
`Clear` removes only the current retained reference and leaves the topic empty.
It does not create a retained zero-byte value. Distribution of the clearing
publication's empty live payload is a separate product/store test obligation,
not another retained version in this model. Ordinary RETAIN0 publications also
remain outside the modeled mutation actions: they cannot change this slot.

`references` is an explicit account updated by the individual ownership deltas.
`ReferencesExact` independently recomputes its required value from the retained
slot, B's outstanding live delivery and each outstanding replay. `payloads` is
the physically retained payload inventory; `PayloadLifetimeExact` requires
exactly the versions that still have a responsibility. Replacing the slot or
ending the producer cannot free bytes still owed elsewhere. `FinishOther` and
`FinishReplay` each return their own reference once; their guards prevent a
second completion from returning it again.

The fixed arrays name a finite workload. Absent cells have no modeled object
cost and are not proposed historical arrays in the product. There are at most
two payload objects and six simultaneous references, including an intentionally
broken duplicate replay. The production byte/topic/reference quotas and checked
preflight allocation remain concrete implementation obligations.

## Replay selection, uncertainty and collection

One subscription operation has two ordered, eligible RH0 filter occurrences
matching this topic for recipient A. Its reserved publication-identity range is
abstracted by four positions: the selected occurrences use positions 1/2 and
positions 3/4 are an unused tail. The real contract reserves at most 256; this
finite workload exercises the distinction between materialized and unused
identities, not the numeric range arithmetic or global sequence allocator.
The two replay occurrences remain separate even though they share destination,
source retained version and immutable payload.

`SubmitReplay` does not freeze the retained value. `CommitReplay` selects the
current slot in the actual atomic durable order. Thus a replacement that wins
before this commit may legitimately select V2, while a later replacement must
leave an admitted V1 replay untouched. An empty slot selects no replay and still
commits an operation whose identity range cannot become executable again.
Every selected replay stores its own source version. `FrozenReplay` compares
that stored version to its selection-time history until its own terminal action.
It also detects recreation of a terminal/unused occurrence.

The successful store commit initially leaves the result `unknown`. Nothing
about delayed/lost delivery reruns selection. `ResolveSameOperation` supplies
terminal knowledge of that original operation, and `ConsumeResult` applies it.
`CollectReceipt` is possible only after consumption. It removes the detailed
receipt and establishes the compact retired guard; it does not remove current
retained or recipient responsibilities. `RepeatRetiredReplay` must then remain
a no-op even if the slot changed or earlier deliveries finished. The mutation
materializes the formerly unused tail and increments the command's execution
count, reproducing the danger of treating a collected receipt as permission.

`ProducerDeath` represents explicit destruction of the producing session. It
changes no retained pointer or recipient reference. `lastWrite` records which
retained mutation last committed, and `RetainedIndependentOfProducer` requires
the current slot to follow that mutation rather than producer liveness.

No Local eligibility, RAP, RH1/RH2, wildcard matching and per-filter result order
are not reimplemented here. Both replay occurrences are already eligible.
Concrete tests must additionally check original publisher ClientID preservation
after source deletion, ordered repeated RH steps, replay RETAIN1 irrespective
of RAP, and live overlap's agreed eligible-contribution aggregation. The existing
routing model checks the matching/No Local reduction boundary. This model makes
no stronger claim about identity lookup after source-session reuse.

## Crash and proof boundary

The recovery configuration permits one `Crash`/`Recover` pair. A process cut
loses availability, not committed retained values, reference accounts, replay
records or receipt authority. Recovery reconstructs that same business state.
Atomic persistence and the concrete epoch/recovery protocol are assumptions,
not proved by making durable variables unchanged in these actions. Genuine
same-file process cuts and typed recovery failures remain separate tests.

One live responsibility is initialized, and each retained mutation occurs at
most once. There is one replay operation, one repeated old command and at most
one process loss. No fairness, timeout bound or universal liveness is assumed;
`Idle` permits no progress. Verifier-only `frozen`/`finished` histories record
selection and legitimate termination; normal mutation/selection guards do not
consult them. `lastWrite` and `executions` likewise check authority rather than
providing a product history table.

## Expected outcomes

| Configuration | Expected result |
| - | - |
| `safety` | All seven invariants pass over replacement, clear, replay selection/finishes, unknown results, receipt collection and producer destruction. |
| `recovery` | The same invariants pass with one abstract process loss and recovery. |
| `mut-free-old-payload` | `ReferencesExact` fails when replacement frees the old payload despite another recipient still owning it. |
| `mut-retarget-replay` | `FrozenReplay` fails when replacement rewrites admitted replay records to the new retained version. |
| `mut-reexecute-retired` | `NoReexecution` fails when replay of a collected operation materializes its previously unused reserved identities. |
| `mut-producer-ownership` | `RetainedIndependentOfProducer` fails when destroying the producer clears the retained reference. |
| `reach-ownership` | `NoOwnershipWitness` is intentionally violated after receipt collection, replacement and producer destruction while B and both admitted V1 replays still own V1, and the topic independently retains V2. |

Each negative configuration must fail its exact declared invariant. Parsing,
compilation, timeouts, other invariants and incomplete exploration are not
successful detections. These are specification mutations, separate from
compiled product mutations and system-client reception.

## Correspondence

The sidecar stays `specification` and `tests: []`; no exact TLC-to-Rust trace
refinement is registered. The following replay column identifies implementation
obligations rather than claiming this model-only lot executed them.

| Model action | Implementation predicate | Replay |
| - | - | - |
| `SubmitReplay` | Freeze one canonical subscription request and reserved causal range before persistence; uncertainty keeps that request identity. | Canonical replay recipe and operation-correlation tests. |
| `CommitReplay` | In one store cut, evaluate ordered eligible filter occurrences against the retained inventory and create distinct immutable replay publications/deliveries. | Both-store ordered RH/overlap/frozen-version tests and commit barriers. |
| `ResolveSameOperation` | Return the original committed replay result without reading the latest slot as a new selection. | Authentic held/lost subscription-result reception. |
| `ConsumeResult` | Known correlated result is applied before confirmation/retirement. | Owner projection/SUBACK and receipt-hole tests. |
| `CollectReceipt` | Detailed receipt collection preserves independent retained/delivery state and closes the entire reserved identity range. | Both-store collection/range non-reexecution tests; detailed frontier mechanics remain in the receipt model. |
| `RepeatRetiredReplay` | A retired old operation cannot materialize an unused identity or repeat its former work. | Repeated operation/range tests after physical receipt removal; `mut-reexecute-retired`. |
| `Replace` | Replace the topic's one reference while preserving old payloads still referenced by live or replay recipients. | Atomic net-capacity and old-value-drain tests; free-old/retarget mutations. |
| `Clear` | Remove the exact topic's retained reference; previously admitted obligations remain independent. | Retained clear plus pending-other-recipient tests; live empty-message forwarding is separate. |
| `ProducerDeath` | Session destruction does not own retained state or responsibilities toward other sessions. | Loaded producer/session termination with retained and queued work; producer-ownership mutation. |
| `FinishOther` | Return only B's committed delivery reference. | Existing guarded Finish/consume accounting and last-reference tests. |
| `FinishReplay` | Each distinct replay's own durable terminal action returns its reference exactly once. | Replay QoS0 consumption and QoS1 Finish tests; network-emission permission remains outside this model. |
| `Crash` | Committed retained/replay/receipt facts survive a real process loss. | Separate product/store SIGKILL witnesses; the model assumes atomic storage. |
| `Recover` | Reconstruct the same current value, frozen work and ownership accounts. | Typed recovery/inventory and same-file restart tests. |
| `Idle` | No assumed fairness, bounded scheduling or permanent availability. | Every configuration; finite system evidence retains its own scope. |
