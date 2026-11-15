# The oracle

An independent judge of mqttiumd runs (DEM-019). It reads contracts,
scenarios, traces and store snapshots, and decides the assertions of a
scenario. It shares no decision logic with the broker: nothing here
imports, runs or was derived from `crates/mqttiumd-core`. It may share
descriptive formats with the product, never the calculation of an expected
answer.

Run from the `mqttiumd` directory:

```bash
python -m pip install -r tests/oracle/requirements.txt
python -m unittest discover -t tests/oracle -s tests/oracle/tests
```

Versions are pinned in `requirements.txt`. Only the JSON Schema level uses a
package; everything else is the standard library.

## What exists

| Module | Role |
| - | - |
| `mqttiumd_oracle/strict.py` | Strict parsing: duplicate keys, non-integer numbers, `-0`, integers beyond 64 bits, `NaN`, lone surrogates; canonical 64-bit decimal strings |
| `mqttiumd_oracle/documents.py` | One snapshot of the contracts, and the validation order of `contracts/README.md`: parsing, format and version, JSON Schema, semantic pass. Files are confined below the root and bounded |
| `mqttiumd_oracle/semantic.py` | The semantic rules of every v1 format, written from `contracts/README.md` |

This is the second reader of the semantic level. It agrees with
`contracts/examples/v1/index.json`, the statement it shares with the Rust
reader: every valid example is accepted and every invalid one is refused
for its declared code, at its declared pointer.

The two readers reach that agreement by different means where it matters:

- The contract files are read once here. The digest is computed from the
  bytes that are then parsed, so the rules applied are the rules the digest
  names.
- 64-bit members are found by walking the schema that applies to a
  document, not by member name: a member is range-checked when its schema
  refers to the `u64` primitive. The Rust reader finds them by name, and a
  test of its own walks the schemas to prove that no 64-bit member is
  missing from its list.

| `mqttiumd_oracle/recipe.py` | The canonical bytes of a recipe, decoded independently |
| `mqttiumd_oracle/trace.py` | A trace read as a whole, and the order it establishes between two records |
| `mqttiumd_oracle/judge.py` | The observations of a run and the assertions of the catalogue |
| `mqttiumd_oracle/results.py` | A qualifying result produced from the observations; a result that is offered, recomputed |

## Reading a trace

A trace is complete when it starts with one header, ends with one footer
that counts its records and declares completeness, has a gapless collector
sequence and a gapless sequence per origin, and holds every cause its
records name. A record that is not a valid record makes the trace
unreadable; so does a record stated by another origin than the one its kind
belongs to, which is how a trace that breaks the origin rule of ADR 0003 is
refused.

The order is one relation (finding F5 of the audit): the local order of
each origin and the explicit causes together, closed transitively. A
trace in which the two form a cycle is incomplete, found in time linear in
its records and causes; `before` is computed from that same relation, and
two records it does not order stay unordered unless the run has a total
order (the simulated store).

A parent does not mean the same thing for every origin. For a record of
the product (the core, the transport, the timer, the store adapter, the
store's evidence, the dispatcher, the fault injector) it is a cause. For a
record of the bench supervisor it is what the supervisor had read when it
acted; for a record of a peer, what the peer observed and the supervisor
record it acted on. The parents of a record of the bench order it
(`before`); they never make it a cause of the product. `causes` is the
order relation restricted to the product: it never includes nor crosses a
record of the bench. The assertions that ask why a record of the product
exists read `causes`: the source of a result the adapter hands over and
the injection that duplicates it (K1), the injection that excuses a
refused input, the rejection a refusing PUBREC answers, the committed
reply a grant follows. Those that ask only what came first read `before`:
the closure before a refusal, the commit before an inspection, the crash
before an observed PUBREC, a resolution after a terminal result.
`one_admission_per_observed_inbound_exchange` counts the admissions of
an exchange by their `StoreCommitStable`, and by one more evidence: a
commit can be durable without that record when the store's evidence dies
with the child, killed inside the transaction after its commit frame (K2b
of the plan of lot L7). On a run whose broker is a child process, an
`Operation` fact `Committed` of a recovery that follows a `BrokerKilled`
is then the admission of that operation, once, unless a
`StoreCommitStable` of it came before. In-process the evidence of the
store is never killed, and a commit without it is no admission.

`broker_killed_for_real` (record 0019), owed by every run, reads the
claim of the trace header. A run in-process has nothing to show, and a
kill it records fails. A run whose broker is a child process shows, for
each crash, a `BrokerKilled` before it and after the previous crash
(missing: inconclusive), with the status of signal 9 (another: FAIL), and
no record of an origin of the child (the core, the transport, the timer,
the store adapter, the evidence of the store, the dispatcher, the fault
injector: every origin but the bench's) of the killed incarnation after
the kill (FAIL; unordered with it: inconclusive).
`terminal_knowledge_never_regresses` counts as a reaction to a late
unknown answer only a record of the product that names it among its
parents: an inspection, a crash or a packet the bench orders after it is
no regression of the product.

Nothing concluded from an incomplete trace is a PASS. A FAIL stands on an
incomplete trace only where no missing record could undo it (two admissions
for one exchange, obligations or a recipe that changed between two
snapshots); elsewhere the missing record could be the very one that
justifies what was observed, and the conclusion is inconclusive. The
distinction is made per reason, not per assertion: an acknowledged exchange
without admission, or an admission outside any exchange, may only lack the
record that was lost, and is inconclusive on an incomplete trace. A trace
of another scenario concludes nothing.

What a store holds must hang together and must hold what a client was
acknowledged for (finding F2 of the audit). At every checkpoint an
obligation names a publication the snapshot holds and a publication a
payload it holds; a missing reference is a FAIL, never a value equal to
another missing one. An exchange whose positive PUBREC a peer observed
before an inspection was committed: that snapshot holds a publication of
the topic the client sent, whose payload has the length and digest of the
payload bytes the client sent, decoded by the oracle from its MQTT 5
PUBLISH. The expectation comes from what the client did and from the
routes of the fixture, not from what survives in the snapshot. A snapshot
of a corrupted store stays a valid document: the bench can represent the
corruption, the judge recognises it. Each publication of the first
checkpoint carries one obligation per target of the route the fixture
gives its topic. Limit, outside S0: once outbound distribution exists,
an obligation legitimately settled between two checkpoints must not fail
this assertion; today an obligation that disappears is a FAIL.

A snapshot the trace cites is evidence only when it is the snapshot of the
same run and of the backend the trace declares, for the checkpoint that
cites it. Otherwise the checkpoint has no usable snapshot.

The order of an inventory is `RecoveryOrder::admit` of the contract
crate, which the Rust reader applies as it is. The oracle keeps its own
copy, `InventoryOrder`, on purpose: the oracle role does not run the code
it judges. One file of cases,
`crates/mqttiumd-lab/tests/support/recovery-order-cases.json`, holds the
two together: a Rust test runs it through `admit`, a Python test through
the copy.

The pages the core received during one recovery are one inventory
(record 0014), in the order it received them. The oracle carries the
order of the facts from page to page, and requires a route table before
the first route; a page out of
place fails `broker_inputs_have_provenance`, since no store of the
contract hands such an inventory to a core. It also applies the two rules
the core applies to a whole inventory: a route, topic and target, is
listed once, and at the page that closes the inventory every route
targets a session the inventory recovers. It does not require that an
inventory holds an epoch, limits or a table when it has no route: record
0014 leaves that open.

The packets the core consumed are audited down to the bytes: the bytes the
transport received name the packet of a peer that carries the same bytes,
and those bytes are exactly the one packet delivered, by its type and its
Packet Identifier. The oracle decodes them itself from the MQTT fixed and
variable headers. One `ClientBytesReceived` therefore carries one whole
packet in v1; a transport that records fragments is a contract change.

Order is what the trace establishes: two records of one origin are ordered
by that origin, a cause precedes its consequence, and a simulated run has a
known total order. Where the trace does not say, the oracle does not
choose: several interleavings of independent records are accepted, and an
event is never moved before its prerequisite.

## Perimeter

A run is judged only against a scenario of its own perimeter. A trace
whose header declares another perimeter than the scenario requires puts
every assertion out of scope, and a qualifying result that holds an
out-of-scope assertion is refused: a run without bytes never passes a full
scenario. Against its own scenario such a run is judged on what it can
show; the two assertions that need what peers observed
(`one_admission_per_observed_inbound_exchange`,
`no_pubrec_observed_before_crash`) stay out of scope there, and the
provenance of packets is not asked of a run that has no transport.

## The assertions

| Assertion | Decided from |
| - | - |
| `trace_complete` | The trace as a whole |
| `all_required_faults_hit` | The `FaultHit` records against the faults of the scenario; anything but the announced count is inconclusive |
| `one_admission_per_observed_inbound_exchange` | The oracle's own exchanges, built from what the peers sent and observed, against the admissions the store committed. An admission is attributed through the recipe a snapshot holds, never through an ExchangeId: a fresh one does not reset the count |
| `same_required_obligations_at_both_checkpoints` | The obligations of the first checkpoint, by DeliveryId, destination, cause, effective QoS and payload reference, against what the fixture's routes require and against every later checkpoint |
| `same_recipe_for_op_A` | The bytes stored for the operation a `FaultHit` names, in every snapshot taken after its commit |
| `no_positive_ack_before_confirmed_commit` | For each positive PUBREC or PUBCOMP, an earlier record of the same incarnation of the core: a committed result delivered to it, or a recovered fact that is sufficient. That the store had committed is not one |
| `fenced_operation_never_executed` | No commit follows the fence of an operation in the trace, and no snapshot holds an operation both fenced and committed |
| `certain_rejection_has_a_closure` | Each certain refusal the store reports against what closes it: a fence written before it in the trace, or a fence or a receipt of rejection in a snapshot. An absence that was only read closes nothing (DEM-MU-01) |
| `no_pubrec_observed_before_crash` | What the peer observed before the crash record |
| `broker_inputs_have_provenance` | Each input the core consumed against its counterpart: a submission in flight, an armed deadline, a page the store adapter read, bytes the transport received |

## The corpus of traces

`build_traces.py` writes `traces/`: histories written by hand, event by
event, with the snapshots they cite, and `traces/index.json`, which states
the status of every assertion on every trace. None comes from a run of the
broker. A faulty trace is one edit of a good one and is refused for one
assertion and no other; the incomplete traces conclude nothing at all.

```bash
python tests/oracle/build_traces.py           # rewrite the corpus
python tests/oracle/build_traces.py --check   # what the tests run
```

## Results

`results.produce` writes a qualifying result from the observations of a
run: the statuses the judge found, the injection counts of the trace, the
verdict they determine and the reasons of whatever is not a PASS.
`results.recompute` takes a result that is offered, checks it as a
document, judges the scenario and trace it cites again, and refuses it
where a status or an injection count differs (`result-not-recomputed`). It
also checks again every condition of a qualifying run (`results.unmet`)
from the scenario and the manifest the result cites, as `produce` checks
them: a qualifying result whose source has gone or changed, whose oracle
is not the one that runs, or whose kept modifications are missing is
refused (`result-not-qualifying` at `/manifest`, naming the condition;
R1 of the audit of `a4aea95f`). A provisional result names no evidence
and is refused as such. The verdict of a result is never taken from the
result.

`mqttiumd-lab validate` checks a result as a document: its schema, the
digests and sizes of the files it cites (scenario, manifest, trace), the
contracts it names, its assertions and faults against its scenario, and
that its verdict is the one they determine. It does not judge the trace
and does not check the conditions of a qualifying run: a qualifying
result whose `source.tar` is missing is still accepted by `validate`.
Acceptance by `validate` says the document is consistent; that it is
evidence is said only by `recompute`.

`python3 tools/reproduce_recompute.py <campaign> <new directory>
[--modified <campaign of a modified tree>] [--oracle <checkout>]` replays
the reception of R1 on a real campaign: independent copies, each with one
defect (none, source removed, source changed by one byte, another oracle,
a kept file removed), `recompute` then `produce` on each, one JSON line
per case, exit 0 when each case is refused for its defect alone.

The command `produce` writes the run-result of a run, for a campaign to
recompute and validate. It writes a qualifying result only when every
condition of a qualifying run holds as far as the files it is given
establish it (`results.unmet`): the manifest is valid and names the
contracts in use; it names the scenario and its fixture with the digests
of the files given; it records the backend, the failure model, the
scheduler and the commands; it identifies the oracle and the generators,
and the oracle files it lists are the ones that judged: every module of
the oracle that runs, read from where it runs and not from the campaign,
is listed with its digest; every modified path the manifest records is
kept in the campaign as the bench keeps it, a tracked one by its diff in
`modified/tracked.patch` and an untracked one as
`modified/untracked/<path>`; the manifest and the trace header say the
same of where the broker ran (`broker`, in-process when absent, record
0019); and the manifest ties the binary to its
source (`artifact.built_from`, record 0017): the campaign holds it with
the digest and size the member names, the binary was built with its
paths remapped (`--remap-path-prefix=<dir>=/mqttiumd-source`), and the
archive, read member by member as a stream with a bound on their number,
holds each contract and oracle file the manifest lists with the same
digest, and each untracked file the campaign keeps with the same
content. Without that member the result is provisional.

What a qualifying result establishes, from the campaign alone: the
verdict is the one the observations determine, the cited files are the
ones the run read, the oracle that judged is the one the manifest
identifies, the modified content is kept, and the campaign holds the
source the bench declares its binary built from, holding the contracts
and the oracle of the run. It does not establish that this source is the
recorded revision: that needs git, and a modified tracked file is kept as
a patch the oracle does not apply. The oracle builds nothing: that the binary which ran is the one that source builds is the
bench's declaration, which `tools/check_campaign_binary.sh` checks
outside the oracle by building it again. That a build on another machine
gives the same binary is not established.

A provisional result never carries PASS: a run whose checks all
pass is written INCONCLUSIVE, with one reason `not qualifying: …` per
unmet condition before the reasons of the assertions. No option makes the
command claim qualifying, and a qualifying result it writes must stand
when recomputed, or the oracle exits as defective.

```bash
PYTHONPATH=tests/oracle python -m mqttiumd_oracle judge --scenario <file> --trace <file>
PYTHONPATH=tests/oracle python -m mqttiumd_oracle recompute --result <file>
PYTHONPATH=tests/oracle python -m mqttiumd_oracle produce --scenario <file> --trace <file> \
    --manifest <file> --out <file> --scope <text>
```

`judge` and `produce` exit with the code of the verdict (0 PASS, 1 FAIL,
2 INCONCLUSIVE, 3 OUT_OF_SCOPE), `produce` with that of the verdict it
writes; `recompute` exits 0 when the result stands and 2 when it
is refused. A defect of the oracle, or contracts it cannot read, exits 70:
the run was not judged, and a caller must never record that as a verdict.

## What does not exist yet

- A run of the QABS scenarios: `tests/scenarios/qabs/` and hand-made traces
  of the three orders exist; no bench plays them yet.

`docs/reception/open-conditions.md` keeps the rows these close.
