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

Nothing concluded from an incomplete trace is a PASS. A FAIL stands on an
incomplete trace only where no missing record could undo it (two admissions
for one exchange, obligations or a recipe that changed between two
snapshots); elsewhere the missing record could be the very one that
justifies what was observed, and the conclusion is inconclusive. The
distinction is made per reason, not per assertion: an acknowledged exchange
without admission, or an admission outside any exchange, may only lack the
record that was lost, and is inconclusive on an incomplete trace. A trace
of another scenario concludes nothing.

A snapshot the trace cites is evidence only when it is the snapshot of the
same run and of the backend the trace declares, for the checkpoint that
cites it. Otherwise the checkpoint has no usable snapshot.

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
where a status or an injection count differs (`result-not-recomputed`). A
provisional result names no evidence and is refused as such. The verdict of
a result is never taken from the result.

```bash
PYTHONPATH=tests/oracle python -m mqttiumd_oracle judge --scenario <file> --trace <file>
PYTHONPATH=tests/oracle python -m mqttiumd_oracle recompute --result <file>
```

`judge` exits with the code of the verdict (0 PASS, 1 FAIL, 2 INCONCLUSIVE,
3 OUT_OF_SCOPE); `recompute` exits 0 when the result stands and 2 when it
is refused. A defect of the oracle, or contracts it cannot read, exits 70:
the run was not judged, and a caller must never record that as a verdict.

## What does not exist yet

- A run of the QABS scenarios: `tests/scenarios/qabs/` and hand-made traces
  of the three orders exist; no bench plays them yet.

`docs/reception/open-conditions.md` keeps the rows these close.
