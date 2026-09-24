# Formal models

MQTTium keeps small TLA+ models next to the code for concurrency and ownership
questions that example-based tests cover poorly: which task owns a resource,
which facts must be published before an `await`, and which interleavings can
wedge the client. A model is evidence for a design decision. It does not replace
the executable regression tests, and it is never a proof of the Python code.

## Layout

Everything lives in `formal/models/`. Each model has:

- `<Model>.tla`: the TLA+ module;
- one or more `.cfg` files: TLC configurations;
- `<Model>.json`: a sidecar declaring what TLC must report for each
  configuration, and which issues and tests the model belongs to.

Sidecars keep every model self-contained. Adding a model never edits a shared
index, so parallel changes do not conflict.

```json
{
  "module": "WritePumpCancellation.tla",
  "status": "legacy-flag",
  "summary": "One sentence stating the ownership rule the model checks.",
  "issues": [509],
  "tests": ["tests/unit/test_write_pump_dependency_cancel.py"],
  "configs": [
    {"config": "WritePumpCancellation-rc15.cfg", "expect": "violation",
     "invariant": "DeadDependencyWriterIsRetired"},
    {"config": "WritePumpCancellation-fixed.cfg", "expect": "pass"}
  ]
}
```

## Expected outcomes

Every configuration declares one outcome, and the runner fails on any other:

| `expect` | Meaning |
| --- | --- |
| `pass` | Exhaustive check with no invariant violation and no deadlock |
| `violation` | TLC reports exactly the named `invariant` as violated |
| `deadlock` | TLC reports a reachable deadlock |

A defect is documented by a configuration describing the released behaviour,
which must keep producing its counterexample, and a configuration describing
the repaired behaviour, which must pass. If a model stops finding its
counterexample, the model has drifted, just as it has when the repaired
configuration starts failing.

Every configuration names `SPECIFICATION`. Terminal states are explicit: the
model defines a `Terminal` predicate and a `Quiescent == Terminal /\ UNCHANGED
vars` step. This lets TLC keep deadlock checking on and still report any stuck
state that is not a legitimate end of the run. Unbounded counters need a
`CONSTRAINT` so that the check terminates.

## Model status

| `status` | Meaning |
| --- | --- |
| `refinement` | Actions encode the implementation's actual predicates and ordering. The released and repaired configurations differ only in those predicates. |
| `legacy-flag` | Imported investigation model. A boolean constant enables or disables the defective action, so the repaired configuration passes by construction. It documents the counterexample but gives no independent evidence for the fix. |

New models must be `refinement` models. A legacy model is replaced when the
structural change that owns its invariant lands.

## Running TLC

```bash
python tools/formal/run_tlc.py            # every model
python tools/formal/run_tlc.py Foo Bar    # selected models
python tools/formal/run_tlc.py --list     # index of models, issues and expectations
```

The runner needs Java 11 or newer. On first use it downloads the pinned
`tla2tools.jar` release, checks its SHA-256, and caches it under
`~/.cache/mqttium/formal/`. To use a pre-fetched copy, set
`MQTTIUM_TLA2TOOLS_JAR`; its hash is still checked. The `Formal models`
workflow runs the same command for changes under `formal/` or `tools/formal/`.
`tests/project/test_formal_models.py` checks the sidecars without Java.

## Tying models to code

A model invariant is only useful if the code keeps satisfying it. For each
invariant, the sidecar's `tests` list names at least one deterministic test
that drives the real client through the counterexample's interleaving. Use the
packet-aware transports in `tests/support.py`. The test must fail on the
released behaviour and pass on the repair.
