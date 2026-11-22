# Durable Will finite-model reception

Date: 2026-11-17. Base revision:
`a0e445a47bd5e86b07a72f7f3d9649ea52d78c69`. This lot adds only
`mqttiumd/models/will/` for integration PR #648.

The model input SHA-256 is
`8204e230a3bf07b01915f079c7dc43188bce94aa3152fddd9f7aaf1da3f199b8`.
The digest concatenates `.cfg`, `.json` and `.tla` files in sorted filename
order as UTF-8 filename, one NUL byte, then the exact file bytes. README and
this report are excluded to avoid a self-referential digest.

## Tools and commands

The repository-pinned `tla2tools-1.7.4.jar` was reused. The canonical runner
verified SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.
It reports TLC Version 2.19, 8 August 2024. The JVM was OpenJDK 21.0.13 on
Linux x86_64. TLC used one worker and a 512 MiB heap cap. No new dependency
or tool was installed and no Cargo build was started by this lot.

Executed from the repository root:

```bash
python tools/formal/run_tlc.py --models-dir mqttiumd/models/will --list
MQTTIUM_TLA2TOOLS_JAR=/tmp/mqttium-l5-cache/mqttium/formal/tla2tools-1.7.4.jar \
JAVA_TOOL_OPTIONS=-Xmx512m PYTHONUNBUFFERED=1 \
python tools/formal/run_tlc.py --models-dir mqttiumd/models/will --verbose
python mqttiumd/models/check_models.py
python -m unittest discover -s mqttiumd/models -p 'test_*.py'
git diff --check
```

The canonical runner exited 0: all eleven outcomes matched their declarations.
The metadata checker found all nine model directories consistent, all sixteen
model-tool unit tests passed, and the whitespace check passed.

## TLC outcomes

| Configuration | Observed result | Distinct states |
| - | - | -: |
| `safety` | Pass, all thirteen invariants | 401,456 |
| `recovery` | Pass, all thirteen invariants | 2,750 |
| `mut-early-connack` | `ConnackAfterDurableInstall` violated | 7 |
| `mut-publish-normal` | `NoAdmissionAfterCancellation` violated | 64 |
| `mut-forget-on-crash` | `AcceptedWillNotLost` violated | 52 |
| `mut-readmit-lost-result` | `AtMostOneAdmission` violated | 86 |
| `mut-rebind-old-close` | `NoStaleClose` violated | 88 |
| `reach-recovery` | `NoRecoveryWitness` violated | 52 |
| `reach-retired-live` | `NoRetiredLiveWitness` violated | 286 |
| `reach-takeover` | `NoTakeoverWitness` violated | 803 |
| `reach-independent-intent` | `NoIndependentIntentWitness` violated | 46 |

The two positive runs exhausted their finite state spaces. Every negative
result named exactly its declared invariant. Reachability checks deliberately
stop at their witness and are not complete safety explorations.

## Received scope

The model checks durable installation before successful CONNACK, early
normal/abnormal closure around an unknown Open result, normal cancellation,
pending publication at loss/takeover/startup, one admission per occurrence,
independent source cleanup, exact payload references, capacity pressure,
same-operation resolution/retirement and an old already-attributed Close.
The independent-intention witness removes the source before admission while
the Will and its own payload reference remain pending. The retired-live
witness removes the source and detailed receipt while an independently due
obligation still retains the payload.

There are at most two fresh attachment occurrences, two potential recipients
per occurrence, three reference credits, one unrelated pressure reference and
one process cut. The safety run uses two occurrences without a cut; the
recovery run uses one occurrence with a possible cut. The model does not check
the whole cross product of two occurrences and a crash.

Atomic persistence, authentic correlations, exact unchanged canonical
recipes, compact retired-identity protection, non-reused causal identities
and ordered lifecycle submission are assumptions. Selection is abstracted at
the atomic admission cut; any earlier concrete route-freezing interval needs
its own tests. Detailed byte budgets, control reserves, actual queues, socket
triggers, Keep Alive, real process signals, SQLite/fsync and power loss are
outside this model. No fairness, bounded-latency or universal-correctness
claim is made.

A normal DISCONNECT that was observed only in volatile memory may be lost
with a process cut before its cancellation commits. Only committed
cancellation can suppress a surviving durable Will at recovery. This is
distinct from the supported normal-close path and from client-side success.

No Rust, contract, oracle, runtime, store, existing model, generator or
workflow changed in this lot. Rust gates, actual client/process-cut campaigns,
compiled-product mutations and oracle qualification were not run by this
model-only lot. The five specification mutations do not replace those
implementation and system tests.
