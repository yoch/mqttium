# Message Expiry finite-model reception

Date: 2026-11-17. Base revision:
`795d200929db643618be22cd01c5538f78ed1d96`. This lot changes only the new
`mqttiumd/models/message-expiry/` directory, tracked by integration PR #648.

The exact model inputs have SHA-256
`f43b06ffc6587eafd912991cbb746b6669c1cea88be1900e7ee2d41bcfeb2dde`.
This digest concatenates each `.cfg`, `.json` and `.tla` file in sorted
filename order as UTF-8 filename, one NUL byte and the file's exact bytes.
README and this report are excluded, avoiding a self-referential digest.

## Tools and commands

The repository-pinned `tla2tools-1.7.4.jar` was reused and hash-checked by
the standard runner. It reports TLC2 Version 2.19, 8 August 2024,
revision `5a47802`. The JVM was OpenJDK 21.0.13 on Linux x86_64. TLC used
one worker and a 512 MiB heap limit. No new tool or dependency was installed.

Commands, from the repository root:

```bash
python tools/formal/run_tlc.py --models-dir mqttiumd/models/message-expiry --list
MQTTIUM_TLA2TOOLS_JAR=/tmp/mqttium-l5-cache/mqttium/formal/tla2tools-1.7.4.jar \
JAVA_TOOL_OPTIONS=-Xmx512m PYTHONUNBUFFERED=1 \
python tools/formal/run_tlc.py --models-dir mqttiumd/models/message-expiry --verbose
python mqttiumd/models/check_models.py
python -m unittest discover -s mqttiumd/models -p 'test_*.py'
git diff --check
```

The TLC runner exited 0: all 15 outcomes matched their declarations. The
metadata checker found all eight model directories consistent, all 16
model-tool unit tests passed, and the whitespace check passed.

## TLC outcomes

| Configuration | Observed result | Distinct states |
| - | - | -: |
| `safety` | Pass, all nine invariants | 6,272 |
| `recovery` | Pass, all nine invariants | 27,824 |
| `domain-change` | Pass, all nine invariants | 36,664 |
| `zero` | Pass, all nine invariants | 200 |
| `maximum` | Pass, all nine invariants | 22,888 |
| `mut-ttl-reset` | `PublicationExpiryFixed` violated | 9 |
| `mut-retry-ttl-reset` | `PublicationExpiryFixed` violated | 292 |
| `mut-stale-send` | `NoStaleFirstSend` violated | 69 |
| `mut-free-started-ref` | `StartedReferenceHeld` violated | 426 |
| `mut-domain-assumption` | `NoAssumedContinuity` violated | 127 |
| `reach-past-ttl-finish` | `NoPastTTLFinishWitness` violated | 1,968 |
| `reach-ambiguous-expiry` | `NoAmbiguousExpiryWitness` violated | 980 |
| `reach-domain-quarantine` | `NoDomainQuarantineWitness` violated | 127 |
| `reach-zero` | `NoFiniteBoundaryWitness` violated | 21 |
| `reach-maximum` | `NoFiniteBoundaryWitness` violated | 43 |

Every negative result named the exact declared invariant. Witness runs
deliberately stop at their target; they are not complete safety explorations.
The five positive runs completed their finite state spaces.

## Bounds and limitations

One original publication, one retained topic, two recipient obligations,
one optional replay, two attempts per delivery, at most three references,
two fixed packet identifiers, five sampled raw ticks, two clock-domain labels
and at most one process loss. The wire maximum 4294967295 is represented
by finite ordered class 4, not by an infinity sentinel. An initial attempt
to use the actual number was refused by the pinned TLC's machine-integer
limit; that tool error was not counted as a detection or a passing check.
The final model explicitly documents the finite abstraction.

No Rust, contract, oracle, runtime, store or existing model file changed.
Rust checks, real process-cut campaigns, product mutations and independent
oracle qualification were not run by this model-only lot. The model assumes
atomic durable transitions and persistence across a process loss. It does
not prove implementation refinement, concrete numeric correctness, host-clock
recognition, universal safety/liveness, bounded scheduling, quarantine cleanup,
fsync or power-loss behavior. The final socket freshness check and durable
Started/Finish correlation require separate implementation evidence.
