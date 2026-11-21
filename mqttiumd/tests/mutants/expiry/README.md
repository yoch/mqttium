# Compiled finite session expiry controls

From `mqttiumd/` on a clean source tree:

```bash
python tools/run_expiry_mutants.py /tmp/mqttiumd-expiry-mutations
```

The runner pins the current Git commit in a private detached worktree. Each
mutation first runs its exact unmodified test, then compiles and executes the
mutated version of that same test. The required assertion code, exit101 and
one failed test must all be observed. A compiler error, zero tests, timeout,
wrong assertion or failed clean positive aborts the reception.

| Mutation | Required detected property | Implementation scope |
| - | - | - |
| `late-open-waits-timer` | `expired_open_resumed_without_timer` | The owner's validation must reject a result that resumes an already expired session even when the timer has not run. |
| `stale-timer-rebound` | `old_timer_rebound_after_open` | An already-associated old timer cannot delete a newer attachment or incarnation. |
| `detach-deadline-recomputed` | `detach_result_recalculated_deadline` | A correlated result cannot replace the immutable deadline derived from the observed close. |
| `continuous-domain-progress-lost` | `continuous_domain_progress_lost` | Repeated same-domain reopen must account elapsed ticks so a detached session actually expires rather than surviving indefinitely. |

`expected.json` names the exact test and package for each patch. The first
three are pure owner transition tests; the fourth exercises both stores and
SQLite reopen through public store APIs with an injected clock domain. They
are implementation controls, separate from the real-client reception,
per-process crash witnesses, finite specification-model mutations and existing
independent-oracle qualifications.

The receipt keeps the source Git identity, each patch and hash, the actual
positive and mutated test-binary hashes, commands, full outputs, precise
reasons and restoration status. Targets are outside the receipt directory;
`MQTTIUMD_EXPIRY_MUTANT_TARGET_ROOT` optionally names an external build cache.
The source worktree is restored between cases and removed on completion.
Neither the caller tree nor shared generated files are mutated. The aggregate
expiry reception also invokes this command and archives its receipt.
