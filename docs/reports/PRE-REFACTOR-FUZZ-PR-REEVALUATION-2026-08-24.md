# Pre-refactor fuzz PR reevaluation — 2026-08-24

## Scope

Five draft pull requests created from pre-refactor commit
`a336f834c66c4b4cb1a612e7c064ae29bb51cb7b` were reevaluated against the
post-consolidation baseline `c3f594d`. They were treated as an audit corpus,
not as merge-ready branches:

| PR | Tip | Subject |
| --- | --- | --- |
| [#372](https://github.com/yoch/mqttium/pull/372) | `0783fe6` | cooperative concurrency scheduler prototype |
| [#373](https://github.com/yoch/mqttium/pull/373) | `eee8726` | delivery and reconnect lifecycle races |
| [#374](https://github.com/yoch/mqttium/pull/374) | `e603235` | persistence/lifecycle race investigation |
| [#375](https://github.com/yoch/mqttium/pull/375) | `f818b0a` | writer/effect waiter liveness fixes |
| [#376](https://github.com/yoch/mqttium/pull/376) | `9004bce` | deterministic runtime soak prototype |

Each branch received separate standards and specification reviews. Claimed
failures were then reproduced on the consolidation baseline, and candidate
production patches were merged into temporary worktrees before any current-tree
implementation.

## Disposition

### #372 — do not merge

The named-checkpoint scheduler is a useful experiment, but its roughly
2,000-line test framework introduces a second generic broker/factory stack,
classifies broad timeout and connection failures as successful outcomes, and
does not install the CI/nightly campaigns described by its documentation. It
reported no product defect. Its reproducible-schedule idea was retained in the
smaller seed/history model already used by the fuzz harness; the framework was
not imported.

### #373 — findings integrated, branch not merged

Four defects reproduced on `c3f594d`:

1. a callback-raised `CancelledError` terminated the callback worker and left
   queued jobs unfinished;
2. `disconnect()` from `on_connect` or `on_message` could self-join the callback
   worker;
3. explicit `connect()` during an automatic-reconnect gap could start against
   the previous writer generation and lose the reconnect race;
4. joining the reader while holding the lifecycle lock could deadlock an
   `on_disconnect` callback that reconnects, after which old teardown could
   close the replacement connection.

The production fixes were adapted to the current delivery controller and
connection-epoch design. Focused deterministic tests were added to existing
lifecycle, delivery, reconnect, and runtime-interleaving modules. The branch's
976-line duplicate broker campaign and polling loops were not imported.

### #374 — correctness findings integrated

Three replay findings reproduced:

- a PUBREL between replay batches could delete a durable QoS 2 row while a
  cursor-held copy was later emitted as `MESSAGE`;
- a direct engine consumer could continue draining a replay cursor after
  transport close;
- Memory and SQLite disagreed when `delivered` changed on a cursor-held row,
  because one cursor exposed a live object and the other a snapshot.

Inbound metadata now includes the delivered flag. Streaming replay refreshes
only payload-free metadata immediately before emission and drops its cursor on
transport close. This preserves bounded payload hydration and removes the
Memory/SQLite difference. The store transition protocol now states explicitly
that a raised transition must leave its mutation unapplied.

The report's receipt-versus-durable-row split is intentional: runtime receipts
are terminal-connection ownership, while durable QoS rows survive for session
recovery. The injected "mutate then raise" store remains a third-party contract
violation, not behaviour to compensate for in the protocol engine.

### #375 — findings integrated with stronger assertions

All three liveness failures reproduced on `c3f594d`:

- writer admission waiters remained parked after the active transport write
  failed;
- a reader waiting to admit PUBACK could therefore deadlock teardown;
- effects collected while a failed flush awaited connection close could leave
  a later `drain()` waiting forever.

Writer failure now advances the writer epoch and wakes all admission waiters.
The regression requires `StaleConnectionEffect` and proves that no dead-writer
item entered the queue or resident accounting. Effect collection during the
failing-close window is settled against the dead connection immediately.

### #376 — do not merge; retain bounded oracle improvements

The runtime-soak prototype provides useful ownership fields, but places
resilience campaigns under unit tests, schedules only its CI profile despite a
documented nightly profile, permits a QoS 2 replay check to pass without proving
that replay occurred, and reduces to any failure rather than preserving failure
identity. The large parallel broker/runner hierarchy was not imported.

The maintained broker soak now checks the useful additional public ownership
signals: packet identifiers, inbound bytes and replay, delivery/writer/effect
waiters, and every receipt class.

## Fuzzing changes derived from the audit

The deterministic engine fuzzer now exercises enhanced AUTH and outbound Topic
Alias establishment, replacement, empty-topic reuse, invalid values, reconnect
reset, and canonical durable-topic ownership. Its invariant set now includes
the outbound message/byte ledger and inbound byte ledger.

The differential stateful campaign now schedules replay page hydration against
PUBREL completion, delivery handoff, continuation, transport close, and resumed
sessions. Memory and SQLite are compared after every operation, and a completed,
delivered, or stale-connection row may never appear in later replay effects.

## Post-audit Topic Alias replay correction

An external review of `c3f594d` identified one additional issue outside the
five draft PRs. MQTTium persisted the canonical Topic Name for QoS 1/2, but the
record could still carry the old connection's `topic_alias` property or a
retained segmented frame containing that property. If a resumed connection
negotiated a lower Topic Alias Maximum, replay rejected and deleted the durable
publication instead of retransmitting it independently of the old mapping.

The finding reproduced with an old maximum of 10 and a resumed maximum of 0.
Replay now ignores the connection-scoped alias property, discards any retained
alias-bearing frame, and re-encodes `DUP=1` with the canonical topic. Other
PUBLISH properties remain intact. Memory and SQLite restart tests cover QoS 1,
QoS 2, alias-only initial wire forms, and retained segmented frames.

Paired fresh-process A/B measurements compared this worktree with `c3f594d` on
publish paths without properties or aliases (nine alternating pairs):

| Scenario | Candidate/base median | Base CV | Candidate CV | Interpretation |
| --- | ---: | ---: | ---: | --- |
| native `publish_nowait`, QoS 0 | 0.9861 | 3.45% | 2.34% | valid, within 3% |
| async `publish(..., nowait=True)`, QoS 0 | 0.9774 | 2.81% | 3.55% | valid, within 3% |
| QoS 1 Memory publish/ACK cycle | 0.9761 | 3.16% | 1.08% | valid, within 3% |

Two additional pinned MQTT 5 runs were too noisy to cite as performance
evidence (both variants had coefficient of variation above 12%). Their medians
also showed no regression beyond 3%, but they are excluded from the valid set.

## Validation

The resulting worktree passed:

```text
python -m pytest -q tests/unit tests/project
1422 passed

python -m pytest -q tests/unit tests/project --cov=mqttium
1422 passed; 90.07% total coverage (89% required)

PYTHONPATH=src python tests/fuzz/fuzz.py \
  --seed 20260824 --iterations 20000
codec: 20000, engine: 20000, websocket: 20000; no failures

python -m pytest -q \
  tests/fuzz/test_hypothesis_fuzz.py \
  tests/fuzz/test_stateful_invariants.py
52 passed

MQTTIUM_REQUIRE_BROKER=1 python -m pytest -q tests/integration
16 passed against Mosquitto 2.0.18 on 127.0.0.1:11883
```

This is source-level and deterministic fake-runtime evidence. Live broker,
platform-matrix, and multi-hour campaign results remain separate release
evidence and are not claimed by this report.
