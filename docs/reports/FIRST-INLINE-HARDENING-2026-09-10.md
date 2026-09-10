# First-inline prototype: adverse findings and disposition

## Scope and identity

This is a follow-up to PR #456's original embedded prototype, not a new release.
Base: `9ad1f01857306ac5079ffb1d073a59fdb60e1931`.
Original tooling head: `2b663b52d69d4ab6b2ffc8481fd689a4bd95e1ca`.
Original runtime patch SHA256:
`c6faba951d28eb8824520ee4340c19d39e066a1d0fd8782503149d4eb535da48`.

The three original differential probes pass on the base and fail on the original
prototype. Two are resource-ownership bugs; the third captures an observable
execution-context change. Do not report all three as fixed while merely changing
the third assertion.

## 1. Cancelled callback worker: fixed ownership

A queued tail was retained when its worker was cancelled before taking the job.
The coroutine's active-job `finally` cannot cover cancellation before coroutine
entry. The worker now has an identity-checked completion handler that retires its
queued jobs and accounting. Replacement explicitly retires a completed owner
before creating the next one; a delayed old completion cannot clear the new
worker's queue. A slow producer re-establishes a worker after admission if the
old one was retired while it waited.

A further adverse test exposed a race in completion-handler-only cleanup:
active-batch reservation release wakes a parked producer before task completion
callbacks run. Clearing the queue there could silently discard that producer's
newly admitted job. Active workers now retire synchronously in an outer
`finally`, before any awakened producer runs; the completion handler remains a
pre-entry fallback. An awaiting shutdown also checks the captured worker identity
before touching a successor. The regression covers normal/eager factories,
active batches of 2/8 and explicit cancellation/shutdown.

Only worker creation with an already-populated queue defers its first step. This
keeps eager factories from executing a job before `callback_task` names its owner,
without adding a loop hop to the ordinary empty-queue startup. No per-message task,
parallel queue, uncounted buffer, or extra callback reservation is added.

Tests cover cancellation inside A, after A but before worker startup, during the
tail, normal/eager factories, late completion callbacks, waiting admission,
restart, `join()`, and shared iterator/callback byte ownership.

## 2. Interrupted effect drain: fixed ownership

The original prototype retired the interrupted prefix but could leave later
effects pending without a scheduled task. Before relaying that cancellation, the
pump now requests a successor if a suffix remains. Existing epoch validation
still discards dead-connection effects; terminal results retain their normal
settlement path. This is specific to an interrupted inline admission, not a
blanket restart after intentional external shutdown.

Tests retain a later PINGRESP or PUBLISH_COMPLETE, check actual receipt settlement
and empty receipt registries, and cover both synchronous drains and scheduled
flushers with normal/eager factories. A separate public disconnect test ensures
that intentionally retired messages are not revived.

## 3. Current-task cancellation: explicit behavioral difference

Real cancellation still propagates to the task actually running the callback.
MQTTium does not call `uncancel()` or swallow it. Generalizing inline execution
therefore generalizes the singleton/pair context to larger eligible bursts:
cancelling that task can terminate the reader and connection.

The previous assertion that a large burst must leave the connection alive is
retained as a historical differential finding, **not** relabelled as a passing
invariant. New tests explicitly verify the new context, propagation, teardown,
no tail resurrection and clean resources for QoS0/1 and bursts 1/2/3/8. Migration
and reference text disclose the difference. This does not establish universal
source compatibility for applications relying on the old task identity. Such a
requirement and true reader-inline execution cannot both hold.

## 4. Fairness: explicit scope, not a false global guarantee

The selected unit is a callback-only **message notification**, with at most one
eligible message prefix taking inline execution per effect-drain invocation.
All matched filters for one message remain ordered together; publish completions
and `both` retain their established rules. A regression test deliberately checks
these exceptions. A global one-user-function-per-turn scheduler was not added:
it would change routing, publish-only paths, and the mission's negative controls.
If that stronger requirement is mandatory, this implementation does not meet it.

## 5. Complexity and remaining integration gates

The exact-two-message helper is gone. Existing worker-batch reservations are not
pair-specific and cannot simply be deleted. The fixes add cold lifecycle cleanup
and retain the original prefix-cancellation protocol; they are not a demonstrated
code-size reduction. Architectural acceptance remains a separate decision.

PR #454 addresses pre-existing live-route and reconnect findings #453/#455. This
change is not a substitute for those fixes, and composition still needs review
before release. No benchmark threshold or public constructor option is changed.
The normal CI now sees the actual source and regression tests; the temporary
branch-only workflow override and embedded prototype tools are removed.

## Evidence boundaries

The original Pi smoke (run `34421558753`) measured the **old** prototype. Its
near-neutral fixed-rate result is historical evidence only, not performance
qualification of this corrected source. Repeat exact-source performance checks
before accepting the trade-off. New tests prove the covered cases, not absence
of every possible concurrency defect. Keep the PR draft; no merge/release approval.
