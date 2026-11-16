# 0029 — Explicit bounded session lifecycle

Status: **Proposed**. This adds zero/infinite session retention to the existing
experimental MQTT 5 profile. Finite intermediate intervals remain unsupported.
It does not accept historical proposals or change the immutable chapters.

## Public matrix and ordering

Absent CONNECT Session Expiry Interval means zero. Both Clean Start values
are supported, with an explicit ClientID and the existing MQTT profile.

| Request | Existing state | Result |
| - | - | - |
| CS0, infinity | Persistent session | Resume same SessionId, SP1 |
| CS0, zero/absent | Persistent session | Resume state/SP1, durably set zero policy; terminate at later close |
| CS0 | Absent | New SessionId, SP0 |
| CS1 | Present or absent | Retire old incarnation if present; new SessionId, SP0 |
| DISCONNECT, expiry0 | Persistent connected session | Durably terminate that binding/incarnation |
| Close, infinite policy | Current binding | Detach transport; retain session |
| Close, zero policy | Current binding | Durably terminate and reclaim |

MQTT 5 sections 3.1.2.4, 3.1.2.11.2, 3.1.4, 3.2.2, 3.14.2.2.2 and 4.1 govern
these choices. A takeover closes the old transport before Clean Start handling.
If its policy was zero, its session ends before the new CS0 lookup: SP0/new
identity, not a resume. An old infinite policy can instead resume and become
zero for its replacement transport. CONNECT0 cannot become nonzero through
DISCONNECT; this is Protocol Error0x82. Duplicate/malformed properties are
distinct from supported encodings with an unsupported finite interval.

## Authority, uncertainty and identities

ClientID is reusable. SessionId is allocated by the existing checked durable
counter and is never reused. Business revision remains stable on resume or
policy change, because existing routes/obligations depend on it. Each successful
open has a separate durable attachment token: its OperationId. Close compares
SessionId and that original token, never a newly looked-up ClientID binding.

Lifecycle changes are canonical receipted recipes, sharing OperationId,
ResolveAndFence, owner result consumption and bounded receipt collection.
Submitted/unknown/terminal/applied are distinct. Only a known correlated result
applied by the owner authorizes CONNACK, projection changes or volatile capacity
release. An unknown result retains the same recipe and operation. A repeated
open cannot run replacement again; after collection its identity remains retired.

One lifecycle flight is serialized in the owner. Lifecycle route cuts wait for
the current subscription control to resolve; new subscriptions/admissions wait
during that cut. Unrelated acknowledgements, continuations and PING can progress.
Old unknown business operations remain bounded detached flights until their
same-operation terminal result is consumed; retirement is not certain rejection.
They cannot emit to retired transports or reinstall stale routing projections.

## Atomic cleanup, accounting and startup

Within the existing finite session/route/reference/slot limits, replacement and
termination are one bounded transaction. Preflight precedes enumeration/copy.
Delete own inbound markers and destination obligations/claims/routes, decrement
each reference exactly once and remove the physical session row. A replacement
frees the old active slot and creates a fresh SessionId in the same transaction,
including at128 active sessions. A cancellation is not a successful delivery.
Publications already owed to another destination survive producer termination.

The specific legacy Admin DeleteSession also physically removes its row after
the same cleanup. Its old terminal-row representation changes deliberately;
identity/refutation guarantees and the monotone next_session guard remain.
It is not used as a retryable product authority.

Actual new epoch opening atomically expires zero-policy sessions before grant
and recovery. Repeating an already-committed OpenEpoch does not repeat expiry,
so it cannot delete current-epoch sessions. Initial typed snapshot validation
precedes cleanup; the post-opening recovery/cache uses the resulting state.
No wall clock or offline duration is needed.

SQLite schema5 explicitly persists policy/attachment. Older formats are refused
before write-affecting pragmas, without reset or implicit migration. Canonical
format1 adds a lifecycle kind while preserving kinds1–7. Readers validate policy,
identity and byte/row bounds before selection/allocation. Session rows, caches,
owner metadata, work notifications, emission registries and lifecycle flights
are bounded by live/configured state, not historical ClientIDs.

## Reception and scope

First retain a red test of512 existing-interface create/delete cycles at quota128:
no historical Terminal rows may accumulate, and SessionId must keep increasing.
Then receive the public matrix,512 complete ephemeral client cycles, full128
persistent capacity, target-only shared fanout cancellation, genuine unknown
results, stale facts, takeover and separate same-file process crashes. Four or
five compiled mutations must fail their exact cardinality/identity/accounting
properties. Original failed diagnostics retain their identities.

The session reception is system evidence, separate from K1/K2b/K3 qualification.
The focused finite model covers two incarnations, another destination, unknown
commands, old events and startup. Existing receipt/PI/pubsub/proof gates remain.
