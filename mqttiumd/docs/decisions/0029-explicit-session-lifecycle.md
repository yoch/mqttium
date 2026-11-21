# 0029 — Explicit bounded session lifecycle

Status: **Accepted with amendments** (21 November 2026, decided under the
repository owner's delegation on the advice of the independent audit of
`8e4303d`). This adds zero/infinite session retention to the existing
experimental MQTT 5 profile. Finite intermediate intervals remain
unsupported. It does not accept historical proposals or change the immutable
chapters.

Amendment of acceptance (21 November 2026).
Amended by 0033. At sixteen connections reserve 544 terminal inputs for 208
frame registrations, including the two facts of an expiring frame. Record
0030 governs finite session expiry and the receipt-window threshold. Record
0048 states the capacities of the runtime and their derivation; the sixteen-
connection numbers below describe the bounded qualification profile.

Amended by 0030 (record 0047): finite session expiry intervals are
supported, and the business threshold of the receipt window is 512 where
this record says 640.

Note (21 November 2026): the budgets this record states for sixteen connections are
the bounded profile of `runtime::Limits`. The connection limit is an option
since record 0046 (proposed), and the queues sized for sixteen connections
grow with it.

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

The physical receipt quota8192 and control reserve512 remain unchanged. New
opens are business commands, terminations are controls. The per-origin span
stays1024; its business threshold becomes640 instead of768, leaving384 sequence
positions for at most128 inbound,128 outgoing and128 session-termination
continuations. A known close behind another unknown result still occupies its
position until the prefix can advance; one simultaneous lifecycle flight alone
would not justify reserving only one extra position. New opens stop at the
business threshold, so repeated replacement cannot create unbounded controls
behind a hole. Same-identity resolution and collection allocate no new command
identity. Control effects retain their explicit Outbox and writer reserves.
Lifecycle Persist/Resolve replies use a separately bounded control lane and
Outbox selection, so sixteen retained ordinary replies cannot consume their
only progress resource. Durable guards establish the cut when this lane passes
an older uncommitted command; retained receipts still resolve older commits.
Physical transport closure/cache installation fences following lifecycle/frame
dispatch. A known session-quota refusal has its own SessionCapacityExceeded
cause and a negative CONNACK0x97 without inventing a SessionId.

The new lifecycle transition reserves up to three effects before mutation:
transport closure, persistence and deadline, or a known change followed by
ephemeral-session closure and its deadline. Existing two-effect transitions and
their minimum Outbox configuration retain their previous contract. A lifecycle
transition that cannot reserve its own three-effect bound refuses before change.

The reactor retains at most one pre-CONNACK packet per transport and does not
release pipelined business packets until the ordered CONNACK is accepted by the
network adapter. An early explicit DISCONNECT-zero hint keeps the original
opening request correlation until its known result; it cannot become a close
of whichever incarnation later uses the ClientID. Sixteen configured transports
have32 reserved end/close inputs. Up to208 accepted frame registrations cover
four queued writes, one active write and eight terminal receipts per transport;
336 reserved terminal facts add the128-effect Outbox bound. Ordinary inputs
remain bounded separately at128, so closure can reconcile accepted frame facts
without requiring spare business input capacity.
Each accepted transport reserves both of its possible future closing facts
against the same32 slots already queued. Each accepted frame likewise reserves
its future terminal fact against the combined frame/fact budget; independent
per-container limits alone would overcommit under repeated healthy traffic.
Orderly shutdown closes transports and gives accepted work and zero-policy
ends at most two seconds. Unresolved outcomes report incomplete shutdown;
they are not converted into certain rejection. Typed startup/read failure
retains its original cause and does not start additional cleanup writes.

The instrumented writer exposes session.open/close.prepared inside the real
transaction and session.open/close.committed after COMMIT before result return.
Normal builds expose neither test controls nor these callbacks. Captured known
lifecycle effects and transport-close facts retain their original operation,
SessionId, attachment and generation when replayed by the test adapter. These
are system controls rather than permission to reinterpret later socket bytes.

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
