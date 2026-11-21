# 0030 — Finite session expiry with a durable temporal anchor

Status: **Accepted** (21 November 2026, by the repository owner). This extends
0029.
Message Expiry, Will, finite-session eviction and general migration are separate.

## Profile and authority

All MQTT 5 Session Expiry Interval values are supported: absent/0, finite
1..0xfffffffe and infinity. CONNECT sets retention after transport closure;
it does not expire an active connection. DISCONNECT may override retention,
except that original CONNECT0 cannot become nonzero (Protocol Error0x82).
Malformed and duplicate properties keep their existing errors. See MQTT 5
3.1.2.11.2, 3.14.2.2.2 and 4.1. Clean Start and Session Present keep 0029's
incarnation semantics.

ClientID is reusable; SessionId, OperationId and attachment identities are not.
OpenAt, Detach and Expire use the existing canonical lifecycle recipe and its
singleton owner flight, durable receipt, unknown-result resolution and reserved
writer lane. Legacy Open/Close action tags0/1 retain their bytes; new tags2/3/4
carry explicit logical nanoseconds. Finite policy is forbidden in legacy Open,
which has no temporal authority. A repeated command keeps its exact recipe and
operation; a collected receipt never authorizes its execution again.

A valid transport close is observed once. Its effective policy and logical
observation are retained even while Open is unknown, while its authentic result
is queued internally, or after the Peer is removed. Known Open installs this
intent on the original generation/attachment. It cannot consult a later ClientID
binding. Zero uses existing Close. Finite or an explicit nonzero override uses
Detach; an unchanged infinite transport still needs no detach write.

Detach atomically records the effective policy, original attachment, detachment
operation and fixed deadline `observed + seconds * 1_000_000_000`; infinity has
no deadline. Delayed commit, Resolve or copied results cannot recompute it.
The deadline is independent of physical receipt retention. Known Detached
updates owner/cache state; unknown results retain the original operation.

## Reconnect versus expiry

The serialized owner lifecycle transition is the decision order. Immediately
before an unaccepted OpenAt is planned, the adapter reads admissible logical
time; a capacity retry refreshes that time before any operation exists. Once
submitted, recipe/time never change. The store applies this decision atomically:
an offline deadline at or before that time retires the old incarnation before
new Open/SP0, even when no timer has run. A winning non-expired resume installs
a new attachment and removes the old detachment/deadline.

A timer only submits Expire. Store and owner compare SessionId, original
attachment, exact detachment token/deadline, offline state and admissible time.
A stale timer after resume, re-detach, takeover, replacement or ClientID reuse
cannot close the current occurrence. Expire uses the same target-only atomic
cleanup as explicit Close: cancellations are not successful deliveries, and
payloads/obligations owed to other destinations survive producer or target loss.

## Time outside the pure core

The product's small safe provider uses Linux CLOCK_BOOTTIME through nix. There
is no serialized Instant and no UTC-based deletion. Logical ticks are checked
u64 nanoseconds. Negative/backward readings, mismatched domains, arithmetic
overflow and invalid durable metadata refuse safely; they never become an
immediate expiration or an empty inventory.

One durable clock row stores `(domain, boot_tick, logical_progress)`. A recognized
domain encodes boot UUID and effective BOOTTIME namespace offset. The provider
requires the current and child time namespaces to match before and after the
bounded offset read, because Linux exposes the children's offsets in
`timens_offsets`. Equal boot/origin establishes continuity; boot UUID alone does
not. Missing/unverifiable evidence uses a random process domain and reports why.

Within one domain, logical time is committed progress plus checked boot-tick
difference. Startup commits its new anchor before temporal cleanup/readiness.
A changed or unproven domain preserves committed progress and counts no unknown
downtime; existing logical deadlines stay fixed. This can delay expiry, never
invent elapsed time. An already detached session retains its deadline across
all restarts. A finite session attached at crash is detached atomically at the
first reliable startup observation, since the exact lost-connection time is
unknown. Repeating the same OpenEpoch does not repeat this transition.

Temporal lifecycle commits coalesce their observed progress into the clock row.
With finite offline sessions, otherwise-idle maintenance requests a checkpoint
every30s, with at most one request/reply outstanding. It uses expected-anchor
CAS, absolute results and no operation receipt/history. A stale CAS response
updates knowledge; it does not reset the checkpoint age unless the returned
anchor covers the requested observation. No row accumulates per tick or session.

The ordinary target is30s plus actual scheduling/commit delay. At an observed
age over31s without covered confirmation, the product emits checkpoint_late;
storage unavailability suspends any fixed-delay claim. The provider's domain
recognition and storage availability remain assumptions, not a universal clock
or power-failure proof. This managed environment cannot inspect time namespace
origin; its normal runs explicitly use conservative reanchoring. Injected
provider tests separately establish continuous-domain and changed-domain cases.

## Bounds, format and reception

Quotas remain128 sessions and8192 receipts. A keyed timer index holds at most
one live finite deadline per session, replaced on Detach and removed on Open
or retirement. At most128 expiry facts have their own lane; only four are
attempted per turn. A blocked fact stays in that lane. Socket, closure, writer
result and ordinary-input budgets remain independent; an empty/blocked timer
does not accumulate historical heap entries.

The1024-position receipt window now admits business below512, reserving512
positions for128 inbound,128 outgoing and up to256 detach/expiry continuations.
The physical512 control reserve remains unchanged. Resolution and checkpoint
allocate no receipt; collection keeps its existing bounded maintenance lane.
Known certain Detach/Expire failure stops observably instead of silently losing
the continuation; it does not claim that the requested transition committed.
A singleton lifecycle flight and at most one checkpoint leave control-lane
capacity for receipt maintenance. No timer directly releases owner credits.

SQLite schema6 adds detachment columns and one clock table. Versions1..5 and
future formats are refused before write-affecting pragmas, with no implicit
migration/reset. Readers validate domain/identity/tick/policy/deadline shape and
row/byte budgets before allocation or selection. JSON preserves legacy expiry
strings and represents finite policy as `{"After":seconds}` with optional
bounded detachment/clock facts. Generic adapter submission events explicitly
name SessionLifecycle; writer results retain the specific Open/Close/Detach/Expire
kind. The stream reader validates actual retained events, not only examples.
The pure crates retain no_std + alloc and no I/O.

Reception comprises the actual-writer DISCONNECT0 RED/GREEN, injected boundary
and restart tests, differential stores, actual mqttium/Paho offline/active paths,
full128 capacity, shared payloads, genuine held results, separate same-file
SIGKILL runs, exact compiled mutations and a finite temporal model. These are
system/implementation/model evidence with separate scopes. Existing K1/K2b/K3
qualification does not qualify the new session semantics.
