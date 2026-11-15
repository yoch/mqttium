# 0021 — Typed, bounded durable reads through the surviving supervisor

Status: **Proposed**. Working choice of S0; not an acceptance of the global
decision records or permission to merge the integration branch into main.

## Problem

SQLite physical consistency does not establish semantic validity. A route
with QoS 99 can pass quick_check while snapshot panics and recovery reports
AllocationFailed. A 65,536-byte ClientID is copied before its protocol limit
is checked. Text-only startup and protocol failures lose their usable cause.

## Shared reader contract

`mqttiumd-store-api` owns driver-independent, value-free `ReadError` codes:
`InvalidData`, `LimitExceeded`, `Unavailable`, `Io`, `AllocationFailed`.
Only an actual fallible allocation failure uses AllocationFailed. Arithmetic
overflow and configured size refusals use LimitExceeded. Decode failures
use InvalidData; driver failures are classified by structured driver codes,
never by parsing their display text. Offending values are not diagnostics.

Ledger, snapshot, established-outcome lookup and epoch opening return Result.
An error cannot become a cached ledger, an empty state, absence, a certain
rejection or permission to resubmit. Opening may already have committed
before its final read fails; a typed error does not roll back that fact.
Write paths retain their conservative PendingOrUnknown outcomes when they
cannot establish a terminal result; this increment does not redesign them.

Recovery retains distinct normal refusals FrontierMoved and EmptyPage and
adds Read(ReadError). No later page substitutes for a failed page. A refused
frontier cannot cause an unlimited retry or silently reuse partial inventory.

ReadBudget bounds rows, aggregate logical bytes and each copied field.
Default snapshot: 65,536 rows, 32 MiB aggregate, 8 MiB per field. Default page:
4,096 rows, 1 MiB aggregate/per field. Field-specific durable/protocol limits
apply in addition, including MAX_CLIENT_ID_BYTES=65,535. Explicit
snapshot_with_budget/recover_with_budget allow smaller bounded calls. Sizes
and checked arithmetic are verified against borrowed SQLite values before
application-owned copies, within the same read transaction. Row count and
logical bytes do not claim a bound on SQLite's internal page cache.

The positional recovery cursor retains a finite S0 inventory ceiling of
65,536 facts (`MAX_RECOVERY_INVENTORY_ROWS`), including fixed facts. A bounded
count can discover one extra row to refuse a larger inventory; prior facts
are not constructed or copied while skipping a prefix. This avoids an
unbounded scan without introducing a keyset-cursor redesign in this increment.
Snapshot accounting charges one 256-byte metadata row, then 128 bytes per
durable row plus encoded cell bytes; page accounting charges 128 bytes per
fact plus dynamic field bytes. All counters use checked arithmetic. Opening
does not prevalidate later inventory facts, so a valid first page can be
observed before an error on the next page.

## Observation and lifecycle contract

The shared read phases are Open, Ledger, Opening, Lookup, Snapshot, Inspect
and Recovery. The fixture wrapper also preserves ReadError when it cannot
inspect the partially installed data set; earlier legitimate fixture commits
are not silently rolled back or recreated. A failure preserves its stable code, phase, relevant process
incarnation and, where established, page number. It must survive an error
before Started. Human-readable bounded text may accompany these fields;
neither the supervisor nor the oracle extracts machine meaning from it.

The trace formats add DurableReadFailed and ProcessControlFailed, specified
by the coordinated catalogue/schema increment. The supervisor records the
read or collection failure. A real signal-9 wait remains observed even if
post-kill evidence drainage subsequently fails. EOF at a frame boundary is
distinct from a truncated frame. One action has one coherent termination;
duplicates, contradictions and activity without progress consume finite
control/deadline budgets.

The independent oracle selects the first relevant lifecycle recovery before
requiring its complete corroborated inventory; a later incarnation cannot
supply missing proof for it. A business campaign missing durable evidence is
INCONCLUSIVE, while an already established safety violation remains FAIL.
Successful reception of that failure is not a qualifying PASS of the exchange.
Result formats preserve the structured failure as well as human explanations.

## Reception and limits

Tests first reproduce invalid QoS, oversized ClientID, first/later page
failures, missing first recovery, inconsistent action termination and broken
post-kill frames. Receive typed failures through both stores, external
inspection, the actual child/supervisor path and the normal oracle CLI, with
cleanup and no unjustified positive acknowledgement. Compare legitimate
epoch-opening effects explicitly; never restore or reset the tested store.
Fresh K1/K2b/K3 qualification uses the new source/oracle identity. Existing
K2a/WAL CI gates stay separate and bounded. The concrete core stays pure;
its readiness cannot be inferred from an incomplete recovery.
