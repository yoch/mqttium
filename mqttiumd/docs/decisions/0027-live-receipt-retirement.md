# 0027 — Live bounded receipt retirement

Status: **Proposed**. This implements the bounded producer-prefix direction of
ARC-019 within ARC-021, without hot epoch rotation or a global broker proof.

The pure owner issues contiguous checked OperationSeq values per
(StoreIncarnation, RunEpoch, LogicalOwnerId). A bounded issuance window records
terminal-result consumption and the publication/payload issuance boundaries at
each operation. Only application of a correlated, validated terminal result,
with its consequences retained in owner state/Outbox, consumes that result.
Receiving a writer reply, losing a socket, a timeout, object deletion, an MQTT
ACK or eviction from a history ring does not confirm consumption. A hole keeps
later consumed entries within the finite window. Live obligations, claims and
inbound QoS 2 state independently retain what their business continuation needs.

A ReceiptClosure closes OperationSeq, PublicationSeq and PayloadSeq prefixes
of one origin. Identity boundaries are captured after the operation's recipe
identities are issued; no eligible unknown admission can lie below them.
The opt-in live protocol reserves sequence zero for its empty prefix. An
unregistered legacy origin keeps its existing zero-sequence semantics; initial
registration refuses an existing zero operation/fence or admission identity
instead of silently retiring it. Product origins register before business input.
Store admission still enforces one origin for operation/publication/payload.
Above the identity boundary, retained exact receipts and live rows guard reuse;
below it, new operations cannot reuse publication/payload identities even after
all business objects disappear. No per-operation tombstone replaces receipts.
An alias refusal for a new operation is itself durably recorded/fenced: an
identity guard alone cannot prove every recipe under that new operation closed.

CollectReceipts is a control, not a Recipe. A transaction validates the bounded
new prefix (at most 128 new sequences, each with a terminal receipt/fence), advances all three
monotone guards, deletes at most 128 physical receipt/fence rows and subtracts
exactly the rows removed. Closure and deletion become visible atomically.
Logical lookup at/below the guard returns RetiredOutcomeUnavailable even when
a bounded sweep leaves physical rows. This forbids execution but does not prove
rejection. Retained nonretired receipts still compare exact canonical bytes.
Repeated or reordered maintenance reports absolute frontiers; its removed-row
count is never a reusable credit grant. An uncertain commit is reconciled by
reading/repeating the same closure. Maintenance creates no receipt recursively.

The product enables collection explicitly; existing fixture campaigns do not
silently acquire this new semantic scope. The origin window is 1024 issued
operations beyond the last durably acknowledged prefix; new business is limited
to a span of 768, leaving 256 sequence positions for continuation commands.
There are at most 128 inbound and 128 outgoing responsibilities. Resolution
uses the existing operation identity. The unchanged 8192 receipt budget reserves
512 rows from new business for terminations/fences, exceeding these 256 live
continuations plus bounded writer/unknown requests. Maintenance itself needs no
receipt slot. At most 64 compact origin rows exist; live work is separate.
Collection normally triggers after 64 consumed operations and continues bounded
sweeps when physical rows remain. Empty or hole-blocked producers do not spin.

Maintenance has a reserved writer request/reply lane. Its explicit Outbox
controls may bypass blocked network effects, without reordering business store
commands or network frames. Ordinary plans leave the required control slots.
Obsolete operation timers can be removed only from the pure owner's explicit
liveness query after application; uncertain operations retain their deadlines.

Startup opens its single new epoch through the existing authority protocol.
Bounded closed-epoch sweeping can then collapse old origin guards behind the
existing collected_through ledger, without deleting live business records or
inventing current-origin consumption. It does not reset an incomplete recovery.
SQLite schema 4 refuses older formats before write-affecting pragmas; no implicit
migration/reset. Readers preserve typed failures and bounded rows/bytes.

The receipt budget ceases to be a cumulative live-process traffic ceiling.
Outgoing QoS 1 Packet Identifiers, finite session/epoch identities, storage
availability and other profile limits remain. The new reception is system-test
evidence; K1/K2b/K3 retain their own qualification. A focused finite model covers
closure/gaps/identity guards and their assumptions, not full MQTT liveness.
