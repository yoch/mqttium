# 0036 — Offline monotone mTLS policy transitions

Status: **Accepted** (21 November 2026, by the repository owner). This decision
extends the schema 11 product boundary of 0035, which is still proposed. An existing secure database can gain principals, ClientIDs
and permission subtrees while its business history and authority are retained.

## Authority and inclusion

The broker and offline command use one bounded parser, one canonical
authorization fingerprint and the same literal-level prefix relation. Every
old principal, ClientID owner and permission must still be covered. For a
permission to hold, the old root must be equal to a new root or a descendant
at a slash boundary. This inclusion is valid for MQTT topic names and for
the allowed subscription filters starting with the same literal levels.
The selector keeps its existing dollar-topic rule.

The operator provides both configs. The authority singleton, complete
ClientID registry, installed session triggers and live owner rows are checked
against the old authorization policy. Existing credentials can rotate within
a principal under the existing rules. An old pin supplied in both configs
cannot change principal within a plan. Pins are absent from durable
authorization as in 0035. Separate normalized credential digests bind a
plan to the supplied pins and never claim that SQLite authenticated them.

## Plan and commitment

`mqttiumd.policy-plan.v1` contains a SHA-256 over compact serialization of
the fixed-order plan body, prefixed by the format name and one zero byte.
The stored form uses the tool's deterministic pretty serialization. The
body includes schema version, old and new authority and credential digests,
the business fingerprint, incarnation, revision, inventory totals, planned
additions and refusal reasons. A denied plan is inapplicable. The tool
recomputes the decision from the two policies on apply.

Plan reads one frozen SQLite snapshot. It checks the version and expected
tables, SQLite integrity, the bounded product snapshot, target relationships
and reference and account totals. It hashes rows in fixed table and key
order with typed, length-delimited cells. The digest includes the semantic
schema, existing live session owners, metadata, routes, retaineds, shared
payloads, obligations, claims, inbound exchanges, Wills and receipts,
including the fences and frontiers. Only the authority singleton and the
ClientID owner registry are outside this business digest. Their contents are
checked separately. The inventory uses the existing snapshot bounds and a
128 MiB page-image cap.

Apply uses the writer's adjacent file lease, the exact database path and a
local filesystem. The operator stops all writers and preserves the lock
file. Using aliases with different adjacent lock names or direct SQLite
writes is outside this coordination protocol. The tool rejects final
symlinks and multiply linked database files.

Under the lease, apply rechecks the plan and copies the frozen old snapshot
through the SQLite backup API. The private, exclusive temporary file becomes
the requested backup only after checking its schema, authority and content,
closing SQLite and syncing the image. Publication uses a non-replacing hard
link in the same private directory and a directory sync. The copy is
standalone and contains committed WAL pages. After backup, apply acquires an
immediate transaction, rechecks the old authority and business fingerprint,
stages the fingerprint and inserts only new ClientID owners. The full new
authority and unchanged business digest are checked before one synchronous
FULL commit and again before success. Session owner rows and causal counters
remain as they were.

On an uncertain result, a repeated command recognizes the full new
authority only with the same planned inventory. It reports `AlreadyApplied`
without writing. A different authority, changed state or corrupt record
causes a typed refusal. Restrictive policy changes require a different
operation with explicit reconciliation of historic MQTT responsibilities.

## Evidence boundaries

The instrumented offline command has two barriers, `policy.fingerprint_staged`
inside the transaction and `policy.committed` after commit and before
readback. SIGKILL at each point leaves complete old or new authority.
Specific compiled controls require detection of a grant reduction, a
ClientID transfer, a fingerprint committed ahead of owner insertion and a
write to a retained table. The packaged demonstration uses normal binaries
and external MQTT clients. These results concern the offline protocol and
the observed process failures. Existing MQTT campaign qualifications keep
their previous scope.
