# 0001 — S0 drives the core through an unknown outcome without a crash

Status: **Accepted with amendments** (14 November 2026, review of PR yoch/mqttium#647).
Applied by lots L1 (transitions), L3 (registry rows, oracle) and L4
(scenario). Amends DEM-017 and DEM-024; completes DEM-015.

Amended by 0015 (record 0047): step 3 of scenario P1-02b is corrected there.

## Context

P1-02 as written in DEM-017 drops the commit result and crashes the broker
immediately afterwards. After the restart the admission is found through
the durable inbound slot (session, Packet Identifier 7), which is the
`AwaitPubrel` row of ARC-011. The path "result lost, process alive" is never
taken by the core:

- `ResolveAndFence` and `Deadline` have no end-to-end witness in S0;
- DEM-MU-05 (a late unknown result replacing a terminal one) has no S0
  scenario to live in;
- QABS-A, B and C exercise the store alone, not the core's reaction.

MQTT 5 forbids a client to resend a PUBLISH on a live connection: resending
happens only after a reconnect with a present session (MQTT 5, section
4.4). On a live connection nothing the client does can restart a stalled
admission, so the broker must resolve it by itself.

## Decision

### Scenario P1-02b

P1-02b joins lot L4 and the S0 reception. It runs without a crash and
delivers an old `PendingOrUnknown` after `Committed`:

1. The producer publishes QoS 2 with Packet Identifier 7. The core publishes
   `Persist` for the admission and the adapter accepts it.
2. The bench holds the command before it enters the writer.
3. The first deadline fires. The core emits `ResolveAndFence`. The fence
   cannot be established while the command is held (the QABS-C condition),
   so the store answers `PendingOrUnknown`. The bench holds that answer.
4. The bench releases the command. It commits. The bench drops its result.
5. The second deadline fires. The core emits a second `ResolveAndFence`,
   receives `Committed`, and PUBREC is observed on the same connection.
6. The bench delivers the held `PendingOrUnknown`. Knowledge stays
   `Committed`; no effect and no frame follow.
7. PUBREL, release, PUBCOMP.

The client sends nothing between its PUBLISH and the PUBREC. Assertions: one
admission for the exchange, the same obligations as P1-02, no positive
acknowledgement before the core knew of the commit, terminal knowledge
never regressed, every injection reached.

### When the delay starts

The delay of resolution starts when the adapter accepts `Persist`, not when
the core publishes it. The transition publishes `Persist` and then
`ArmDeadline` into the owner's effect box. The box dispatches in order and
stops at the first refusal, so the timer adapter receives `ArmDeadline`
only after the store adapter accepted `Persist`. `ArmDeadline` carries a
delay, counted by the timer adapter from its own acceptance. A `Persist`
that is still in the box has no deadline running: nothing about it is
unknown yet.

### Guards

- A `Deadline` input is applied only when its token is the one armed for an
  operation that is still unresolved. Tokens are never reused. A deadline
  for an operation that became terminal is ignored as stale.
- A result of `ResolveAndFence` is matched to its operation through the
  `EffectId` the core issued.
- PUBREC is emitted only when the exchange generation recorded with the
  operation is still the generation of the slot, and on the connection
  generation that is current at that moment. The commit is reconciled even
  when neither holds.

### Replayed PUBLISH

After a reconnect, a PUBLISH replayed for the exchange while its outcome is
unknown creates no command and no acknowledgement; the pending resolution
continues, and PUBREC goes to the new connection once it completes. The
oracle role writes this row, and the row for the same packet on a live
connection, into `contracts/v1/p1.transitions.json`.

## Consequences

- `P1-02b` is in the requirement register with this record as its source.
- The contract type `ArmDeadline` carries a delay (`LogicalDuration`).
- The scenario format needs vocabulary it does not have yet: firing a
  deadline, releasing a held command, delivering a held result. Lot L3
  proposes it as a contract change; see the open conditions.
