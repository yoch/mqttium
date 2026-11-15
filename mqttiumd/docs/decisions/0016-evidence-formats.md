# 0016 — Trace records of the input audit, what every run owes, the perimeter, and the store snapshot

Status: **Proposed** (15 November 2026, lot L3, first wave). Written by lot
L3, recorded by the integration owner. It completes record 0003, DEM-011,
DEM-016, DEM-017 and DEM-019. It becomes a requirement only when the owner
of the repository accepts it.

## Decision

### The input audit

- Every record kind is stated by one origin only; a record of a kind
  stated by another origin makes the trace unreadable. Two origins join
  the existing ones: `dispatcher` and `timer`.
- An input the core consumed is recorded by the core (`PacketDelivered`,
  `StoreResultDelivered`, `DeadlineDelivered`, `RecoveryPageDelivered`).
  What makes it legitimate is recorded by another origin: `PeerPacketSent`
  by the peer and `ClientBytesReceived` by the transport;
  `CommandSubmitted`, `ResolveSubmitted` and `RecoveryPageRead` by the
  store adapter; `DeadlineArmed` by the timer.
- `broker_inputs_have_provenance` holds when each consumed input has its
  counterpart. A result answers a submission with the same effect and
  operation that no earlier result answered. A deadline carries a token
  armed and not yet delivered. A recovery page equals a page the store
  adapter read, fact for fact. A packet is exactly the bytes the transport
  received on that connection, and those bytes are caused by a packet of a
  peer that carries the same bytes.
- In v1 one `ClientBytesReceived` carries one whole packet. A transport
  that records fragments is a contract change.
- A recovery page lists its facts in the trace: a digest would hide what
  DEM-MU-03 changes. The facts follow the order of record 0014.
- Hidden truth of the store judges a run and is never an input:
  `StoreCommitStable` names the command kind, `StoreResultReady` the effect
  it answers, `StoreFenceStable` and `StoreFenceRefused` what became of a
  fence, `FaultHit` the operation and effect it stopped.

### What every run owes, and its perimeter

- Each entry of the assertion catalogue declares `every_run`. A qualifying
  result reports the assertions of its scenario and those every run owes,
  `trace_complete` and `broker_inputs_have_provenance`. A scenario does not
  have to list them and cannot leave them out.
- A trace header declares the `perimeter` of its run, `full` or
  `core-without-bytes`. A run is judged only against a scenario of its own
  perimeter; a run without bytes never passes a full scenario.

### The store snapshot

- `InspectDurableState` writes one document `mqttiumd.store-snapshot` per
  checkpoint, read from the store independently of the broker, and the
  trace cites it by digest. It is evidence for the oracle and never an
  input of the broker.
- It lists the durable sets of DEM-011 as tables, each present even when
  empty, rows in the order of their key. Its metadata hold the
  incarnation, the last opened epoch and the attempt that opened it
  (record 0011). An operation carries the exact canonical bytes of its
  recipe.
- A snapshot shows what the store holds, sound or not. Readers check form
  only: 64-bit ranges, unique keys, order. Whether the rows are sound
  together is asserted by the oracle: a faulty store must produce a
  readable snapshot and a failed assertion.
- A decision in a snapshot is terminal: an unknown outcome is knowledge of
  the core, not a durable fact.

## Consequences

- The collector of lot L4 attributes `dispatcher` and `timer` records from
  a channel of their own.
- A result produced before `every_run` no longer qualifies.
- The oracle decodes MQTT headers itself to compare bytes and packets; it
  shares no code with the broker.
- A new durable table or phase is a contract change.

## Limits

- No bench produces these traces or snapshots yet: the oracle has judged
  hand-written traces only.
- One quantity has two encodings today: the limits of an admission are
  JSON integers in a recovered fact of a trace and decimal strings in a
  snapshot. It is an open point of the review of lot L3.
