# 0003 — Provenance of the broker's inputs; detection of DEM-MU-03

Status: **Accepted with amendments** (14 November 2026, review of PR yoch/mqttium#647).
Applied by lots L2 (correlation), L3 (assertion, collector) and L4.
Completes DEM-012 and DEM-018.

## Context

DEM-MU-03 injects into the bench the fault "re-inject the `OperationId` the
bench remembered during recovery". A broker that receives `op_A` from the
bench and a broker that recovers it from the store behave identically, so
no assertion on packets or durable state can fail for this mutant. A
criterion based on the order of events ("the identity appeared before the
broker produced it") is not enough either: it depends on labels the bench
writes about itself.

## Decision

### Provenance policy

Every input of the broker belongs to exactly one of four classes, and
nothing else is an input:

1. **Durable reader**: a page read by the store adapter from the store the
   broker itself opened.
2. **Identities created by the core**: minted in this incarnation by the
   owner's counters.
3. **Correlated results**: the answer to an effect the core emitted. The
   dispatcher keeps the effects in flight and refuses a result whose
   `EffectId` is not among them.
4. **Client bytes**: bytes that arrived through the transport.

### Enforcement is structural

- The control channel of ADR 0002 has no message that carries an identity
  towards the broker.
- A result that correlates to no effect in flight is refused by the
  dispatcher and recorded.
- Recovery facts are built by the store adapter only.

### A label is not a proof

`origin_component` in a trace record is what the emitter says about
itself. The collector assigns the origin from the channel a record arrived
on and refuses a record that claims another one. The oracle reasons on the
collector's attribution.

### Detection

An assertion `broker_inputs_have_provenance` joins the catalogue with lot
L3. DEM-MU-03 must fail it: the remembered identity can only arrive as an
uncorrelated result or through a channel that has no such message.

## Consequences

A scenario binding stays a name known to the bench and the oracle only. The
trace format gains the records this audit needs with lot L3, as a contract
change.
