# ADR 0048 — Capacities of the runtime and their derivation

Status: **Accepted** (21 November 2026, decided under the repository
owner's delegation on the advice of the independent audit of `8e4303d`).

## Context

Eight records (0022, 0025, 0026, 0029, 0033, 0034, 0035 and 0043) state
budgets of the product runtime as numbers for sixteen connections. Since
record 0046 the connection limit is an option and `runtime::Limits`
derives the queues that grow with it. The numbers of those records were
read as rules; they are the values of one profile.

## Decision

**The sixteen-connection numbers describe the bounded qualification
profile.** The effective capacities of the runtime and their derivation
are published for each supported connection limit. Changing that limit
leaves durable quotas, writer admission limits and the scope of historical
evidence under their own decisions.

**Derived from physical facts**, per connection limit `c`:

| Capacity | Derivation | At 16 | At 128 |
| - | - | -: | -: |
| Closing inputs | two facts per transport | 32 | 256 |
| Frame registrations | per transport: 4 output places, 1 active frame, 8 terminal receipts | 208 | 1,664 |
| Terminal inputs | two facts per registered frame, plus the effect box | 544 | 4,352 |

**Multiplied for every further sixteen connections**, with `s` the number
of sixteens that hold `c`:

| Capacity | Derivation | At 16 | At 128 |
| - | - | -: | -: |
| Ordinary inputs | 92 per sixteen, and eight per connection for the publications its announced window lets wait at once (record 0046); the 32 places of the writer and the 4 of the deadlines are apart | 220 | 1,760 |
| Effect box | 128 per sixteen | 128 | 1,024 |
| Outgoing identifier bindings | 128 per sixteen, eight per connection | 128 | 1,024 |
| Timers | 512 per sixteen | 512 | 4,096 |

**Independent of the connection limit**: 128 durable sessions, 16 targets
per publication, 32 writer requests, 16 commands in flight, one lifecycle
operation in flight, eight simultaneous TLS handshakes. A connection limit
above 128 is refused: more transports than sessions could never all be
established.

`Limits::new` brings a value outside 1 to 128 to its nearest bound. The
readiness record of the executable publishes the effective values.

## Consequences

- A record that states one of these numbers states the bounded profile.
  The eight records above carry that note.
- The paliers of the second table are not derived from a physical fact.
  At 17 connections they give 256 bindings where eight per connection
  need 136. They are safe upper bounds, not justified ones.

## Left open

- The capacities of the second table are to be measured at 1, 16, 17, 127
  and 128 connections and replaced by derivations where one exists.
- A ceiling in a configuration proves neither that it can be reached
  together with every other ceiling nor that the cost of serving it
  stays acceptable: the load at 64 connections collapses under overload
  (report 0027, open conditions).
- The durable quotas (sessions, subscriptions, retained topics, targets)
  and the cost model of the core are decided elsewhere.
