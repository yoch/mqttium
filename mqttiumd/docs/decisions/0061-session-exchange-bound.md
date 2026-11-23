# 0061 — A session holds a bounded number of QoS 2 exchanges, and an exchange that awaits its PUBREL holds no payload

Status: **Proposed** (23 November 2026). Applied to `mqttiumd` (core tier
and runtime, ADR 0044). It closes the point record 0060 left open, by the
decision of the repository owner: a bound per session in the admission of
the core, and a table sized for the sessions.
[Report 0035](../reports/0035-session-exchange-bound.md) holds the
evidence.

It changes one public type of the core tier: `OwnerConfig` has a new field,
`session_qos2_exchanges`. `LeaseTable` has a new method, `lighten`. No durable
encoding, injection point, schema or contract file changes; the contract
digest that `validate` prints moves because the register lists this record
and the records it amends.

## Context

An inbound exchange holds an entry of the core's exchange table and a lease
of the core's account from its admission to its end. A QoS 1 exchange ends
by the broker's own PUBACK. A QoS 2 exchange ends by its PUBCOMP, which
needs the PUBREL of the client: between PUBREC and PUBREL, only the client
can end it.

The runtime bounds a connection to eight exchanges (`receiving::MAXIMUM`,
announced as Receive Maximum to MQTT 5). Nothing bounded a session. A
session that reconnects leaves the exchanges of its earlier connection at
PUBREC and gets a new window. Two resources of every client were held that
way.

- **Entries.** The table had `max(128, connections × 8)` entries. With
  `--max-connections 16`, one MQTT 3.1.1 session that left eight exchanges
  at PUBREC on each of sixteen connections held the 128 entries.
- **Payload bytes.** The lease of a live exchange kept the bytes of its
  payload until PUBCOMP, although the publication was committed and
  routed at PUBREC and an exchange recovered after a restart holds none.
  Eight sessions that each left eight exchanges of 1 MiB at PUBREC held
  the 64 MiB the core lends to every admission.

In both cases a QoS 1 or QoS 2 PUBLISH of any other client found no credit,
waited, and its connection was closed after five seconds
(`input_capacity`), for as long as those sessions lived.

## Decision

### A bound per session, at admission

`OwnerConfig::session_qos2_exchanges` is the number of inbound QoS 2
exchanges one session can hold at a time, across its connections. The
product sets it to eight, the window. A PUBLISH received again for an
exchange the session holds is not a new exchange and is answered as
before.

Only QoS 2 exchanges are counted: they are the ones a client can keep
open. A QoS 1 exchange ends by the broker's own PUBACK, or with its
connection, and a QoS 1 publication is admitted whatever the session
owes. Counting it would also make a publication wait for the receipt the
core gets of a PUBACK already on the wire, which record 0024 excludes and
`a_new_identifier_progresses_while_the_old_core_ack_receipt_is_held`
tests.

A QoS 2 publication that would be one more than the bound gets one of two
answers, by who can make room.

- **The owner is admitting or releasing one of the session's QoS 2
  exchanges**: an admission whose result is awaited, a first PUBREC held
  behind an earlier answer, a release in flight. The publication waits
  (`NeedCapacity`, `ExchangeSlots`, `ResourceReleased`), as it waits for a
  credit of the table. The wait is the broker's to end and ends with its
  own progress. This is the case of a client that sends its PUBREL again
  and then publishes.
- **Every QoS 2 exchange of the session awaits a PUBREL**: only the client
  can make room. The connection is closed (`CloseReason::CapacityExceeded`,
  event code `capacity_exceeded`); the publication is not acknowledged, so
  it is not lost: the client still owns it. A wait here would be the
  client's to end, and the queue of waiting inputs of the runtime resumes
  from its head: a publication that waits for a PUBREL that never comes
  would hold the waiting packets of other connections behind it.

For MQTT 5 the client was announced Receive Maximum 8, and the
specification counts against it the publications for which the client has
not received PUBACK or PUBCOMP, whatever the connection ([MQTT-3.3.4-7]):
a ninth QoS 2 exchange of the session is beyond it. For MQTT 3.1.1 nothing
is announced. A client that follows [MQTT-4.4.0-1] sends
again, when it reconnects, the PUBREL of every exchange acknowledged with
PUBREC, and never meets the bound: the connection of a 3.1.1 client still
holds a ninth publication back rather than refuse it (record 0060).

### An exchange that awaits its PUBREL holds no payload

When an exchange comes to await its PUBREL, by the result of its admission,
by a held PUBREC that leaves, or by the end of the connection that held
that PUBREC, its lease is replaced by a lease of one exchange and no byte:
what the lease of a recovered exchange holds. The bytes of the account are
those of the publications the owner is admitting or acknowledging.
`LeaseTable::lighten` does it in the slot of the lease, through the release
and the charge the table already has (record 0010): the old key is stale,
the slot moves to its next generation. A slot at its last generation keeps
its lease as it is.

A QoS 1 exchange keeps its bytes until its PUBACK is written, as before.

### A table sized for the sessions

The product sizes the exchange table as `SESSIONS × receiving::MAXIMUM`,
32,768 entries, whatever `--max-connections`: every session the broker can
hold can owe its eight PUBREL at once and an entry is left for none of
them to take from another. It is the size the default limit of 4,096
connections already gave, and the size of the exchange account of the
store. `max(128, connections × 8)` of record 0051 is replaced.

### MQTT 5 is told why

A connection the core closes with `CapacityExceeded` is sent DISCONNECT
0x97 (Quota exceeded) first when it is MQTT 5, the way a taken-over
session is sent 0x8E. This covers every cause of that close: the bound of
this record, a topic or a payload over the limits, more targets than one
admission may have.

This amends records 0046 (the broker no longer holds 128 credits for all),
0051 (the formula) and 0060 (its open section).

## Consequences

- A session that owes eight PUBREL and publishes a new QoS 2 message is
  closed. A device that loses its state, keeps its client identifier and
  its session, and never sends those PUBREL can publish nothing at QoS 2
  until its session ends (Clean Session, Clean Start, expiry); it still
  publishes at QoS 0 and 1. Before this record it went on until the table
  of every client was full.
- The test of upstream ticket 3809 written with record 0060 stated the
  earlier behaviour: twenty-four exchanges left at PUBREC by one session,
  all acknowledged. It now states this one.
- With a small `--max-connections` the broker reserves the table of the
  default limit. Report 0035 has the measured memory.
- The readiness event reports `max_exchange_slots` 32,768 for every
  connection limit.
- Core tests that stated the bytes of an exchange at PUBREC now state none.
- The bench of the lab sets no bound of its own
  (`session_qos2_exchanges` at its maximum): the table of a fixture bounds
  its scenarios as before.

## Not chosen

- **A refusal on the wire** (PUBREC or PUBACK 0x97 from the core). It
  keeps an MQTT 5 connection open, but the core has no answer of its own
  for an exchange it did not admit, and such an answer has to take its
  place among the held acknowledgements of the connection (record 0050).
  MQTT 3.1.1 cannot carry it and would be closed all the same.
- **A larger bound.** Any bound stops the device described above at some
  count, and the table grows with it.
- **Forgetting the oldest exchange.** A PUBLISH received again for it
  would be delivered twice.

## Not done

- The table also holds the exchanges the broker is ending by itself: QoS 1
  exchanges until their PUBACK is written, and exchanges of a session that
  ended whose operations are still in flight. They are bounded by the
  windows of the connections and end with the broker's progress; with
  every session at its bound the table can be asked for more than
  `sessions × 8` entries, and a publication then waits for one.
- A database written before this record can hold a session with more than
  eight exchanges. They are recovered; that session admits nothing new
  until it is under the bound.
- The wait of a publication for a credit still resumes from the head of
  one queue (record 0046).
- `lighten` is covered by the unit tests of the lease table, not by the
  proof of `release_if_current`, whose files are unchanged.
- No model has the bound.
