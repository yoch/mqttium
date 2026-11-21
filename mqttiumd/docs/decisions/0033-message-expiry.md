# 0033 — Message lifetime, transmission authority and temporal quarantine

Status: **Proposed**. This extends the implemented profile without accepting
0030, 0032 or other proposed decisions.

Note (21 November 2026): the budgets this record states for sixteen connections are
the bounded profile of `runtime::Limits`. The connection limit is an option
since record 0046 (proposed), and the queues sized for sixteen connections
grow with it.

## Lifetime and temporal authority

Message Expiry is separate from Session Expiry. Absence means no lifetime;
present zero expires at reception; every positive u32, including 0xffffffff,
is a finite number of seconds. Reception freezes an immutable domain, raw
CLOCK_BOOTTIME tick and interval when the complete TCP frame is read, before
the reader channel, pending CONNECT queue or core ingress deferral. Deadline arithmetic
is checked. Retry, Resolve, retained replay, replacement and PUBREL never
renew this origin. Replay copies the original metadata into its own occurrence.

The existing safe provider supplies time outside the pure core. An equal,
established boot/origin domain counts checked tick differences, including a
process interruption when origin continuity is recognized. A process-private
domain establishes continuity only within that process. A domain mismatch is
temporal uncertainty, never elapsed time or freshness. Existing expiring values
and unsent work are quarantined and retain their bounded references; they cannot
be replayed or newly emitted. Replacement, clearing or explicit target-session
termination can release these responsibilities. This deliberately loses
availability rather than publishing potentially stale state. It is not strict
cross-restart expiry compliance on an unrecognized time origin.

Unreadable, regressive and overflowing observations refuse or suspend the
affected path observably. They never saturate to an expiration or reset a TTL.
UTC is diagnostic only and cannot authorize deletion. No new time dependency
or historical anchor table is introduced. Session expiry retains its separate
conservative logical-time policy from 0030.

## Admission and replay

QoS1/2 admission commits expiry, retained replacement/clearing, all live
responsibilities, references and result together. Inbound QoS2 handshakes
continue even after the Application Message expires. Retained QoS0 remains a
durable operation without an invented MQTT identifier, acknowledgement or
Receive Maximum credit; ordinary QoS0 remains volatile.

The original publication owns expiry independently of its producer session.
Retained and each owed copy carry the same temporal origin. Subscription updates
use one frozen admissible observation when materializing ordered RH replays.
They skip established-expired or temporally uncertain values, never re-read the
current value during emission and never create a fresh lifetime. Existing RH,
RAP, No Local and overlap semantics remain those of 0032.

## Transmission boundary

A claim reserves an identifier; it does not prove that bytes left a socket.
Every zero-progress socket write attempt checks the frozen expiry and patches
the remaining interval immediately before trying the first byte. Remaining
seconds round upward within the original integer interval; established expiry
blocks a new transmission. Once positive bytes have been written, that MQTT
frame completes or its connection closes. Mid-frame expiry cannot truncate it.
The pre-attempt sample establishes local write eligibility; neither physical
network departure nor arbitrary host preemption is timestamped or bounded.
This is an explicit limitation, not a strict real-time wire-deadline claim.

A separately correlated first-positive-write observation may establish a
durable Started marker for this exact delivery and claim. A known started QoS1
exchange keeps its payload, identifier and correlation through PUBACK and known
Finish, including after the lifetime. Retransmission keeps the original claim
and reports remaining zero after expiry, rather than inventing freshness.

The crash window between first write and durable Started cannot be eliminated
by pretending that Claim is transmission. A recovered claim lacking Started
is transmission-uncertain. If its lifetime is no longer established fresh, it
is quarantined, not re-emitted or freed as certainly unsent. This bounded
availability limitation is explicit. A NotStarted observation applies only to
its exact frame/connection and cannot erase an earlier attempt's uncertainty.
Only claim-level certain-never-started work can be conditionally expired.

The PUBACK association is registered before socket submission. A valid PUBACK
may precede collection of the local callback. Old start/stop facts, durable
answers, pages and callbacks cannot act on a new claim or connection generation.

## Cleanup, capacity and formats

A receiptless writer-maintenance lane scans at most four publications per
request, with a live key cursor, one frozen observation per complete scan, one request
outstanding and absolute results. A stopped clock cannot reuse a completed
scan header. A lost-result retry preserves its header and has a three-attempt
budget; stale/duplicated replies are ignored without returning credit twice.
It removes current retained references and unclaimed expired recipient copies
conditionally, preserving inbound QoS2 markers and every potentially started
claim. Removed identities are reconciled with the owner; the timer itself
grants no credits. Exact, guarded expiry of a certainly unsent claimed copy
uses the normal correlated operation and unknown-result resolution.

Each delivery has at most one Start/Finish/Expire control flight at a time;
known results are consumed before allocating a subsequent control identity.
The existing512 control-window reserve remains shared with inbound releases
and lifecycle work; resolution and receiptless maintenance need no new receipt.
Two socket facts per accepted expiring frame require544 terminal input slots,
while frame registration stays208 and the physical/business quotas stay fixed.

All existing quotas remain:32 retained topics/1MiB,128 references/4MiB shared
payload,16 live destinations,8 filters,256 replay candidates and8192 receipts.
Old owed versions remain charged until their own responsibility ends. Work
progress and controls retain independent bounded lanes; no row is added for
each scan or clock reading.

New recipe tags13..16 extend canonical format1; historical tags1..12 retain
their bytes and meaning. SQLite schema9 explicitly refuses old/future formats
before write-affecting setup; no reset or migration is supplied. Readers
validate lifetime shape, checked deadline and Started/cause relationships before
selection or allocation. Default new fields are omitted from legacy evidence.

## Evidence and scope

MQTT5 sections3.3.2.3.3,3.3.1.3 and4.1 guide this feature; QoS2 acknowledgement
continuation also follows4.3.3-13. Finite model results, implementation tests,
network system receptions and historical oracle qualification remain distinct.
The preview claims only its exercised profile, with explicit quarantine on
unknown continuity, not universal clock, MQTT, performance or power-loss proof.
