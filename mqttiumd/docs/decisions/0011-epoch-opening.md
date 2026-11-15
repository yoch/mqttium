# 0011 — The opening of an epoch is one conditional, idempotent command with an identity

Status: **Proposed** (15 November 2026, lot L2, first wave). Written by
lot L2 after four reviews of its contract change, recorded by the
integration owner. It amends record 0009, section "A durable fact is not
an authorisation to construct". It becomes a requirement only when the
owner of the repository accepts it; until then the requirement register
does not list it.

## Context

Record 0009 asks for at most one creation authority per live opening of an
epoch and gives lot L2 the contract of the opening. It does not say what
the command is, what the store keeps, or how an unknown outcome is
resolved. Each rule below closes a counterexample found in review.

## Decision

### An opening closes every older epoch in the same transaction

`OpenEpoch` is one command. In the order of authority of the writer it
closes every epoch opened before and opens the next one, or changes
nothing. There is no separate command that closes the older epochs.

Reason: DEM-012 and V3-006 require the older epochs to be closed before
admissions reopen. With two commands, a crash or an unknown outcome between
them leaves a state the contract would have to name: older epochs closed
and none open, or two epochs open, in which a command of the dead process
could execute beside those of the new one.

### An opening is conditional on the last opened epoch

Start-up verifies the data set and reads the last epoch opened in it;
`OpenEpoch` carries both. The writer opens the epoch after the one the
command expects only while that epoch is still the last one opened.

Reason, by counterexample: attempt 1 is held on its way and start-up gives
up on it; attempt 2 commits epoch 5 and its grant is handed over; the
writer then applies attempt 1, which opens epoch 6 and closes epoch 5. The
process holds an authority for a closed epoch.

### An attempt has an identity the store knows

It is the nonce its start-up drew, 128 bits, and its number within that
start-up. The command carries it, the ledger keeps the attempt that opened
the last epoch, and every reply carries the whole command it answers.

Reason: the ticket alone is a counter that restarts at one in every
process. Two processes alive on one data set, the older one only presumed
dead, would both submit ticket 1 on the same condition; with an idempotent
command both would be answered the same commit, and two authorities would
exist for one epoch. The nonce is what tells their commands apart. It also
keeps the reply of a dead predecessor, or of another opening, from being
taken for one's own.

The nonce is drawn outside the pure crates, by the store adapter. That no
two start-ups share one is a precondition of that trusted code, not a
property of a type: **the safety of an epoch's identity rests on these 128
bits**. In the deterministic store the nonce is derived from the seed of
the run and the rank of the start-up, so that a restart inside a scenario
never draws the nonce of the process it replaces. A test shows what a
shared nonce does: two grants.

### An attempt designates one command

As an `OperationId` designates one recipe, the identity of an attempt
designates one `OpenEpoch`. The store refuses, for certain and for good,
the opener of the last epoch that asks for the next one under the same
identity (`AttemptReused`): the precondition of the nonce is broken and
that start-up stops. Two limits remain and are trusted code: two start-ups
with one nonce that read the same value form the same command and are both
answered with its commit; and once a later epoch was opened the ledger no
longer holds the identity to compare with.

### The ledger is an epoch and its opener

The durable state of the epochs is the incarnation, the last opened epoch
and the attempt that opened it, 24 bytes: the nonce, then the ticket, big
endian. An opened epoch without a recorded opener, or an opener without an
opened epoch, is not a ledger: a store that reads such values is corrupt
or is not this format, and opens nothing. Interpreting them would tell the
command that did open the epoch that it opened nothing.

### The command is idempotent, and only a certain rejection says that nothing happened

The command that opened the last epoch, presented again, is answered the
same commit and writes nothing. Another attempt on a condition that was
taken is rejected for certain. An attempt whose epoch was opened and since
closed is *superseded*: the ledger keeps the opener of the last epoch
only, so it no longer knows whether that attempt was the one, and it does
not call that a rejection. A condition ahead of the ledger is undecided:
it could still come true, so a certain rejection could be contradicted
later. That last case arises only when a data set was restored to an
earlier state without a new incarnation.

### A commit is read, not decided

A store reports `Committed` only by reading the ledger that is durable
after it handled the command, and only for the command that ledger records
as its opener. The decision to open carries no epoch and reports nothing.

### An unknown opening is resolved by a new opening, never by adopting what is read

The outcome is unknown when the store says so or when start-up stops
waiting; the second is a transition of the process, not a reply. Start-up
then reads the last opened epoch again and submits a new attempt on that
value. Reading the value forms a condition and adopts nothing: the grant
comes only from the committed reply to the new attempt. After the store
answered that the condition moved, the value read again must be higher.

### A reply is honoured only for the command in flight

A reply whose command is not exactly the one in flight changes nothing.
Once the command is recognised, a commit for another data set or another
epoch than the one after its condition cannot be true: with a commit
together with a rejection, it stops start-up. The opening is then faulted
for good.

### A commit is not forgotten, and a grant for a closed epoch is never handed over

`Superseded` says that the epoch an attempt asked for is closed; it does
not say that the attempt did not commit. An opening keeps whether the
commit of its attempt was reported, before or after `Superseded`, and a
certain rejection of the same attempt is then a fault that stops start-up.
A grant for an epoch known closed is never handed over, even if the commit
of that attempt arrives later or was already held. A grant that was handed
over cannot be taken back: the opening records that its epoch was reported
closed and stays there, with no new attempt and no recovered fact
confirming that grant; stopping its holder is start-up's (lot L4).

## Consequences

- `crates/mqttiumd-contract/src/command.rs` and `outcome.rs` gain the
  opening: `OpenEpoch`, `OpeningAttempt`, `StartupNonce`, `EpochLedger`,
  `OpeningReply`, `EpochOpening`, `EpochGrant`. The contract digest is
  unchanged: no file under `contracts/` is touched.
- Lot L1 builds the creation authority by consuming an `EpochGrant` and in
  no other way.
- Lot L3 shows the opener in the store snapshot. Lot L7 stores it: one
  more value in the metadata of the data set.
- Lot L4 wires start-up through this path and stops the holder of a grant
  whose epoch was reported closed.

## What the fourth review leaves open

Five points in the handling of replies that disagree or arrive in another
order, found on the revision that was merged into the integration branch
(`251026a1`). They are open conditions of the reception of lot L2; see
`../reception/open-conditions.md`.
