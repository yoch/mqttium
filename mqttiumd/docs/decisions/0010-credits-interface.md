# 0010 — Credits: the interface of `release_if_current`, its guards and its precondition

Status: **Accepted** (21 November 2026, by the repository owner;
proposed 15 November 2026, lot L6, first wave). Written by
lot L6, recorded by the integration owner.

## Context

DEM-021 names `release_if_current` as the function of the pilot proof, and
DEM-024 gives lots L1 and L6 one credit function. Lot L6 owns
`crates/mqttiumd-core/src/credits.rs`; its public interface is a contract.
Five choices of that interface are not in the chapters.

## Decision

### The order of the guards

`release_if_current(lease, account, key)` examines four conditions in a
fixed order.

1. A key that is not the key of the record is `Ok(Stale)`. It is examined
   first: after a slot is reused, an old key may name another account than
   the new reservation, and a release that comes late is not a fault.
2. An account that is not the account of the lease is `Err(WrongAccount)`,
   also for a lease that is already released: the caller means this lease
   and presented an account it is not charged to.
3. A released lease is `Ok(AlreadyReleased)`, before any arithmetic: a
   released lease owes nothing, so its cost is never compared with a usage.
4. Both subtractions are computed and checked, bytes before slots. Only
   then are both usages and the state written. A refusal writes nothing.

### `Account::reserve` belongs to the same module

The fields of `Account` and `LeaseRecord` are private. Without `reserve`,
lot L1 could create a held lease only through a second charging routine,
which DEM-024 forbids. `reserve` checks both dimensions against the limits
before any mutation.

### The constructors `from_parts` check nothing

`LeaseRecord::from_parts` and `Account::from_parts` rebuild a state from
its parts. Recovery rebuilds what it read, and the proof crate must state
incoherent states: a checked constructor would exclude from the proof the
states whose branches DEM-021 asks to verify. One caller has a reason to
use them: the proof crate. Recovery in the core of lot L1 does not: it
charges a recovered lease through `Account::reserve`, once per durable
row, so that the core has one charging path. A transition never calls
them.

### An account over its limit is not refused by a release

A release only lowers a usage. An account whose usage exceeds a limit is
released exactly or refused whole; refusing it because it is incoherent
would keep it incoherent longer.

### Precondition: one record per held lease

An account counts usage and does not know its leases. The function cannot
tell a second record of a held lease from the first: releasing both
subtracts the cost twice, and the account then admits more than its limit.
`LeaseRecord` is neither `Clone` nor `Copy`, but `from_parts` and the
accessors can still build such a second record. Whoever owns the lease
table holds each record exactly once. This is a precondition of the
function, not something it checks.

Removing the precondition would need the account to know its leases: a
table inside `credits.rs`, with state and an allocation or a fixed bound.
That is a change of the design of the interface and is not made here.

## Evidence

In `proofs/credits/PROOF.md`, run in CI by the job `pilot proof`:

- order, rank 1: mutant `account-checked-before-the-key`, check P2a,
  witness W2c, unit test
  `an_old_key_of_another_account_is_stale_not_a_fault`;
- order, rank 2: check P4a with witness W4b, unit test
  `a_wrong_account_is_reported_for_a_released_lease_too`;
- order, rank 3: mutant `release-repeated`, checks P3a and P3b;
- order, rank 4: mutants `bytes-subtracted-before-slots-checked` (P5b) and
  `state-written-before-checks` (P5d);
- an account over its limit: harnesses p8 and p9;
- the precondition: harness p12, which builds the forbidden second record
  and shows that both releases succeed, and unit test
  `a_second_record_of_a_held_lease_is_not_detected`.

## Consequences

- Lot L1 calls `release_if_current` and `Account::reserve` and writes no
  variant.
- The lease table of lot L1 owes the precondition; it is an open condition
  of the pilot proof.
- A change of the order of the guards, of an error or of a constructor is
  a contract change.
