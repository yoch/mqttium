# 0007 — How the purity of the core is enforced

Status: **Proposed**. Completes DEM-003. Lot L0 is built on it.

## Context

DEM-003 forbids the core network, disk, system clock, ambient randomness
and async runtime, through "a verification of dependencies and APIs", and
mentions `no_std + alloc` as an optional strengthening. A list of forbidden
APIs is never complete. The default `HashMap` seeds its hasher from the
operating system, so its iteration order is ambient randomness, which
DEM-013 excludes from replay.

## Decision

`mqttiumd-contract` and `mqttiumd-core` are `no_std + alloc` from the first
commit, and:

- have no external dependency; the core depends on the contract only;
- forbid `unsafe`;
- use no conditional compilation other than `cfg(test)` and `cfg(kani)`;
- contain no `async` and no `.await`;
- encode and decode by hand: canonical bytes for recipes, eight big-endian
  bytes for identities. No serialization framework enters these crates.

`crates/mqttiumd-lab/tests/boundary.rs` checks the dependency lists with
`cargo metadata` and the source rules by scanning, and proves that the scan
rejects what it must.

## Consequences

Files, sockets, clocks, threads and `HashMap` are unavailable by
construction rather than by lint. JSON lives in `mqttiumd-lab`. A Verus
trial that needs a library dependency in the core conflicts with this
record and must say so; DEM-021 already allows discarding an unsuitable
tool with justification, and Kani needs only `cfg(kani)`.
