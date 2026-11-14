# 0007 — How the purity of the core is enforced

Status: **Accepted with amendments** (14 November 2026, review of PR yoch/mqttium#647).
Applied in lot L0. Completes DEM-003.

## Context

DEM-003 forbids the core network, disk, system clock, ambient randomness
and async runtime, through "a verification of dependencies and APIs", and
mentions `no_std + alloc` as an optional strengthening. A list of forbidden
APIs is never complete. The default `HashMap` seeds its hasher from the
operating system, so its iteration order is ambient randomness, which
DEM-013 excludes from replay.

## Decision

`mqttiumd-contract` and `mqttiumd-core` are `no_std + alloc` from the first
commit. Four layers enforce the boundary; each covers what the others do
not.

| Layer | Enforced by | Guarantees |
| - | - | - |
| `#![no_std]` outside tests | The compiler | No file, socket, clock, thread or randomly seeded `HashMap` |
| `#![forbid(unsafe_code)]` | The compiler | No `unsafe` in these crates |
| Dependencies | `cargo metadata`, in `tests/boundary.rs` | Only the listed dependencies; no build script; no feature |
| Token rules | The lexed sources, in `tests/boundary.rs` | No `unsafe`, `async`, `await`; no `extern` but `extern crate alloc;`; conditional compilation only as the `no_std` crate attribute and as `cfg(test)` or `cfg(kani)` on a module; no `include!`, `env!` or `#[path]` |

The token rules apply to Rust tokens, so comments, strings and spacing do
not matter, and `cfg!(…)` is seen. They are deliberately coarse: an
identifier that merely starts with `cfg` is refused.

Encoding is written by hand in these crates: canonical bytes for recipes,
eight big-endian bytes for identities. No serialization framework enters
them.

### What no layer covers

- `--cfg` flags passed from outside the sources. `cfg(kani)` is allowed on
  a module, so a build with `--cfg kani` compiles proof modules; the run
  manifest records the flags the binary was built with.
- Global state in a `static`. Nothing in these crates uses one today; a
  review must refuse it.

### Proof dependencies

A dependency needed by a proof tool may be admitted by an explicit decision
record that names it and its scope. It is then added to the allow-list of
`tests/boundary.rs`. DEM-021 allows discarding an unsuitable tool with
justification; Kani needs only `cfg(kani)`.

## Consequences

Files, sockets, clocks, threads and `HashMap` are unavailable by
construction rather than by lint. JSON lives in `mqttiumd-lab`.
