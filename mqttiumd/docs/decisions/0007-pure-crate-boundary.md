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
| Token rules | The lexed sources, in `tests/boundary.rs` | Closed lists: a macro invocation names a listed macro or one the file defines; an attribute has a listed name; a local macro captures identifiers and literals only and cannot assemble an invocation or an attribute; conditional compilation only as the `no_std` crate attribute and as `cfg(test)` or `cfg(kani)` on a module; no `unsafe`, `async`, `await`; no `extern` but `extern crate alloc;` |

The token rules apply to Rust tokens, so comments, strings and spacing do
not matter.

They are closed lists rather than lists of forbidden words, because a
forbidden word can be respelled. A raw identifier designates the same name
as the plain one, so `r#cfg!`, `r#env!` and `#[r#path]` are the macros and
the attribute they name. A macro can be imported under another name, so
`use core::env as e; e!(…)` reaches `env!` without writing it at the call.
With an allow-list the alias is refused at the call, because `e` is not
listed; the `use` declaration is refused as well, and nothing may be
renamed to the name of an allowed macro.

### Local macros (amendment of 15 November 2026)

The rules read the sources, not their expansion. A local macro could
therefore build, at expansion, what the rules refuse in place. The third
review of lot L0 gave two forms, both compiled: `$write!(…)` called with
`env`, which the allow-list accepted because the variable was named
`write`, and `#[$attr]` called with `path = "…"`, which an exemption let
through. The same defect has other forms, compiled as well: the `!`
passed as a token tree, a repetition that matches nothing between a name
and its `!`, a name produced by a repetition, an invocation formed by the
end of one iteration and the start of the next.

Three rules close them without expanding anything:

- A name never comes from a macro variable where a rule would check it:
  `$name!(…)` and `#[$name …]` are refused whatever the variable is
  called, and a `use` declaration holds no `$`.
- A variable captures an `ident` or a `literal`. The list of fragments is
  data, like the other lists; a token tree, a path, a meta item, an
  expression or an item is refused until a decision adds it.
- Inside a macro definition, a `!` before a group directly follows a name
  written there, and a `#` directly opens an attribute. No repetition
  stands between those tokens.

The macros of the contract crate declare their documentation as
`$(#[doc = $doc:literal])*`: the attribute name is written in the macro
and only its text comes from the call.

This is not an analysis of macro expansion. The rules close the forms
listed here, each with a compiled counterexample in the tests; a form
nobody thought of would be a new defect of this layer, and `no_std` and
the dependency list still hold underneath it.

Only keywords are compared as written: `r#async` is an ordinary
identifier and stays accepted.

The lists are data in `tests/boundary.rs`. Adding a macro or an attribute
is a decision, like adding a dependency. The rules are deliberately
coarse: an identifier that merely starts with `cfg` is refused, and so is
a macro nobody listed yet.

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
