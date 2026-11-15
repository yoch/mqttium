# Mission L4 — bench runner and first S0 campaign

Read `README.md` of this directory first, then `mqttiumd/AGENTS.md`,
`docs/reception/open-conditions.md` and decision records 0001, 0002, 0003,
0009, 0011, 0013, 0015 and 0016. Records 0010 to 0016 are proposed: they
are the working choices of S0, not accepted decisions.

## Goal

One documented command runs a scenario through the real components and
leaves a campaign directory that somebody else can check:

```text
cargo run -p mqttiumd-lab --locked -- run \
    --contracts contracts/v1 \
    --scenario tests/scenarios/p1/P1-02.json \
    --out <campaign directory>
```

The command builds and initialises the real core (`mqttiumd-core`), the
deterministic store (`mqttiumd-lab::store`) and their adapters, drives the
scenario, injects the faults the scenario declares, collects the evidence
while it runs, and writes the trace, the snapshots and a manifest. The
independent oracle (`tests/oracle`) then computes the verdict from that
directory. The runner never computes a verdict.

## What the runner must guarantee

- The scenario drives the components: no step is answered by the runner
  in place of a component.
- Every decision of the core comes from the core, every durable result
  from the store. The start-up goes through the real path: the opening of
  an epoch, its grant, `OwnerAuthority::from_grant`, one owner per box
  (open condition "Creation authority, the real path").
- The evidence is collected during the run. The collector assigns the
  origin of a record from its channel and refuses a record that claims
  another (record 0003).
- A snapshot is the state actually obtained, written through
  `store::evidence`.
- A fault that the scenario declares and that was not reached is reported
  as not reached, never as passed.
- The manifest names the content the binary was built from, the binary
  that ran, the effective parameters (scenario, fixture, backend, failure
  model, scheduler version and seed, commands), and identifies the lab
  binary, the oracle and the generators separately.
- The campaign directory keeps every artefact with its SHA-256 and the
  command that replays the run. A run on a modified tree keeps the
  modified content.
- A provisional `run-result` is never PASS (schema `run-result`). No
  assertion is removed to obtain a passing verdict.

State exactly what a campaign covers: the backend (deterministic store),
the kind of crash (simulated: the adapter is dropped and the store
recovered in the same process), and the perimeter of the scenario.

## Order of delivery

1. P1-02 through the components.
2. P1-02b: resolution on a live process, deadlines, lost and delayed
   results. Write `tests/scenarios/p1/P1-02b.json`.
3. QABS-A, B and C: commit against fence, and a refused fence. Follow
   record 0015: a held command does not prevent a fence; use
   `refuse_fence` at `store.fence.transit` when a scenario needs a
   resolution that stays unknown.
4. The regression of finding F3: resolution answers that arrive with a
   steady delay longer than the old window.

Then show that the loop detects a defect. On isolated copies, never in
the integration tree: a payload still owed is lost, a result is altered
between the store and the core, a positive acknowledgement is sent too
early. For each, check the cause the oracle reports, not only a non-zero
exit.

## Paths

Owned by this lot: `crates/mqttiumd-lab/src/bench/`, the `run` command in
`crates/mqttiumd-lab/src/main.rs`, `crates/mqttiumd-lab/tests/bench_*.rs`,
`tests/scenarios/p1/P1-02b.json`, `tools/run_campaign.*` if a wrapper is
needed, and the `campaign` step of the workflow (proposed to the
integrator, who owns `.github/workflows/mqttiumd.yml`).

Not owned: `crates/mqttiumd-core` (L1), `crates/mqttiumd-lab/src/store`
(L2), `contracts/`, `tests/oracle` and the schemas (L3). When the runner
needs something from one of them, ask the integrator, with the smallest
change that would do; do not work around a missing interface in the
runner.

Campaign directories are generated: they are never committed. The
workflow uploads one as an artefact.

## Reception

- The command above runs P1-02 on a clean checkout and the oracle judges
  the directory it wrote.
- The same seed gives the same trace, byte for byte.
- Each of the three mutations is rejected by the oracle for its own
  reason.
- A composition test goes through the real start-up path and shows that
  two owners of one identity cannot be built (finding F4, with L1).
- The closing report lists: the exact revision, the commands run and
  their results, the scenarios really executed, what was not done, and
  the limits.

## Findings of the audit this lot depends on

F2, F5 and K1 (oracle, L3) decide whether a trace of the runner can be
judged; F3 and F4 (core, L1) and F1 and F6 (store, L2) change behaviour
the scenarios exercise. They are listed in
`docs/reception/open-conditions.md`. Take them by merging the integration
branch; do not wait for them to start.
