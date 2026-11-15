# L5 — Model M1

Depends on: L0. Runs with: L1, L2, L3, L6. Its correspondence table is
completed with L4.

## Goal

A TLA+ model of admission and of the authorisation of PUBREC, small enough
to be explored, with every bound published.

## Requirements

DEM-020; V3-018; ARC-027. Decision records 0001, 0005.

## Deliverables

1. **M1a**, under `models/m1/`: one producer, two destinations, two
   possible publications, two admission identities, at most one crash. No
   collection, expiry or Will.
   - Variables by domain: durable, volatile, internal transport,
     connections, external transport, history of the verifier.
   - The history of the verifier appears in no guard of the product. A
     crash erases the volatile domain; recovery reads durable data only.
   - Actions: reception, preparation, submission, delayed command, commit,
     rejection, delivery or loss of the result, timeout, resolution,
     authorisation and observation of PUBREC, crash, recovery.
   - Finite domains are consumed without wrapping: using up both identities
     suspends creation and removes no delayed command.
2. **Invariants**: one admission per independent exchange; atomicity over
   both obligations; an answer only on available knowledge; one recipe per
   identity; a fence implies no later execution; accounts match their
   objects. No fairness assumption is added to make a safety property hold.
3. **Liveness**, as two separate properties, each with its assumptions
   stated.
   - After a crash and a recovery: no more crashes, the store available,
     a fair scheduler. This one may assume that the client usefully sends
     its PUBLISH again.
   - On the live path of P1-02b (record 0001): from the unknown outcome
     to its resolution, then to PUBREC, with no new packet from the
     client. A client retransmission is not among its assumptions.
4. **Non-vacuity**: configurations that are expected to reach a commit, a
   lost result, an unknown outcome, a rejection, a crash after commit and a
   recovery.
5. **Mutations of the model**, each expected to give a counterexample:
   acknowledgement before commit, unknown treated as rejection, recipe
   changed under an identity, second admission at replay.
6. **M1b**: resets, session generations, capacity refusals, fences and the
   change of epoch, with the race between a fence and an application made
   explicit.
7. **A report per campaign**: model, configuration, tool version, states
   explored, bounds, counterexamples. An interrupted exploration is
   inconclusive.
8. **A draft correspondence table** from model actions to registry rules
   and scenario steps.

## Tooling

`tools/install-tools.sh tlc` fetches the pinned TLC jar; a Java runtime is
required. The repository already pins and hash-checks TLC in
`tools/formal/run_tlc.py`, with sidecar files that declare the outcome each
configuration must give (`pass` or `violation`). Use that runner and that
sidecar format. `tools/install-tools.sh tlc` already calls the runner's
own download and digest check: that stays the only source of the jar.

The runner reads `formal/models/` only. The first deliverable of this lot
is an option `--models-dir` that selects another directory and keeps
`formal/models/` as the default. It is a change to the client's tools and
is proposed apart from the model. It is received when:

- the client's models run unchanged, with the same outcomes, when the
  option is absent;
- the option selects the models of `mqttiumd/models/` and no other;
- a directory that does not exist, and a selection that holds no model,
  are refused;
- a configuration declared as a violation fails for the exact invariant
  its sidecar names, not for any failure;
- a campaign that ran no configuration is not a success.

M1a comes after that.

## Reception

- Every configuration gives the outcome its sidecar declares.
- Each non-vacuity configuration really reaches its target.
- Each model mutation gives a counterexample for its property.
- The report states what M1a does not cover: PUBREL and PUBCOMP belong to
  M2; the atomicity of the commit is an assumption that the SQLite tests
  must discharge.

## Out of scope

The Rust code, the oracle, the models M2 to M6.
