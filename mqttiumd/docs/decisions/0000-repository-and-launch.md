# 0000 — Repository, layout, language and scope of the launch

Status: **Accepted** (14 November 2026, with the integration owner).
Applied in lot L0.

## Context

Chapter 4 ends on an open point: the target repository and its starting
revision are not named. DEM-002 proposes four initial crates; ARC-026
proposed eight components and allows logical boundaries to precede crates.
The repository guide requires maintained content in English, and the
specification is in French.

## Decision

- mqttiumd lives in the `mqttiumd/` directory of `yoch/mqttium`, as a Cargo
  workspace of its own. The starting revision is `92ecb71`.
- Nothing under `mqttiumd/` enters the Python distribution, and the Python
  CI does not build it. `.github/workflows/mqttiumd.yml` runs only when
  `mqttiumd/**` changes.
- Initial crates follow DEM-002: `mqttiumd-contract` (ARC-026's types and
  store contract), `mqttiumd-core`, `mqttiumd-lab` (ARC-026's simulator and
  conformance bench), then `mqttiumd-store-sqlite` with lot L7,
  `mqttiumd-codec` with L8 and `mqttiumd-runtime` with P3.
- Code, schemas, decision records and documentation are in English. The
  four chapters are archived verbatim in French under `docs/spec/fr/`.
- The licence is Apache-2.0, as for the repository.
- This launch delivers the bootstrap and lot L0 only. Each later lot gets
  its own branch and review.

## Consequences

- The pinned TLC runner `tools/formal/run_tlc.py` and its sidecar format can
  serve lot L5, and the Python codec of MQTTium can serve as the independent
  peer decoder of lot L8. Both are implementations independent of the Rust
  code they check.
- The subtree can later move to its own repository with its history.
- Proofs designate a revision of the whole repository; the manifest also
  records which paths under `mqttiumd/` are modified.
