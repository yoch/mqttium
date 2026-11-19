# ADR 0042 — Checked single-listener Mosquitto cutover

Status: **Proposed**

The Linux operator tool accepts an explicit Mosquitto 2.0.21 source profile
and emits a file/line decision for each directive, identity and ACL grant.
The supported subset is the loopback mTLS configuration exercised by the
packaged reference. CFG-001/002/003/006 govern this subset. The operator
supplies certificate leaves and authorized ClientIDs for each ACL username.
The checker validates them with the CA and presents their fingerprints.
The verified start recomputes the plan and passes the checked credentials
and policy to the native TLS loader using sealed memory files.

Native policies may add exact publish and subscribe topic grants. The prior
prefix-only fingerprint remains byte-identical for all prefix-only policies.
A nonempty exact list selects a distinct fingerprint domain and adds both
sorted exact lists per principal. The offline monotonicity check can cover
an old exact topic by the same literal or by a new encompassing subtree.
Replacing a subtree by its root alone reduces the policy and is refused.

Mosquitto may grant a broad subscription and suppress individual forbidden
deliveries. mqttiumd requires one rule to cover the complete filter at
SUBACK. The operator can subscribe to the permitted branches separately.
Each broker retains a private, independent store across the same-address
handoff. The procedure provides service rollback with the original stored
state. Application reconciliation governs messages produced during the trial.
The demonstration and wire comparisons are in preview/CUTOVER.md.
