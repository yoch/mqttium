# Will continuation journal

Base: a0e445a47bd5e86b07a72f7f3d9649ea52d78c69, PR648 draft, main
874f298b4de7c3a13006cb1ff767dfb09e4031ac. Before-contract digest:
sha256:36a15f70c77acaf2611ccd5a955d9898fc65de2bcac93b861d607ac3b2d95f1e.

The public mqttium/Paho sensor-process test was committed before implementation.
Both runs on the archived normal A0 binary refuse CONNECT with reason131 and
`will_connect_not_accepted`; no timeout was counted as detection. Contract
ownership is serialized in the integration branch; consumers resume its pin.
Next: schema10 intentions, pure Will admission, transport disposition and the
first real online/client-SIGKILL/offline reception. ADR0034 remains Proposed.

Implemented: schema10, kind17, OpenWillAt/EndWill, bounded pending reader,
reserved Outbox/writer lanes, takeover barrier and source-independent admission.
Public normal development run first passed on f534fd65+recorded runtime diff.
Source-matching2fe462b0 hooks runs passed seven groups (including128capacity and
finite expiry), six own SIGKILL cuts and four genuine result-loss/hold cases.
The full clean2fe launcher then passed normal7 and hooks10; aggregate controls
and package receipt are retained under its own pin. Development failures for
nested/infinite-detachment diagnostics and48-field readiness are preserved,
not counted as negative controls. Scalar diagnostics and a separate capabilities
event retain the40-field reader budget. ADR0034 remains Proposed.

Contract digest after: sha256:fb5ab104cd30e970b24d8eb5a10ecbf87d4012d8cc356414911d04f38282d8fc.
Commands: cargo fmt --check; strict workspace Clippy; sequential normal and
kill-points workspace suites; validate P1-02; build_corpus/build_traces checks;
267 oracle tests;632 second-reader documents;185 system-pilot tests. Final CI
must rerun the exact published content and keep its independent qualification
scope. Next action: finish clean launcher, publish with expected-head lease,
check exact-head CI, download/extract final normal archive and run will-demo.

First publication: a90a11e599dc292b5a6df3a65d51e6a51ad7796f, tree
3ceba3909dfefec19163c34c76b916887715f79d, guarded against A0; connector and
full fetched tree matched. CI run44184519426 received normal1138 and
instrumented1153 locally before publication. Its independent stream gates then
exposed a diagnostic-schema mismatch, not a Will business invariant failure:
current bounded detached_by text/infinite omitted deadline versus historical
structured identity/required null. New red/green format tests keep both forms
readable, preserve checked components and still require finite deadlines.
The corrected format checker accepts8861 original events without skipping; this
is diagnostic compatibility checking, not relabeling those runs as new evidence.

The old session protocol matrix also still expected DISCONNECT4 to be unsupported.
It now tests supported4 without an installed Will (clean closure, same persistent
session, no invented publication), retaining a separate valid unsupported0x80
reason refusal. The public normal matrix was actually rerun and passed.
New digest: sha256:57b2d617cf8369b2c6fe14325f328b6378aafc98155f47a3568da20528448151.
Final source must run fresh campaigns after this reader identity change.

The clean96db7b34 Will launcher and CI Will job both passed: normal7 groups,
six own-process SIGKILL cuts, four genuine unknown-result cases, six compiled
negative controls and the extracted Mosquitto sensor demo. Two other CI gates
exposed test tooling issues, retained as failures rather than detections:
the old detachment mutant no longer applied around EndWill, and an external
corruption UPDATE raced the writer's maintenance transaction. The mutant now
changes only the same deadline predicate on the current source. The corruption
probe arms a one-shot work.read.before barrier on the actual writer, outside
any SQLite transaction; normal builds expose neither this option nor the hook.
Its real run reached the barrier, observed quick_check=ok, refused InvalidData
in Work, exited2, released the writer, and refused the same file on restart
with InvalidData in Snapshot. No Unavailable/SQLITE_BUSY result is accepted.
The new injection point is documented and included in the derived example.
Contract digest: sha256:65345f213724ab31e29e5d5b24bbb310ba6e7d2b346a91d9d7c6cb741fec012f.
Next: receive the four finite-expiry compiled controls, publish this correction
with an expected96db7b34 lease, then download and execute the final CI preview.
