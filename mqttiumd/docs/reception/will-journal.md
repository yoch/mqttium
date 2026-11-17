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
