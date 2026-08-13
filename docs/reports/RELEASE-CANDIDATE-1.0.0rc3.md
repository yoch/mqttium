# Release candidate report — 1.0.0rc3

Candidate source: `19df6bd` plus the release metadata and ingress-batch fix
validated for the `v1.0.0rc3` tag.

## Findings closed before release

The specialized codec merge (#216) exposed a real asynchronous ingress edge:
with the default `local_receive_maximum=100`, a 256-packet decoder batch could
admit a 101st QoS 1/2 PUBLISH before the first automatic PUBACKs reached the
effect pump. The failure reproduced against the release-gate Mosquitto limits
as `ProtocolError("Receive Maximum exceeded")` during the reconnect soak; the
pre-#216 baseline did not reproduce it. The reader now ends a decode batch
exactly when pending automatic PUBACKs fill the remaining Receive Maximum
window. This also covers a window partly occupied by manual-ACK or QoS 2 state,
without shrinking the 256-packet batch for QoS 0 and control traffic. Targeted
read-loop tests assert both sides of that boundary. The final 30-second
gate-config soaks completed with 18,500 MQTT 3.1.1 messages and 17,500 MQTT 5
messages received, with stable resource assessment.

Cursor Bugbot then identified the mixed in-batch order automatic QoS 1 → QoS 2:
the QoS 2 acquisition could fill the last slot after the automatic PUBACK was
queued. The shared slot-acquisition path now raises the same handoff boundary,
and a direct QoS 1 → QoS 2 → QoS 1 read-loop regression covers the report.

The retained memory thresholds were also revalidated against RC2 and the
current candidate. The property-heavy peak was identical (20.0136 MiB), and
the WebSocket logical counters match the bounded 256 KiB coalescing path; the
threshold update is maintenance of the benchmark contract, not a source-code
performance claim.

## Local validation

The reviewed `quick` profile passed on the release tree after the Bugbot fix
(`/tmp/mqttium-rc3/7648cfb/quick-bugbot-fixed`): ruff format/check, mypy,
Bandit, unit coverage (1,002 tests, 88.69% total),
mandatory broker integration, hot-path allocation profiling, memory thresholds,
application stress, and both 30-second reconnect soaks.

The package gate was run independently because the strict RC profile stopped
before packaging at runner preflight: the host exceeded the configured CPU/load
and temperature eligibility guard. `validate_pyproject`, isolated build,
strict Twine metadata checks, wheel contents, isolated wheel installation,
`pip check`, version assertion, and public-module imports all passed for
`mqttium-1.0.0rc3`.

The strict performance comparison is therefore intentionally not claimed from
this host. Existing merged CI and #216 evidence remain the authority for the
performance change; the release is not published on the basis of an ineligible
local runner measurement.
