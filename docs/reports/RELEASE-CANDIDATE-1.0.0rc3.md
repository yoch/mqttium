# Release candidate report — 1.0.0rc3

Candidate source: `19df6bd` plus the release metadata and ingress-batch fix
validated for the `v1.0.0rc3` tag.

## Findings closed before release

The specialized codec merge (#216) exposed a real asynchronous ingress edge:
with the default `local_receive_maximum=100`, a 256-packet decoder batch could
admit a 101st QoS 1/2 PUBLISH before the first automatic PUBACKs reached the
effect pump. The failure reproduced against the release-gate Mosquitto limits
as `ProtocolError("Receive Maximum exceeded")` during the reconnect soak; the
pre-#216 baseline did not reproduce it. The reader now caps each decode batch
at `min(256, local_receive_maximum)`, and the read-loop batching tests assert
the bound. A 30-second gate-config soak then completed with 19,000 MQTT 3.1.1
messages and 17,000 MQTT 5 messages received, with stable resource assessment.

The retained memory thresholds were also revalidated against RC2 and the
current candidate. The property-heavy peak was identical (20.0136 MiB), and
the WebSocket logical counters match the bounded 256 KiB coalescing path; the
threshold update is maintenance of the benchmark contract, not a source-code
performance claim.

## Local validation

The final `quick` profile passed on the release tree (`/tmp/mqttium-rc3/19df6bd/quick-final2`):
ruff format/check, mypy, Bandit, unit coverage (998 tests, 88.66% total),
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
