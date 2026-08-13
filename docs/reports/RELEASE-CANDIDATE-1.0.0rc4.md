# Release candidate report — 1.0.0rc4

Candidate source: merge commit `3ca1447`, with release metadata validated at
`a5eb003` for the `v1.0.0rc4` tag.

## Scope and review

RC4 contains the protocol-engine rationalisation merged through #218. The
change removes parallel SUBSCRIBE/UNSUBSCRIBE identifier state, shares their
allocation and rollback path, runs one inbound PUBREL state machine over both
store interfaces, centralises outbound flow-window reset, and removes
unreachable dispatch code. It also avoids a second validation of the rebuilt
offline queue after a resumed-session CONNACK.

A separate full review checked the affected state transitions, failure paths,
effect ordering, packet-identifier ownership and negotiated-limit handling.
The 10,000-message resumed-session scenario performs exactly 10,000 negotiated
validations on RC4 versus 19,999 on RC3. Sequential measurements in both run
orders observed roughly 34–40% lower CONNACK handling time. These timings are
supporting review evidence rather than a release-gate performance claim because
the host did not pass the strict runner eligibility guard described below.

The complete #218 GitHub matrix passed on Python 3.11–3.14, Linux, macOS and
Windows, including quality, fuzz, packaging and Linux soak jobs. Cursor Bugbot
completed its review with no issue and zero annotations.

## Local validation

The clean `quick` manifest at
`/tmp/mqttium-rc4/a5eb003/quick/manifest.json` passed in full:

- Ruff formatting and lint, mypy and Bandit;
- 1,003 unit tests with 88.95% source coverage, above the 87.36% release gate;
- 15 mandatory Mosquitto integration tests, executed rather than skipped;
- hot-path call/allocation profiling, all versioned memory thresholds and the
  application stress suite;
- 30-second forced-reconnect soaks with stable resource assessment and no idle
  violations: 18,500 messages received under MQTT 3.1.1 and 17,000 under MQTT
  5.

The clean package manifest at
`/tmp/mqttium-rc4/a5eb003/package/manifest.json` also passed. It validated the
project metadata, built `mqttium-1.0.0rc4` as wheel and sdist, ran strict Twine
and wheel-content checks, installed the wheel without dependencies in an
isolated environment, imported every packaged module, and exercised TCP, TLS,
WebSocket, Unix-domain sockets, SQLite restart, Paho VERSION2 and clean
shutdown against the installed distribution.

## Strict performance-runner limitation

The combined `rc` profile stopped before comparative measurements because the
host failed the enforced runner preflight. Rechecks reported one-minute load
per CPU around 0.39 against a 0.25 maximum, CPU use above the 20% maximum, and
on the final attempt an 85°C temperature against the 80°C maximum. No threshold
was weakened and no timing from that ineligible run is claimed. The valid local
quality, robustness and package gates were therefore recorded separately, as
they were for RC3.
