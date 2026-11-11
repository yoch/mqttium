# Security policy

## Supported versions

Security fixes are applied to the latest stable release line and the `main`
development branch. Users should upgrade to the newest patch release; older
pre-release and superseded minor lines may not receive fixes.

The current stable line is `1.2.x`, starting with `1.2.0`. Fixes land on
`main` and ship in a subsequent release. A source revision is not a new PyPI
release, even while its version string still names the last published version.

| Version | Security fixes |
| --- | --- |
| Latest `1.2.x` patch release | Supported |
| `main` | Supported for development and coordinated disclosure |
| Older releases and pre-releases | Not guaranteed |

## Reporting a vulnerability

Do not disclose a suspected vulnerability in a public issue, discussion, pull
request, benchmark artifact, or broker log.

Use GitHub's
[private vulnerability reporting](https://github.com/yoch/mqttium/security/advisories/new)
and include:

- affected MQTTium and Python versions;
- protocol, transport, broker, and deployment assumptions;
- reproduction steps or a minimal proof of concept;
- expected confidentiality, integrity, or availability impact;
- any known mitigation;
- whether the report or artifacts contain sensitive data.

You should receive an initial acknowledgement within seven days. A remediation
timeline depends on severity, reproducibility, coordination needs, and release
risk. Please allow a fix and advisory to be prepared before public disclosure.

## Verifying a release

Every distribution on PyPI is published by `publish.yml` in this repository
through Trusted Publishing, and PyPI serves a signed attestation that says so.
To check that a release file was published from this repository:

```bash
python -m pip install pypi-attestations
pypi-attestations verify pypi --repository https://github.com/yoch/mqttium pypi:mqttium-1.2.1-py3-none-any.whl
```

The command fetches that file and its attestation from PyPI, prints `OK`
followed by the file name, and fails if the file was not published from this
repository. It works for the source distribution too.

MQTTium has no runtime dependency, so installing it brings in no other
package.

## Security boundaries

MQTTium implements MQTT transport and protocol behaviour. Applications remain
responsible for credential storage, TLS trust configuration, broker access
control, topic authorization, payload validation, secret redaction, and
application-level idempotency.

Plain TCP and `ws://` do not provide confidentiality or peer authentication.
Use TLS with hostname and certificate verification on untrusted networks.

The [threat model](docs/threat-model.md) lists what the library defends
against, the tests behind each defence, and the risks left to the application.
