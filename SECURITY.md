# Security policy

## Supported versions

Security fixes are applied to the latest stable release line and the `main`
development branch. Users should upgrade to the newest patch release; older
pre-release and superseded minor lines may not receive fixes.

| Version | Security fixes |
| --- | --- |
| Latest stable release | Supported |
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

## Security boundaries

MQTTium implements MQTT transport and protocol behaviour. Applications remain
responsible for credential storage, TLS trust configuration, broker access
control, topic authorization, payload validation, secret redaction, and
application-level idempotency.

Plain TCP and `ws://` do not provide confidentiality or peer authentication.
Use TLS with hostname and certificate verification on untrusted networks.
