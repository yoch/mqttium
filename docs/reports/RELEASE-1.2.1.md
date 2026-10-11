# Release 1.2.1 evidence

Date: 2026-10-11. Candidate: `main` at
`80093237797c479de31e34f89e8aa1abbe8d0ba0`. The release commit adds only the
version string, the frozen changelog section, release-facing installation and
support text, and this report. Baseline: `1.2.0`
(`7227c97ff15a36580ccaff3b9c32fa007aed966b`).

## Why 1.2.1

Python 3.15.0 is released, and the project now runs its blocking checks on it
(#617). The published `1.2.0` wheel is `py3-none-any` with
`requires-python >=3.11`, so it already installs on 3.15; what 1.2.1 changes is
the declared support: the `Programming Language :: Python :: 3.15` classifier,
the PyPI description, and the `stable` documentation, which otherwise keeps
listing 3.11–3.14 until the next release.

The maintainer chose a patch version because the library itself does not
change.

## Content

| Item | Pull request |
| --- | --- |
| Python 3.15 support: blocking unit, integration, resilience, fuzz, macOS and Windows endpoint, wheel and sdist checks; 3.15 in the publication matrix and classifiers | #617 |
| ARM64 correctness CI on Python 3.14 and the distribution's Python 3.15 | #620 |
| Remaining documentation pages list Python 3.15 | #621 |
| Development tooling only: WebSocket memory benchmark fixture, Tachyon sampling controller, artifact action version comments, memory scenario unit test | #616, #619, #618, #621 |

## Source equivalence

- `git diff v1.2.0 80093237 -- src` is empty. The release commit changes only
  `__version__` in `src/`.
- The distributions differ from 1.2.0 by the version, the added 3.15
  classifier and the README text in the package description.
- The 1.2.0 qualification was run on `b2ae1018`, whose `src/` differs from
  `v1.2.0` only by the version string.

The 1.2.0 performance, soak, broker-interoperability and ARM64 network-gate
evidence therefore applies unchanged to the 1.2.1 runtime and is carried
forward rather than repeated (see [RELEASE-1.2.0](RELEASE-1.2.0.md)). Those
measurements used Python 3.12 and the runner's Python 3.14. No performance
claim is made for Python 3.15.

## Python 3.15 evidence on `80093237`

| Evidence | Result |
| --- | --- |
| [CI 38144658013](https://github.com/yoch/mqttium/actions/runs/38144658013) | Passed: unit and Mosquitto integration on 3.11–3.15; resilience and fuzz on 3.12 and 3.15; macOS 15 and Windows 2025 on 3.11 and 3.15; built wheel and sdist installed and exercised on 3.15 |
| [ARM64 CI 38144658019](https://github.com/yoch/mqttium/actions/runs/38144658019) | Passed on the Raspberry Pi 5: unit, resilience and mandatory integration on system Python 3.14.8 and Debian's Python 3.15.0rc3 |
| [Internal performance health 38141807054](https://github.com/yoch/mqttium/actions/runs/38141807054) (on `c4e4e452`, same `src/`) | Passed: all 13 memory scenarios within thresholds on 3.12; Tachyon captures on 3.15 (diagnostic only) |

The ARM64 runner's 3.15 interpreter was a release candidate on this date; the
hosted 3.15 jobs use the final release.

## Qualification of the release commit

The release pull request runs CI and the publication workflow's distribution
validation, which builds once and installs the wheel and sdist on the
publication matrix, including 3.15. After publication, verify the PyPI page,
Read the Docs `stable` on the `v1.2.1` tag, and an install of `mqttium==1.2.1`
on Python 3.15.
