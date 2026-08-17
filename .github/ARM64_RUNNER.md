# ARM64 self-hosted runner

MQTTium uses the repository-scoped ARM64 self-hosted runner as an additional
platform, not as a replacement for GitHub-hosted CI.

## Trust boundary

This repository is public. Workflows that execute on the self-hosted runner
must not be triggered by `pull_request` or `pull_request_target` unless a
separate threat model explicitly makes that safe. A pull request can contain
arbitrary code, so running it directly on a persistent machine would give that
code access to the runner account and whatever the host can reach.

The ARM64 workflows therefore run only from trusted default-branch code:

- `ARM64 CI`: push to `main` or manual dispatch;
- `ARM64 Benchmarks`: weekly schedule or manual dispatch;
- `ARM64 Paired Regression`: manual dispatch;
- `ARM64 Finalization Soak`: manual dispatch;
- `Published ARM64 Smoke`: manual dispatch of an exact published version.

`ARM64 Paired Regression` necessarily executes the selected base/candidate
source trees. Treat that workflow as a privileged maintainer operation: use
only refs or exact SHAs whose code has already been reviewed and intentionally
trusted for execution on the persistent runner. Never point it at an arbitrary
external pull-request head merely to obtain a performance number.

The existing GitHub-hosted PR, release, fuzz, macOS, and Windows workflows
remain the primary portability/version gates.

## Validated host model

The repository runner probe on 2026-08-17 observed the actual `rpi5` runner as:

- Raspberry Pi 5 Model B, AArch64, 4 logical CPUs;
- Debian GNU/Linux 13 (trixie);
- GitHub Actions runner 2.336.0;
- system Python 3.13.5;
- a working native `python3 -m venv` / pip environment;
- native Mosquitto broker/client tools and `taskset` available;
- all CPU governors set to `performance` during strict validation.

`actions/setup-python` does **not** currently provide a Python ARM64 build for
Debian 13, so the self-hosted workflows deliberately use the validated system
Python 3.13 and create a fresh venv for each job. Python 3.11-3.14 compatibility
continues to be enforced by the existing GitHub-hosted matrix rather than being
claimed by this ARM64 runner.

## Host setup

Keep the runner account unprivileged. Do not add `github-runner` to sudoers.

Install the small set of system dependencies once from an administrator
account:

```bash
sudo apt update
sudo apt install -y python3-venv mosquitto mosquitto-clients openssl util-linux
```

`taskset` is provided by `util-linux`; the workflow verifies it explicitly.
The ARM64 workflows intentionally do not run `apt`, `sudo`, Docker, or `tc`.

For benchmark evidence, use active cooling and set every CPU frequency governor
to `performance` before running the dedicated benchmark workflows. The
benchmark preflight rejects excessive background load, a non-performance
governor, or missing/unsafe temperature readings. This is intentionally stricter
than ordinary ARM64 CI.

If the host can reboot unattended, make the governor selection persistent with
a root-owned host configuration rather than granting the runner account
privilege to change it. A reboot that restores `ondemand` is safe: strict
benchmark jobs fail closed at preflight instead of producing misleading
performance evidence.

The system Mosquitto service should remain stopped/disabled when the machine is
used as the dedicated benchmark runner. `ARM64 Paired Regression` refuses to run
if a Mosquitto process already exists, then starts its own isolated broker for
the job.

WAN/netem measurements stay on GitHub-hosted infrastructure. Giving the
persistent runner permission to mutate network qdiscs would widen its host
privileges for little additional ARM-specific coverage.

## Authoritative writer-regression contract

`ARM64 Paired Regression` separates diagnostic measurements from release
evidence.

The broad `paired_network.py` sweeps are **advisory**. They are useful for
investigation, but that harness has historically failed its own neutral A/A noise
budget and therefore must not become a strict release gate merely because it is
running on fixed hardware.

When the candidate provides `benchmarks/paired_writer_capacity.py`, the workflow
adds four strict writer-specific campaigns under one enforced eligible-host
preflight:

1. closed-loop capacity A/A: capacity baseline versus itself;
2. closed-loop capacity A/B: capacity baseline versus candidate;
3. paced callback latency A/A: pre-eager latency baseline versus itself;
4. paced callback latency A/B: pre-eager latency baseline versus candidate.

The isolated broker is pinned to CPU 0 and publisher workers to CPU 2. Capacity
uses MQTT 3.1.1, 256-byte payloads, inflight 20, application outstanding 64,
writer queue 200, eight ABBA pairs, 100,000 QoS 0 operations and 40,000 QoS 1
operations. A/A must remain neutral and A/B must retain at least 95% of the
reviewed capacity baseline for both QoS levels.

Paced latency uses MQTT 3.1.1, 256-byte payloads, window 64, callback completion,
eight ABBA pairs and fixed rates 2,500 and 10,000 messages/s. Those are the two
same-regime Pi 5 cells validated for issue #253; 7,500/s lies in this host's
kernel timer/pacing transition and is not used as an ARM64 acceptance point.

A strict latency A/A can occasionally classify an otherwise eligible run as
statistically invalid rather than proving a regression. The workflow retries
that **control only** exactly once, and only when the harness returns its
`invalid measurement` exit code 2. It reruns the host preflight first and retains
both attempts as artifacts. Operational failures are never retried, two invalid
controls still fail the workflow, and A/B never runs unless a control passes.
This avoids accepting noisy evidence without silently turning the thresholds
into a best-of-N search.

Defaults are intentionally reviewable historical anchors rather than moving
branches: capacity starts from `v1.0.0rc5` and latency from exact pre-eager commit
`3962f328331b8414a755332aefc3b3d7c261dc6f`. A maintainer may override them at
manual dispatch only with other reviewed/trusted refs.

## Persistent-runner hygiene

Unlike GitHub-hosted runners, this machine survives between jobs. ARM64
workflows therefore:

- create a fresh virtual environment below `RUNNER_TEMP`;
- use isolated Mosquitto configuration and data paths;
- record and terminate the broker PID in an `always()` cleanup step;
- disable persisted checkout credentials;
- avoid Docker and privileged host operations.

A hard-killed job can still bypass cleanup. If a later job reports a
pre-existing runner-owned Mosquitto process, inspect and terminate it before
retrying. Do not weaken the preflight to make the benchmark proceed on a dirty
host.

## ChatGPT Persistent Workbench

The pull-request controller smoke runs on `ubuntu-24.04`, because it imports
controller code from the pull request and must not execute that code directly
on a self-hosted machine.

The actual workbench remains a separate trusted self-hosted workload. Its
runner selector is configurable with:

- `CGW_RUNNER_ARCH` (default: `x64`);
- `CGW_RUNNER_LABEL` (default: `cgw`).

The workbench needs Docker and a persistent workspace. Docker access is
effectively host-root-equivalent, so do not point it at the Raspberry Pi merely
to reuse the ARM64 CI runner. Only do so if the Pi is intentionally isolated
for that trust level and the required ARM64 container images are available.
