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

The existing GitHub-hosted PR, release, fuzz, macOS, and Windows workflows
remain the primary gates.

## Host setup

Keep the runner account unprivileged. Do not add `github-runner` to sudoers.

Install the small set of system dependencies once from an administrator
account:

```bash
sudo apt update
sudo apt install -y mosquitto mosquitto-clients openssl
```

The ARM64 workflows intentionally do not run `apt`, `sudo`, Docker, or `tc`.

For benchmark evidence, use active cooling and set the CPU frequency governor
to `performance` before running the dedicated benchmark workflows. The
benchmark preflight rejects excessive background load, a non-performance
governor, or missing/unsafe temperature readings.

WAN/netem measurements stay on GitHub-hosted infrastructure. Giving the
persistent runner permission to mutate network qdiscs would widen its host
privileges for little additional ARM-specific coverage.

## Persistent-runner hygiene

Unlike GitHub-hosted runners, this machine survives between jobs. ARM64
workflows therefore:

- create a fresh virtual environment below `RUNNER_TEMP`;
- use isolated Mosquitto configuration and data paths;
- record and terminate the broker PID in an `always()` cleanup step;
- disable persisted checkout credentials;
- avoid Docker and privileged host operations.

A hard-killed job can still bypass cleanup. If a later job reports that one of
the dedicated ports is already occupied, inspect and terminate the stale
runner-owned Mosquitto process before retrying.

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
