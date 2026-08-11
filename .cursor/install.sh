#!/usr/bin/env bash
# Idempotent Cloud Agent setup for MQTTium.
#
# Installs the system packages the test suite needs (the Mosquitto broker used by
# the integration tests and the venv/OpenSSL support the TLS tests require) and
# builds a virtual environment with every development extra. Safe to re-run: the
# venv is reused when present and pip installs converge.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if command -v sudo >/dev/null 2>&1; then
    sudo apt-get update -qq
    sudo apt-get install -y -qq mosquitto mosquitto-clients python3-venv openssl
fi

if [ ! -x .venv/bin/python ]; then
    python3 -m venv .venv
fi

.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e ".[dev,fuzz,security,release,benchmark]"

# Expose the venv interpreter and dev tools on PATH so the documented commands
# (python -m pytest, ruff, mypy, bandit, ...) work verbatim in fresh shells.
if command -v sudo >/dev/null 2>&1; then
    for tool in python ruff mypy pytest bandit; do
        sudo ln -sf "$repo_root/.venv/bin/$tool" "/usr/local/bin/$tool"
    done
fi

echo "MQTTium development environment ready."
