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
#
# These are exec wrappers, not symlinks: symlinking the venv's own "python"
# (itself a symlink to python3) into /usr/local/bin makes CPython canonicalize
# the whole chain to the system interpreter and lose the venv, so a bare
# "python" would import against system site-packages. Invoking the venv
# interpreter by its own path keeps venv detection working.
if command -v sudo >/dev/null 2>&1; then
    for tool in python ruff mypy pytest bandit; do
        printf '#!/bin/sh\nexec "%s/.venv/bin/%s" "$@"\n' "$repo_root" "$tool" |
            sudo tee "/usr/local/bin/$tool" >/dev/null
        sudo chmod +x "/usr/local/bin/$tool"
    done
fi

echo "MQTTium development environment ready."
