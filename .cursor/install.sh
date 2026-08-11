#!/usr/bin/env bash
# Idempotent Cloud Agent setup for MQTTium.
#
# Provides everything the documented workflow needs:
#   * Docker (so brokers can be launched in containers) plus a native mosquitto
#     fallback and the mosquitto clients for debugging;
#   * every supported CPython (3.11 - 3.14) via uv, each in its own venv so the
#     whole CI test matrix can run locally;
#   * a primary 3.12 environment with all development extras, exposed on PATH as
#     the bare `python`, `ruff`, `mypy`, `pytest` and `bandit` commands.
# Safe to re-run: existing venvs are reused and installs converge.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

primary_version=3.12
matrix_versions=(3.11 3.13 3.14)

# --- System packages -------------------------------------------------------
if command -v sudo >/dev/null 2>&1; then
    sudo apt-get update -qq
    sudo apt-get install -y -qq \
        docker.io iptables uidmap \
        mosquitto mosquitto-clients \
        python3-venv openssl ca-certificates curl
fi

# --- uv + all supported CPython versions -----------------------------------
uv=""
for candidate in "$HOME/.local/bin/uv" "$(command -v uv 2>/dev/null || true)"; do
    if [ -n "$candidate" ] && "$candidate" --version >/dev/null 2>&1; then
        uv="$candidate"
        break
    fi
done
if [ -z "$uv" ]; then
    curl -LsSf https://astral.sh/uv/install.sh | sh
    uv="$HOME/.local/bin/uv"
fi
if command -v sudo >/dev/null 2>&1; then
    sudo ln -sf "$(readlink -f "$uv")" /usr/local/bin/uv
    uv=/usr/local/bin/uv
fi
"$uv" python install "$primary_version" "${matrix_versions[@]}"

# --- Virtual environments --------------------------------------------------
# Primary 3.12 environment carries every development extra.
if [ ! -x .venv/bin/python ]; then
    "$uv" venv --python "$primary_version" .venv
fi
VIRTUAL_ENV="$repo_root/.venv" "$uv" pip install -e ".[dev,fuzz,security,release,benchmark]"

# Matrix environments carry what the unit + fuzz suites need.
for version in "${matrix_versions[@]}"; do
    venv=".venv-$version"
    if [ ! -x "$venv/bin/python" ]; then
        "$uv" venv --python "$version" "$venv"
    fi
    VIRTUAL_ENV="$repo_root/$venv" "$uv" pip install -e ".[dev,fuzz]"
done

# --- PATH wrappers ---------------------------------------------------------
# Exec wrappers, not symlinks: symlinking a venv's own `python` (itself a
# symlink) into /usr/local/bin makes CPython canonicalize the chain to the
# system interpreter and lose the venv. Invoking the interpreter by its own
# path keeps venv detection working.
make_wrapper() {
    local name="$1" target="$2"
    printf '#!/bin/sh\nexec "%s" "$@"\n' "$target" | sudo tee "/usr/local/bin/$name" >/dev/null
    sudo chmod +x "/usr/local/bin/$name"
}

if command -v sudo >/dev/null 2>&1; then
    for tool in python ruff mypy pytest bandit; do
        make_wrapper "$tool" "$repo_root/.venv/bin/$tool"
    done
    make_wrapper "python$primary_version" "$repo_root/.venv/bin/python"
    for version in "${matrix_versions[@]}"; do
        make_wrapper "python$version" "$repo_root/.venv-$version/bin/python"
    done
fi

# --- Pre-pull the broker image so the first boot is fast -------------------
if bash "$repo_root/.cursor/docker.sh"; then
    docker pull eclipse-mosquitto:2 >/dev/null 2>&1 || true
fi

echo "MQTTium development environment ready (python $primary_version default; matrix ${matrix_versions[*]})."
