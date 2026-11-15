#!/usr/bin/env bash
# Installs the proof, model and fuzzing tools of mqttiumd at pinned versions.
#
#   tools/install-tools.sh               install everything
#   tools/install-tools.sh kani tlc      install only the tools named
#
# Tools: kani (lot L6), verus (lot L6), tlc (lot L5), fuzz (lot L8), deny.
# Each step is skipped when the pinned version is already present. The
# toolchain that builds the product stays the one of rust-toolchain.toml:
# Kani and Verus bring their own compiler and never build the product.
#
# Changing a version here is a change of the evidence that the tool
# produces: say so in the report of the lot.
set -euo pipefail

KANI_VERSION="0.68.0"
VERUS_RELEASE="0.2026.11.02.8bf68ee"
VERUS_SHA256="f3a0119292df8855e4435da8130bee1873b0e6a60c306accf81a08ea687bba3c"
VERUS_TOOLCHAIN="1.99.1-x86_64-unknown-linux-gnu"
CARGO_FUZZ_VERSION="0.13.2"
CARGO_DENY_VERSION="0.20.2"

VERUS_HOME="${VERUS_HOME:-/opt/verus}"
BIN="${CARGO_HOME:-$HOME/.cargo}/bin"
REPOSITORY="$(cd "$(dirname "$0")/../.." && pwd)"

# Whether the tool that PATH selects is the pinned one: the command
# resolves, succeeds, and one whole line of its output is exactly the
# expected line. A version that merely contains the pinned text, or a
# command that prints it and fails, is not the pinned tool.
#
#   present "<expected line>" command [arguments]
#
# The output is captured first: a pipe into `grep -q` would end the command
# early and, under pipefail, read as a failure.
present() {
    expected="$1"
    shift
    command -v "$1" >/dev/null 2>&1 || return 1
    output="$("$@" 2>/dev/null)" || return 1
    grep -Fxq -- "$expected" <<<"$output"
}

# After an installation: the tool that PATH selects must now be the pinned
# one. An installation into a directory that PATH does not search, or
# behind another copy, is a failure, not a success.
selected() {
    if present "$@"; then
        echo "  $2: $(command -v "$2")"
        return 0
    fi
    echo "$2: PATH selects $(command -v "$2" || echo nothing)," \
        "which does not report: $1" >&2
    return 1
}

# Both entry points are checked: `kani` and the `cargo-kani` that the
# proofs run. `cargo-kani` is called as cargo calls it, by its own path.
install_kani() {
    standalone="Kani Rust Verifier $KANI_VERSION (standalone)"
    plugin="Kani Rust Verifier $KANI_VERSION (cargo plugin)"
    if present "$standalone" kani --version && present "$plugin" cargo-kani kani --version; then
        echo "kani $KANI_VERSION: present"
        return
    fi
    cargo install --locked kani-verifier --version "$KANI_VERSION"
    cargo kani setup
    selected "$standalone" kani --version
    selected "$plugin" cargo-kani kani --version
    echo "kani $KANI_VERSION: installed"
}

install_verus() {
    version_line="  Version: $VERUS_RELEASE"
    if present "$version_line" verus --version; then echo "verus $VERUS_RELEASE: present"; return; fi
    if [ "$(uname -s)-$(uname -m)" != "Linux-x86_64" ]; then
        echo "verus: only the Linux x86-64 release is pinned here" >&2
        return 1
    fi
    rustup toolchain install "$VERUS_TOOLCHAIN" --profile minimal
    mkdir -p "$VERUS_HOME"
    archive="$VERUS_HOME/verus.zip"
    curl -sSfL -o "$archive" \
        "https://github.com/verus-lang/verus/releases/download/release/$VERUS_RELEASE/verus-$VERUS_RELEASE-x86-linux.zip"
    if ! echo "$VERUS_SHA256  $archive" | sha256sum -c -; then
        rm -f "$archive"
        echo "verus: the release archive does not have the pinned digest" >&2
        return 1
    fi
    unzip -q -o "$archive" -d "$VERUS_HOME"
    rm "$archive"
    ln -sf "$VERUS_HOME/verus-x86-linux/verus" "$BIN/verus"
    ln -sf "$VERUS_HOME/verus-x86-linux/cargo-verus" "$BIN/cargo-verus"
    selected "$version_line" verus --version
    echo "verus $VERUS_RELEASE: installed"
}

# The TLC jar is pinned and hash-checked by the runner of the repository.
install_tlc() {
    java -version >/dev/null 2>&1 || { echo "tlc: a Java runtime is required" >&2; return 1; }
    jar="$(cd "$REPOSITORY" && python3 -c \
        'import sys; sys.path.insert(0, "tools/formal"); import run_tlc; print(run_tlc.tla2tools_jar())')"
    echo "tlc: $jar"
}

install_cargo_tool() {
    crate="$1" version="$2" subcommand="$3"
    if present "$crate $version" "$crate" "$subcommand" --version; then
        echo "$crate $version: present"
        return
    fi
    cargo install --locked "$crate" --version "$version"
    selected "$crate $version" "$crate" "$subcommand" --version
    echo "$crate $version: installed"
}

[ "$#" -gt 0 ] || set -- kani verus tlc fuzz deny
for tool in "$@"; do
    case "$tool" in
        kani) install_kani ;;
        verus) install_verus ;;
        tlc) install_tlc ;;
        fuzz) install_cargo_tool cargo-fuzz "$CARGO_FUZZ_VERSION" fuzz ;;
        deny) install_cargo_tool cargo-deny "$CARGO_DENY_VERSION" deny ;;
        *) echo "unknown tool: $tool (kani, verus, tlc, fuzz, deny)" >&2; exit 2 ;;
    esac
done
