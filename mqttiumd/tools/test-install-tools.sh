#!/usr/bin/env bash
# Discriminating test of tools/install-tools.sh: how it recognises a tool
# that is already present, and whether the tool it reports is the one PATH
# selects.
#
#   tools/test-install-tools.sh [path to install-tools.sh]
#
# Nothing real is installed: each case runs the installer for cargo-fuzz
# with a PATH made of a case directory, a fake CARGO_HOME/bin and the
# system directories. `cargo` is a fake that records `cargo install` and,
# when the case says so, drops a tool reporting the pinned version into
# CARGO_HOME/bin.
set -euo pipefail

INSTALLER="${1:-$(cd "$(dirname "$0")" && pwd)/install-tools.sh}"
VERSION="$(sed -n 's/^CARGO_FUZZ_VERSION="\(.*\)"$/\1/p' "$INSTALLER")"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
status=0

script() { # path, body
    mkdir -p "$(dirname "$1")"
    printf '#!/bin/sh\n%s\n' "$2" >"$1"
    chmod +x "$1"
}

# name, body of the cargo-fuzz found first on PATH ("" for none),
# whether `cargo install` really provides the pinned tool (yes/no)
prepare() {
    dir="$work/$1"
    mkdir -p "$dir/front" "$dir/home/bin"
    [ -z "$2" ] || script "$dir/front/cargo-fuzz" "$2"
    provide=":"
    if [ "$3" = yes ]; then
        provide="printf '#!/bin/sh\\necho cargo-fuzz $VERSION\\n' >'$dir/home/bin/cargo-fuzz'; chmod +x '$dir/home/bin/cargo-fuzz'"
    fi
    script "$dir/front/cargo" "
if [ \"\$1\" = install ]; then echo \"\$*\" >>'$dir/installs'; $provide; exit 0; fi
sub=\"\$1\"; exec \"cargo-\$sub\" \"\$@\""
    : >"$dir/installs"
}

run() { # name -> $code, output in $work/<name>/output
    dir="$work/$1"
    set +e
    PATH="$dir/front:$dir/home/bin:/usr/bin:/bin" CARGO_HOME="$dir/home" \
        bash "$INSTALLER" fuzz >"$dir/output" 2>&1
    code=$?
    set -e
}

expect() { # name, description, condition...
    name="$1" description="$2"
    shift 2
    if "$@"; then
        echo "ok    $name: $description"
    else
        echo "FAIL  $name: $description"
        sed 's/^/        /' "$work/$name/output"
        status=1
    fi
}

installed() { grep -q "^install --locked cargo-fuzz --version $VERSION\$" "$work/$1/installs"; }
not_installed() { [ ! -s "$work/$1/installs" ]; }
succeeded() { [ "$code" -eq 0 ]; }
failed() { [ "$code" -ne 0 ]; }
says() { grep -q -- "$2" "$work/$1/output"; }

# 1. The exit code counts: the pinned text from a failing command is not
#    a present tool.
prepare failing-command "echo 'cargo-fuzz $VERSION'; exit 1" no
run failing-command
expect failing-command "a failing version command is not 'present'" installed failing-command

# 2. The pinned version as a substring of another version.
prepare substring-of-version "echo 'cargo-fuzz 1$VERSION'" no
run substring-of-version
expect substring-of-version "a longer version is not 'present'" installed substring-of-version

# 3. The pinned version as a substring of any output.
prepare substring-of-noise "echo 'warning: cargo-fuzz $VERSION is yanked; this is cargo-fuzz 0.0.1'" no
run substring-of-noise
expect substring-of-noise "the pinned text inside another line is not 'present'" installed substring-of-noise

# 4. The tool PATH selects: an installation behind another copy on PATH
#    must not be reported as done.
prepare shadowed-install "echo 'cargo-fuzz 0.0.1'" yes
run shadowed-install
expect shadowed-install "the installation ran" installed shadowed-install
expect shadowed-install "an installation shadowed on PATH is a failure" failed
expect shadowed-install "the failure names what PATH selects" \
    says shadowed-install "PATH selects $work/shadowed-install/front/cargo-fuzz"

# 5. An installation that provides nothing must not be reported as done.
prepare empty-install "" no
run empty-install
expect empty-install "an installation that provides no tool is a failure" failed

# 6. The ordinary path still works: install once, then present.
prepare ordinary "" yes
run ordinary
expect ordinary "a real installation succeeds" succeeded
expect ordinary "it is reported as installed" says ordinary "cargo-fuzz $VERSION: installed"
: >"$work/ordinary/installs"
run ordinary
expect ordinary "the second run succeeds" succeeded
expect ordinary "the second run finds the tool present" says ordinary "cargo-fuzz $VERSION: present"
expect ordinary "the second run installs nothing" not_installed ordinary

exit "$status"
