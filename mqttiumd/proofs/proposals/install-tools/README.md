# Proposed correction of `tools/install-tools.sh`

`tools/install-tools.sh` is a shared file: lot L6 does not change it. This
directory hands the correction to the integration owner and is removed
from the lot once the correction is merged.

| File | Becomes |
| - | - |
| `install-tools.sh` | `tools/install-tools.sh`, replacing the current file |
| `test-install-tools.sh` | `tools/test-install-tools.sh`, new |

## The defects

1. `present()` runs the version command with `|| true`, so a command that
   fails is read like one that succeeds.
2. `present()` accepts the pinned version anywhere in the output: `10.68.0`
   contains `0.68.0`, and so does an error message that quotes it.
3. After an installation nothing looks at what `PATH` selects. A tool
   installed into a directory that `PATH` does not search, or behind
   another copy, is reported as installed. For the cargo tools the check
   ran `cargo <subcommand>`, and cargo looks in `$CARGO_HOME/bin` before
   `PATH`.

## The correction

- `present "<expected line>" command…` requires the command to resolve on
  `PATH`, to succeed, and one whole line of its output to be exactly the
  expected line.
- `selected` runs the same check after each installation and fails, naming
  the file `PATH` selects, when it is not the pinned tool.
- Kani is checked through both entry points, `kani` and `cargo-kani`; the
  cargo tools are called by their own file, as `PATH` resolves it.

The expected lines were read from the pinned tools as installed here:
`Kani Rust Verifier 0.68.0 (standalone)`, `Kani Rust Verifier 0.68.0
(cargo plugin)`, `  Version: 0.2026.11.02.8bf68ee`, `cargo-fuzz 0.13.2`,
`cargo-deny 0.20.2`.

## The test

`test-install-tools.sh [installer]` installs nothing real: it runs the
installer for cargo-fuzz against fake tools on a private `PATH`.

Against the current installer it fails five checks (the three forms of a
wrongly recognised tool, the shadowed installation, the installation that
provides nothing); against the corrected one it passes all twelve. Test
first: commit the test while it fails, then the correction.
