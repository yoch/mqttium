# Proposed CI job for the pilot proof

`.github/workflows/mqttiumd.yml` is a shared file: lot L6 does not change
it. This is the job the reception of the lot needs, handed to the
integration owner; the directory is removed from the lot once the job is
merged. It has not been run in CI: only its commands were run, locally.

```yaml
  pilot-proof:
    name: pilot proof
    runs-on: ubuntu-24.04
    timeout-minutes: 30
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7
        with:
          ref: ${{ github.event.pull_request.head.sha || github.sha }}
          persist-credentials: false
      - name: Install the pinned toolchain
        run: |
          channel="$(sed -n 's/^channel = "\(.*\)"$/\1/p' rust-toolchain.toml)"
          rustup toolchain install "$channel" --profile minimal --component rustfmt --component clippy
      # Kani brings its own compiler; it never builds the product.
      - name: Install the pinned Kani
        run: tools/install-tools.sh kani
      - name: The preflight refuses a wrong tool
        run: proofs/preflight-selftest.sh
      # Runs the preflight first, then the digests, the harnesses with
      # their witnesses and the coverage of the function.
      - name: Pilot proof of release_if_current
        run: proofs/credits/run.sh
      - name: Mutants, each caught as aimed
        run: python3 proofs/credits/mutants.py
      - name: The proof workspace is formatted and lint-clean
        working-directory: mqttiumd/proofs
        run: |
          cargo fmt --all --check
          cargo clippy --workspace --all-targets --locked -- -D warnings
```

Notes for the owner of the workflow:

- The job inherits `defaults.run.working-directory: mqttiumd`.
- Measured locally on `b0d6b940`, tools already installed:

  | Step | Time |
  | - | - |
  | `proofs/preflight-selftest.sh` | 1 s |
  | `proofs/credits/run.sh`, from an empty `proofs/target` | 20 s |
  | `python3 proofs/credits/mutants.py` | 135 s |
  | `fmt` and `clippy` on `proofs/` | a few seconds |

- What `tools/install-tools.sh kani` leaves on disk: `~/.kani`, 485 MB
  (the Kani bundle with CBMC); the nightly toolchain it installs under
  `~/.rustup/toolchains`, 594 MB, which `~/.kani` links to; and
  `~/.cargo/bin/kani` and `~/.cargo/bin/cargo-kani`, under 1 MB each.
  The time of a cold installation and the size of the download itself
  were not measured: here the tools were installed once, before any
  timing.
- What can be cached, keyed on `KANI_VERSION` of the installer:
  `~/.kani`, the two binaries, and the toolchain
  `nightly-2026-08-21-x86_64-unknown-linux-gnu` of `~/.rustup`. The
  preflight fails when the cache holds another version or another
  toolchain than `proofs/TOOLCHAIN` pins, so a stale cache cannot pass.
  `proofs/target` (31 MB) is not worth caching.
- Verus is not needed: the trial is not part of the reception.
