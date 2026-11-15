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
- Measured locally: the proof about twenty seconds, the mutants about two
  minutes, both after the tools are installed. Installing Kani compiles
  `kani-verifier` and downloads its bundle; that time was not measured in
  CI. A cache of `~/.cargo/bin/kani`, `~/.cargo/bin/cargo-kani` and
  `~/.kani`, keyed on `KANI_VERSION`, would avoid it.
- With the current installer the step "Install the pinned Kani" accepts a
  tool it should refuse (see `../install-tools/`). The preflight does not
  rely on it: it checks the tool `PATH` selects itself.
- Verus is not needed: the trial is not part of the reception.
