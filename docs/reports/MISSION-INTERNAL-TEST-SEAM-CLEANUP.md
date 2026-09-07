# Mission — internal test-seam cleanup

Base: `main@95786d44cc45054b8b813951e90df92390e0338b`

## Goal

Remove obsolete private compatibility facades retained only so tests, fuzzers or benchmarks can address the pre-session architecture.

## Success criteria

- Classify every compatibility alias as runtime seam vs test-only seam.
- Retain only seams with a demonstrated runtime/Paho/hot-benchmark requirement.
- Rewrite tests/fuzzers to target current owners (`InboundSession`, `OutboundSession`) or observable behavior rather than preserving old `ProtocolEngine._foo` names.
- Delete obsolete properties/setters and comments that fossilize the previous architecture.
- No supported API change; all affected names are Internal.
- Exact-head CI/fuzz/stateful suites green.

## Non-goals

- Do not remove measured hot-path forwarding decisions merely for aesthetics.
- Do not simplify reconnect/epoch/cancellation machinery without an independently proven redundant contract.
