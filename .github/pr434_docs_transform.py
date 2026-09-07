from pathlib import Path


def replace(path: str, old: str, new: str, *, label: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise AssertionError(f"{label}: expected exactly one match, found {count}")
    p.write_text(text.replace(old, new), encoding="utf-8")


replace(
    "docs/api-stability.md",
    "- `mqttium.persistence` store protocols and implementations;\n",
    "- `mqttium.persistence.InflightStore` and the shipped persistence implementations;\n",
    label="api stability persistence bullet",
)
replace(
    "docs/api-stability.md",
    "A Provisional designation is not permission for silent breakage. An incompatible\nchange still requires a changelog entry and migration guidance.\n",
    "A Provisional designation is not permission for silent breakage. An incompatible\nchange still requires a changelog entry and migration guidance. The persistence\ncontract is one complete `InflightStore` interface: bounded replay and conditional\nmetadata transitions are required capabilities, not optional runtime-detected\nextensions.\n",
    label="api stability provisional persistence clarification",
)

replace(
    "docs/migration.md",
    "Third-party `InflightStore` implementations remain supported. Implementing the\noptional `PagedInflightStore` protocol enables incremental replay without\nmaterialising every payload at once. The fallback eager path is correct but can\nuse substantially more memory for large sessions.\n",
    "Third-party `InflightStore` implementations remain supported, but the Provisional\ncontract is now complete rather than capability-discovered. Custom stores must\nimplement the bounded replay, metadata lookup, conditional transition, and\nconditional completion methods declared by `InflightStore`; MQTTium no longer\nfalls back to eager whole-store hydration or read/mutate/write state transitions.\n\nThe former `PagedInflightStore`, `BoundedInboundReplayStore`, and\n`TransitionInflightStore` capability protocols are removed. The shipped stores\nalso no longer expose the retired whole-object helpers `update_out`, `out_items`,\n`out_pages`, `pop_in`, `update_in`, `in_items`, `in_pages`, or `contains_in`. Use\n`out_summary_pages()` / `in_index_pages()` for ordered metadata inspection,\n`get_out()` / `get_in()` when a payload is actually needed, and the\n`transition_*()` / `complete_*()` methods for state changes.\n",
    label="migration persistence contract",
)

replace(
    "docs/sessions-and-persistence.md",
    "| Updating an outbound or inbound record that is absent | `KeyError` |\n",
    "| Updating metadata for an outbound or inbound record that is absent | `KeyError` |\n",
    label="sqlite failure table wording",
)
replace(
    "docs/sessions-and-persistence.md",
    "Third-party `InflightStore` implementations remain supported. Implementing the\noptional paged and transition protocols avoids eager replay and payload reads on\nacknowledgement; the minimum store protocol remains correct but may use more\nmemory.\n\nThe runtime capability matrix is deliberately additive rather than a second\nstore hierarchy:\n\n| Contract | Required guarantee | Operational consequence |\n| --- | --- | --- |\n| `InflightStore` | atomic `batch()` mutations and ordered whole-record iteration | correctness and third-party compatibility; replay may materialise the store |\n| `PagedInflightStore` | ordered pages and payload-free outbound summaries | outbound recovery memory proportional to one page |\n| `BoundedInboundReplayStore` | metadata count plus message/byte-bounded hydration | inbound replay memory bounded by one batch, including large sessions |\n| `TransitionInflightStore` | conditional atomic state changes and metadata-only lookup | acknowledgements avoid payload reads; QoS 2 phase-two compaction is durable |\n\nBoth shipped stores implement every extension, and sessions resolve those\ncapabilities once when the engine is constructed. A legacy third-party store\ntherefore keeps the base correctness semantics with eager replay and\nbest-effort phase-two compaction; it does not silently acquire atomic\nconditional-transition guarantees from a read/update fallback.\n",
    "Third-party `InflightStore` implementations remain supported through one complete\nProvisional contract. MQTTium does not detect persistence capabilities at runtime\nand there is no weaker eager-replay or read/mutate/write fallback. A custom store\nmust provide the same semantic guarantees used by the shipped stores:\n\n| Required part of `InflightStore` | Guarantee | Operational consequence |\n| --- | --- | --- |\n| `batch()`, point reads/writes/deletes, and clear operations | atomic mutation groups and durable record ownership | rollback and session cleanup have one store path |\n| `out_summary_pages()` and `in_index_pages()` | ordered payload-free metadata pages | recovery accounting does not hydrate every payload |\n| `in_replay_pages()` and `in_count()` | message/byte-bounded inbound hydration | large inbound sessions replay with bounded resident payload memory |\n| `out_meta()` / `in_meta()`, `transition_*()`, and `complete_*()` | conditional metadata-only state changes | ACK handling avoids payload reads and QoS state changes remain atomic |\n| logical-size and delivered-state metadata updates | restart-safe admission accounting and inbound delivery state | recovered byte limits match durable ownership |\n\nThe former `PagedInflightStore`, `BoundedInboundReplayStore`, and\n`TransitionInflightStore` capability protocols are removed. So are the shipped\nstores' legacy whole-object iteration/update helpers. Code that needs a full\nrecord after reading a metadata page should call `get_out()` or `get_in()` for\nthat identifier.\n",
    label="sessions persistence capability matrix",
)

replace(
    "CHANGELOG.md",
    "## [Unreleased]\n\n### Changed\n\n",
    "## [Unreleased]\n\n### Changed\n\n- Unify the Provisional persistence API around one complete `InflightStore`\n  contract. Bounded replay, payload-free metadata paging, and conditional state\n  transitions/completion are now required instead of runtime-detected optional\n  capabilities. Remove `PagedInflightStore`, `BoundedInboundReplayStore`, and\n  `TransitionInflightStore`, together with the eager/whole-object fallback paths.\n  `MemoryInflightStore` and `SqliteInflightStore` also drop the retired helpers\n  `update_out`, `out_items`, `out_pages`, `pop_in`, `update_in`, `in_items`,\n  `in_pages`, and `contains_in`. Third-party stores must implement the modern\n  `InflightStore` contract; see the migration and persistence guides.\n\n",
    label="unreleased persistence changelog",
)

# Public documentation outside the historical mission report must no longer
# advertise the deleted capability hierarchy.
for path in Path("docs").rglob("*.md"):
    if path.as_posix().startswith("docs/reports/"):
        continue
    text = path.read_text(encoding="utf-8")
    for legacy in ("PagedInflightStore", "BoundedInboundReplayStore", "TransitionInflightStore"):
        # The migration/persistence guides intentionally name the removed APIs
        # once to tell users what changed; other documents must not describe them.
        if legacy in text and path.as_posix() not in {
            "docs/migration.md",
            "docs/sessions-and-persistence.md",
        }:
            raise AssertionError(f"legacy capability {legacy} still documented in {path}")

print("PR434 persistence documentation migration completed")
