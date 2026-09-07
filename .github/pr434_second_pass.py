from __future__ import annotations

import ast
from pathlib import Path


def _tree(path: str) -> ast.Module:
    return ast.parse(Path(path).read_text(encoding="utf-8"))


def _class_method(path: str, class_name: str, method_name: str) -> ast.FunctionDef:
    for node in _tree(path).body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for item in node.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)) and item.name == method_name:
                    assert isinstance(item, ast.FunctionDef)
                    return item
    raise AssertionError(f"missing {class_name}.{method_name} in {path}")


def _function(path: str, name: str) -> ast.FunctionDef:
    for node in _tree(path).body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"missing function {name} in {path}")


def _replace_span(path: str, start: int, end: int, replacement: str) -> None:
    p = Path(path)
    lines = p.read_text(encoding="utf-8").splitlines(keepends=True)
    lines[start - 1 : end] = [replacement.rstrip() + "\n"] if replacement else []
    p.write_text("".join(lines), encoding="utf-8")


def replace_method(path: str, class_name: str, method_name: str, replacement: str) -> None:
    node = _class_method(path, class_name, method_name)
    assert node.end_lineno is not None
    _replace_span(path, node.lineno, node.end_lineno, replacement)


def remove_methods(path: str, class_name: str, names: tuple[str, ...]) -> None:
    spans: list[tuple[int, int, str]] = []
    for name in names:
        node = _class_method(path, class_name, name)
        assert node.end_lineno is not None
        spans.append((node.lineno, node.end_lineno, name))
    for start, end, _name in sorted(spans, reverse=True):
        _replace_span(path, start, end, "")


def replace_test(path: str, name: str, replacement: str) -> None:
    node = _function(path, name)
    assert node.end_lineno is not None
    _replace_span(path, node.lineno, node.end_lineno, replacement)


def remove_test(path: str, name: str) -> None:
    node = _function(path, name)
    assert node.end_lineno is not None
    _replace_span(path, node.lineno, node.end_lineno, "")


def assert_no_runtime_attribute_users(names: set[str]) -> None:
    offenders: list[str] = []
    for path in Path("src").rglob("*.py"):
        if path.as_posix() in {
            "src/mqttium/persistence/memory.py",
            "src/mqttium/persistence/sqlite.py",
        }:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr in names:
                offenders.append(f"{path}:{node.lineno}:{node.attr}")
    if offenders:
        raise AssertionError("legacy persistence method still used by runtime:\n" + "\n".join(offenders))


legacy = {
    "update_out",
    "out_items",
    "out_pages",
    "pop_in",
    "update_in",
    "in_items",
    "in_pages",
    "contains_in",
}
assert_no_runtime_attribute_users(legacy)

memory = "src/mqttium/persistence/memory.py"
replace_method(
    memory,
    "MemoryInflightStore",
    "out_summary_pages",
    '''    def out_summary_pages(
        self, page_size: int = 256
    ) -> Iterator[tuple[OutboundMessageSummary, ...]]:
        for page in self._pages(self._out, page_size):
            yield tuple(OutboundMessageSummary.from_message(message) for message in page)''',
)
replace_method(
    memory,
    "MemoryInflightStore",
    "complete_in",
    '''    def complete_in(
        self,
        mid: int,
        expected_state: InboundQoSState,
    ) -> InboundRecordMeta | None:
        msg = self._in.get(mid)
        if msg is None or msg.state is not expected_state:
            return None
        self._in.pop(mid)
        if not self._in:
            self._in = {}
        return InboundRecordMeta(
            mid=mid,
            state=msg.state,
            user_acked=msg.user_acked,
            delivered=msg.delivered,
            logical_size=msg.logical_size,
        )''',
)
remove_methods(
    memory,
    "MemoryInflightStore",
    (
        "update_out",
        "out_items",
        "out_pages",
        "pop_in",
        "update_in",
        "in_items",
        "in_pages",
        "contains_in",
    ),
)

sqlite = "src/mqttium/persistence/sqlite.py"
remove_methods(
    sqlite,
    "SqliteInflightStore",
    (
        "update_out",
        "out_items",
        "out_pages",
        "pop_in",
        "update_in",
        "in_items",
        "in_pages",
        "contains_in",
    ),
)
p = Path(sqlite)
text = p.read_text(encoding="utf-8")
old = '''_OUT_PAGE_SQL = (
    "SELECT mid, seq, qos, retain, state, dup, logical_size, topic, properties, payload"
    " FROM outbound WHERE mid IN"
)
'''
if text.count(old) != 1:
    raise AssertionError(f"expected one dead _OUT_PAGE_SQL block, found {text.count(old)}")
p.write_text(text.replace(old, ""), encoding="utf-8")

# Migrate store tests from removed whole-object page helpers to the supported
# metadata/summary paging contract. These tests still prove ordering, deletion
# tolerance, page-size validation and SQLite variable splitting.
test_sqlite = "tests/unit/test_sqlite_store.py"
p = Path(test_sqlite)
text = p.read_text(encoding="utf-8")
text = text.replace("store.out_pages(", "store.out_summary_pages(")
text = text.replace("store.in_pages(", "store.in_index_pages(")
text = text.replace("pages = store.out_pages(", "pages = store.out_summary_pages(")
text = text.replace("pages = store.out_pages", "pages = store.out_summary_pages")
text = text.replace("pages = store.in_pages", "pages = store.in_index_pages")
text = text.replace("store.out_pages(page_size=2)", "store.out_summary_pages(page_size=2)")
old_assert = '    assert [message.mid for message in store.out_items()] == [1, 2]\n'
new_assert = (
    '    assert [message.mid for page in store.out_summary_pages() for message in page] == [1, 2]\n'
)
if text.count(old_assert) != 1:
    raise AssertionError("batch ordering assertion did not match")
text = text.replace(old_assert, new_assert)
p.write_text(text, encoding="utf-8")
remove_test(test_sqlite, "test_update_out_only_touches_hot_state_columns")

packet_test = "tests/unit/test_packet_id_and_store_consistency.py"
replace_test(
    packet_test,
    "test_update_out_preserves_retransmission_order",
    '''def test_transition_out_preserves_retransmission_order(tmp_path) -> None:  # noqa: ANN001
    """A metadata transition must not move a durable record in replay order."""
    from mqttium.persistence.sqlite import SqliteInflightStore

    sqlite = SqliteInflightStore(tmp_path / "order.db")
    try:
        for mid in (1, 2, 3):
            sqlite.put_out(
                OutboundMessage(
                    mid=mid,
                    topic=f"t/{mid}",
                    payload=b"p",
                    qos=QoS.AT_LEAST_ONCE,
                    retain=False,
                    state=OutboundQoSState.WAIT_PUBACK,
                )
            )
        changed = sqlite.transition_out(
            1,
            OutboundQoSState.WAIT_PUBACK,
            OutboundQoSState.WAIT_PUBREC,
        )
        assert changed is not None
        assert [m.mid for page in sqlite.out_summary_pages() for m in page] == [1, 2, 3]
    finally:
        sqlite.close()''',
)

# No implementation should keep the retired names after this pass.
for checked in (memory, sqlite):
    tree = ast.parse(Path(checked).read_text(encoding="utf-8"))
    methods = {
        item.name
        for node in tree.body
        if isinstance(node, ast.ClassDef)
        for item in node.body
        if isinstance(item, ast.FunctionDef)
    }
    overlap = methods & legacy
    if overlap:
        raise AssertionError(f"legacy methods remain in {checked}: {sorted(overlap)}")

print("PR434 second-pass persistence pruning completed")
