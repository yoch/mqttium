from pathlib import Path

path = Path("tests/fuzz/test_stateful_invariants.py")
text = path.read_text(encoding="utf-8")
blocks = (
    '        ("out_items()", lambda s: [_norm_out(m) for m in s.out_items()]),\n',
    '''        (\n            f"out_pages({page_size})",\n            lambda s: [_norm_out(m) for page in s.out_pages(page_size) for m in page],\n        ),\n''',
    '        (f"pop_in({mid})", lambda s: _norm_in(s.pop_in(mid))),\n',
    '        ("in_items()", lambda s: [_norm_in(m) for m in s.in_items()]),\n',
    '''        (\n            f"in_pages({page_size})",\n            lambda s: [_norm_in(m) for page in s.in_pages(page_size) for m in page],\n        ),\n''',
    '        (f"contains_in({mid})", lambda s: s.contains_in(mid)),\n',
)
for block in blocks:
    count = text.count(block)
    if count != 1:
        raise AssertionError(f"expected exactly one stateful legacy operation block, found {count}: {block!r}")
    text = text.replace(block, "")
path.write_text(text, encoding="utf-8")
print("PR434 stateful store model migrated to modern operations")
