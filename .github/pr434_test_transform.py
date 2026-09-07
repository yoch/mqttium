from pathlib import Path


def rewrite(path: str, old: str, new: str, *, count: int = 1, label: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    actual = text.count(old)
    if actual != count:
        raise AssertionError(f"{label}: expected {count}, found {actual}")
    p.write_text(text.replace(old, new), encoding="utf-8")


def remove_between(path: str, start: str, end: str, *, label: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    if text.count(start) != 1 or text.count(end) != 1:
        raise AssertionError(
            f"{label}: non-unique boundaries start={text.count(start)} end={text.count(end)}"
        )
    left, rest = text.split(start, 1)
    _removed, right = rest.split(end, 1)
    p.write_text(left + end + right, encoding="utf-8")


# The public capability Protocol no longer exists: paging is part of the one
# supported store contract rather than a runtime-detected extension.
path = "tests/unit/test_configuration_atomicity_and_teardown.py"
rewrite(
    path,
    "from mqttium.persistence import MemoryInflightStore, PagedInflightStore\n",
    "",
    label="configuration test capability import",
)
rewrite(
    path,
    '''\n\ndef test_paged_store_protocol_is_exported_from_the_public_package() -> None:\n    assert isinstance(MemoryInflightStore(), PagedInflightStore)\n''',
    "",
    label="obsolete paged protocol export test",
)

path = "tests/unit/test_sqlite_store.py"
rewrite(
    path,
    "from mqttium.persistence.memory import MemoryInflightStore, PagedInflightStore\n",
    "from mqttium.persistence.memory import MemoryInflightStore\n",
    label="sqlite test capability import",
)
rewrite(
    path,
    "from mqttium.persistence.sqlite import SqliteInflightStore\nfrom mqttium.protocol.engine import EngineConfig, ProtocolEngine\nfrom mqttium.types import InboundMessage, OutboundMessage, Properties\n",
    "from mqttium.persistence.sqlite import SqliteInflightStore\nfrom mqttium.types import InboundMessage, OutboundMessage, Properties\n",
    label="sqlite fallback-only top-level engine import",
)
remove_between(
    path,
    "def test_shipped_stores_satisfy_the_paged_protocol(tmp_path: Path) -> None:\n",
    "def test_pages_split_the_fetch_under_the_sql_variable_limit(tmp_path: Path) -> None:\n",
    label="paged capability and eager fallback tests",
)

path = "tests/unit/test_store_transitions.py"
rewrite(
    path,
    '"""Payload-free record transitions and the whole-object fallback path."""\n',
    '"""Payload-free record transitions required by the persistence contract."""\n',
    label="transition test module contract",
)
rewrite(
    path,
    "from contextlib import AbstractContextManager, contextmanager, nullcontext\n",
    "from contextlib import contextmanager\n",
    label="legacy store context imports",
)
rewrite(path, "\nimport pytest\n", "", label="legacy pytest import")
rewrite(
    path,
    "from mqttium.persistence.memory import MemoryInflightStore, TransitionInflightStore\n",
    "from mqttium.persistence.memory import MemoryInflightStore\n",
    label="transition protocol import",
)
rewrite(
    path,
    "from mqttium.types import InboundMessage, OutboundMessage\n",
    "from mqttium.types import OutboundMessage\n",
    label="legacy inbound fixture import",
)
remove_between(
    path,
    "class PlainInflightStore:\n",
    "def connack(\n",
    label="legacy whole-object store fixture",
)
remove_between(
    path,
    "def test_shipped_stores_satisfy_the_transition_protocol(tmp_path: Path) -> None:\n",
    "def test_puback_settles_a_sqlite_record_without_reading_the_payload(tmp_path: Path) -> None:\n",
    label="runtime capability protocol test",
)
remove_between(
    path,
    '@pytest.mark.parametrize("qos", [1, 2])\n',
    '@pytest.mark.parametrize("store_factory", [MemoryInflightStore, PlainInflightStore])\n',
    label="legacy fallback behavior tests",
)
rewrite(
    path,
    '@pytest.mark.parametrize("store_factory", [MemoryInflightStore, PlainInflightStore])\ndef test_negative_pubrec_never_answers_with_pubrel(store_factory: type) -> None:\n    """MQTT 5 §4.3.3: reason >= 0x80 ends the exchange, with or without transitions.\n\n    The reason-code test used to live inside the transition branch only, so a\n    store without conditional transitions reached the "no such record" test\n    first and answered an unknown identifier with an orphan PUBREL 0x92. Both\n    store shapes must now stay silent.\n    """\n    engine = connected_engine(store_factory(), protocol=MQTTProtocolVersion.MQTTv5)\n',
    'def test_negative_pubrec_never_answers_with_pubrel() -> None:\n    """MQTT 5 §4.3.3: a negative PUBREC ends the exchange without PUBREL."""\n    engine = connected_engine(MemoryInflightStore(), protocol=MQTTProtocolVersion.MQTTv5)\n',
    label="negative PUBREC modern contract test",
)

# No old capability names or fallback fixture may survive the migrated tests.
for checked_path in (
    "tests/unit/test_configuration_atomicity_and_teardown.py",
    "tests/unit/test_sqlite_store.py",
    "tests/unit/test_store_transitions.py",
):
    text = Path(checked_path).read_text(encoding="utf-8")
    for forbidden in ("PlainInflightStore", "TransitionInflightStore", "PagedInflightStore"):
        if forbidden in text:
            raise AssertionError(f"legacy store term remains in {checked_path}: {forbidden}")

print("PR434 persistence test migration completed")
