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

# A store without bounded replay is no longer supported. Keep every streaming
# replay test, remove only the explicit eager-fallback contract test.
path = "tests/unit/test_inbound_replay_streaming.py"
remove_between(
    path,
    "def test_a_store_without_paging_keeps_the_eager_replay() -> None:\n",
    "async def test_client_replays_every_message_through_the_effect_pump() -> None:\n",
    label="legacy eager inbound replay test",
)

# Retransmission no longer writes a whole record merely to persist DUP/cache
# state. Inject the replay failure at the real materialisation boundary instead.
path = "tests/unit/test_engine_transaction_failures.py"
rewrite(
    path,
    '''class _FailPubrelReplayStore(MemoryInflightStore):\n    def update_out(self, msg: OutboundMessage) -> None:\n        if msg.state is OutboundQoSState.WAIT_PUBCOMP:\n            raise RuntimeError("PUBREL replay write failed")\n        super().update_out(msg)\n''',
    '''class _FailPubrelReplayStore(MemoryInflightStore):\n    def get_out(self, mid: int) -> OutboundMessage | None:\n        msg = super().get_out(mid)\n        if msg is not None and msg.state is OutboundQoSState.WAIT_PUBCOMP:\n            raise RuntimeError("PUBREL replay materialisation failed")\n        return msg\n''',
    label="PUBREL replay modern failure boundary",
)

# Preserve the durable-delete ownership test by moving its primary fault from
# the obsolete update_out write to the still-real get_out materialisation.
path = "tests/unit/test_outbound_durable_delete_ownership.py"
rewrite(
    path,
    '''    original_update = type(store).update_out\n    original_delete = type(store).delete_out\n    faults = {"update": 1, "delete": 1}\n\n    def update_out(self: Any, msg: Any) -> None:\n        if msg.mid == handle.mid and faults["update"]:\n            faults["update"] -= 1\n            raise _Boom("retransmit update failed")\n        original_update(self, msg)\n\n    def delete_out(self: Any, mid: int) -> bool:\n''',
    '''    original_get = type(store).get_out\n    original_delete = type(store).delete_out\n    faults = {"get": 1, "delete": 1}\n\n    def get_out(self: Any, mid: int) -> Any:\n        if mid == handle.mid and faults["get"]:\n            faults["get"] -= 1\n            raise _Boom("replay materialisation failed")\n        return original_get(self, mid)\n\n    def delete_out(self: Any, mid: int) -> bool:\n''',
    label="durable delete modern primary failure",
)
rewrite(
    path,
    '''    monkeypatch.setattr(type(store), "update_out", update_out)\n    monkeypatch.setattr(type(store), "delete_out", delete_out)\n''',
    '''    monkeypatch.setattr(type(store), "get_out", get_out)\n    monkeypatch.setattr(type(store), "delete_out", delete_out)\n''',
    label="durable delete modern monkeypatch",
)
rewrite(
    path,
    '    assert str(raised.value.__context__) == "retransmit update failed"\n',
    '    assert str(raised.value.__context__) == "replay materialisation failed"\n',
    label="durable delete modern context assertion",
)

# Runtime-detected transition capability is gone. Behavioral transition tests
# remain elsewhere; this test existed only to prove the optional Protocol.
path = "tests/unit/test_packet_id_and_store_consistency.py"
remove_between(
    path,
    "def test_both_built_in_stores_compact_through_the_transition_extension() -> None:\n",
    "__PR434_EOF_SENTINEL__",
    label="transition extension capability test",
) if False else None
p = Path(path)
text = p.read_text(encoding="utf-8")
old = '''\n\ndef test_both_built_in_stores_compact_through_the_transition_extension() -> None:\n    """The engine never depends on update_out for compaction: both built-in\n    stores implement TransitionInflightStore, so on_pubrec takes that path."""\n    from mqttium.persistence.memory import TransitionInflightStore\n    from mqttium.persistence.sqlite import SqliteInflightStore\n\n    assert isinstance(MemoryInflightStore(), TransitionInflightStore)\n    assert issubclass(SqliteInflightStore, TransitionInflightStore)\n'''
if text.count(old) != 1:
    raise AssertionError(f"transition extension capability block: found {text.count(old)}")
p.write_text(text.replace(old, ""), encoding="utf-8")

# No old capability names or fallback fixture may survive the migrated tests.
for checked_path in (
    "tests/unit/test_configuration_atomicity_and_teardown.py",
    "tests/unit/test_sqlite_store.py",
    "tests/unit/test_store_transitions.py",
    "tests/unit/test_inbound_replay_streaming.py",
    "tests/unit/test_packet_id_and_store_consistency.py",
):
    text = Path(checked_path).read_text(encoding="utf-8")
    for forbidden in ("PlainInflightStore", "TransitionInflightStore", "PagedInflightStore"):
        if forbidden in text:
            raise AssertionError(f"legacy store term remains in {checked_path}: {forbidden}")

print("PR434 persistence test migration completed")
