"""SqliteInflightStore durability, migration and hot-path tests."""

from __future__ import annotations

import base64
from pathlib import Path

from mqttium.enums import InboundQoSState, OutboundQoSState, QoS
from mqttium.persistence.sqlite import SqliteInflightStore
from mqttium.types import InboundMessage, OutboundMessage, Properties


def outbound(mid: int = 7) -> OutboundMessage:
    return OutboundMessage(
        mid=mid,
        topic="a/b",
        payload=b"payload",
        qos=QoS.AT_LEAST_ONCE,
        retain=False,
        state=OutboundQoSState.WAIT_PUBACK,
        dup=False,
    )


def test_sqlite_outbound_roundtrip_and_blob_storage(tmp_path: Path) -> None:
    path = tmp_path / "inflight.db"
    store = SqliteInflightStore(path)
    store.put_out(outbound())
    storage_type = store._conn.execute(
        "SELECT typeof(payload) FROM outbound WHERE mid=7"
    ).fetchone()[0]
    assert storage_type == "blob"
    store.close()

    reopened = SqliteInflightStore(path)
    got = reopened.get_out(7)
    assert got is not None
    assert got.topic == "a/b"
    assert got.payload == b"payload"
    assert got.state is OutboundQoSState.WAIT_PUBACK
    assert reopened.pop_out(7) is not None
    assert reopened.get_out(7) is None
    reopened.close()


def test_legacy_base64_text_payload_is_read_lazily(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "legacy.db")
    store.put_out(outbound())
    store._conn.execute(
        "UPDATE outbound SET payload=? WHERE mid=7",
        (base64.b64encode(b"legacy").decode("ascii"),),
    )
    store._conn.commit()

    got = store.get_out(7)
    assert got is not None
    assert got.payload == b"legacy"
    store.close()


def test_batch_commits_once(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "batch.db")
    trace: list[str] = []
    store._conn.set_trace_callback(trace.append)

    with store.batch():
        store.put_out(outbound(1))
        store.put_out(outbound(2))

    assert sum(statement == "COMMIT" for statement in trace) == 1
    assert [message.mid for message in store.out_items()] == [1, 2]
    store.close()


def test_update_out_only_touches_hot_state_columns(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "hot.db")
    store.put_out(outbound())
    changed = OutboundMessage(
        mid=7,
        topic="must-not-rewrite",
        payload=b"must-not-rewrite",
        qos=QoS.EXACTLY_ONCE,
        retain=True,
        state=OutboundQoSState.WAIT_PUBCOMP,
        dup=True,
    )
    store.update_out(changed)

    got = store.get_out(7)
    assert got is not None
    assert got.topic == "a/b"
    assert got.payload == b"payload"
    assert got.qos is QoS.AT_LEAST_ONCE
    assert got.state is OutboundQoSState.WAIT_PUBCOMP
    assert got.dup is True
    store.close()


def test_sqlite_properties_binary_and_user_property(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "props.db")
    props = Properties()
    props.set("correlation_data", b"\x00\xffbinary")
    props.set("content_type", "application/octet-stream")
    props.add_user_property("k", "v")
    props.add_user_property("k2", "v2")
    props.values["subscription_identifier"] = [11, 22]
    msg = OutboundMessage(
        mid=9,
        topic="p",
        payload=b"x",
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        state=OutboundQoSState.WAIT_PUBREC,
        properties=props,
    )
    store.put_out(msg)
    got = store.get_out(9)
    assert got is not None
    assert got.properties is not None
    assert got.properties.get("correlation_data") == b"\x00\xffbinary"
    assert got.properties.get("content_type") == "application/octet-stream"
    assert got.properties.get("user_property") == [("k", "v"), ("k2", "v2")]
    assert got.properties.get("subscription_identifier") == [11, 22]
    store.close()


def test_sqlite_inbound_manual_ack_flag(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "in.db")
    msg = InboundMessage(
        mid=3,
        topic="t",
        payload=b"x",
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        state=InboundQoSState.WAIT_PUBREL,
        delivered=True,
        user_acked=True,
    )
    store.put_in(msg)
    got = store.get_in(3)
    assert got is not None
    assert got.user_acked is True
    store.clear_in()
    assert store.get_in(3) is None
    store.close()


def test_sqlite_context_manager_and_idempotent_close(tmp_path: Path) -> None:
    with SqliteInflightStore(tmp_path / "context.db") as store:
        store.put_out(outbound())
        assert store.get_out(7) is not None
    store.close()


def test_delete_out_uses_single_delete_without_select(tmp_path: Path) -> None:
    store = SqliteInflightStore(tmp_path / "delete.db")
    store.put_out(outbound())
    trace: list[str] = []
    store._conn.set_trace_callback(trace.append)

    assert store.delete_out(7) is True
    assert store.delete_out(7) is False

    statements = [statement for statement in trace if statement.startswith(("SELECT", "DELETE"))]
    assert statements == [
        "DELETE FROM outbound WHERE mid=7",
        "DELETE FROM outbound WHERE mid=7",
    ]
    store.close()
