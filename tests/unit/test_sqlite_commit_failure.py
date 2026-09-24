"""A reported commit failure must not become durable during later cleanup."""

from contextlib import closing, nullcontext
import sqlite3

import pytest

from mqttium.enums import OutboundQoSState, QoS
from mqttium.persistence import SqliteInflightStore
from mqttium.types import OutboundMessage
from tests.support import stored_record


@pytest.mark.parametrize("batched", [False, True])
@pytest.mark.parametrize("deny_rollback", [False, True])
def test_failed_commit_never_becomes_durable_on_close(tmp_path, batched, deny_rollback):
    path = tmp_path / "commit.db"
    store = SqliteInflightStore(path)
    record = stored_record(
        OutboundMessage(
            mid=1,
            topic="t",
            payload=b"body",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBACK,
        )
    )
    store.put_out(record)

    def authorizer(action, operation, *_):
        if action == sqlite3.SQLITE_TRANSACTION and (
            operation == "COMMIT" or (deny_rollback and operation == "ROLLBACK")
        ):
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    try:
        store._conn.set_authorizer(authorizer)
        with pytest.raises(sqlite3.DatabaseError, match="not authorized") as caught:
            with store.batch() if batched else nullcontext():
                store.complete_out(1, OutboundQoSState.WAIT_PUBACK)
        assert store._batch_depth == 0
        assert not store._transaction_started
        if deny_rollback:
            assert store._closed
            assert "Rollback also failed" in caught.value.__notes__[0]
        else:
            assert not store._conn.in_transaction
            store._conn.set_authorizer(None)
            assert store.get_out(1) is not None
            # A rolled-back failure does not prevent a subsequent independent commit.
            store.put_out(
                stored_record(
                    OutboundMessage(
                        mid=2,
                        topic="next",
                        payload=b"ok",
                        qos=QoS.AT_LEAST_ONCE,
                        retain=False,
                        state=OutboundQoSState.WAIT_PUBACK,
                    )
                )
            )
        with closing(sqlite3.connect(path)) as observer:
            assert observer.execute("SELECT mid FROM outbound WHERE mid=1").fetchone() == (1,)
        store.close()
        store.close()
        with closing(sqlite3.connect(path)) as observer:
            assert observer.execute("SELECT mid FROM outbound WHERE mid=1").fetchone() == (1,)
            assert observer.execute("SELECT COUNT(*) FROM outbound").fetchone()[0] == (
                1 if deny_rollback else 2
            )
    finally:
        if not store._closed:
            store._conn.set_authorizer(None)
            store._conn.rollback()
            store.close()


def _record(mid):
    return stored_record(
        OutboundMessage(
            mid=mid,
            topic="t",
            payload=b"body",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBACK,
        )
    )


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("deny_rollback", [False, True])
def test_failed_batch_rollback_never_becomes_durable(tmp_path, nested, deny_rollback):
    path = tmp_path / "rollback.db"
    store = SqliteInflightStore(path)
    body_failure = ValueError("batch body failed")

    def authorizer(action, operation, *_):
        if deny_rollback and action == sqlite3.SQLITE_TRANSACTION and operation == "ROLLBACK":
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    store._conn.set_authorizer(authorizer)
    try:
        # Nested: the caller swallows the inner failure, so the outer batch is
        # rollback-only; otherwise the body failure crosses the batch.
        expected = RuntimeError if nested else ValueError
        with pytest.raises(expected) as caught:
            with store.batch():
                if nested:
                    with pytest.raises(ValueError):
                        with store.batch():
                            store.put_out(_record(1))
                            raise body_failure
                else:
                    store.put_out(_record(1))
                    raise body_failure
        if not nested:
            assert caught.value is body_failure
        assert store._batch_depth == 0
        assert not store._transaction_started
        if deny_rollback:
            assert store._closed
            assert "Rollback also failed" in caught.value.__notes__[0]
            with pytest.raises(sqlite3.ProgrammingError):
                store.put_out(_record(2))
        else:
            assert not store._closed
            assert not store._conn.in_transaction
            store._conn.set_authorizer(None)
            store.put_out(_record(2))
        store.close()
        store.close()
        with closing(sqlite3.connect(path)) as observer:
            mids = [row[0] for row in observer.execute("SELECT mid FROM outbound ORDER BY mid")]
        assert mids == ([] if deny_rollback else [2])
    finally:
        if not store._closed:
            store._conn.set_authorizer(None)
            store.close()


def test_successful_batches_do_not_build_the_rollback_only_refusal(tmp_path, monkeypatch):
    from mqttium.persistence import sqlite as sqlite_module

    built = []

    class CountingRuntimeError(RuntimeError):
        def __init__(self, *args):
            built.append(args)
            super().__init__(*args)

    # Module globals shadow builtins, so this counts every construction there.
    monkeypatch.setattr(sqlite_module, "RuntimeError", CountingRuntimeError, raising=False)
    store = SqliteInflightStore(tmp_path / "quiet.db")
    try:
        for mid in range(1, 101):
            with store.batch():
                if mid % 2:
                    store.put_out(_record(mid))
        assert built == []
        with pytest.raises(CountingRuntimeError, match="nested batch failure"):
            with store.batch():
                with pytest.raises(ValueError):
                    with store.batch():
                        raise ValueError("inner")
        assert len(built) == 1
    finally:
        store.close()
