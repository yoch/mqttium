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
