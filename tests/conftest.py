"""Repository-wide deterministic test controls."""

from __future__ import annotations

import os
import random
import threading
from collections.abc import Iterator
from pathlib import Path

import pytest

from mqttium.persistence.sqlite import SqliteInflightStore


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Shuffle resilience scenarios when ``MQTTIUM_TEST_ORDER_SEED`` is set.

    Threaded compatibility tests retain their natural collection order because
    pytest fixture scheduling can otherwise perturb their network-loop timing.
    Scheduled race campaigns target the resilience suite explicitly.
    """
    value = os.environ.get("MQTTIUM_TEST_ORDER_SEED")
    if value is None:
        return
    try:
        seed = int(value)
    except ValueError as exc:
        raise pytest.UsageError("MQTTIUM_TEST_ORDER_SEED must be an integer") from exc
    positions = [index for index, item in enumerate(items) if "tests/resilience/" in item.nodeid]
    if not positions:
        return
    selected = [items[index] for index in positions]
    random.Random(seed).shuffle(selected)
    for index, item in zip(positions, selected, strict=True):
        items[index] = item


@pytest.fixture(scope="session", autouse=True)
def no_mqttium_thread_leaks() -> Iterator[None]:
    """Fail when a compatibility network thread survives the test session."""
    baseline = {thread.ident for thread in threading.enumerate()}
    yield
    leaked = [
        thread
        for thread in threading.enumerate()
        if thread.ident not in baseline and thread.is_alive() and thread.name.startswith("mqttium-")
    ]
    assert not leaked, f"MQTTium threads survived the test session: {[t.name for t in leaked]}"


@pytest.fixture(autouse=True)
def close_test_sqlite_stores() -> Iterator[None]:
    """Give every test-created SQLite store deterministic ownership.

    Python 3.13+ warns when a ``sqlite3.Connection`` reaches its finalizer
    without being closed.  Tests that pass a store into ``ProtocolEngine`` do
    not otherwise retain a convenient owner, so garbage-collection timing can
    report that leak against an unrelated later test.  Keep each store alive
    for the current test and close it during fixture teardown instead.
    """
    stores: list[SqliteInflightStore] = []
    original_init = SqliteInflightStore.__init__

    def tracked_init(self: SqliteInflightStore, path: str | Path) -> None:
        original_init(self, path)
        stores.append(self)

    SqliteInflightStore.__init__ = tracked_init
    try:
        yield
    finally:
        SqliteInflightStore.__init__ = original_init
        for store in reversed(stores):
            store.close()
