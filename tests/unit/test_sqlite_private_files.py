"""New durable state is private without changing process-wide creation policy."""

import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import stat
import subprocess
import sys

import pytest

from mqttium.enums import OutboundQoSState, QoS
from mqttium.persistence import SqliteInflightStore
from mqttium.types import OutboundMessage
from tests.support import stored_record


def _record(mid):
    return stored_record(
        OutboundMessage(
            mid=mid,
            topic="private/topic",
            payload=b"private payload",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            state=OutboundQoSState.WAIT_PUBACK,
        )
    )


@pytest.mark.skipif(os.name != "posix", reason="POSIX permission-bit contract")
def test_new_database_sidecars_and_every_new_parent_are_private(tmp_path):
    # Change umask only in a child process: other clients/threads must not see
    # a temporary process-wide umask while a store is being constructed.
    script = """
import json
import os
from pathlib import Path
import stat
import sys
from mqttium.persistence import SqliteInflightStore
os.umask(0o022)
path = Path(sys.argv[1]) / "new" / "nested" / "state.db"
store = SqliteInflightStore(path)
try:
    paths = [path.parent.parent, path.parent, path,
             Path(str(path) + "-wal"), Path(str(path) + "-shm")]
    assert all(item.exists() for item in paths)
    print(json.dumps([stat.S_IMODE(item.stat().st_mode) for item in paths]))
    assert os.umask(0o022) == 0o022
finally:
    store.close()
"""
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[2] / "src")
    completed = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path)],
        env=environment,
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert json.loads(completed.stdout) == [0o700, 0o700, 0o600, 0o600, 0o600]


@pytest.mark.skipif(os.name != "posix", reason="POSIX permission-bit contract")
def test_existing_permissive_store_and_parent_permissions_are_preserved(tmp_path):
    parent = tmp_path / "deployment-owned"
    parent.mkdir(mode=0o755)
    parent.chmod(0o755)
    path = parent / "state.db"
    store = SqliteInflightStore(path)
    store.put_out(_record(1))
    store.close()
    path.chmod(0o644)
    reopened = SqliteInflightStore(path)
    try:
        assert stat.S_IMODE(path.stat().st_mode) == 0o644
        assert stat.S_IMODE(parent.stat().st_mode) == 0o755
        assert reopened.get_out(1).payload == b"private payload"
    finally:
        reopened.close()


@pytest.mark.skipif(os.name != "posix", reason="POSIX symbolic-link contract")
def test_dangling_database_symlink_is_refused_without_creating_its_target(tmp_path):
    target = tmp_path / "target.db"
    link = tmp_path / "link.db"
    link.symlink_to(target)
    with pytest.raises(FileNotFoundError):
        SqliteInflightStore(link)
    assert link.is_symlink()
    for created in (target, Path(f"{target}-wal"), Path(f"{target}-shm")):
        assert not created.exists()


@pytest.mark.skipif(os.name != "posix", reason="POSIX symbolic-link contract")
def test_existing_database_symlink_keeps_its_target_permissions(tmp_path):
    target = tmp_path / "target.db"
    store = SqliteInflightStore(target)
    store.put_out(_record(1))
    store.close()
    target.chmod(0o640)
    link = tmp_path / "link.db"
    link.symlink_to(target)
    linked = SqliteInflightStore(link)
    try:
        assert linked.get_out(1).payload == b"private payload"
        linked.put_out(_record(2))
    finally:
        linked.close()
    assert stat.S_IMODE(target.stat().st_mode) == 0o640
    reopened = SqliteInflightStore(target)
    try:
        assert reopened.get_out(2).payload == b"private payload"
    finally:
        reopened.close()


def test_protected_store_reopens_and_concurrent_connections_keep_all_commits(tmp_path):
    path = tmp_path / "private" / "state.db"
    initial = SqliteInflightStore(path)
    initial.put_out(_record(1))
    initial.close()
    stores = [SqliteInflightStore(path), SqliteInflightStore(path)]

    def write_range(index):
        for mid in range(2 + index * 20, 22 + index * 20):
            stores[index].put_out(_record(mid))

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            list(executor.map(write_range, (0, 1)))
        assert all(stores[0].get_out(mid) is not None for mid in range(1, 42))
    finally:
        for store in stores:
            store.close()
    reopened = SqliteInflightStore(path)
    try:
        assert all(reopened.get_out(mid) is not None for mid in range(1, 42))
        if os.name == "posix":
            assert stat.S_IMODE(path.stat().st_mode) == 0o600
    finally:
        reopened.close()


def test_memory_store_does_not_create_a_literal_memory_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    store = SqliteInflightStore(":memory:")
    try:
        store.put_out(_record(1))
        assert store.get_out(1).payload == b"private payload"
        assert not (tmp_path / ":memory:").exists()
    finally:
        store.close()
