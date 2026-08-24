"""Runtime soak harness: ownership oracles, reduction, and a CI-sized campaign."""

from __future__ import annotations

from collections.abc import Sequence

import pytest

from mqttium.enums import MQTTProtocolVersion

from benchmarks.runtime_soak_lib.ownership import OwnershipSnapshot, connected_idle_violations
from benchmarks.runtime_soak_lib.profiles import PROFILES
from benchmarks.runtime_soak_lib.runner import SoakFailure, SoakSession, run_soak
from benchmarks.runtime_soak_lib.schedule import Op, OpKind, reduce_schedule, schedule_for_seed


def test_schedule_is_seed_reproducible() -> None:
    first = schedule_for_seed(7, operations=40, protocol=MQTTProtocolVersion.MQTTv5)
    second = schedule_for_seed(7, operations=40, protocol=MQTTProtocolVersion.MQTTv5)
    other = schedule_for_seed(8, operations=40, protocol=MQTTProtocolVersion.MQTTv5)
    assert first == second
    assert first != other
    assert first[0].kind is OpKind.CONNECT
    assert any(op.kind is OpKind.QUIESCE for op in first)


def test_reduce_schedule_finds_shortest_failing_prefix() -> None:
    ops = [
        Op(OpKind.CONNECT),
        Op(OpKind.SUBSCRIBE),
        Op(OpKind.PUBLISH, qos=1),
        Op(OpKind.DROP_NETWORK),
        Op(OpKind.QUIESCE),
    ]

    def replay(candidate: Sequence[Op]) -> bool:
        return OpKind.DROP_NETWORK not in {op.kind for op in candidate}

    reduced = reduce_schedule(ops, replay)
    assert [op.kind for op in reduced] == [OpKind.DROP_NETWORK]


def test_connected_idle_oracle_ignores_expected_connection_tasks() -> None:
    snapshot = OwnershipSnapshot(
        connection_state="CONNECTED",
        connection_epoch=3,
        reconnect_attempt=1,
        outbound_pending_messages=0,
        outbound_pending_bytes=0,
        outbound_queued_messages=0,
        outbound_flow_inflight=0,
        packet_ids_in_use=0,
        inbound_inflight=0,
        inbound_pending_bytes=0,
        inbound_replay_pending=False,
        effects_pending=0,
        effects_waiters=0,
        writer_queued_messages=0,
        writer_queued_bytes=0,
        writer_waiters=0,
        writer_resident_messages=0,
        decoder_buffered_bytes=0,
        delivery_iterator_queued=0,
        delivery_callback_queued=0,
        delivery_pending_bytes=0,
        delivery_waiters=0,
        receipts_publish=0,
        receipts_publish_batches=0,
        receipts_subscribe=0,
        receipts_unsubscribe=0,
        receipts_publish_waiters=0,
        store_out_rows=0,
        store_in_rows=0,
        named_mqttium_tasks=("mqttium-reader", "mqttium-writer"),
        task_reader=True,
        task_writer=True,
        task_keepalive=False,
        task_reconnect=False,
        task_effect_flush=False,
        task_callback_worker=True,
        internal_publish_receipts=0,
        internal_batch_receipts=0,
        internal_subscribe_futs=0,
        internal_unsubscribe_futs=0,
        internal_publish_waiters=0,
    )
    assert connected_idle_violations(snapshot) == []
    leaking = OwnershipSnapshot(**{**snapshot.as_dict(), "receipts_publish": 2})
    assert "receipts_publish=2" in connected_idle_violations(leaking)


@pytest.mark.timeout(25)
async def test_ci_runtime_soak_fake_broker() -> None:
    profile = PROFILES["ci"]
    try:
        report = await run_soak(
            profile,
            seed=1,
            protocol=MQTTProtocolVersion.MQTTv311,
            operations=24,
        )
    except SoakFailure as exc:
        pytest.fail(f"ci soak failed: {exc}; history={exc.history}")
    assert report.ok
    assert report.checkpoints >= 1
    assert report.ownership["outbound_pending_messages"] == 0
    assert report.ownership["receipts_publish"] == 0
    assert report.ownership["writer_resident_messages"] == 0


@pytest.mark.timeout(25)
async def test_drop_network_reconnect_then_quiesce() -> None:
    profile = PROFILES["ci"]
    try:
        report = await run_soak(
            profile,
            seed=1,
            protocol=MQTTProtocolVersion.MQTTv5,
            operations=8,
        )
    except SoakFailure as exc:
        pytest.fail(f"mqtt5 soak failed: {exc}; history={exc.history}; reduced={exc.reduced}")
    assert report.ok
    assert report.checkpoints >= 1


@pytest.mark.timeout(25)
async def test_explicit_drop_schedule_reaches_idle() -> None:
    ops = [
        Op(OpKind.CONNECT),
        Op(OpKind.SUBSCRIBE),
        Op(OpKind.PUBLISH, qos=1),
        Op(OpKind.DRAIN),
        Op(OpKind.DROP_NETWORK, session_present=True),
        Op(OpKind.QUIESCE),
        Op(OpKind.GRACEFUL_SHUTDOWN),
        Op(OpKind.QUIESCE),
    ]

    async def _go() -> None:
        session = SoakSession(
            seed=1,
            protocol=MQTTProtocolVersion.MQTTv311,
            timeout=5.0,
            durable=True,
            sqlite=False,
            backend="fake",
            host="127.0.0.1",
            port=1883,
        )
        await session.setup()
        try:
            await session.run_ops(ops)
            assert session._checkpoints >= 2
        finally:
            await session.close()

    try:
        await _go()
    except SoakFailure as exc:
        pytest.fail(f"drop schedule failed: {exc}; history={exc.history}")


@pytest.mark.timeout(25)
async def test_reduced_session_present_drop_reaches_idle() -> None:
    """Broker must finish inbound QoS 2 after a session-present reconnect."""
    ops = [
        Op(OpKind.CONNECT),
        Op(OpKind.SUBSCRIBE),
        Op(OpKind.PUBLISH, qos=2),
        Op(OpKind.DROP_NETWORK, session_present=True),
        Op(OpKind.QUIESCE),
        Op(OpKind.GRACEFUL_SHUTDOWN),
        Op(OpKind.QUIESCE),
    ]
    session = SoakSession(
        seed=7,
        protocol=MQTTProtocolVersion.MQTTv311,
        timeout=5.0,
        durable=True,
        sqlite=False,
        backend="fake",
        host="127.0.0.1",
        port=1883,
    )
    await session.setup()
    try:
        await session.run_ops(ops)
        assert session._checkpoints >= 2
    except SoakFailure as exc:
        pytest.fail(f"session-present drop failed: {exc}; history={exc.history}")
    finally:
        await session.close()
