"""Logical ownership snapshots for quiescence checks.

RSS / allocator samples are diagnostic only. The oracles that fail a soak are
the client-owned queues, receipts, packet identifiers, persistence rows, and
named asyncio tasks.
"""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, fields
from typing import Any

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState

_MQTTIUM_TASK_PREFIX = "mqttium-"


@dataclass(frozen=True, slots=True)
class OwnershipSnapshot:
    """Point-in-time logical ownership of one ``AsyncClient``."""

    connection_state: str
    connection_epoch: int
    reconnect_attempt: int
    outbound_pending_messages: int
    outbound_pending_bytes: int
    outbound_queued_messages: int
    outbound_flow_inflight: int
    packet_ids_in_use: int
    inbound_inflight: int
    inbound_pending_bytes: int
    inbound_replay_pending: bool
    effects_pending: int
    effects_waiters: int
    writer_queued_messages: int
    writer_queued_bytes: int
    writer_waiters: int
    writer_resident_messages: int
    decoder_buffered_bytes: int
    delivery_iterator_queued: int
    delivery_callback_queued: int
    delivery_pending_bytes: int
    delivery_waiters: int
    receipts_publish: int
    receipts_publish_batches: int
    receipts_subscribe: int
    receipts_unsubscribe: int
    receipts_publish_waiters: int
    store_out_rows: int
    store_in_rows: int
    named_mqttium_tasks: tuple[str, ...]
    task_reader: bool
    task_writer: bool
    task_keepalive: bool
    task_reconnect: bool
    task_effect_flush: bool
    task_callback_worker: bool
    internal_publish_receipts: int
    internal_batch_receipts: int
    internal_subscribe_futs: int
    internal_unsubscribe_futs: int
    internal_publish_waiters: int

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


_CONNECTED_IDLE_ZERO = (
    "outbound_pending_messages",
    "outbound_pending_bytes",
    "outbound_queued_messages",
    "outbound_flow_inflight",
    "packet_ids_in_use",
    "inbound_inflight",
    "inbound_pending_bytes",
    "effects_pending",
    "effects_waiters",
    "writer_queued_messages",
    "writer_queued_bytes",
    "writer_waiters",
    "writer_resident_messages",
    "decoder_buffered_bytes",
    "delivery_iterator_queued",
    "delivery_callback_queued",
    "delivery_pending_bytes",
    "delivery_waiters",
    "receipts_publish",
    "receipts_publish_batches",
    "receipts_subscribe",
    "receipts_unsubscribe",
    "receipts_publish_waiters",
    "store_out_rows",
    "store_in_rows",
    "internal_publish_receipts",
    "internal_batch_receipts",
    "internal_subscribe_futs",
    "internal_unsubscribe_futs",
    "internal_publish_waiters",
)

_DISCONNECTED_IDLE_ZERO = _CONNECTED_IDLE_ZERO


def take_ownership(client: AsyncClient) -> OwnershipSnapshot:
    """Read public stats plus the private owners they summarise."""
    stats = client.stats()
    store = client._engine.store
    pump = client._write_pump
    receipts = client._receipts
    batches = client._batch_receipts
    named = tuple(
        sorted(
            task.get_name()
            for task in asyncio.all_tasks()
            if task.get_name().startswith(_MQTTIUM_TASK_PREFIX) and not task.done()
        )
    )
    return OwnershipSnapshot(
        connection_state=stats.state.name,
        connection_epoch=stats.connection_epoch,
        reconnect_attempt=stats.reconnect_attempt,
        outbound_pending_messages=stats.outbound.pending_messages,
        outbound_pending_bytes=stats.outbound.pending_bytes,
        outbound_queued_messages=stats.outbound.queued_messages,
        outbound_flow_inflight=stats.outbound.flow_inflight,
        packet_ids_in_use=stats.outbound.packet_ids_in_use,
        inbound_inflight=stats.inbound.inflight,
        inbound_pending_bytes=stats.inbound.pending_bytes,
        inbound_replay_pending=stats.inbound.replay_pending,
        effects_pending=stats.effects.pending,
        effects_waiters=stats.effects.waiters,
        writer_queued_messages=stats.writer.queued_messages,
        writer_queued_bytes=stats.writer.queued_bytes,
        writer_waiters=stats.writer.waiters,
        writer_resident_messages=pump.resident_messages,
        decoder_buffered_bytes=stats.decoder.buffered_bytes,
        delivery_iterator_queued=stats.delivery.iterator_queued,
        delivery_callback_queued=stats.delivery.callback_queued,
        delivery_pending_bytes=stats.delivery.pending_bytes,
        delivery_waiters=stats.delivery.waiters,
        receipts_publish=stats.receipts.publish,
        receipts_publish_batches=stats.receipts.publish_batches,
        receipts_subscribe=stats.receipts.subscribe,
        receipts_unsubscribe=stats.receipts.unsubscribe,
        receipts_publish_waiters=stats.receipts.publish_waiters,
        store_out_rows=sum(1 for _ in store.out_items()),
        store_in_rows=sum(1 for _ in store.in_items()),
        named_mqttium_tasks=named,
        task_reader=stats.tasks.reader,
        task_writer=stats.tasks.writer,
        task_keepalive=stats.tasks.keepalive,
        task_reconnect=stats.tasks.reconnect,
        task_effect_flush=stats.tasks.effect_flush,
        task_callback_worker=stats.tasks.callback_worker,
        internal_publish_receipts=_receipt_count(receipts),
        internal_batch_receipts=_receipt_count(batches),
        internal_subscribe_futs=len(client._sub_futs),
        internal_unsubscribe_futs=len(client._unsub_futs),
        internal_publish_waiters=client._publish_waiters,
    )


def _receipt_count(mapping: dict[int, object]) -> int:
    total = 0
    for value in mapping.values():
        if isinstance(value, list) or (
            hasattr(value, "__len__") and not isinstance(value, (str, bytes))
        ):
            try:
                total += len(value)  # type: ignore[arg-type]
            except TypeError:
                total += 1
        else:
            total += 1
    return total


def connected_idle_violations(snapshot: OwnershipSnapshot) -> list[str]:
    """Fields that must be zero/false while connected after a full drain."""
    violations = [
        f"{name}={getattr(snapshot, name)}"
        for name in _CONNECTED_IDLE_ZERO
        if getattr(snapshot, name)
    ]
    if snapshot.inbound_replay_pending:
        violations.append("inbound_replay_pending=True")
    if snapshot.task_reconnect:
        violations.append("task_reconnect=True")
    if snapshot.task_effect_flush:
        violations.append("task_effect_flush=True")
    if snapshot.connection_state != ConnectionState.CONNECTED.name:
        violations.append(f"connection_state={snapshot.connection_state}")
    return violations


def disconnected_idle_violations(snapshot: OwnershipSnapshot) -> list[str]:
    """Fields that must be quiet after graceful or forced shutdown."""
    violations = [
        f"{name}={getattr(snapshot, name)}"
        for name in _DISCONNECTED_IDLE_ZERO
        if getattr(snapshot, name)
    ]
    if snapshot.inbound_replay_pending:
        violations.append("inbound_replay_pending=True")
    for name in (
        "task_reader",
        "task_writer",
        "task_keepalive",
        "task_reconnect",
        "task_effect_flush",
        "task_callback_worker",
    ):
        if getattr(snapshot, name):
            violations.append(f"{name}=True")
    if snapshot.named_mqttium_tasks:
        violations.append(f"named_mqttium_tasks={list(snapshot.named_mqttium_tasks)}")
    if snapshot.connection_state not in {
        ConnectionState.DISCONNECTED.name,
        ConnectionState.NEW.name,
    }:
        violations.append(f"connection_state={snapshot.connection_state}")
    return violations


def drift_from(baseline: OwnershipSnapshot, current: OwnershipSnapshot) -> list[str]:
    """Compare two connected-idle snapshots, ignoring monotonic counters."""
    ignored = {
        "connection_epoch",
        "reconnect_attempt",
        "named_mqttium_tasks",
        "task_reader",
        "task_writer",
        "task_keepalive",
        "task_callback_worker",
    }
    drift = []
    for item in fields(OwnershipSnapshot):
        if item.name in ignored:
            continue
        before = getattr(baseline, item.name)
        after = getattr(current, item.name)
        if before != after:
            drift.append(f"{item.name}: {before!r} -> {after!r}")
    return drift
