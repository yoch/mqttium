from __future__ import annotations

from mqttium.api import AsyncClient
from mqttium.enums import ConnectionState
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message


def _message_effect(value: bytes) -> EngineEffect:
    return EngineEffect(
        EffectKind.MESSAGE,
        Message(topic="request", payload=value),
        requires_delivery_mark=False,
    )


async def test_sync_pair_publish_nowait_appends_sends_behind_message_prefix() -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=2)
    client._engine.state = ConnectionState.CONNECTED

    pump = client._effect_pump
    first = _message_effect(b"first")
    second = _message_effect(b"second")
    pump.pending.extend((first, second))
    pump.pending_epoch = client._connection_epoch
    pump.enqueued = 2
    pump.pending_high_water = 2

    writes: list[object] = []
    snapshots: list[tuple[EffectKind, ...]] = []
    queued_bounds: list[int] = []

    def capture_write(item: object, *, epoch: int | None = None) -> bool:
        assert epoch == client._connection_epoch
        writes.append(item)
        return True

    client._try_enqueue_outbound = capture_write  # type: ignore[assignment]

    def on_message(message: Message) -> None:
        queued_bounds.append(client.stats().delivery.callback_queued)
        client.publish_nowait("response", message.payload, qos=1)
        kinds = tuple(effect.kind for effect in pump.pending)
        snapshots.append(kinds)
        # The owning drain has not consumed either input message yet. Reentrant
        # SEND work must append behind that fixed prefix, never overtake it.
        assert kinds[:2] == (EffectKind.MESSAGE, EffectKind.MESSAGE)
        assert EffectKind.SEND in kinds[2:]

    client.on_message = on_message
    pump.drain_inline()

    assert queued_bounds == [1, 1]
    assert len(writes) == 2
    assert len(snapshots) == 2
    assert snapshots[0][:2] == (EffectKind.MESSAGE, EffectKind.MESSAGE)
    assert snapshots[1][:2] == (EffectKind.MESSAGE, EffectKind.MESSAGE)
    assert snapshots[1].count(EffectKind.SEND) >= snapshots[0].count(EffectKind.SEND)

    mids = list(client._receipts)
    assert len(mids) == 2
    assert [client._engine.store.get_out(mid).payload for mid in mids] == [b"first", b"second"]

    stats = client.stats()
    assert stats.delivery.callback_queued == 0
    assert not stats.tasks.callback_worker
    assert not stats.tasks.effect_flush
    assert stats.effects.pending == 0
    assert stats.effects.enqueued == stats.effects.applied
    assert not pump.pending


async def _probe_callback_to_send_order(count: int) -> list[str]:
    """Return logical callback/SEND handoff order for one message prefix."""
    client = AsyncClient(
        message_delivery="callback",
        max_pending_callbacks=max(4, count),
    )
    client._engine.state = ConnectionState.CONNECTED
    pump = client._effect_pump
    pump.pending.extend(_message_effect(str(index).encode()) for index in range(count))
    pump.pending_epoch = client._connection_epoch
    pump.enqueued = count
    pump.pending_high_water = count

    events: list[str] = []

    def capture_write(_item: object, *, epoch: int | None = None) -> bool:
        assert epoch == client._connection_epoch
        events.append("send")
        return True

    client._try_enqueue_outbound = capture_write  # type: ignore[assignment]

    def on_message(message: Message) -> None:
        events.append(f"callback:{message.payload.decode()}")
        client.publish_nowait("response", message.payload, qos=1)

    client.on_message = on_message
    pump.drain_inline()

    if client.stats().tasks.callback_worker:
        await client._callback_queue.join()
        await client._shutdown_callback_worker(drain=False)

    assert not pump.pending
    assert len(client._receipts) == count
    return events


async def test_publish_nowait_send_handoff_has_exact_pair_discontinuity() -> None:
    """Characterize the scheduling boundary relevant to responder RTT.

    A singleton has no following callback to block its reentrant SEND. Exactly
    two eligible messages run in one EffectPump drain, so both callbacks finish
    before either SEND can be applied. Three messages retain worker batching;
    once the worker owns them, each callback's publish_nowait can drain its SEND
    before the next callback starts.
    """
    assert await _probe_callback_to_send_order(1) == ["callback:0", "send"]
    assert await _probe_callback_to_send_order(2) == [
        "callback:0",
        "callback:1",
        "send",
        "send",
    ]
    assert await _probe_callback_to_send_order(3) == [
        "callback:0",
        "send",
        "callback:1",
        "send",
        "callback:2",
        "send",
    ]
