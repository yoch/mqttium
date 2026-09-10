from __future__ import annotations

from tests.support import stored_record


from mqttium.api import AsyncClient
from mqttium.enums import InboundQoSState, QoS
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.engine import EffectKind, EngineEffect
from mqttium.types import InboundMessage, Message


def _effect(
    *,
    qos: QoS = QoS.AT_MOST_ONCE,
    mid: int | None = None,
    requires_delivery_mark: bool = False,
) -> EngineEffect:
    return EngineEffect(
        kind=EffectKind.MESSAGE,
        data=Message(topic="hot/path", payload=b"payload", qos=qos, mid=mid),
        requires_delivery_mark=requires_delivery_mark,
    )


async def test_auto_qos1_single_effect_skips_absent_delivery_mark() -> None:
    client = AsyncClient(message_delivery="iterator")
    marked: list[int] = []
    client._engine.mark_inbound_delivered = marked.append  # type: ignore[method-assign]

    await client._apply_effect(
        _effect(qos=QoS.AT_LEAST_ONCE, mid=7),
        nowait=False,
        epoch=client._connection_epoch,
    )

    assert marked == []
    assert client._delivery.messages_queue.qsize() == 1


async def test_replayed_persisted_qos1_marks_even_when_current_mode_is_auto_ack() -> None:
    store = MemoryInflightStore()
    store.put_in(
        stored_record(
            InboundMessage(
                mid=7,
                topic="hot/path",
                payload=b"payload",
                qos=QoS.AT_LEAST_ONCE,
                retain=False,
                state=InboundQoSState.WAIT_PUBACK,
                delivered=False,
            )
        )
    )
    client = AsyncClient(
        message_delivery="iterator",
        manual_ack=False,
        store=store,
    )

    client._engine.inbound.replay_session()
    effects = client._engine.take_effects()
    message_effect = next(effect for effect in effects if effect.kind is EffectKind.MESSAGE)
    assert message_effect.requires_delivery_mark is True

    await client._apply_effect(
        message_effect,
        nowait=False,
        epoch=client._connection_epoch,
    )

    record = store.get_in(7)
    assert record is not None
    assert record.delivered is True
