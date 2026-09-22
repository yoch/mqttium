from __future__ import annotations

from mqttium.api import AsyncClient
from mqttium.enums import InboundQoSState, QoS
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.protocol.engine import EffectKind, EngineEffect
from mqttium.types import InboundMessage, Message
from tests.support import apply_delivery_effect, stored_record


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


async def test_auto_qos1_single_effect_skips_absent_delivery_mark(monkeypatch) -> None:
    client = AsyncClient(message_delivery="iterator")
    marked: list[int] = []
    monkeypatch.setattr(
        type(client._engine.inbound), "mark_delivered", lambda _self, mid: marked.append(mid)
    )

    pending = client._apply_delivery_effect(
        _effect(qos=QoS.AT_LEAST_ONCE, mid=7),
        epoch=client._connection_epoch,
    )

    assert pending is None
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

    await apply_delivery_effect(client, message_effect)

    record = store.get_in(7)
    assert record is not None
    assert record.delivered is True
