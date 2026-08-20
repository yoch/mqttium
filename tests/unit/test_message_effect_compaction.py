from __future__ import annotations

from collections import deque

import pytest

from mqttium.api._effects import EffectPump, _MessageEffectBatch
from mqttium.protocol.effects import EffectKind, EngineEffect


class _Engine:
    def __init__(self, effects: list[EngineEffect]) -> None:
        self.effects = effects

    def take_effects(self) -> list[EngineEffect]:
        effects = self.effects
        self.effects = []
        return effects


class _Owner:
    def __init__(self, effects: list[EngineEffect]) -> None:
        self._connection_epoch = 1
        self._engine = _Engine(effects)
        self.batch_limit: int | None = None
        self.batch_seen: list[object] = []
        self.slow_seen: list[object] = []

    def _apply_effect_inline(self, _effect: EngineEffect, _epoch: int) -> bool:
        return False

    def _apply_message_effect_batch_inline(self, effects: deque[EngineEffect], _epoch: int) -> int:
        consumed = 0
        for effect in effects:
            if self.batch_limit is not None and consumed >= self.batch_limit:
                break
            self.batch_seen.append(effect.data)
            consumed += 1
        return consumed

    def _apply_decoded_message_effect_batch_inline(
        self, _effects: deque[EngineEffect], _epoch: int
    ) -> int:
        return 0

    async def _apply_effect(
        self,
        effect: EngineEffect,
        *,
        nowait: bool,
        epoch: int | None = None,
    ) -> None:
        assert not nowait
        assert epoch == self._connection_epoch
        assert effect.kind is EffectKind.MESSAGE
        self.slow_seen.append(effect.data)

    def _settle_terminal_effect(self, _effect: EngineEffect) -> None:
        raise AssertionError("no terminal publish effect expected")


def _message(value: object, *, marked: bool = False) -> EngineEffect:
    return EngineEffect(
        EffectKind.MESSAGE,
        value,
        requires_delivery_mark=marked,
    )


def test_collect_compacts_safe_messages_but_counts_logical_effects() -> None:
    owner = _Owner([_message(f"m{i}") for i in range(5)])
    pump = EffectPump(owner)

    pump.collect_from_engine()

    assert len(pump.pending) == 1
    effect = pump.pending[0]
    assert effect.kind is EffectKind.MESSAGE_BATCH
    assert isinstance(effect.data, _MessageEffectBatch)
    assert effect.data.messages == ["m0", "m1", "m2", "m3", "m4"]
    stats = pump.stats()
    assert stats.enqueued == 5
    assert stats.applied == 0
    assert stats.pending == 5
    assert stats.pending_high_water == 5
    assert stats.multi_effect_batches == 1


def test_compaction_never_crosses_send_marked_or_decoded_boundaries() -> None:
    owner = _Owner(
        [
            _message("before-send"),
            EngineEffect(EffectKind.SEND, b"wire"),
            _message("after-send"),
            _message("marked", marked=True),
            _message("decoded-neighbor"),
            EngineEffect(EffectKind.DECODED_MESSAGE, "decoded", requires_delivery_mark=False),
            _message("tail-0"),
            _message("tail-1"),
        ]
    )
    pump = EffectPump(owner)

    pump.collect_from_engine()

    assert [effect.kind for effect in pump.pending] == [
        EffectKind.SEND,
        EffectKind.MESSAGE,
        EffectKind.MESSAGE,
        EffectKind.MESSAGE,
        EffectKind.MESSAGE,
        EffectKind.DECODED_MESSAGE,
        EffectKind.MESSAGE_BATCH,
    ]
    assert pump.pending[1].data == "before-send"
    assert pump.pending[2].data == "after-send"
    assert pump.pending[3].data == "marked"
    assert pump.pending[4].data == "decoded-neighbor"
    batch = pump.pending[-1].data
    assert isinstance(batch, _MessageEffectBatch)
    assert batch.messages == ["tail-0", "tail-1"]
    assert pump.stats().pending == 8
    assert pump.reordered_batches == 1


def test_compact_batch_can_be_consumed_partially_without_splitting() -> None:
    owner = _Owner([_message(f"m{i}") for i in range(5)])
    pump = EffectPump(owner)
    pump.collect_from_engine()
    owner.batch_limit = 3

    assert pump._consume_message_batch(owner._connection_epoch)

    assert len(pump.pending) == 1
    batch = pump.pending[0].data
    assert isinstance(batch, _MessageEffectBatch)
    assert batch.offset == 3
    assert owner.batch_seen == ["m0", "m1", "m2"]
    assert pump.applied == 3
    assert pump.stats().pending == 2

    owner.batch_limit = None
    assert pump._consume_message_batch(owner._connection_epoch)
    assert owner.batch_seen == ["m0", "m1", "m2", "m3", "m4"]
    assert not pump.pending
    assert pump.applied == pump.enqueued == 5


async def test_scheduled_slow_path_expands_batch_one_logical_message_at_a_time() -> None:
    owner = _Owner([_message("a"), _message("b"), _message("c")])
    pump = EffectPump(owner)
    pump.collect_from_engine()
    owner.batch_limit = 0

    await pump._run_scheduled()

    assert owner.slow_seen == ["a", "b", "c"]
    assert not pump.pending
    assert pump.applied == pump.enqueued == 3
    assert pump.stats().pending == 0


def test_discard_counts_unconsumed_batch_messages_logically() -> None:
    owner = _Owner([_message(f"m{i}") for i in range(5)])
    pump = EffectPump(owner)
    pump.collect_from_engine()
    owner.batch_limit = 2
    assert pump._consume_message_batch(owner._connection_epoch)
    assert pump.applied == 2

    pump.discard_connection_effects()

    assert not pump.pending
    assert pump.applied == pump.enqueued == 5
    assert pump.stats().pending == 0


@pytest.mark.parametrize("count", [2, 3, 17, 256])
def test_virtual_batch_view_preserves_order_without_materializing_effects(count: int) -> None:
    owner = _Owner([_message(i) for i in range(count)])
    pump = EffectPump(owner)
    pump.collect_from_engine()

    assert pump._consume_message_batch(owner._connection_epoch)

    assert owner.batch_seen == list(range(count))
    assert pump.inline_effects == count
    assert pump.applied == count
