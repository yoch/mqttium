from __future__ import annotations

import argparse
from pathlib import Path


def replace_once(text: str, old: str, new: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"expected one occurrence, found {count}: {old!r}")
    return text.replace(old, new, 1)


def patch_effects(root: Path) -> None:
    path = root / "src/mqttium/api/_effects.py"
    text = path.read_text()
    text = replace_once(
        text,
        "    def _apply_effect_inline(self, effect: EngineEffect, epoch: int) -> bool: ...\n\n",
        "    def _apply_effect_inline(self, effect: EngineEffect, epoch: int) -> bool: ...\n\n"
        "    def _apply_message_effect_batch_inline(\n"
        "        self, effects: deque[EngineEffect], epoch: int\n"
        "    ) -> int: ...\n\n",
    )
    old = '''                while self.pending:
                    effect = self.pending[0]
                    epoch = self.pending_epoch
                    if epoch is None:
                        self.pending.clear()
                        break
                    if epoch != self.owner._connection_epoch:
                        self.discard_connection_effects()
                        continue
                    try:
                        await self.owner._apply_effect(effect, nowait=False, epoch=epoch)
'''
    new = '''                while self.pending:
                    effect = self.pending[0]
                    epoch = self.pending_epoch
                    if epoch is None:
                        self.pending.clear()
                        break
                    if epoch != self.owner._connection_epoch:
                        self.discard_connection_effects()
                        continue
                    if effect.kind is EffectKind.MESSAGE:
                        applied_inline = self.owner._apply_message_effect_batch_inline(
                            self.pending, epoch
                        )
                        if applied_inline:
                            for _ in range(applied_inline):
                                self.pending.popleft()
                                self.inline_effects += 1
                                self._complete()
                            if not self.pending:
                                self.pending_epoch = None
                            continue
                    try:
                        await self.owner._apply_effect(effect, nowait=False, epoch=epoch)
'''
    text = replace_once(text, old, new)
    path.write_text(text)


def patch_client(root: Path) -> None:
    path = root / "src/mqttium/api/async_client.py"
    text = path.read_text()
    marker = "    async def _flush_effects(self, *, nowait: bool = False) -> None:\n"
    method = '''    def _apply_message_effect_batch_inline(
        self,
        effects: deque[EngineEffect],
        epoch: int,
    ) -> int:
        if epoch != self._connection_epoch or not effects:
            return 0
        first: Message = effects[0].data
        if first.qos != QoS.AT_MOST_ONCE:
            return 0
        callback = self.on_message
        callback_delivery = callback is not None and self._message_delivery in (
            "auto",
            "callback",
            "both",
        )
        iterator_delivery = self._message_delivery in ("iterator", "both") or (
            self._message_delivery == "auto" and callback is None
        )
        if not callback_delivery and not iterator_delivery:
            return 0
        small_limit = self._delivery_small_message_limit
        callback_worker_ready = False
        applied = 0
        for effect in effects:
            if effect.kind is not EffectKind.MESSAGE:
                break
            msg: Message = effect.data
            if msg.qos != QoS.AT_MOST_ONCE:
                break
            if small_limit is not None:
                if (
                    small_limit <= 0
                    or msg.properties
                    or len(msg.payload) + 4 * len(msg.topic) > small_limit
                ):
                    break
            # Check every destination before mutating either queue. If one is
            # full, the existing async path preserves backpressure and atomic
            # `both` delivery for this message and the remainder of the batch.
            if iterator_delivery and self._messages.full():
                break
            if callback_delivery and self._callback_queue.full():
                break
            if iterator_delivery:
                self._messages.put_nowait(msg)
            if callback_delivery:
                assert callback is not None
                if not callback_worker_ready:
                    self._ensure_callback_worker()
                    callback_worker_ready = True
                self._callback_queue.put_nowait((callback, (msg,), None))
            applied += 1
        if applied and iterator_delivery:
            self._message_ready.set()
        return applied

'''
    text = replace_once(text, marker, method + marker)
    path.write_text(text)


def write_tests(root: Path) -> None:
    path = root / "tests/unit/test_batched_message_effects.py"
    path.write_text(
        '''from __future__ import annotations

from collections import deque

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import QoS
from mqttium.protocol.engine import EffectKind, EngineEffect
from mqttium.types import Message


def _effect(*, qos: QoS = QoS.AT_MOST_ONCE, mid: int | None = None) -> EngineEffect:
    return EngineEffect(
        kind=EffectKind.MESSAGE,
        data=Message(topic="hot/path", payload=b"payload", qos=qos, mid=mid),
    )


@pytest.mark.asyncio
async def test_small_qos0_callback_messages_apply_as_one_synchronous_batch() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[Message] = []
    client.on_message = seen.append
    effects = deque([_effect(), _effect(), _effect()])

    applied = client._apply_message_effect_batch_inline(
        effects, client._connection_epoch
    )

    assert applied == 3
    await client._callback_queue.join()
    assert len(seen) == 3
    await client._shutdown_callback_worker(drain=False)


def test_batch_stops_before_half_delivering_both_mode() -> None:
    client = AsyncClient(
        message_delivery="both",
        max_pending_callbacks=1,
        max_pending_messages=1,
    )
    client.on_message = lambda _message: None
    sentinel = (lambda: None, (), None)
    client._callback_queue.put_nowait(sentinel)

    applied = client._apply_message_effect_batch_inline(
        deque([_effect()]), client._connection_epoch
    )

    assert applied == 0
    assert client._messages.empty()
    assert client._callback_queue.get_nowait() is sentinel
    client._callback_queue.task_done()


@pytest.mark.parametrize("qos", [QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
def test_batch_defers_acknowledged_messages(qos: QoS) -> None:
    client = AsyncClient(message_delivery="callback")
    client.on_message = lambda _message: None

    applied = client._apply_message_effect_batch_inline(
        deque([_effect(qos=qos, mid=7)]), client._connection_epoch
    )

    assert applied == 0
    assert client._callback_queue.empty()


def test_batch_stops_at_first_non_qos0_effect() -> None:
    client = AsyncClient(message_delivery="iterator")
    effects = deque(
        [
            _effect(),
            _effect(qos=QoS.AT_LEAST_ONCE, mid=7),
            _effect(),
        ]
    )

    applied = client._apply_message_effect_batch_inline(
        effects, client._connection_epoch
    )

    assert applied == 1
    assert client._messages.qsize() == 1
    assert client._message_ready.is_set()
'''
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    patch_effects(args.root)
    patch_client(args.root)
    write_tests(args.root)


if __name__ == "__main__":
    main()
