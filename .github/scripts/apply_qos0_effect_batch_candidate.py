from __future__ import annotations

import sys
from pathlib import Path


BATCH_METHOD = '''    def _apply_message_effect_batch_inline(
        self,
        effects: deque[EngineEffect],
        epoch: int,
    ) -> int:
        if epoch != self._connection_epoch or len(effects) < 2:
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


def apply(root: Path) -> None:
    effects_path = root / "src/mqttium/api/_effects.py"
    effects = effects_path.read_text()
    protocol_marker = '''    def _apply_effect_inline(self, effect: EngineEffect, epoch: int) -> bool: ...

'''
    protocol_add = '''    def _apply_effect_inline(self, effect: EngineEffect, epoch: int) -> bool: ...

    def _apply_message_effect_batch_inline(
        self, effects: deque[EngineEffect], epoch: int
    ) -> int: ...

'''
    if protocol_marker not in effects:
        raise RuntimeError("EffectOwner marker did not match")
    effects = effects.replace(protocol_marker, protocol_add, 1)

    run_marker = '''                    try:
                        await self.owner._apply_effect(effect, nowait=False, epoch=epoch)
'''
    run_add = '''                    if effect.kind is EffectKind.MESSAGE:
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
    if run_marker not in effects:
        raise RuntimeError("scheduled effect marker did not match")
    effects_path.write_text(effects.replace(run_marker, run_add, 1))

    client_path = root / "src/mqttium/api/async_client.py"
    client = client_path.read_text()
    marker = '''    async def _flush_effects(self, *, nowait: bool = False) -> None:
'''
    if marker not in client:
        raise RuntimeError("AsyncClient flush marker did not match")
    client_path.write_text(client.replace(marker, BATCH_METHOD + marker, 1))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: apply_qos0_effect_batch_candidate.py ROOT")
    apply(Path(sys.argv[1]))
