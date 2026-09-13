#!/usr/bin/env python3
"""Apply the narrow direct-QoS0 singleton fast path to exact fb619a27."""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path('.')
delivery = root / 'src/mqttium/api/_delivery.py'
client = root / 'src/mqttium/api/async_client.py'


def replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text()
    if text.count(old) != 1:
        raise SystemExit(f'anchor mismatch in {path}: expected exactly one match')
    path.write_text(text.replace(old, new))

old_delivery = '''    def can_dispatch_callback_inline(self, callback: Callable[..., Any]) -> bool:\n        \"\"\"Whether a plain synchronous callback can run without a queue hop.\"\"\"\n        return (\n            not self._callback_active\n            and self.callback_queue.empty()\n            and not self._is_async_callback(callback)\n        )\n'''
new_delivery = old_delivery + '''\n    def can_dispatch_direct_message_inline(\n        self,\n        message: Message,\n        callback: Callable[[Message], Any] | None,\n        property_wire_size: int | None,\n    ) -> bool:\n        \"\"\"Whether one direct-decode message may run after the engine lock.\"\"\"\n        if (\n            callback is None\n            or self._callback_state != \"open\"\n            or not self.can_dispatch_callback_inline(callback)\n        ):\n            return False\n        return (\n            self._is_small(message)\n            if property_wire_size is None\n            else self._is_small_decoded(message, property_wire_size)\n        )\n'''
replace_once(delivery, old_delivery, new_delivery)

old_client = '''                        if captured:\n                            if self._delivery.deliver_callback_messages_inline(\n                                captured, self._message_callback, captured_property_sizes\n                            ):\n                                self._effect_pump.record_inline_batch(len(captured))\n                            else:\n                                _extend_message_effects(\n                                    self._engine._effects, captured, captured_property_sizes\n                                )\n                        if handled and self._engine.has_pending_effects:\n                            self._collect_effects_locked()\n                    if self._effect_pump.pending:\n                        await self._drain_effects()\n'''
new_client = '''                        direct_inline_message: Message | None = None\n                        if captured:\n                            wire_size = (\n                                captured_property_sizes[0]\n                                if captured_property_sizes is not None and len(captured) == 1\n                                else None\n                            )\n                            if len(captured) == 1 and self._delivery.can_dispatch_direct_message_inline(\n                                captured[0], self._message_callback, wire_size\n                            ):\n                                direct_inline_message = captured[0]\n                            elif self._delivery.deliver_callback_messages_inline(\n                                captured, self._message_callback, captured_property_sizes\n                            ):\n                                self._effect_pump.record_inline_batch(len(captured))\n                            else:\n                                _extend_message_effects(\n                                    self._engine._effects, captured, captured_property_sizes\n                                )\n                        if handled and self._engine.has_pending_effects:\n                            self._collect_effects_locked()\n                    # Eligibility was checked under the engine lock, but user code\n                    # runs only after leaving it. There is no await in between, so\n                    # callback/queue state cannot change before dispatch.\n                    if direct_inline_message is not None:\n                        callback = self._message_callback\n                        assert callback is not None\n                        self._delivery.dispatch_callback_inline(callback, direct_inline_message)\n                        self._effect_pump.record_inline_batch(1)\n                    if self._effect_pump.pending:\n                        await self._drain_effects()\n'''
replace_once(client, old_client, new_client)

for path in (delivery, client):
    print(path, hashlib.sha256(path.read_bytes()).hexdigest())
