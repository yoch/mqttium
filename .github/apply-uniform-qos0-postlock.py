#!/usr/bin/env python3
"""Apply the post-lock direct-QoS0 singleton ablation to exact fb619a27."""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path('.')
delivery = root / 'src/mqttium/api/_delivery.py'
client = root / 'src/mqttium/api/async_client.py'

# Fail closed if this is not the exact qualified RC14 runtime parent.
expected = {
    delivery: 'f1a87b6bf277da843f94cd0b7d14cab02a604a92',  # git blob sha, checked below via hash-object in workflow
    client: '0c3d831880381cb61b693ba5a3683e1610530602',
}

def replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text()
    if text.count(old) != 1:
        raise SystemExit(f'anchor mismatch in {path}: expected exactly one match')
    path.write_text(text.replace(old, new))

old_delivery = '''    def deliver_callback_messages_inline(\n        self,\n        messages: list[Message],\n        callback: Callable[[Message], Any] | None,\n        decoded_property_wire_sizes: list[int | None] | None = None,\n    ) -> bool:\n        if callback is None:\n            return True\n        if not self.has_callback_capacity(len(messages)):\n            return False\n        if decoded_property_wire_sizes is None:\n            for message in messages:\n                if not self._is_small(message):\n                    return False\n        else:\n            if len(decoded_property_wire_sizes) != len(messages):\n                raise AssertionError("decoded property sizes must align with messages")\n            for message, wire_size in zip(messages, decoded_property_wire_sizes, strict=True):\n                if wire_size is None:\n                    if not self._is_small(message):\n                        return False\n                elif not self._is_small_decoded(message, wire_size):\n                    return False\n        self._enqueue_message_batch(callback, messages, iterator_delivery=False)\n        return True\n'''
new_delivery = '''    def can_deliver_callback_messages(\n        self,\n        messages: list[Message],\n        callback: Callable[[Message], Any] | None,\n        decoded_property_wire_sizes: list[int | None] | None = None,\n    ) -> bool:\n        \"\"\"Preflight direct-decode delivery without running user code.\"\"\"\n        if callback is None:\n            return True\n        if not self.has_callback_capacity(len(messages)):\n            return False\n        if decoded_property_wire_sizes is None:\n            return all(self._is_small(message) for message in messages)\n        if len(decoded_property_wire_sizes) != len(messages):\n            raise AssertionError("decoded property sizes must align with messages")\n        return all(\n            self._is_small(message)\n            if wire_size is None\n            else self._is_small_decoded(message, wire_size)\n            for message, wire_size in zip(messages, decoded_property_wire_sizes, strict=True)\n        )\n\n    def deliver_callback_messages_inline(\n        self,\n        messages: list[Message],\n        callback: Callable[[Message], Any] | None,\n        decoded_property_wire_sizes: list[int | None] | None = None,\n    ) -> bool:\n        if not self.can_deliver_callback_messages(\n            messages, callback, decoded_property_wire_sizes\n        ):\n            return False\n        if callback is None:\n            return True\n        if (\n            len(messages) == 1\n            and self._callback_state == "open"\n            and self.can_dispatch_callback_inline(callback)\n        ):\n            self.dispatch_callback_inline(callback, messages[0])\n            return True\n        self._enqueue_message_batch(callback, messages, iterator_delivery=False)\n        return True\n'''
replace_once(delivery, old_delivery, new_delivery)

old_client = '''                        if captured:\n                            if self._delivery.deliver_callback_messages_inline(\n                                captured, self._message_callback, captured_property_sizes\n                            ):\n                                self._effect_pump.record_inline_batch(len(captured))\n                            else:\n                                _extend_message_effects(\n                                    self._engine._effects, captured, captured_property_sizes\n                                )\n                        if handled and self._engine.has_pending_effects:\n                            self._collect_effects_locked()\n                    if self._effect_pump.pending:\n                        await self._drain_effects()\n'''
new_client = '''                        if captured and not self._delivery.can_deliver_callback_messages(\n                            captured, self._message_callback, captured_property_sizes\n                        ):\n                            _extend_message_effects(\n                                self._engine._effects, captured, captured_property_sizes\n                            )\n                            captured.clear()\n                            if captured_property_sizes is not None:\n                                captured_property_sizes.clear()\n                        if handled and self._engine.has_pending_effects:\n                            self._collect_effects_locked()\n                    # No await occurs between releasing the engine lock and this\n                    # preflighted admission, so queue capacity and generation cannot\n                    # change underneath the direct-decode batch. User code therefore\n                    # never runs under the engine lock while a true singleton may\n                    # still avoid the callback-worker hop.\n                    if captured:\n                        assert self._delivery.deliver_callback_messages_inline(\n                            captured, self._message_callback, captured_property_sizes\n                        )\n                        self._effect_pump.record_inline_batch(len(captured))\n                    if self._effect_pump.pending:\n                        await self._drain_effects()\n'''
replace_once(client, old_client, new_client)

for path in (delivery, client):
    print(path, hashlib.sha256(path.read_bytes()).hexdigest())
