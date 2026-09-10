#!/usr/bin/env python3
"""Apply the narrow singleton-message-run inline ablation to exact fd37."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path

EXPECTED_SHA256 = "78b771ddba5db5e7c8f0ddafce5f04fd4ac370e845e39b06d615b47d15e5b8bd"

path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("src/mqttium/api/_delivery.py")
actual = hashlib.sha256(path.read_bytes()).hexdigest()
if actual != EXPECTED_SHA256:
    raise SystemExit(f"unexpected _delivery.py source: {actual}")

text = path.read_text()
method_anchor = """    def deliver_message_batch_inline(\n        self,\n        effects: deque[EngineEffect],\n"""
method_replacement = """    def _inline_message_candidate(self, effect: EngineEffect) -> Message | None:\n        \"\"\"Return one cheap, non-persisted message eligible for inline dispatch.\"\"\"\n        if effect.kind not in (EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE):\n            return None\n        message: Message = effect.data\n        size = effect.decoded_property_wire_size\n        if effect.requires_delivery_mark or not (\n            self._is_small(message) if size is None else self._is_small_decoded(message, size)\n        ):\n            return None\n        return message\n\n    def deliver_message_batch_inline(\n        self,\n        effects: deque[EngineEffect],\n"""
if text.count(method_anchor) != 1:
    raise SystemExit("helper insertion anchor did not match exactly once")
text = text.replace(method_anchor, method_replacement)

body_anchor = """        cb = callback if callback_delivery else None\n        capacity = len(effects)\n"""
body_replacement = """        cb = callback if callback_delivery else None\n\n        # Keep the ordinary worker loop below unchanged. Only the head of a\n        # one-message eligible run may avoid the queue hop.\n        if (\n            cb is not None\n            and not iterator_delivery\n            and effects\n            and self.can_dispatch_callback_inline(cb)\n        ):\n            first = self._inline_message_candidate(effects[0])\n            if first is not None and (\n                len(effects) == 1 or self._inline_message_candidate(effects[1]) is None\n            ):\n                self.dispatch_callback_inline(cb, first)\n                return 1\n\n        capacity = len(effects)\n"""
if text.count(body_anchor) != 1:
    raise SystemExit("singleton inline insertion anchor did not match exactly once")
text = text.replace(body_anchor, body_replacement)
path.write_text(text)
print(hashlib.sha256(path.read_bytes()).hexdigest())
