#!/usr/bin/env python3
"""Apply sync singleton inline until the persistent callback worker takes ownership."""

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
method_replacement = """    def _inline_message_candidate(self, effect: EngineEffect) -> Message | None:\n        \"\"\"Return one non-persisted small message eligible for sync inline.\"\"\"\n        if effect.kind not in (EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE):\n            return None\n        message: Message = effect.data\n        size = effect.decoded_property_wire_size\n        if effect.requires_delivery_mark or not (\n            self._is_small(message) if size is None else self._is_small_decoded(message, size)\n        ):\n            return None\n        return message\n\n    def deliver_message_batch_inline(\n        self,\n        effects: deque[EngineEffect],\n"""
if text.count(method_anchor) != 1:
    raise SystemExit("helper anchor mismatch")
text = text.replace(method_anchor, method_replacement)

body_anchor = """        cb = callback if callback_delivery else None\n        capacity = len(effects)\n"""
body_replacement = """        cb = callback if callback_delivery else None\n\n        # Before a callback worker has ever been needed, a sole eligible sync\n        # message may run in the current effect-drain turn. Once any burst,\n        # async callback, or queued work starts the persistent worker, that task\n        # owns all later message callbacks until lifecycle shutdown. A draining\n        # or closed callback subsystem is never re-opened implicitly by inline\n        # execution merely because no worker task is currently referenced.\n        if (\n            cb is not None\n            and not iterator_delivery\n            and self._callback_state == \"open\"\n            and self.callback_task is None\n            and len(effects) == 1\n            and self.can_dispatch_callback_inline(cb)\n        ):\n            message = self._inline_message_candidate(effects[0])\n            if message is not None:\n                self.dispatch_callback_inline(cb, message)\n                return 1\n\n        capacity = len(effects)\n"""
if text.count(body_anchor) != 1:
    raise SystemExit("body anchor mismatch")
text = text.replace(body_anchor, body_replacement)
path.write_text(text)
print(hashlib.sha256(path.read_bytes()).hexdigest())
