#!/usr/bin/env python3
"""Apply the no-helper singleton-message-run inline ablation to exact fd37."""

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
anchor = """        cb = callback if callback_delivery else None\n        capacity = len(effects)\n"""
replacement = """        cb = callback if callback_delivery else None\n\n        # Keep the ordinary worker path below unchanged. Only an idle sync\n        # callback at the head of a one-message eligible run may avoid the\n        # queue hop; async, reentrant, `both`, and consecutive message bursts\n        # retain the uniform bounded worker.\n        if (\n            cb is not None\n            and not iterator_delivery\n            and effects\n            and self.can_dispatch_callback_inline(cb)\n        ):\n            first = effects[0]\n            if first.kind in (EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE):\n                first_message: Message = first.data\n                first_size = first.decoded_property_wire_size\n                first_small = (\n                    self._is_small(first_message)\n                    if first_size is None\n                    else self._is_small_decoded(first_message, first_size)\n                )\n                if not first.requires_delivery_mark and first_small:\n                    second_eligible_message = False\n                    if len(effects) > 1:\n                        second = effects[1]\n                        if second.kind in (EffectKind.MESSAGE, EffectKind.DECODED_MESSAGE):\n                            second_message: Message = second.data\n                            second_size = second.decoded_property_wire_size\n                            second_small = (\n                                self._is_small(second_message)\n                                if second_size is None\n                                else self._is_small_decoded(second_message, second_size)\n                            )\n                            second_eligible_message = (\n                                not second.requires_delivery_mark and second_small\n                            )\n                    if not second_eligible_message:\n                        self.dispatch_callback_inline(cb, first_message)\n                        return 1\n\n        capacity = len(effects)\n"""
if text.count(anchor) != 1:
    raise SystemExit("singleton inline insertion anchor did not match exactly once")
text = text.replace(anchor, replacement)
path.write_text(text)
print(hashlib.sha256(path.read_bytes()).hexdigest())
