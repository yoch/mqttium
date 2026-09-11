#!/usr/bin/env python3
"""Apply the final strict policy edits to an fd37-derived candidate worktree."""
from __future__ import annotations

import sys
from pathlib import Path

root = Path(sys.argv[1]).resolve()


def replace(rel: str, old: str, new: str) -> None:
    path = root / rel
    text = path.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{rel}: expected one anchor, found {count}")
    path.write_text(text.replace(old, new, 1))


replace(
    "src/mqttium/api/_delivery.py",
    '            and not iterator_delivery\n            and len(effects) == 1\n',
    '            and not iterator_delivery\n'
    '            and self._callback_state == "open"\n'
    '            and len(effects) == 1\n',
)

replace(
    "tests/unit/test_effect_pump.py",
    '''async def test_idle_sync_message_callback_runs_in_worker_after_engine_lock() -> None:\n    client = AsyncClient(client_id="effect-inline-message", message_delivery="callback")\n    seen: list[tuple[str, bool]] = []\n    client.on_message = lambda message: seen.append((message.topic, client._engine_lock.locked()))\n\n    async with client._engine_lock:\n        client._engine._emit(\n            EffectKind.MESSAGE,\n            Message(topic="inline/message", payload=b"x"),\n        )\n        client._collect_effects_locked()\n        assert seen == []\n\n    client._drain_effects_inline()\n    assert seen == []\n    await client._callback_queue.join()\n    assert seen == [("inline/message", False)]\n    await client._shutdown_callback_worker(drain=False)\n''',
    '''async def test_idle_sync_message_singleton_runs_inline_after_engine_lock() -> None:\n    client = AsyncClient(client_id="effect-inline-message", message_delivery="callback")\n    seen: list[tuple[str, bool]] = []\n    client.on_message = lambda message: seen.append((message.topic, client._engine_lock.locked()))\n\n    async with client._engine_lock:\n        client._engine._emit(\n            EffectKind.MESSAGE,\n            Message(topic="inline/message", payload=b"x"),\n        )\n        client._collect_effects_locked()\n        assert seen == []\n\n    client._drain_effects_inline()\n    assert seen == [("inline/message", False)]\n    assert client._callback_queue.empty()\n    assert client._callback_worker_task is None\n    await client._shutdown_callback_worker(drain=False)\n''',
)

replace(
    "tests/unit/test_hotpath_recon.py",
    "async def test_qos1_inbound_reply_serial_uses_uniform_worker() -> None:\n",
    "async def test_qos1_inbound_reply_serial_uses_singleton_inline() -> None:\n",
)
replace(
    "tests/unit/test_hotpath_recon.py",
    '    assert counters["qos1_v311_field_decodes"] == 40\n'
    '    assert counters["callback_inline_rate"] == 0.0\n'
    '    assert counters["send_ack_effects"] == 40\n',
    '    assert counters["qos1_v311_field_decodes"] == 40\n'
    '    assert counters["callback_inline_rate"] == 1.0\n'
    '    assert counters["send_ack_effects"] == 40\n',
)

replace(
    "tests/unit/test_uniform_callbacks.py",
    '"""Uniform message callback ownership, bounded rounds, and lifecycle regressions."""',
    '"""Bounded message callback ownership, rounds, and lifecycle regressions."""',
)
replace(
    "tests/unit/test_uniform_callbacks.py",
    '''        assert seen == []\n        bound(client, 64)\n        assert client._callback_queue.qsize() == count\n        await asyncio.wait_for(client._callback_queue.join(), 1)\n        assert seen == [str(i).encode() for i in range(count)]\n        assert all(t is client._callback_worker_task and t is not caller for t in owners)\n        if mode == "both":\n            assert client._messages.qsize() == count\n''',
    '''        inline = not direct and mode in ("callback", "auto") and count == 1\n        bound(client, 64)\n        if inline:\n            assert seen == [b"0"]\n            assert owners == [caller]\n            assert client._callback_queue.empty()\n            assert client._callback_worker_task is None\n        else:\n            assert seen == []\n            assert client._callback_queue.qsize() == count\n            await asyncio.wait_for(client._callback_queue.join(), 1)\n            assert seen == [str(i).encode() for i in range(count)]\n            assert all(t is client._callback_worker_task and t is not caller for t in owners)\n        if mode == "both":\n            assert client._messages.qsize() == count\n''',
)

strict = root / "tests/unit/test_message_callback_singleton_inline.py"
text = strict.read_text()
if "import pytest\n" not in text:
    anchor = "from collections import deque\n\n"
    if text.count(anchor) != 1:
        raise SystemExit("strict test: import anchor not unique")
    text = text.replace(anchor, anchor + "import pytest\n\n", 1)
if "from mqttium.errors import MessageDeliveryError\n" not in text:
    anchor = "from mqttium.api import AsyncClient\n"
    if text.count(anchor) != 1:
        raise SystemExit("strict test: mqttium import anchor not unique")
    text = text.replace(anchor, anchor + "from mqttium.errors import MessageDeliveryError\n", 1)

text += '''\n\n@pytest.mark.parametrize("state", ["draining", "closed"])\nasync def test_non_open_delivery_never_uses_inline(state: str) -> None:\n    client = AsyncClient(message_delivery="callback")\n    seen: list[bytes] = []\n\n    def callback(message: Message) -> None:\n        seen.append(message.payload)\n\n    client._delivery._callback_state = state\n    with pytest.raises(MessageDeliveryError, match="Callback delivery is closing"):\n        client._delivery.deliver_message_batch_inline(deque([effect(b"late")]), callback)\n    assert seen == []\n    client._delivery._callback_state = "open"\n    await stop(client)\n'''
strict.write_text(text)
