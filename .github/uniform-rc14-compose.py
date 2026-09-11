#!/usr/bin/env python3
"""Finish the RC14 + simplified delivery synthesis inside a merge worktree."""
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


# Tighten the measured clean policy: only a sole pending effect may run user
# message code inline. This prevents user code from overtaking any effect that
# was already owned by the pump while retaining the post-lock QoS1 fast path.
replace(
    "src/mqttium/api/_delivery.py",
    '''        # Keep the ordinary worker loop below unchanged. An idle synchronous\n        # callback-only run containing exactly one eligible MESSAGE may avoid\n        # the queue hop. A second eligible MESSAGE makes the entire message run\n        # worker-owned; non-message effects behind a singleton do not.\n        if (\n            cb is not None\n            and not iterator_delivery\n            and self._callback_state == "open"\n            and effects\n            and self.can_dispatch_callback_inline(cb)\n        ):\n            first = self._inline_message_candidate(effects[0])\n            if first is not None and (\n                len(effects) == 1 or self._inline_message_candidate(effects[1]) is None\n            ):\n                self.dispatch_callback_inline(cb, first)\n                return 1\n''',
    '''        # Keep the ordinary worker loop below unchanged. Only a sole pending\n        # eligible MESSAGE may avoid the queue hop. If any other effect is already\n        # owned by the pump, user code cannot overtake it.\n        if (\n            cb is not None\n            and not iterator_delivery\n            and self._callback_state == "open"\n            and len(effects) == 1\n            and self.can_dispatch_callback_inline(cb)\n        ):\n            first = self._inline_message_candidate(effects[0])\n            if first is not None:\n                self.dispatch_callback_inline(cb, first)\n                return 1\n''',
)
replace(
    "src/mqttium/api/_delivery.py",
    '''        """Admit a consecutive eligible prefix; never run application code.\n\n        The effect owner may finish admission synchronously without creating a\n        flusher. User execution belongs exclusively to the callback worker.\n        Iterator/callback capacity is preflighted once for the whole prefix.\n        No user code or suspension can invalidate that preflight.\n        Persisted deliveries and exact-byte reservations retain the slow path.\n        """\n''',
    '''        """Admit a consecutive eligible prefix with one narrow sync fast path.\n\n        A sole pending eligible callback-only MESSAGE may execute an idle declared-sync\n        callback inline. Async, multi-effect, reentrant, iterator/both and persisted\n        work remains worker/slow-path owned. AsyncClient refuses this method while\n        holding the engine lock, so the inline exception never runs user code in that\n        critical section. Iterator/callback capacity stays ordinary queue capacity.\n        """\n''',
)

# PR454 reconnect fix, expressed in the uniform controller state model. A callback
# that disconnects/reconnects before its worker job returns must not let old queued
# work survive into the replacement generation.
replace(
    "src/mqttium/api/_delivery.py",
    '''        if self.callback_task is asyncio.current_task():\n            # An own-worker shutdown cannot join itself. Return to finish the\n            # active notification; a reconnect may reopen the same consumer.\n            return\n''',
    '''        if self.callback_task is asyncio.current_task():\n            # An own-worker shutdown cannot join itself. Retire unstarted work\n            # now; the active notification may finish and a reconnect may reopen\n            # the same worker incarnation for the replacement generation.\n            self._callback_state = "closed"\n            self._discard_callback_queue()\n            return\n''',
)

# RC14 live-routing tests describe the same observable obligations using the old
# batch-reservation/stop-state representation. Keep the tests, update only their
# internal accounting model to the ordinary queue/generation controller.
replace(
    "tests/unit/test_callback_route_lifecycle.py",
    "        assert not delivery._callback_stop\n",
    '        assert delivery._callback_state == "open"\n',
)

path = root / "tests/unit/test_callback_route_reconfiguration.py"
text = path.read_text()
old = (
    '            # An inline pair transfers only its unstarted tail. The completed\n'
    '            # first callback no longer reserves a slot while the tail awaits.\n'
    '            expected_pending = 1 if burst == 2 else burst\n'
    '            assert client.stats().delivery.callback_queued == expected_pending\n'
)
new = (
    '            # The ordinary queue reports only unstarted notifications; the\n'
    '            # currently active callback is deliberately excluded from qsize.\n'
    '            expected_pending = burst - 1\n'
    '            assert client.stats().delivery.callback_queued == expected_pending\n'
)
if text.count(old) != 1:
    raise SystemExit(f"reconfiguration queue anchor count={text.count(old)}")
text = text.replace(old, new, 1)
old = (
    '            if burst == 2:\n'
    '                assert client._delivery.try_enqueue_callback(lambda: seen.append("refill"))\n'
    '            assert client.stats().delivery.callback_queued == burst\n'
    '            assert not client._delivery.try_enqueue_callback(lambda: seen.append("overflow"))\n'
)
new = (
    '            # Exactly one queue slot is free because the active callback is not queued.\n'
    '            assert client._delivery.try_enqueue_callback(lambda: seen.append("refill"))\n'
    '            assert client.stats().delivery.callback_queued == burst\n'
    '            assert not client._delivery.try_enqueue_callback(lambda: seen.append("overflow"))\n'
)
if text.count(old) != 1:
    raise SystemExit(f"reconfiguration refill anchor count={text.count(old)}")
text = text.replace(old, new, 1)
old = '            assert seen == expected + ["reentrant"] + (["refill"] if burst == 2 else [])\n'
new = '            assert seen == expected + ["reentrant", "refill"]\n'
if text.count(old) != 1:
    raise SystemExit(f"reconfiguration fifo anchor count={text.count(old)}")
path.write_text(text.replace(old, new, 1))

# _CallbackHandoff is an implementation-specific bridge for the old captured-sync
# inline scheduler. The stable per-notification dispatcher supersedes it, while the
# other PR454 tests below continue to enforce live routes, FIFO, QoS and reconnect.
handoff = root / "tests/unit/test_callback_route_handoff.py"
if handoff.exists():
    handoff.unlink()

# One focused invariant distinguishes strict sole-pending from the measured clean
# variant and guards lifecycle closure explicitly.
(root / "tests/unit/test_strict_singleton_message_inline.py").write_text('''"""Strict sole-pending MESSAGE fast-path invariants."""\n\nfrom __future__ import annotations\n\nimport asyncio\nfrom collections import deque\n\nimport pytest\n\nfrom mqttium.api import AsyncClient\nfrom mqttium.errors import MessageDeliveryError\nfrom mqttium.protocol.effects import EffectKind, EngineEffect\nfrom mqttium.types import Message\n\n\ndef msg(payload: bytes, *, persisted: bool = False) -> EngineEffect:\n    return EngineEffect(\n        EffectKind.MESSAGE,\n        Message(topic="strict/x", payload=payload),\n        requires_delivery_mark=persisted,\n    )\n\n\nasync def stop(client: AsyncClient) -> None:\n    await client._shutdown_callback_worker(drain=False)\n\n\nasync def test_sole_sync_message_runs_inline() -> None:\n    client = AsyncClient(message_delivery="callback")\n    seen: list[bytes] = []\n    caller = asyncio.current_task()\n    owners = []\n\n    def callback(message: Message) -> None:\n        seen.append(message.payload)\n        owners.append(asyncio.current_task())\n\n    effects = deque([msg(b"one")])\n    assert client._delivery.deliver_message_batch_inline(effects, callback) == 1\n    assert seen == [b"one"]\n    assert owners == [caller]\n    assert client._callback_queue.empty()\n    assert client._callback_worker_task is None\n    await stop(client)\n\n\nasync def test_following_non_message_effect_forces_worker() -> None:\n    client = AsyncClient(message_delivery="callback")\n    seen: list[bytes] = []\n    client.on_message = lambda message: seen.append(message.payload)\n    effects = deque([msg(b"one"), EngineEffect(EffectKind.PINGRESP)])\n    assert client._delivery.deliver_message_batch_inline(effects, client.on_message) == 1\n    assert seen == []\n    assert client._callback_queue.qsize() == 1\n    await client._callback_queue.join()\n    assert seen == [b"one"]\n    await stop(client)\n\n\nasync def test_two_messages_keep_whole_run_worker_owned() -> None:\n    client = AsyncClient(message_delivery="callback")\n    seen: list[bytes] = []\n    callback = lambda message: seen.append(message.payload)\n    effects = deque([msg(b"one"), msg(b"two")])\n    assert client._delivery.deliver_message_batch_inline(effects, callback) == 2\n    assert seen == []\n    assert client._callback_queue.qsize() == 2\n    await client._callback_queue.join()\n    assert seen == [b"one", b"two"]\n    await stop(client)\n\n\nasync def test_async_and_both_stay_worker_owned() -> None:\n    async def callback(message: Message) -> None:\n        await asyncio.sleep(0)\n\n    client = AsyncClient(message_delivery="callback")\n    effects = deque([msg(b"async")])\n    assert client._delivery.deliver_message_batch_inline(effects, callback) == 1\n    assert client._callback_queue.qsize() == 1\n    await client._callback_queue.join()\n    await stop(client)\n\n    both = AsyncClient(message_delivery="both")\n    seen: list[bytes] = []\n    effects = deque([msg(b"both")])\n    assert both._delivery.deliver_message_batch_inline(\n        effects, lambda message: seen.append(message.payload)\n    ) == 1\n    assert seen == []\n    assert both._messages.qsize() == 1\n    assert both._callback_queue.qsize() == 1\n    await both._callback_queue.join()\n    await stop(both)\n\n\nasync def test_persisted_message_stays_slow_path() -> None:\n    client = AsyncClient(message_delivery="callback")\n    effects = deque([msg(b"persisted", persisted=True)])\n    assert client._delivery.deliver_message_batch_inline(effects, lambda _m: None) == 0\n    await stop(client)\n\n\n@pytest.mark.parametrize("state", ["draining", "closed"])\nasync def test_non_open_delivery_never_inlines(state: str) -> None:\n    client = AsyncClient(message_delivery="callback")\n    seen: list[bytes] = []\n    client._delivery._callback_state = state\n    with pytest.raises(MessageDeliveryError, match="Callback delivery is closing"):\n        client._delivery.deliver_message_batch_inline(\n            deque([msg(b"late")]), lambda message: seen.append(message.payload)\n        )\n    assert seen == []\n    client._delivery._callback_state = "open"\n    await stop(client)\n''')
