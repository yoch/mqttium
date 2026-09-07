from __future__ import annotations

import subprocess
from pathlib import Path

BASE = "4fe90af36660bce06c12e9a8c57cb9a6223abe5a"


def read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def write(path: str, text: str) -> None:
    Path(path).write_text(text, encoding="utf-8")


def replace_once(text: str, old: str, new: str, *, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new, 1)


def replace_between(text: str, start: str, end: str, new: str, *, label: str) -> str:
    start_index = text.find(start)
    if start_index < 0:
        raise RuntimeError(f"{label}: start marker not found")
    end_index = text.find(end, start_index)
    if end_index < 0:
        raise RuntimeError(f"{label}: end marker not found")
    return text[:start_index] + new + text[end_index:]


def restore(path: str) -> None:
    result = subprocess.run(
        ["git", "show", f"{BASE}:{path}"],
        check=True,
        capture_output=True,
    )
    Path(path).write_bytes(result.stdout)


def simplify_delivery() -> None:
    path = "src/mqttium/api/_delivery.py"
    text = read(path)
    text = replace_once(
        text,
        "        inline_callback_burst: int = 1,\n",
        "",
        label="delivery constructor option",
    )
    text = replace_once(
        text,
        "        self.inline_callback_burst = inline_callback_burst\n",
        "",
        label="delivery option state",
    )
    text = replace_once(
        text,
        "        # At most one parked inline awaitable. A sync callback may fill the\n"
        "        # bounded queue reentrantly before returning an awaitable; that\n"
        "        # continuation is not a new admission. It runs after the physical jobs\n"
        "        # already queued at park time, using public qsize() rather than mutating\n"
        "        # asyncio.Queue internals.\n"
        "        self._inline_continuation: tuple[Callable[..., Any], Awaitable[Any]] | None = None\n"
        "        self._inline_continuation_after = 0\n",
        "",
        label="inline continuation state",
    )
    text = replace_once(
        text,
        "                if (\n"
        "                    len(messages) == 2\n"
        "                    and self.inline_callback_burst == 2\n"
        "                    and not iterator_delivery\n"
        "                    and self.can_dispatch_callback_inline(callback)\n"
        "                ):\n"
        "                    self._dispatch_strict_sync_message_burst_inline(callback, messages)\n"
        "                    return 2\n",
        "                if (\n"
        "                    len(messages) == 2\n"
        "                    and not iterator_delivery\n"
        "                    and self.can_dispatch_callback_inline(callback)\n"
        "                ):\n"
        "                    self._dispatch_sync_message_pair_inline(callback, messages)\n"
        "                    return 2\n",
        label="automatic sync pair",
    )

    callback_block = '''    def can_dispatch_callback_inline(self, callback: Callable[..., Any]) -> bool:
        """Whether a plain synchronous callback can run without a queue hop."""
        return (
            not self._callback_active
            and self.callback_queue.empty()
            and not self._is_async_callback(callback)
        )

    @staticmethod
    def _sync_awaitable_error(result: Any) -> TypeError:
        """Reject a sync callback that dynamically returned async work."""
        if inspect.iscoroutine(result):
            result.close()
        return TypeError(
            "synchronous callbacks must not return awaitables; "
            "declare asynchronous callbacks with 'async def'"
        )

    def run_sync_callback(self, callback: Callable[..., Any], *args: Any) -> None:
        """Invoke one declared-sync callback and isolate application failures."""
        try:
            result = callback(*args)
        except asyncio.CancelledError as exc:
            self._propagate_callback_cancellation(callback, exc)
        except Exception as exc:
            self.report_callback_error(callback, exc)
        else:
            if result is not None and inspect.isawaitable(result):
                self.report_callback_error(callback, self._sync_awaitable_error(result))

    def try_dispatch_callback_inline(self, callback: Callable[..., Any], *args: Any) -> bool:
        """Run one idle synchronous callback now, isolating application errors."""
        if not self.can_dispatch_callback_inline(callback):
            return False
        self.dispatch_callback_inline(callback, *args)
        return True

    def _dispatch_sync_message_pair_inline(
        self,
        callback: Callable[[Message], Any],
        messages: list[Message],
    ) -> None:
        """Run one eligible two-message synchronous burst inline.

        The second message remains reserved in the existing logical callback
        bound while the first callback runs, so reentrant admissions queue
        behind the pair without weakening ``max_pending_callbacks``.
        """
        self._reserve_callback_batch(2)
        self._callback_active = True
        try:
            self.run_sync_callback(callback, messages[0])
            self.run_sync_callback(callback, messages[1])
        finally:
            self._callback_active = False
            self._release_callback_batch(2)

    def dispatch_callback_inline(self, callback: Callable[..., Any], *args: Any) -> None:
        """Invoke a callback after the caller established inline eligibility."""
        self._callback_active = True
        try:
            self.run_sync_callback(callback, *args)
        finally:
            self._callback_active = False

'''
    text = replace_between(
        text,
        "    def can_dispatch_callback_inline",
        "    def has_callback_capacity",
        callback_block,
        label="callback dispatch block",
    )
    text = replace_once(
        text,
        "        self._discard_inline_continuation()\n",
        "",
        label="continuation discard hook",
    )
    text = replace_once(
        text,
        "                due = self._take_due_inline_continuation()\n"
        "                if due is not None:\n"
        "                    await due\n",
        "",
        label="worker continuation hook",
    )
    text = replace_once(
        text,
        "    @staticmethod\n"
        "    async def invoke(callback: Callable[..., Any] | None, *args: Any) -> Any:\n"
        "        if callback is None:\n"
        "            return None\n"
        "        result = callback(*args)\n"
        "        if isinstance(result, Awaitable):\n"
        "            return await result\n"
        "        return result\n",
        "    @classmethod\n"
        "    async def invoke(cls, callback: Callable[..., Any] | None, *args: Any) -> Any:\n"
        "        if callback is None:\n"
        "            return None\n"
        "        if cls._is_async_callback(callback):\n"
        "            return await callback(*args)\n"
        "        result = callback(*args)\n"
        "        if result is not None and inspect.isawaitable(result):\n"
        "            raise cls._sync_awaitable_error(result)\n"
        "        return result\n",
        label="worker invoke contract",
    )
    if "inline_callback_burst" in text or "_inline_continuation" in text:
        raise RuntimeError("delivery simplification left obsolete callback state")
    write(path, text)


def simplify_async_client() -> None:
    path = "src/mqttium/api/async_client.py"
    text = read(path)
    text = replace_once(
        text,
        "    inline_callback_burst: int,\n",
        "",
        label="client validation option",
    )
    text = replace_once(
        text,
        "    if inline_callback_burst not in (1, 2):\n"
        "        raise ValueError(\"inline_callback_burst must be 1 or 2\")\n",
        "",
        label="client option validation",
    )
    text = replace_once(
        text,
        "        inline_callback_burst: ``1`` preserves the default one-callback inline\n"
        "            fairness policy. ``2`` opts callback-only delivery into executing\n"
        "            exactly two adjacent plain synchronous message callbacks in the\n"
        "            same reader/effect turn. In that mode such callbacks must not\n"
        "            return awaitables; declared async callbacks keep the worker path.\n",
        "",
        label="client option docs",
    )
    text = replace_once(
        text,
        "        inline_callback_burst: Literal[1, 2] = 1,\n",
        "",
        label="client constructor option",
    )
    text = replace_once(
        text,
        "            inline_callback_burst=inline_callback_burst,\n",
        "",
        label="client validation call",
    )
    text = replace_once(
        text,
        "            inline_callback_burst=inline_callback_burst,\n",
        "",
        label="delivery construction option",
    )
    text = replace_once(
        text,
        "        self._dispatch_callback_inline = self._delivery.dispatch_callback_inline\n",
        "        self._dispatch_callback_inline = self._delivery.dispatch_callback_inline\n"
        "        self._run_sync_callback = self._delivery.run_sync_callback\n",
        label="sync runner binding",
    )
    text = replace_once(
        text,
        "        self._topic_callbacks: TopicMatcher | None = None\n",
        "        self._topic_callbacks: TopicMatcher | None = None\n"
        "        self._topic_async_callbacks = 0\n",
        label="topic async state",
    )

    route_block = '''    @property
    def on_message(self) -> OnMessage | None:
        """Default callback used when no topic-specific callback matches."""
        return self._on_message

    @on_message.setter
    def on_message(self, callback: OnMessage | None) -> None:
        self._on_message = callback
        self._refresh_message_callback()

    def _refresh_message_callback(self) -> None:
        """Select a statically sync or async topic route on configuration changes."""
        matcher = self._topic_callbacks
        if matcher is None:
            self._message_callback = self._on_message
            return
        fallback = self._on_message
        route_is_async = self._topic_async_callbacks > 0 or (
            fallback is not None and self._delivery._is_async_callback(fallback)
        )
        self._message_callback = (
            self._dispatch_topic_message_async
            if route_is_async
            else self._dispatch_topic_message_sync
        )

    def message_callback_add(self, topic_filter: str, callback: OnMessage) -> None:
        """Register a message callback for one MQTT topic filter.

        Matching filtered callbacks run instead of ``on_message``, in
        registration order. Replacing the callback for an existing filter
        keeps that order. Filters are validated as SUBSCRIBE topic filters.
        Shared-subscription filters match the filter string literally.
        """
        validate_subscribe_filter(topic_filter)
        matcher = self._topic_callbacks
        if matcher is None:
            matcher = TopicMatcher()
            self._topic_callbacks = matcher
        else:
            try:
                previous = matcher[topic_filter]
            except KeyError:
                pass
            else:
                if self._delivery._is_async_callback(previous):
                    self._topic_async_callbacks -= 1
        matcher[topic_filter] = callback
        if self._delivery._is_async_callback(callback):
            self._topic_async_callbacks += 1
        self._refresh_message_callback()

    def message_callback_remove(self, topic_filter: str) -> None:
        """Remove the callback registered for ``topic_filter``, if any."""
        matcher = self._topic_callbacks
        if matcher is None:
            return
        try:
            callback = matcher[topic_filter]
        except KeyError:
            return
        del matcher[topic_filter]
        if self._delivery._is_async_callback(callback):
            self._topic_async_callbacks -= 1
        if not matcher:
            self._topic_callbacks = None
            self._topic_async_callbacks = 0
        self._refresh_message_callback()

    def _dispatch_topic_message_sync(self, message: Message) -> None:
        """Dispatch a topic route known at configuration time to be synchronous."""
        matcher = self._topic_callbacks
        if matcher is not None:
            callbacks = tuple(matcher.iter_match(message.topic))
            if callbacks:
                for callback in callbacks:
                    self._run_sync_callback(callback, message)
                return
        callback = self._on_message
        if callback is not None:
            self._run_sync_callback(callback, message)

    async def _dispatch_topic_message_async(self, message: Message) -> None:
        """Dispatch a route containing at least one declared-async callback."""
        matcher = self._topic_callbacks
        if matcher is not None:
            callbacks = tuple(matcher.iter_match(message.topic))
            if callbacks:
                for callback in callbacks:
                    try:
                        await self._invoke(callback, message)
                    except asyncio.CancelledError as exc:
                        self._delivery._propagate_callback_cancellation(callback, exc)
                    except Exception as exc:
                        self._report_callback_error(callback, exc)
                return
        callback = self._on_message
        if callback is None:
            return
        try:
            await self._invoke(callback, message)
        except asyncio.CancelledError as exc:
            self._delivery._propagate_callback_cancellation(callback, exc)
        except Exception as exc:
            self._report_callback_error(callback, exc)

'''
    text = replace_between(
        text,
        "    @property\n    def on_message",
        "    async def subscribe",
        route_block,
        label="topic callback router",
    )
    if "inline_callback_burst" in text or "_continue_topic_callbacks" in text:
        raise RuntimeError("client simplification left obsolete callback machinery")
    write(path, text)


def rewrite_tests() -> None:
    restore("tests/project/test_public_api_surface.py")
    restore("tests/unit/test_topic_callback_dispatch_contract.py")

    topic_path = "tests/unit/test_topic_callback_dispatch_contract.py"
    text = read(topic_path)
    text = replace_once(text, "import inspect\n", "", label="topic inspect import")
    text = replace_once(
        text,
        "    routed = client._message_callback\n\n"
        "    def fallback(_message: Message) -> None:\n"
        "        pass\n\n"
        "    client.on_message = fallback\n"
        "    assert client.on_message is fallback\n"
        "    assert client._message_callback is routed\n",
        "    def fallback(_message: Message) -> None:\n"
        "        pass\n\n"
        "    client.on_message = fallback\n"
        "    assert client.on_message is fallback\n"
        "    assert client._message_callback is not fallback\n"
        "    assert client._message_callback is not None\n"
        "    assert not client._delivery._is_async_callback(client._message_callback)\n",
        label="router identity test",
    )
    text = replace_between(
        text,
        "async def test_reentrant_sync_match_can_fill_queue_before_async_match",
        "async def test_filter_mutation_during_dispatch_does_not_change_current_matches",
        '''def test_topic_router_switches_between_sync_and_async_configuration() -> None:
    client = AsyncClient(message_delivery="callback")

    def sync_callback(_message: Message) -> None:
        pass

    async def async_callback(_message: Message) -> None:
        pass

    client.message_callback_add("sync/#", sync_callback)
    assert client._message_callback is not None
    assert not client._delivery._is_async_callback(client._message_callback)

    client.message_callback_add("async/#", async_callback)
    assert client._message_callback is not None
    assert client._delivery._is_async_callback(client._message_callback)

    client.message_callback_remove("async/#")
    assert client._message_callback is not None
    assert not client._delivery._is_async_callback(client._message_callback)


def test_async_fallback_makes_topic_router_async_until_replaced() -> None:
    client = AsyncClient(message_delivery="callback")
    client.message_callback_add("sync/#", lambda _message: None)

    async def async_fallback(_message: Message) -> None:
        pass

    client.on_message = async_fallback
    assert client._message_callback is not None
    assert client._delivery._is_async_callback(client._message_callback)

    client.on_message = lambda _message: None
    assert client._message_callback is not None
    assert not client._delivery._is_async_callback(client._message_callback)


async def test_mixed_topic_route_is_one_worker_job_and_keeps_registration_order() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []

    def first(_message: Message) -> None:
        seen.append("first")
        assert client._delivery.try_enqueue_callback(lambda: seen.append("reentrant"))

    async def second(_message: Message) -> None:
        await asyncio.sleep(0)
        seen.append("second")

    client.message_callback_add("outer/#", first)
    client.message_callback_add("outer/+", second)
    callback = client._message_callback
    assert callback is not None
    assert client._delivery._is_async_callback(callback)

    client._delivery.spawn_callback(callback, Message(topic="outer/x", payload=b"x"))
    await client._callback_queue.join()

    assert seen == ["first", "second", "reentrant"]
    await client._shutdown_callback_worker(drain=False)


async def test_filter_mutation_during_dispatch_does_not_change_current_matches''',
        label="remove continuation contract tests",
    )
    write(topic_path, text)

    old_path = Path("tests/unit/test_strict_sync_inline_burst.py")
    if not old_path.exists():
        raise RuntimeError("expected old strict-sync test file")
    old_path.unlink()

    new_tests = '''from __future__ import annotations

import asyncio
from collections import deque

from mqttium.api import AsyncClient
from mqttium.protocol.effects import EffectKind, EngineEffect
from mqttium.types import Message


def _effect(i: int) -> EngineEffect:
    return EngineEffect(
        EffectKind.MESSAGE,
        Message(topic="pair/test", payload=str(i).encode()),
        requires_delivery_mark=False,
    )


async def test_idle_sync_pair_runs_inline_without_public_option() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []
    client.on_message = lambda message: seen.append(message.payload.decode())

    applied = client._apply_message_effect_batch_inline(
        deque([_effect(0), _effect(1)]), client._connection_epoch
    )

    assert applied == 2
    assert seen == ["0", "1"]
    assert client._callback_worker_task is None
    assert client.stats().delivery.callback_queued == 0


async def test_sync_pair_reserves_tail_and_keeps_reentrant_fifo() -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=2)
    seen: list[str] = []

    def callback(message: Message) -> None:
        value = message.payload.decode()
        seen.append(value)
        assert client.stats().delivery.callback_queued >= 1
        if value == "0":
            assert client._delivery.try_enqueue_callback(lambda: seen.append("reentrant"))
            assert client.stats().delivery.callback_queued == 2
            assert not client._delivery.try_enqueue_callback(lambda: seen.append("overflow"))

    client.on_message = callback
    assert (
        client._apply_message_effect_batch_inline(
            deque([_effect(0), _effect(1)]), client._connection_epoch
        )
        == 2
    )
    assert seen == ["0", "1"]
    assert client._callback_queue.maxsize == 2
    await client._callback_queue.join()
    assert seen == ["0", "1", "reentrant"]
    assert client.stats().delivery.callback_queued == 0
    await client._shutdown_callback_worker(drain=False)


async def test_sync_pair_keeps_captured_callback_for_tail() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []

    def replacement(message: Message) -> None:
        seen.append(f"new:{message.payload.decode()}")

    def original(message: Message) -> None:
        value = message.payload.decode()
        seen.append(f"old:{value}")
        if value == "0":
            client.on_message = replacement

    client.on_message = original
    assert (
        client._apply_message_effect_batch_inline(
            deque([_effect(0), _effect(1)]), client._connection_epoch
        )
        == 2
    )
    assert seen == ["old:0", "old:1"]


async def test_larger_sync_burst_keeps_worker_fairness_path() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []
    client.on_message = lambda message: seen.append(message.payload.decode())

    assert (
        client._apply_message_effect_batch_inline(
            deque([_effect(0), _effect(1), _effect(2)]), client._connection_epoch
        )
        == 3
    )
    assert seen == []
    await client._callback_queue.join()
    assert seen == ["0", "1", "2"]
    await client._shutdown_callback_worker(drain=False)


async def test_async_pair_keeps_worker_path() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []

    async def callback(message: Message) -> None:
        await asyncio.sleep(0)
        seen.append(message.payload.decode())

    client.on_message = callback
    assert (
        client._apply_message_effect_batch_inline(
            deque([_effect(0), _effect(1)]), client._connection_epoch
        )
        == 2
    )
    assert seen == []
    await client._callback_queue.join()
    assert seen == ["0", "1"]
    await client._shutdown_callback_worker(drain=False)


async def test_both_mode_pair_keeps_iterator_and_worker_path() -> None:
    client = AsyncClient(message_delivery="both", max_pending_messages=4)
    seen: list[str] = []
    client.on_message = lambda message: seen.append(message.payload.decode())

    assert (
        client._apply_message_effect_batch_inline(
            deque([_effect(0), _effect(1)]), client._connection_epoch
        )
        == 2
    )
    assert seen == []
    assert client._messages.qsize() == 2
    await client._callback_queue.join()
    assert seen == ["0", "1"]
    await client._shutdown_callback_worker(drain=False)


async def test_sync_pair_isolates_exception_and_continues() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []
    errors: list[BaseException] = []

    def callback(message: Message) -> None:
        value = message.payload.decode()
        seen.append(value)
        if value == "0":
            raise RuntimeError("boom")

    client.on_message = callback
    client._delivery.report_callback_error = (  # type: ignore[method-assign]
        lambda _callback, exc: errors.append(exc)
    )
    assert (
        client._apply_message_effect_batch_inline(
            deque([_effect(0), _effect(1)]), client._connection_epoch
        )
        == 2
    )
    assert seen == ["0", "1"]
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)


async def test_sync_returning_coroutine_is_rejected_inline_and_closed() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []
    errors: list[BaseException] = []

    async def continuation() -> None:
        seen.append("awaited")

    def callback(message: Message):  # type: ignore[no-untyped-def]
        seen.append(f"call:{message.payload.decode()}")
        if message.payload == b"0":
            return continuation()
        return None

    client.on_message = callback
    client._delivery.report_callback_error = (  # type: ignore[method-assign]
        lambda _callback, exc: errors.append(exc)
    )
    assert (
        client._apply_message_effect_batch_inline(
            deque([_effect(0), _effect(1)]), client._connection_epoch
        )
        == 2
    )
    await asyncio.sleep(0)

    assert seen == ["call:0", "call:1"]
    assert len(errors) == 1
    assert isinstance(errors[0], TypeError)
    assert "async def" in str(errors[0])
    assert client._callback_worker_task is None


async def test_sync_returning_coroutine_is_rejected_on_worker_too() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[str] = []
    errors: list[BaseException] = []

    async def continuation() -> None:
        seen.append("awaited")

    def callback():  # type: ignore[no-untyped-def]
        seen.append("called")
        return continuation()

    client._delivery.report_callback_error = (  # type: ignore[method-assign]
        lambda _callback, exc: errors.append(exc)
    )
    client._delivery.spawn_callback(callback)
    await client._callback_queue.join()
    await asyncio.sleep(0)

    assert seen == ["called"]
    assert len(errors) == 1
    assert isinstance(errors[0], TypeError)
    await client._shutdown_callback_worker(drain=False)


async def test_sync_returning_future_is_rejected_without_taking_ownership() -> None:
    client = AsyncClient(message_delivery="callback")
    errors: list[BaseException] = []
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    def callback(_message: Message):  # type: ignore[no-untyped-def]
        return future

    client.on_message = callback
    client._delivery.report_callback_error = (  # type: ignore[method-assign]
        lambda _callback, exc: errors.append(exc)
    )
    assert (
        client._apply_message_effect_batch_inline(
            deque([_effect(0), _effect(1)]), client._connection_epoch
        )
        == 2
    )

    assert len(errors) == 2
    assert all(isinstance(error, TypeError) for error in errors)
    assert not future.done()
    future.cancel()


async def test_real_task_cancellation_restores_pair_bound() -> None:
    client = AsyncClient(message_delivery="callback", max_pending_callbacks=2)
    seen: list[str] = []

    async def run_delivery() -> None:
        def callback(message: Message) -> None:
            value = message.payload.decode()
            seen.append(value)
            if value == "0":
                task = asyncio.current_task()
                assert task is not None
                task.cancel()
                raise asyncio.CancelledError

        client.on_message = callback
        client._apply_message_effect_batch_inline(
            deque([_effect(0), _effect(1)]), client._connection_epoch
        )

    task = asyncio.create_task(run_delivery())
    try:
        await task
    except asyncio.CancelledError:
        pass
    else:
        raise AssertionError("real task cancellation did not propagate")

    assert seen == ["0"]
    assert client.stats().delivery.callback_queued == 0
    assert client._callback_queue.maxsize == 2
    assert client._callback_worker_task is None
'''
    write("tests/unit/test_sync_callback_contract.py", new_tests)


def rewrite_docs() -> None:
    for path in (
        "CHANGELOG.md",
        "docs/api-stability.md",
        "docs/configuration-and-sizing.md",
        "docs/migration.md",
        "docs/reference/async-client.md",
    ):
        restore(path)

    path = "CHANGELOG.md"
    text = read(path)
    text = replace_once(
        text,
        "### Changed\n\n",
        "### Changed\n\n"
        "- Simplify callback scheduling around an explicit callable-form contract: `def`\n"
        "  callbacks are synchronous and `async def` callbacks are asynchronous. A\n"
        "  synchronous callback that returns an awaitable is now reported as a callback\n"
        "  `TypeError` instead of creating a hidden continuation. This removes the\n"
        "  continuation parking/resume state from callback delivery. Idle callback-only\n"
        "  pairs of small messages may now execute synchronously in one effect-drain turn;\n"
        "  larger bursts, async callbacks, queued/reentrant delivery, and iterator/both\n"
        "  delivery keep the bounded worker path. No new constructor setting is added.\n\n",
        label="changelog callback simplification",
    )
    write(path, text)

    path = "docs/api-stability.md"
    text = read(path)
    text = replace_once(
        text,
        "  `auth_handler`, and topic-filtered callbacks registered with\n"
        "  `message_callback_add`.\n\n",
        "  `auth_handler`, and topic-filtered callbacks registered with\n"
        "  `message_callback_add`.\n\n"
        "Callback form is part of that contract: declare synchronous callbacks with\n"
        "`def` and asynchronous callbacks with `async def`. A synchronous callable must\n"
        "not dynamically return an awaitable; MQTTium reports that as a callback\n"
        "`TypeError` instead of scheduling hidden continuation work.\n\n",
        label="api stability callback form",
    )
    write(path, text)

    path = "docs/migration.md"
    text = read(path)
    text = replace_once(
        text,
        "## Durable sessions\n",
        "## Callback callable form\n\n"
        "Use `def` for synchronous callbacks and `async def` for callbacks that await.\n"
        "A synchronous callback that returns a coroutine, `Future`, or other awaitable is\n"
        "no longer implicitly handed to the callback worker; it is reported as a callback\n"
        "`TypeError`. Convert such callbacks to `async def`. This removes hidden scheduling\n"
        "state and makes callback execution mode explicit from the callable itself.\n\n"
        "## Durable sessions\n",
        label="migration callback form",
    )
    write(path, text)

    path = "docs/reference/async-client.md"
    text = read(path)
    text = replace_once(
        text,
        "Callbacks execute outside protocol-engine critical sections. Synchronous\n"
        "`on_publish` and eligible `on_message` / topic-filtered callbacks may execute\n"
        "inline when callback delivery is idle; async, reentrant and queued callbacks\n"
        "use the bounded worker. Synchronous callbacks must not block the event loop.\n"
        "Callback failures go to the event loop's exception handler without silently\n"
        "changing protocol state.\n",
        "Callbacks execute outside protocol-engine critical sections. Declare synchronous\n"
        "callbacks with `def` and asynchronous callbacks with `async def`; a synchronous\n"
        "callable that returns an awaitable violates the callback contract and is reported\n"
        "as a callback `TypeError` rather than being scheduled implicitly. Synchronous\n"
        "callbacks must not block the event loop.\n\n"
        "Eligible idle `on_publish` and message callbacks may execute inline. For\n"
        "callback-only message delivery, an adjacent pair of small synchronous messages may\n"
        "run in the same effect-drain turn while retaining the hard\n"
        "`max_pending_callbacks` bound. Larger bursts, declared-async callbacks, and\n"
        "queued/reentrant delivery use the bounded worker. Callback failures go to the event\n"
        "loop's exception handler without silently changing protocol state.\n",
        label="reference callback contract",
    )
    write(path, text)


def main() -> None:
    simplify_delivery()
    simplify_async_client()
    rewrite_tests()
    rewrite_docs()


if __name__ == "__main__":
    main()
