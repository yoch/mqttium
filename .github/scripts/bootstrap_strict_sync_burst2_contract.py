from __future__ import annotations

from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    target = Path(path)
    text = target.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one anchor, found {count}")
    target.write_text(text.replace(old, new, 1), encoding="utf-8")


replace_once(
    "CHANGELOG.md",
    "## [Unreleased]\n\n### Changed\n",
    """## [Unreleased]\n\n### Added\n\n- Add the Stable `inline_callback_burst` `AsyncClient` setting. The default\n  `1` keeps the existing singleton-inline / burst-worker scheduling policy.\n  Setting `2` opts callback-only delivery into running exactly two adjacent\n  small, idle, strictly synchronous message callbacks in the reader/effect\n  turn while retaining the hard `max_pending_callbacks` bound. Declared async\n  callbacks, iterator/both delivery and larger bursts keep the bounded worker\n  path. In the opt-in mode a nominally synchronous callback that returns an\n  awaitable is reported as a `TypeError` instead of being scheduled.\n\n### Changed\n""",
)

replace_once(
    "docs/api-stability.md",
    """Constructor keyword arguments are part of the native contract. New optional\nkeywords may be added compatibly. Existing Stable defaults will not change\nwithout the SemVer and deprecation process below.\n""",
    """Constructor keyword arguments are part of the native contract. New optional\nkeywords may be added compatibly. Existing Stable defaults will not change\nwithout the SemVer and deprecation process below.\n\n`inline_callback_burst` is a Stable opt-in scheduling control. Its default of\n`1` preserves the existing callback-burst worker policy. Setting it to `2` may\nrun exactly two adjacent small message callbacks in the reader/effect-drain\nturn when delivery is callback-only, idle, and the callback is declared\nsynchronous. Those callbacks must therefore be short and non-blocking and must\nnot return awaitables; violating the latter rule is reported as a callback\n`TypeError`. Declared async callbacks, iterator/both delivery, larger bursts,\nand non-idle delivery keep the bounded worker path. The hard\n`max_pending_callbacks` admission bound remains in force.\n""",
)

replace_once(
    "docs/reference/async-client.md",
    """Callbacks execute outside protocol-engine critical sections. Synchronous\n`on_publish` and eligible `on_message` / topic-filtered callbacks may execute\ninline when callback delivery is idle; async, reentrant and queued callbacks\nuse the bounded worker. Synchronous callbacks must not block the event loop.\nCallback failures go to the event loop's exception handler without silently\nchanging protocol state.\n""",
    """Callbacks execute outside protocol-engine critical sections. Synchronous\n`on_publish` and eligible `on_message` / topic-filtered callbacks may execute\ninline when callback delivery is idle; async, reentrant and queued callbacks\nuse the bounded worker. Synchronous callbacks must not block the event loop.\nCallback failures go to the event loop's exception handler without silently\nchanging protocol state.\n\n### Optional two-message synchronous burst\n\n`inline_callback_burst=1` is the Stable default and preserves the existing\npolicy: an isolated eligible message callback may run inline, while a callback\nburst is handed to the bounded worker. `inline_callback_burst=2` is an explicit\nlatency/throughput opt-in for callback-only consumers whose message callback is\nstrictly synchronous and short. When the reader has exactly two adjacent small\nmessage effects and callback delivery is idle, both callbacks run in the same\nreader/effect-drain turn before it yields.\n\nThe opt-in does not apply to declared `async def` callbacks, iterator or `both`\ndelivery, larger bursts, or an already active/queued callback path. It does not\nrelax `max_pending_callbacks`: the second callback consumes the same logical\nreservation that the worker batch would have consumed, so reentrant callback\nwork queues behind the two-message burst.\n\nA callable that is declared synchronous but returns an awaitable remains\nsupported by the default `inline_callback_burst=1` path. Under the explicit\n`inline_callback_burst=2` contract, however, that return value is invalid:\nMQTTium reports a callback `TypeError`; a coroutine result is closed rather than\nscheduled. Use the default or declare the callback `async def` when it needs to\nawait. Because the opt-in executes two user calls in the reader turn, do not use\nit for blocking I/O, long CPU work, or callbacks with unbounded service time.\n""",
)

replace_once(
    "docs/configuration-and-sizing.md",
    """| `max_pending_callbacks` | `1_024` | Callback queue count bound |\n| `max_pending_delivery_bytes` | `64 MiB` | Payload bytes retained for application delivery |\n""",
    """| `max_pending_callbacks` | `1_024` | Callback queue count bound |\n| `inline_callback_burst` | `1` | Keep callback bursts on the worker; `2` opts an eligible two-message sync burst into the reader turn |\n| `max_pending_delivery_bytes` | `64 MiB` | Payload bytes retained for application delivery |\n""",
)
replace_once(
    "docs/configuration-and-sizing.md",
    """| `callback_shutdown_timeout` | `5.0` | Callback drain allowance during shutdown |\n\n### Connection and authentication\n""",
    """| `callback_shutdown_timeout` | `5.0` | Callback drain allowance during shutdown |\n\nLeave `inline_callback_burst=1` unless callback-only delivery has a measured\ntwo-message burst bottleneck and the message callback is guaranteed to be\nstrictly synchronous, short, and non-blocking. With `2`, exactly two adjacent\nsmall callbacks may run before the reader/effect-drain turn yields. Declared\nasync callbacks, iterator/both delivery, larger bursts, and busy callback paths\nstill use the bounded worker. A sync callable that returns an awaitable violates\nthe opt-in contract and is reported as a callback `TypeError`. The setting does\nnot increase `max_pending_callbacks`.\n\n### Connection and authentication\n""",
)

replace_once(
    "docs/migration.md",
    """The writer has its own byte and message limits. Applications sending large\npayloads should size the byte budget explicitly rather than relying only on a\nmessage count.\n\n## Durable sessions\n""",
    """The writer has its own byte and message limits. Applications sending large\npayloads should size the byte budget explicitly rather than relying only on a\nmessage count.\n\n## Optional two-callback inline burst\n\nExisting applications do not need to change anything: `inline_callback_burst=1`\nkeeps the established callback scheduling behavior. A callback-only service can\nopt into the measured two-message fast path when its message handler is short,\nnon-blocking, and strictly synchronous:\n\n```python\nclient = AsyncClient(\n    \"client-id\",\n    message_delivery=\"callback\",\n    inline_callback_burst=2,\n)\n```\n\nThe opt-in may run exactly two adjacent small message callbacks in the\nreader/effect-drain turn before yielding. Declared `async def` callbacks,\niterator/both delivery, larger bursts, and busy callback paths keep the bounded\nworker. A nominally synchronous callable that returns an awaitable is valid in\nthe default mode but is a contract violation with `inline_callback_burst=2`;\nMQTTium reports a callback `TypeError` and does not schedule that awaitable.\nKeep the default when callback service time is not tightly bounded.\n\n## Durable sessions\n""",
)

test_path = Path("tests/unit/test_strict_sync_inline_burst.py")
test_text = test_path.read_text(encoding="utf-8")
marker = "async def test_opt_in_batch2_isolates_sync_exception_and_continues()"
if marker in test_text:
    raise SystemExit("strict sync error tests already present")

test_text += r'''


async def test_opt_in_batch2_isolates_sync_exception_and_continues() -> None:
    client = AsyncClient(message_delivery="callback", inline_callback_burst=2)
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
    assert client.stats().delivery.callback_queued == 0


async def test_opt_in_batch2_reports_callback_self_cancellation_and_continues() -> None:
    client = AsyncClient(message_delivery="callback", inline_callback_burst=2)
    seen: list[str] = []
    errors: list[BaseException] = []

    def callback(message: Message) -> None:
        value = message.payload.decode()
        seen.append(value)
        if value == "0":
            raise asyncio.CancelledError("self-cancel")

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
    assert isinstance(errors[0], asyncio.CancelledError)
    assert client.stats().delivery.callback_queued == 0


async def test_opt_in_batch2_rejects_non_coroutine_awaitable_without_owning_it() -> None:
    client = AsyncClient(message_delivery="callback", inline_callback_burst=2)
    seen: list[str] = []
    errors: list[BaseException] = []
    future: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    def callback(message: Message):  # type: ignore[no-untyped-def]
        seen.append(message.payload.decode())
        if message.payload == b"0":
            return future
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
    assert seen == ["0", "1"]
    assert len(errors) == 1
    assert isinstance(errors[0], TypeError)
    assert not future.done()
    future.cancel()


async def test_opt_in_batch2_propagates_real_task_cancellation_and_restores_bound() -> None:
    client = AsyncClient(
        message_delivery="callback",
        max_pending_callbacks=2,
        inline_callback_burst=2,
    )
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
    with pytest.raises(asyncio.CancelledError):
        await task

    assert seen == ["0"]
    assert client.stats().delivery.callback_queued == 0
    assert client._callback_queue.maxsize == 2
    assert client._callback_worker_task is None
'''

test_path.write_text(test_text, encoding="utf-8")
