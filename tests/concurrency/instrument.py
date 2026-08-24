"""Test-only monkeypatches that insert named checkpoints into mqttium runtimes."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from mqttium.api._delivery import ApplicationDelivery
from mqttium.api._effects import EffectPump
from mqttium.api._writer import WritePump
from mqttium.api.async_client import AsyncClient

from tests.concurrency.scheduler import CooperativeScheduler

CHECKPOINT_CATALOG = (
    "publish.enter",
    "publish.leave",
    "publish.wait_space",
    "writer.enqueue.before",
    "writer.enqueue.after",
    "writer.enqueue.wait",
    "writer.batch.extract",
    "transport.write.before",
    "transport.write.after",
    "transport.close.before",
    "transport.close.after",
    "effect.drain.before",
    "effect.drain.after",
    "effect.apply.before",
    "effect.apply.after",
    "delivery.put.before",
    "delivery.put.after",
    "delivery.callback.before",
    "delivery.callback.after",
    "epoch.invalidate.before",
    "epoch.invalidate.after",
    "reconnect.loop.before",
    "client.disconnect.enter",
    "client.disconnect.leave",
)

RestoreOp = tuple[Any, str, Any]


class RuntimeHooks:
    """Wrap selected AsyncClient/pump methods without touching production code.

    Install after the client exists. Re-install transport wrappers after every
    connect: `WritePump.reset()` replaces the queue, and reconnect builds a new
    transport.
    """

    def __init__(self, scheduler: CooperativeScheduler) -> None:
        self.scheduler = scheduler
        self._restore: list[RestoreOp] = []
        self._wrapped: set[tuple[int, str]] = set()

    def install(self, client: AsyncClient) -> None:
        self.wrap_async(client, "publish", "publish.enter", "publish.leave")
        self.wrap_async(
            client, "_wait_publish_space", "publish.wait_space", after=None, before_only=True
        )
        self.wrap_async(client, "disconnect", "client.disconnect.enter", "client.disconnect.leave")
        self.wrap_async(
            client,
            "_invalidate_connection_epoch",
            "epoch.invalidate.before",
            "epoch.invalidate.after",
        )
        self.wrap_async(
            client, "_reconnect_loop", "reconnect.loop.before", after=None, before_only=True
        )
        self.wrap_async(client, "_apply_effect", "effect.apply.before", "effect.apply.after")
        self._install_writer(client._write_pump)
        self._install_effects(client._effect_pump)
        self._install_delivery(client._delivery)
        if client._transport is not None:
            self.wrap_transport(client._transport)

    def _install_writer(self, pump: WritePump) -> None:
        self.wrap_async(pump, "enqueue", "writer.enqueue.before", "writer.enqueue.after")
        original_wait = pump.space.wait

        async def wait_with_checkpoint(*args: Any, **kwargs: Any) -> Any:
            await self.scheduler.checkpoint("writer.enqueue.wait")
            return await original_wait(*args, **kwargs)

        pump.space.wait = wait_with_checkpoint  # type: ignore[method-assign]
        self._restore.append((pump.space, "wait", original_wait))
        original_start = pump.start

        def start_and_instrument(transport: Any) -> None:
            original_start(transport)
            self._instrument_writer_queue(pump)
            self.wrap_transport(transport)

        pump.start = start_and_instrument  # type: ignore[method-assign]
        self._restore.append((pump, "start", original_start))
        if pump.task is not None and not pump.task.done():
            self._instrument_writer_queue(pump)
            if pump.transport is not None:
                self.wrap_transport(pump.transport)

    def _instrument_writer_queue(self, pump: WritePump) -> None:
        original_get = pump.queue.get

        async def get_with_checkpoint(*args: Any, **kwargs: Any) -> Any:
            item = await original_get(*args, **kwargs)
            await self.scheduler.checkpoint("writer.batch.extract")
            return item

        pump.queue.get = get_with_checkpoint  # type: ignore[method-assign]
        self._restore.append((pump.queue, "get", original_get))

    def _install_effects(self, pump: EffectPump) -> None:
        self.wrap_async(pump, "drain", "effect.drain.before", "effect.drain.after")
        original_collect = pump.collect_from_engine

        def collect_and_trace() -> None:
            self.scheduler.trace("effect.collect")
            original_collect()

        pump.collect_from_engine = collect_and_trace  # type: ignore[method-assign]
        self._restore.append((pump, "collect_from_engine", original_collect))

    def _install_delivery(self, delivery: ApplicationDelivery) -> None:
        self.wrap_async(delivery, "put_message", "delivery.put.before", "delivery.put.after")
        original_invoke = delivery.invoke

        async def invoke_with_checkpoint(callback: Callable[..., Any], *args: Any) -> Any:
            await self.scheduler.checkpoint("delivery.callback.before")
            try:
                return await original_invoke(callback, *args)
            finally:
                await self.scheduler.checkpoint("delivery.callback.after")

        delivery.invoke = invoke_with_checkpoint  # type: ignore[method-assign]
        self._restore.append((delivery, "invoke", original_invoke))

    def wrap_transport(self, transport: Any) -> None:
        self.wrap_async(transport, "write", "transport.write.before", "transport.write.after")
        if hasattr(transport, "write_many"):
            self.wrap_async(
                transport,
                "write_many",
                "transport.write.before",
                "transport.write.after",
            )
        self.wrap_async(transport, "close", "transport.close.before", "transport.close.after")

    def wrap_async(
        self,
        obj: Any,
        attr: str,
        before: str,
        after: str | None,
        *,
        before_only: bool = False,
    ) -> None:
        key = (id(obj), attr)
        if key in self._wrapped:
            return
        original = getattr(obj, attr)

        async def wrapped(*args: Any, **kwargs: Any) -> Any:
            await self.scheduler.checkpoint(before)
            try:
                return await original(*args, **kwargs)
            finally:
                if after is not None and not before_only:
                    await self.scheduler.checkpoint(after)

        setattr(obj, attr, wrapped)
        self._wrapped.add(key)
        self._restore.append((obj, attr, original))

    def uninstall(self) -> None:
        for obj, attr, original in reversed(self._restore):
            try:
                setattr(obj, attr, original)
            except Exception:
                pass
        self._restore.clear()
        self._wrapped.clear()


def factory_wrapping(
    factory: Callable[..., Awaitable[Any]], hooks: RuntimeHooks
) -> Callable[..., Awaitable[Any]]:
    """Wrap a transport factory so reconnects keep write/close checkpoints."""

    async def wrapped(host: str, port: int, *, ssl: object = None) -> Any:
        transport = await factory(host, port, ssl=ssl)
        hooks.wrap_transport(transport)
        return transport

    return wrapped
