"""Bounded, connection-prioritized lifecycle notification ownership."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from mqttium.api._cancel import owner_cancelled
from mqttium.api._delivery import ApplicationDelivery
from mqttium.packets import ConnAckPacket


class LifecycleOwner(Protocol):
    _lifecycle_lock: asyncio.Lock
    on_connect: Callable[[ConnAckPacket], Any] | None
    on_disconnect: Callable[[BaseException | None], Any] | None


@dataclass(frozen=True, slots=True)
class _Notification:
    token: int
    connected: bool
    callback: Callable[..., Any] | None
    value: ConnAckPacket | BaseException | None


class LifecycleHooks:
    """Own one hook and replace obsolete pending state without blocking MQTT.

    API operations initiated directly by the running hook preserve that hook.
    External replacement cancels it, but transport work never joins it. The
    supervisor reaps the old child before starting its successor, keeping both
    task and notification counts bounded even during rapid transitions.
    """

    def __init__(self, owner: LifecycleOwner) -> None:
        self.owner = owner
        self.token = 0
        self.task: asyncio.Task[None] | None = None
        self.hook_task: asyncio.Task[None] | None = None
        self.pending: _Notification | None = None
        self._holds = 0
        self._released = asyncio.Event()
        self._released.set()
        self._reconnect_ready = asyncio.Event()
        self._reconnect_ready.set()

    def begin_operation(
        self, *, replace_connection: bool = True, preserve_hook: bool = False
    ) -> asyncio.Task[None] | None:
        """Supersede old notifications and return the hook owner to preserve.

        Automatic reconnect preserves a running hook (#508): it may await work
        only the replacement connection completes, and a later on_connect
        still waits behind it in the worker.
        """
        current = asyncio.current_task()
        direct_origin = self.hook_task if self.hook_task is current else None
        origin = self.hook_task if preserve_hook else direct_origin
        if replace_connection:
            self.token += 1
        self.pending = None
        self._reconnect_ready.set()
        if not preserve_hook:
            self._cancel_obsolete(direct_origin)
        return origin

    def _cancel_obsolete(self, origin: asyncio.Task[None] | None) -> None:
        task = self.hook_task
        if (
            task is not None
            and task is not origin
            and not task.done()
            and not owner_cancelled(task)
        ):
            task.cancel()

    def hold(self) -> None:
        """Prevent notification starts while a transport owner finishes cleanup."""
        self._holds += 1
        self._released.clear()

    def release(self) -> None:
        self._holds -= 1
        assert self._holds >= 0
        if not self._holds:
            self._released.set()

    def connected(self, packet: ConnAckPacket, token: int) -> None:
        if token != self.token or packet.reason_code != 0:
            return
        self._reconnect_ready.set()
        self.pending = _Notification(token, True, self.owner.on_connect, packet)
        self._ensure_worker()

    def disconnected(
        self,
        error: BaseException | None,
        token: int,
        origin: asyncio.Task[None] | None = None,
    ) -> None:
        if token != self.token:
            return
        self._cancel_obsolete(origin)
        self._reconnect_ready.clear()
        self.pending = _Notification(token, False, self.owner.on_disconnect, error)
        self._ensure_worker()

    def retiring(self, token: int, origin: asyncio.Task[None] | None = None) -> None:
        """Close retry admission before resource cleanup can suspend."""
        if token == self.token:
            self.pending = None
            self._reconnect_ready.clear()
            self._cancel_obsolete(origin)

    async def wait_reconnect(self) -> None:
        await self._reconnect_ready.wait()

    def _ensure_worker(self) -> None:
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self._run(), name="mqttium-lifecycle")

    async def _invoke(self, event: _Notification) -> None:
        # Retain ownership before an eager task factory may invoke user code.
        await asyncio.sleep(0)
        await self._released.wait()
        async with self.owner._lifecycle_lock:
            pass
        if event.token != self.token:
            return
        if not event.connected:
            # The disconnect hook is now the lifecycle owner, no longer a
            # pending state a retry could overwrite: automatic reconnect may
            # proceed while it runs (#508).
            self._reconnect_ready.set()
        try:
            await ApplicationDelivery.invoke(event.callback, event.value)
        except asyncio.CancelledError as exc:
            if owner_cancelled():
                raise
            ApplicationDelivery.report_callback_error(event.callback, exc)
        except Exception as exc:
            ApplicationDelivery.report_callback_error(event.callback, exc)

    async def _run(self) -> None:
        await asyncio.sleep(0)
        try:
            while self.pending is not None:
                await self._released.wait()
                # Setup can resolve CONNACK before its caller releases the lock.
                # Only the barrier is held here, never the user invocation.
                async with self.owner._lifecycle_lock:
                    pass
                if self._holds:
                    continue
                event = self.pending
                if event is None:
                    continue
                self.pending = None
                if event.token != self.token:
                    continue
                if event.callback is not None:
                    task = asyncio.create_task(self._invoke(event), name="mqttium-lifecycle-hook")
                    self.hook_task = task
                    try:
                        await task
                    except asyncio.CancelledError:
                        # A hook that ends by raising CancelledError on its own
                        # (or is superseded) must not stop the lifecycle worker.
                        if owner_cancelled():
                            raise
                    finally:
                        self.hook_task = None
                if not event.connected and event.token == self.token and self.pending is None:
                    self._reconnect_ready.set()
        finally:
            self.task = None
