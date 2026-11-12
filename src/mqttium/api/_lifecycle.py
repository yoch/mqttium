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
        # Whether hook_task delivers on_connect (True) or on_disconnect.
        self._hook_connected = False
        self.pending: _Notification | None = None
        # Kind of the last notification taken for delivery: True after
        # on_connect, False after on_disconnect. What the application was last
        # told is this, or the pending notification when there is one.
        self._delivered_connected = False
        # The cause of an on_disconnect a connection replacement superseded,
        # handed to settle() if that replacement ends without a connection.
        self._superseded_cause: BaseException | None = None
        # The cause most recently reported to on_disconnect, for stats().
        self.last_disconnect_error: BaseException | None = None
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
        pending = self.pending
        if replace_connection:
            if pending is not None and not pending.connected and pending.token == self.token:
                assert not isinstance(pending.value, ConnAckPacket)
                self._superseded_cause = pending.value
            self.token += 1
            self.pending = None
        elif pending is not None and pending.connected:
            # disconnect() supersedes an on_connect not yet started, never the
            # report of a loss already recorded: that one is the latest state.
            self.pending = None
        self._reconnect_ready.set()
        # A running on_disconnect still reports the latest state: only a
        # connection replacement makes it obsolete, not a disconnect().
        if not preserve_hook and (replace_connection or self._hook_connected):
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
        self._superseded_cause = None
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
        self.last_disconnect_error = error
        self._superseded_cause = None
        self._cancel_obsolete(origin)
        self._reconnect_ready.clear()
        self.pending = _Notification(token, False, self.owner.on_disconnect, error)
        self._ensure_worker()

    @property
    def announced_connected(self) -> bool:
        pending = self.pending
        if pending is not None and pending.token == self.token:
            return pending.connected
        return self._delivered_connected

    def settle(self, error: BaseException | None) -> None:
        """Report the loss of a connection the application was told about.

        A lifecycle operation that superseded the connection's own
        notification, then ended without a connection, must not leave
        ``on_connect`` as the last word.
        """
        if self.announced_connected:
            superseded = self._superseded_cause
            self.disconnected(superseded if superseded is not None else error, self.token)

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
                self._delivered_connected = event.connected
                if event.callback is not None:
                    task = asyncio.create_task(self._invoke(event), name="mqttium-lifecycle-hook")
                    self.hook_task = task
                    self._hook_connected = event.connected
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
