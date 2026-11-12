"""Enhanced-authentication handler ownership.

The ``auth_handler`` is user code and may await the client itself: publish,
subscribe, disconnect, or a message or receipt the same read delivered. It
therefore never runs inside the effect pump or the reader. Each Server AUTH
is handed to one task owned by this class, in arrival order. A handler that
is tearing its own connection down is detached: it finishes its call, and a
later connection's challenges go to a new task. The handler's answer goes back through ``ProtocolEngine.respond_auth()``, which drops an
answer the exchange no longer waits for (#501 #502 #523 #527 #528 #535).
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable, Collection
from typing import TYPE_CHECKING, Any, Protocol

from mqttium.api._cancel import dependency_failure, owner_cancelled
from mqttium.errors import MQTTError, MQTTTimeoutError
from mqttium.packets import AuthPacket

if TYPE_CHECKING:
    from mqttium.api._effects import EffectPump
    from mqttium.protocol.engine import ProtocolEngine


class AuthOwner(Protocol):
    _connection_epoch: int
    _engine: ProtocolEngine
    _engine_lock: asyncio.Lock
    _effect_pump: EffectPump
    _auth_timeout: float

    @property
    def auth_handler(self) -> Callable[[AuthPacket], Any] | None: ...

    def _invoke_auth_handler(
        self, handler: Callable[[AuthPacket], Any], packet: AuthPacket
    ) -> Awaitable[Any]: ...

    async def _auth_failed(self, exc: BaseException) -> None: ...


class AuthExchange:
    """Run auth_handler calls in order, outside every protocol lane."""

    def __init__(self, owner: AuthOwner) -> None:
        self.owner = owner
        self.pending: deque[tuple[AuthPacket, object, int]] = deque()
        self.task: asyncio.Task[None] | None = None
        # Handlers finishing a call for a connection that is gone. Kept only
        # so that each stays referenced and visible until it ends.
        self.detached: set[asyncio.Task[None]] = set()

    def hand_off(self, packet: AuthPacket, challenge: object, epoch: int) -> None:
        self.pending.append((packet, challenge, epoch))
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self._run(), name="mqttium-auth")

    def retire(self, spare: Collection[asyncio.Task[Any]] = ()) -> None:
        """Forget the connection's challenges and stop a running handler.

        Never joined: the handler may be the caller (disconnect() from the
        handler) or be waiting for work only this teardown completes. A
        handler that is running the teardown itself is not cancelled: it is
        the current task when it retires directly, and one of the ``spare``
        tasks when the reader or a connecting task retires on its behalf. It
        is detached instead, and may still be running when a later
        connection's challenge starts a new handler call.
        """
        self.pending.clear()
        task = self.task
        if task is None or task.done():
            return
        if task is asyncio.current_task() or task in spare:
            # It finishes its own call, but no longer serves the exchange: a
            # later connection's challenge must not queue behind a handler
            # that is waiting for that very connection to be torn down.
            self.task = None
            self.detached.add(task)
            task.add_done_callback(self.detached.discard)
        else:
            task.cancel()

    @property
    def running(self) -> bool:
        task = self.task
        return (task is not None and not task.done()) or bool(self.detached)

    async def _run(self) -> None:
        owner = self.owner
        # hand_off() runs during effect collection; an eager task factory
        # would otherwise start the handler inside that critical section.
        await asyncio.sleep(0)
        this_task = asyncio.current_task()
        while self.pending and self.task is this_task:
            packet, challenge, epoch = self.pending.popleft()
            if epoch != owner._connection_epoch:
                continue
            failure = await self._answer(packet, challenge, epoch)
            if failure is not None and epoch == owner._connection_epoch:
                # No await since the epoch check: the failure is this connection's.
                self.pending.clear()
                await owner._auth_failed(failure)
                return

    async def _answer(
        self, packet: AuthPacket, challenge: object, epoch: int
    ) -> BaseException | None:
        owner = self.owner
        handler = owner.auth_handler
        if handler is None:
            return MQTTError("AUTH handler is no longer available")
        try:
            async with asyncio.timeout(owner._auth_timeout):
                response = await owner._invoke_auth_handler(handler, packet)
        except TimeoutError:
            return MQTTTimeoutError("AUTH handler timed out")
        except asyncio.CancelledError as exc:
            if owner_cancelled():
                raise
            return dependency_failure(exc, "AUTH handler", "AUTH handler cancelled")
        except Exception as exc:
            return exc
        if not isinstance(response, AuthPacket) or epoch != owner._connection_epoch:
            return None
        try:
            async with owner._engine_lock:
                owner._engine.respond_auth(
                    challenge,
                    reason_code=response.reason_code,
                    properties=response.properties,
                )
                owner._effect_pump.collect_from_engine()
        except Exception as exc:
            return exc
        owner._effect_pump.drain_inline()
        return None
