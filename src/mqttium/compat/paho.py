"""Paho-compatible sync façade over AsyncClient.

Only ``CallbackAPIVersion.VERSION2`` is supported. See ``docs/COMPAT.md`` for
the supported surface, intentional divergences, and rejected features.
"""

from __future__ import annotations

import asyncio
import enum
import threading
from collections import deque
from contextlib import suppress
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from mqttium.api.async_client import AsyncClient
from mqttium.api.models import PublishReceipt
from mqttium.dispatch.matcher import TopicMatcher
from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.packets import ConnAckPacket
from mqttium.types import Message, Properties


class CallbackAPIVersion(enum.Enum):
    VERSION2 = 2


@dataclass(frozen=True, slots=True)
class DisconnectFlags:
    """VERSION2 disconnect flags (Paho-compatible subset)."""

    is_disconnect_packet_from_server: bool = False


@dataclass
class MQTTMessageInfo:
    """Paho-like publish handle."""

    mid: int | None
    _receipt: PublishReceipt | None = field(default=None, repr=False)
    _loop: asyncio.AbstractEventLoop | None = field(default=None, repr=False)
    rc: int = 0

    def wait_for_publish(self, timeout: float | None = None) -> None:
        """Block until the publish completes; raise on protocol/transport error."""
        if self._receipt is None or self._receipt.is_done():
            if self._receipt is not None and self._receipt._error is not None:
                raise self._receipt._error
            return
        if self._loop is None:
            raise RuntimeError("No event loop for wait_for_publish")
        fut = asyncio.run_coroutine_threadsafe(self._receipt.wait(), self._loop)
        fut.result(timeout)

    def is_published(self) -> bool:
        return self._receipt is None or self._receipt.is_done()


class MQTTMessage:
    """Paho-like inbound message (``topic`` is a ``str``, like Paho)."""

    __slots__ = ("_topic", "payload", "qos", "retain", "mid", "dup", "properties")

    def __init__(self, msg: Message) -> None:
        self._topic = msg.topic.encode("utf-8")
        self.payload = msg.payload
        self.qos = int(msg.qos)
        self.retain = msg.retain
        self.mid = msg.mid or 0
        self.dup = msg.dup
        self.properties = msg.properties

    @property
    def topic(self) -> str:
        return self._topic.decode("utf-8")


class Client:
    """Sync Paho-shaped wrapper around ``AsyncClient`` (VERSION2 only)."""

    def __init__(
        self,
        callback_api_version: CallbackAPIVersion = CallbackAPIVersion.VERSION2,
        client_id: str = "",
        *,
        userdata: Any = None,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
        clean_session: bool | None = None,
        clean_start: bool = True,
    ) -> None:
        if callback_api_version is not CallbackAPIVersion.VERSION2:
            raise ValueError("mqttium.compat.paho only supports CallbackAPIVersion.VERSION2")
        if clean_session is not None:
            clean_start = bool(clean_session)
        self._userdata = userdata
        self._async = AsyncClient(
            client_id=client_id,
            protocol=protocol,
            clean_start=clean_start,
        )
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._started = threading.Event()
        self._topic_callbacks = TopicMatcher()
        self._in_callback = False
        self._qos0_pending: deque[tuple[str, bytes, bool]] = deque()
        self._qos0_lock = threading.Lock()
        self._qos0_drain_scheduled = False

        self.on_connect: Callable[..., Any] | None = None
        self.on_disconnect: Callable[..., Any] | None = None
        self.on_message: Callable[..., Any] | None = None
        self.on_publish: Callable[..., Any] | None = None

        self._async.on_connect = self._dispatch_connect
        self._async.on_disconnect = self._dispatch_disconnect
        self._async.on_message = self._dispatch_message
        self._async.on_publish = self._dispatch_publish

    def user_data_set(self, userdata: Any) -> None:
        self._run_loop_mutation(lambda: setattr(self, "_userdata", userdata))

    def username_pw_set(self, username: str, password: bytes | str | None = None) -> None:
        pwd = password.encode("utf-8") if isinstance(password, str) else password

        def _set_credentials() -> None:
            self._async._engine.config.username = username
            self._async._engine.config.password = pwd

        self._run_loop_mutation(_set_credentials)

    def will_set(
        self,
        topic: str,
        payload: bytes | str = b"",
        qos: int = 0,
        retain: bool = False,
    ) -> None:
        data = payload.encode("utf-8") if isinstance(payload, str) else payload
        message = Message(
            topic=topic,
            payload=data,
            qos=QoS(qos),
            retain=retain,
        )
        self._run_loop_mutation(lambda: setattr(self._async._engine.config, "will", message))

    def message_callback_add(self, sub: str, callback: Callable[..., Any]) -> None:
        self._run_loop_mutation(lambda: self._topic_callbacks.__setitem__(sub, callback))

    def message_callback_remove(self, sub: str) -> None:
        def _remove() -> None:
            with suppress(KeyError):
                del self._topic_callbacks[sub]

        self._run_loop_mutation(_remove)

    @property
    def is_connected(self) -> bool:
        return bool(self._run_loop_mutation(lambda: self._async.is_connected))

    def loop_start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._started.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name="mqttium-paho-loop", daemon=True
        )
        self._thread.start()
        if not self._started.wait(timeout=5.0):
            raise RuntimeError("Failed to start background loop")

    def loop_stop(self) -> None:
        loop = self._loop
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            self._thread = None
        self._loop = None

    def _run_loop(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._started.set()
        try:
            loop.run_forever()
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()

    def _submit(
        self,
        coro: Any,
        timeout: float | None = 30.0,
        *,
        wait: bool = True,
    ) -> Any:
        if wait and self._on_loop_in_callback():
            raise RuntimeError(
                "Do not call blocking Client methods from a callback on the "
                "network thread (would deadlock). Schedule work on another thread "
                "or use mqttium.api.AsyncClient."
            )
        if self._loop is None:
            self.loop_start()
        assert self._loop is not None
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        if not wait:
            return fut
        return fut.result(timeout)

    def _run_loop_mutation(self, mutation: Callable[[], Any]) -> Any:
        """Run a short synchronous mutation on the network loop when active."""
        loop = self._loop
        thread = self._thread
        if loop is None or not loop.is_running() or thread is None:
            return mutation()
        if threading.current_thread() is thread:
            return mutation()

        handoff: dict[str, Any] = {}
        done = threading.Event()

        def _run() -> None:
            try:
                handoff["result"] = mutation()
            except BaseException as exc:
                handoff["error"] = exc
            finally:
                done.set()

        loop.call_soon_threadsafe(_run)
        if not done.wait(timeout=5.0):
            raise RuntimeError("mutation handoff to event loop timed out")
        error = handoff.get("error")
        if error is not None:
            raise error
        return handoff.get("result")

    def _queue_loop_command(self, command: Callable[[], Any]) -> Any:
        """Run an engine command on the dedicated loop and flush its effects.

        Callback code already executes on that loop, so it may run the short
        synchronous engine command directly. Calls from other threads use a
        bounded handoff and never mutate AsyncClient state off-loop.
        """
        if self._loop is None:
            self.loop_start()
        assert self._loop is not None

        if self._on_loop_in_callback():
            with self._async._state_mutex:
                result = command()
            self._async._schedule_effect_flush()
            return result

        handoff: dict[str, Any] = {}
        done = threading.Event()

        async def _run() -> None:
            try:
                async with self._async._engine_lock:
                    with self._async._state_mutex:
                        handoff["result"] = command()
                        self._async._collect_effects_locked()
                await self._async._drain_effects()
            except BaseException as exc:  # propagate the real loop-side failure
                handoff["error"] = exc
            finally:
                done.set()

        fut = asyncio.run_coroutine_threadsafe(_run(), self._loop)
        if not done.wait(timeout=5.0):
            fut.cancel()
            raise RuntimeError("command handoff to event loop timed out")
        error = handoff.get("error")
        if error is not None:
            raise error
        return handoff["result"]

    def connect(self, host: str, port: int = 1883, keepalive: int = 60) -> int:
        self._run_loop_mutation(lambda: setattr(self._async._engine.config, "keepalive", keepalive))
        self._submit(self._async.connect(host, port))
        return 0

    def reconnect(self) -> int:
        host, port, keepalive = self._run_loop_mutation(
            lambda: (
                self._async._host,
                self._async._port,
                self._async._engine.config.keepalive,
            )
        )
        if not host:
            raise RuntimeError("reconnect() called before connect()")
        return self.connect(host, port, keepalive=keepalive)

    def disconnect(self) -> int:
        try:
            self._submit(self._async.disconnect(), timeout=10.0)
        except Exception:
            return 1
        return 0

    def _enqueue_qos0_publish(
        self,
        topic: str,
        payload: bytes,
        retain: bool,
    ) -> None:
        """Queue one QoS 0 request without handing control to the loop per call."""
        assert self._loop is not None
        schedule = False
        with self._qos0_lock:
            self._qos0_pending.append((topic, payload, retain))
            if not self._qos0_drain_scheduled:
                self._qos0_drain_scheduled = True
                schedule = True
        if schedule:
            self._loop.call_soon_threadsafe(self._drain_qos0_publishes)

    def _drain_qos0_publishes(self) -> None:
        """Move a coalesced QoS 0 batch into the engine on its owning loop."""
        with self._qos0_lock:
            batch = list(self._qos0_pending)
            self._qos0_pending.clear()
            self._qos0_drain_scheduled = False

        errors: list[BaseException] = []
        queued = False
        with self._async._state_mutex:
            for topic, payload, retain in batch:
                try:
                    self._async._engine.queue_publish(
                        topic,
                        payload,
                        qos=QoS.AT_MOST_ONCE,
                        retain=retain,
                    )
                except BaseException as exc:
                    errors.append(exc)
                else:
                    queued = True
            if queued:
                self._async._collect_effects_locked()

        if queued:
            self._async._schedule_effect_flush()
        for error in errors:
            self._async._spawn_callback(self._dispatch_publish, None, error)

    def publish(
        self,
        topic: str,
        payload: bytes | str = b"",
        qos: int = 0,
        retain: bool = False,
    ) -> MQTTMessageInfo:
        """Queue a publish without waiting for TCP writer progress.

        QoS 0 requests are coalesced in a thread-safe façade queue and consumed
        on the network loop. QoS 1/2 wait only for the loop to allocate the MID
        and register the receipt; effect draining remains asynchronous.
        """
        data = payload.encode("utf-8") if isinstance(payload, str) else payload
        requested_qos = QoS(qos)
        if self._loop is None:
            self.loop_start()
        assert self._loop is not None

        if requested_qos is QoS.AT_MOST_ONCE:
            receipt = PublishReceipt(mid=None, qos=requested_qos, _event=None)
            self._enqueue_qos0_publish(topic, data, retain)
            return MQTTMessageInfo(mid=None, _receipt=receipt, _loop=self._loop)

        if self._on_loop_in_callback():
            with self._async._state_mutex:
                handle = self._async._engine.queue_publish(
                    topic,
                    data,
                    qos=requested_qos,
                    retain=retain,
                )
                assert handle.mid is not None
                receipt = PublishReceipt(
                    mid=handle.mid,
                    qos=handle.qos,
                    _event=asyncio.Event(),
                )
                self._async._receipts[handle.mid] = receipt
                self._async._collect_effects_locked()
            self._async._schedule_effect_flush()
            return MQTTMessageInfo(mid=receipt.mid, _receipt=receipt, _loop=self._loop)

        handoff: dict[str, Any] = {}
        done = threading.Event()

        async def _queue() -> None:
            try:
                async with self._async._engine_lock:
                    with self._async._state_mutex:
                        handle = self._async._engine.queue_publish(
                            topic,
                            data,
                            qos=requested_qos,
                            retain=retain,
                        )
                        assert handle.mid is not None
                        receipt = PublishReceipt(
                            mid=handle.mid,
                            qos=handle.qos,
                            _event=asyncio.Event(),
                        )
                        self._async._receipts[handle.mid] = receipt
                        self._async._collect_effects_locked()
                        handoff["receipt"] = receipt
                self._async._schedule_effect_flush()
            except BaseException as exc:
                handoff["error"] = exc
            finally:
                done.set()

        fut = asyncio.run_coroutine_threadsafe(_queue(), self._loop)
        if not done.wait(timeout=5.0):
            fut.cancel()
            raise RuntimeError("publish handoff to event loop timed out")
        error = handoff.get("error")
        if error is not None:
            raise error
        receipt = handoff["receipt"]
        return MQTTMessageInfo(mid=receipt.mid, _receipt=receipt, _loop=self._loop)

    def _dispatch_publish(self, mid: int | None, error: BaseException | None) -> None:
        if self.on_publish is None:
            return
        reason_code = 0 if error is None else 1
        self._safe_callback(
            self.on_publish,
            self,
            self._userdata,
            mid,
            reason_code,
            None,
        )

    def subscribe(self, topic: str, qos: int = 0) -> tuple[int, int]:
        """Paho-style: return ``(0, mid)`` without awaiting SUBACK."""
        mid = self._queue_loop_command(lambda: self._async._engine.queue_subscribe(topic, qos=qos))
        return (0, mid)

    def unsubscribe(self, topic: str) -> tuple[int, int]:
        mid = self._queue_loop_command(lambda: self._async._engine.queue_unsubscribe(topic))
        return (0, mid)

    def _on_loop_in_callback(self) -> bool:
        """True only on the network loop thread, inside a user callback."""
        return self._in_callback and threading.current_thread() is self._thread

    def _safe_callback(self, cb: Callable[..., Any], *args: Any) -> None:
        # Only the loop thread may flip _in_callback: an off-loop invocation
        # (e.g. the QoS 0 on_publish fast path) must not clobber the flag the
        # loop thread relies on to detect re-entrant blocking calls.
        on_loop = threading.current_thread() is self._thread
        if on_loop:
            self._in_callback = True
        try:
            cb(*args)
        except Exception as exc:
            loop = self._loop
            if loop is not None:
                context = {
                    "message": "mqttium Paho-compatible callback failed",
                    "exception": exc,
                    "callback": cb,
                }
                if on_loop:
                    loop.call_exception_handler(context)
                else:
                    loop.call_soon_threadsafe(loop.call_exception_handler, context)
        finally:
            if on_loop:
                self._in_callback = False

    def _dispatch_connect(self, connack: ConnAckPacket) -> None:
        cb = self.on_connect
        if cb is None:
            return
        flags = {"session present": bool(connack.session_present)}
        props = connack.properties or Properties()
        self._safe_callback(cb, self, self._userdata, flags, connack.reason_code, props)

    def _dispatch_disconnect(self, exc: BaseException | None) -> None:
        cb = self.on_disconnect
        if cb is None:
            return
        info = self._async._last_disconnect
        from_broker = bool(info and info.from_broker)
        reason = info.reason_code if info is not None else (0 if exc is None else 1)
        props = info.properties if info is not None else None
        flags = DisconnectFlags(is_disconnect_packet_from_server=from_broker)
        self._safe_callback(cb, self, self._userdata, flags, reason, props)

    def _dispatch_message(self, msg: Message) -> None:
        wrapped = MQTTMessage(msg)
        matched = False
        for cb in self._topic_callbacks.iter_match(msg.topic):
            matched = True
            self._safe_callback(cb, self, self._userdata, wrapped)
        # Paho: default on_message only if no filtered callback matched.
        if not matched and self.on_message is not None:
            self._safe_callback(self.on_message, self, self._userdata, wrapped)
