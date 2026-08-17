"""Paho-compatible sync façade over AsyncClient.

Only ``CallbackAPIVersion.VERSION2`` is supported. See ``docs/COMPAT.md`` for
the supported surface, intentional divergences, and rejected features.
"""

from __future__ import annotations

import asyncio
import enum
import threading
from collections import deque
from collections.abc import Callable
from concurrent.futures import Future
from contextlib import suppress
from dataclasses import dataclass, field
from queue import Empty, SimpleQueue
from typing import Any

from mqttium.api.async_client import AsyncClient
from mqttium.api.models import PublishReceipt
from mqttium.dispatch.matcher import TopicMatcher
from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.errors import FlowControlError
from mqttium.packets import ConnAckPacket
from mqttium.topics import validate_subscribe_filter
from mqttium.types import Message, Properties


class CallbackAPIVersion(enum.Enum):
    VERSION2 = 2


MQTT_ERR_SUCCESS = 0
MQTT_ERR_QUEUE_SIZE = 15

_LOOP_HANDOFF_TIMEOUT = 5.0
# Façade correlation identifiers wrap like Paho's, so an application that
# already treats `mid` as a 16-bit value keeps working. They are NOT the wire
# packet identifiers: see the `mid` row in docs/COMPAT.md.
_MAX_FACADE_MID = 65_535
_PUBLISH_BATCH_MAX_MESSAGES = 256
_PUBLISH_BATCH_MAX_BYTES = 1 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class ConnectFlags:
    """VERSION2 connect flags (Paho-compatible subset)."""

    session_present: bool = False


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
    _handoff: Future[None] | None = field(default=None, repr=False)
    rc: int = 0

    def _raise_for_rc(self) -> None:
        if self.rc == MQTT_ERR_QUEUE_SIZE:
            raise ValueError("Message is not queued due to ERR_QUEUE_SIZE")
        if self.rc != MQTT_ERR_SUCCESS:
            raise RuntimeError(f"Message publish failed with rc={self.rc}")

    def wait_for_publish(self, timeout: float | None = None) -> None:
        """Block until the publish completes; raise on protocol/transport error."""
        self._raise_for_rc()
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        if self._loop is not None and running_loop is self._loop:
            raise RuntimeError("wait_for_publish() cannot block the network event loop")
        if self._handoff is not None:
            self._handoff.result(timeout)
            # publish() returns before loop-side admission, so a refusal can
            # land on rc after the handle was handed back. Re-check rather than
            # report success for a publication that was never queued.
            self._raise_for_rc()
        if self._receipt is None or self._receipt.is_done():
            if self._receipt is not None and self._receipt._error is not None:
                raise self._receipt._error
            return
        if self._loop is None:
            raise RuntimeError("No event loop for wait_for_publish")
        fut = asyncio.run_coroutine_threadsafe(self._receipt.wait(), self._loop)
        fut.result(timeout)

    def is_published(self) -> bool:
        self._raise_for_rc()
        if self._handoff is not None:
            if not self._handoff.done():
                return False
            self._handoff.result()
            self._raise_for_rc()
        return self._receipt is None or self._receipt.is_done()


class _PendingPublish:
    """One cross-thread publish waiting for loop-side admission."""

    __slots__ = (
        "topic",
        "payload",
        "retain",
        "qos",
        "info",
        "completion",
        "logical_size",
    )

    def __init__(
        self,
        topic: str,
        payload: bytes,
        retain: bool,
        qos: QoS,
        info: MQTTMessageInfo | None,
        completion: Future[None],
    ) -> None:
        self.topic = topic
        self.payload: bytes | None = payload
        self.retain = retain
        self.qos = qos
        # The handle the producer thread already holds. QoS 0 has none: it has
        # no receipt to attach and no packet identifier to correlate.
        self.info = info
        self.completion = completion
        topic_size = len(topic) if topic.isascii() else len(topic.encode("utf-8"))
        self.logical_size = topic_size + len(payload)

    def discard_payload(self) -> None:
        self.topic = ""
        self.payload = None


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
        max_pending_outbound_messages: int | None = 10_000,
        max_pending_outbound_bytes: int | None = 64 * 1024 * 1024,
        max_pending_inbound_bytes: int | None = 64 * 1024 * 1024,
        max_pending_publish_requests: int = 10_000,
        max_pending_publish_bytes: int = 64 * 1024 * 1024,
        max_outbound_inflight: int | None = None,
    ) -> None:
        if callback_api_version is not CallbackAPIVersion.VERSION2:
            raise ValueError("mqttium.compat.paho only supports CallbackAPIVersion.VERSION2")
        if clean_session is not None:
            clean_start = bool(clean_session)
        if max_pending_publish_requests <= 0:
            raise ValueError("max_pending_publish_requests must be greater than 0")
        if max_pending_publish_bytes <= 0:
            raise ValueError("max_pending_publish_bytes must be greater than 0")
        self._userdata = userdata
        self._async = AsyncClient(
            client_id=client_id,
            protocol=protocol,
            clean_start=clean_start,
            max_pending_outbound_messages=max_pending_outbound_messages,
            max_pending_outbound_bytes=max_pending_outbound_bytes,
            max_pending_inbound_bytes=max_pending_inbound_bytes,
            max_outbound_inflight=max_outbound_inflight,
            publish_backpressure="error",
        )
        # Cache the hot adapter boundary methods once. This avoids repeated
        # AsyncClient attribute traversal in every Paho publication while keeping
        # protocol and receipt state behind the AsyncClient boundary.
        self._queue_qosn_on_loop = self._async._queue_qosn_on_loop
        self._queue_qos0_on_loop = self._async._queue_qos0_on_loop
        self._try_direct_qos0_publish = self._async._try_direct_qos0_publish
        self._finalize_async_commands = self._async._finalize_loop_commands
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._started = threading.Event()
        self._stopping = threading.Event()
        self._topic_callbacks = TopicMatcher()
        self._publish_pending: SimpleQueue[_PendingPublish] = SimpleQueue()
        self._publish_spillover: _PendingPublish | None = None
        self._publish_schedule_lock = threading.Lock()
        self._loop_state_lock = threading.Lock()
        self._max_pending_publish_requests = max_pending_publish_requests
        self._max_pending_publish_bytes = max_pending_publish_bytes
        self._pending_publish_requests = 0
        self._pending_publish_bytes = 0
        self._publish_drain_scheduled = False
        # Paho allocates its wrapping 1..65535 message id on the calling thread
        # and refuses the publication if that id is still active. Keep that
        # lifetime rule entirely inside the façade instead of moving the
        # protocol engine's packet-id pool off-loop.
        self._facade_mid_lock = threading.Lock()
        self._last_facade_mid = 0
        self._active_facade_mids: set[int] = set()
        # Receipt identity is the authoritative correlation/lifetime index.
        # It survives callback toggles and lets the settle hook decide whether
        # a façade MID can retire or is owned by a queued publish dispatcher.
        self._facade_receipts: dict[int, tuple[int, int]] = {}
        self._settle_facade_receipt = self._facade_receipt_settled
        # Fast dispatcher index: real packet id -> (receipt identity, façade
        # mid), oldest first. It is loop-confined and only needs to be populated
        # while on_publish is installed; the authoritative receipt index above
        # preserves correlation while callbacks are disabled.
        self._facade_mid_map: dict[int, deque[tuple[int, int]]] = {}

        self.on_connect: Callable[..., Any] | None = None
        self.on_disconnect: Callable[..., Any] | None = None
        self.on_message: Callable[..., Any] | None = None
        self._on_publish: Callable[..., Any] | None = None

        self._async.on_connect = self._dispatch_connect
        self._async.on_disconnect = self._dispatch_disconnect
        self._async.on_message = self._dispatch_message
        # on_publish is installed on demand: see the property below.

    @property
    def on_publish(self) -> Callable[..., Any] | None:
        return self._on_publish

    @on_publish.setter
    def on_publish(self, callback: Callable[..., Any] | None) -> None:
        """Install the inner callback only while the façade user wants one.

        The native client keeps its direct QoS 0 writer path only while
        ``AsyncClient.on_publish`` is ``None``, and routes every QoS 1/2
        completion through the callback queue when it is not. Installing the
        dispatcher unconditionally therefore charged every façade user for a
        callback they had not asked for, and cost them the fast path outright.

        Removing the callback clears only the fast wire-MID lookup. Receipt
        identity remains authoritative, so an already-queued dispatcher or a
        callback reinstalled before completion still resolves the façade MID.
        """
        # Installed on the loop together with `_on_publish`, so completion sees
        # an internally consistent callback state.
        self._run_loop_mutation(lambda: self._install_publish_dispatch(callback))

    def _install_publish_dispatch(self, callback: Callable[..., Any] | None) -> None:
        self._on_publish = callback
        self._async.on_publish = self._dispatch_publish if callback is not None else None
        self._facade_mid_map.clear()
        if callback is None:
            return
        # Rebuild the fast index from the authoritative receipt bindings. Dict
        # insertion order preserves FIFO for a wire MID that has been reused.
        for receipt_id, (real_mid, facade_mid) in self._facade_receipts.items():
            pending = self._facade_mid_map.get(real_mid)
            entry = (receipt_id, facade_mid)
            if pending is None:
                self._facade_mid_map[real_mid] = deque((entry,))
            else:
                pending.append(entry)

    def user_data_set(self, userdata: Any) -> None:
        self._run_loop_mutation(lambda: setattr(self, "_userdata", userdata))

    def max_queued_messages_set(self, queue_size: int) -> None:
        if queue_size < 0:
            raise ValueError("queue_size must be non-negative")
        # Paho uses zero for unlimited; the native MQTTium API uses None.
        value = None if queue_size == 0 else queue_size
        self._run_loop_mutation(
            lambda: self._async._reconfigure(max_pending_outbound_messages=value)
        )

    def max_queued_bytes_set(self, queue_size: int | None) -> None:
        if queue_size is not None and queue_size < 0:
            raise ValueError("queue_size must be non-negative or None")
        self._run_loop_mutation(
            lambda: self._async._reconfigure(max_pending_outbound_bytes=queue_size)
        )

    def username_pw_set(self, username: str, password: bytes | str | None = None) -> None:
        pwd = password.encode("utf-8") if isinstance(password, str) else password

        def _set_credentials() -> None:
            self._async._reconfigure(
                username=username,
                password=pwd,
            )

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
        self._run_loop_mutation(lambda: self._async._reconfigure(will=message))

    def message_callback_add(self, sub: str, callback: Callable[..., Any]) -> None:
        validate_subscribe_filter(sub)
        self._run_loop_mutation(lambda: self._topic_callbacks.__setitem__(sub, callback))

    def message_callback_remove(self, sub: str) -> None:
        def _remove() -> None:
            with suppress(KeyError):
                del self._topic_callbacks[sub]

        self._run_loop_mutation(_remove)

    def is_connected(self) -> bool:
        return bool(self._run_loop_mutation(lambda: self._async.is_connected))

    def loop_start(self) -> None:
        while True:
            stopping_thread: threading.Thread | None = None
            with self._loop_state_lock:
                thread = self._thread
                if thread is not None and thread.is_alive():
                    if self._stopping.is_set():
                        stopping_thread = thread
                    else:
                        started = self._started
                        break
                else:
                    self._started.clear()
                    self._stopping.clear()
                    thread = threading.Thread(
                        target=self._run_loop, name="mqttium-paho-loop", daemon=True
                    )
                    self._thread = thread
                    try:
                        thread.start()
                    except BaseException:
                        if self._thread is thread:
                            self._thread = None
                        raise
                    started = self._started
                    break

            assert stopping_thread is not None
            if stopping_thread is threading.current_thread():
                raise RuntimeError("Cannot restart a stopping network loop from its own thread")
            stopping_thread.join(timeout=5.0)
            if stopping_thread.is_alive():
                raise RuntimeError("Previous background loop did not stop")

        if not started.wait(timeout=5.0):
            raise RuntimeError("Failed to start background loop")

    def loop_stop(self) -> None:
        self._stopping.set()
        self._fail_pending_publish_requests(
            RuntimeError("network loop stopped before publish admission")
        )
        with self._loop_state_lock:
            thread = self._thread
            started = self._started
        if thread is not None and thread.is_alive() and not started.is_set():
            started.wait(timeout=5.0)
        with self._loop_state_lock:
            # A concurrent restart may have installed another loop generation
            # while this stop waited for the old thread to publish readiness.
            # Never stop or clear state owned by that replacement.
            if self._thread is not thread:
                return
            loop = self._loop
        if loop is not None and loop.is_running():
            if self._on_network_thread():
                loop.stop()
                return
            loop.call_soon_threadsafe(loop.stop)
        if thread is not None:
            thread.join(timeout=5.0)
        with self._loop_state_lock:
            if self._thread is thread:
                self._thread = None
            if self._loop is loop:
                self._loop = None

    def _run_loop(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._started.set()
        try:
            loop.run_forever()
        finally:
            self._stopping.set()
            self._fail_pending_publish_requests(
                RuntimeError("network loop stopped before publish admission")
            )
            # No completion from this loop generation can be correlated after
            # it stops. Receipt hooks may still run while tasks are cancelled;
            # their discard/remove operations are deliberately idempotent.
            self._facade_mid_map.clear()
            self._facade_receipts.clear()
            with self._facade_mid_lock:
                self._active_facade_mids.clear()
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()
            with self._loop_state_lock:
                if self._loop is loop:
                    self._loop = None
                if self._thread is threading.current_thread():
                    self._thread = None

    def _submit(
        self,
        coro: Any,
        timeout: float | None = 30.0,
        *,
        wait: bool = True,
    ) -> Any:
        if wait and self._on_network_thread():
            raise RuntimeError(
                "Do not call blocking Client methods from the network thread "
                "(would deadlock). Schedule work on another thread or use "
                "mqttium.api.AsyncClient."
            )
        if self._loop is None:
            self.loop_start()
        assert self._loop is not None
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        if not wait:
            return fut
        return fut.result(timeout)

    def _await_on_loop(
        self,
        command: Callable[[], Any],
        *,
        flush_effects: bool,
        what: str,
    ) -> Any:
        """Run a short synchronous command on the network loop and wait for it.

        The single bounded cross-thread handoff: nothing off the network thread
        may touch AsyncClient or engine state directly. The loop-side failure is
        re-raised on the calling thread rather than being swallowed.
        """
        loop = self._loop
        assert loop is not None

        handoff: dict[str, Any] = {}
        done = threading.Event()

        async def _run() -> None:
            try:
                handoff["result"] = command()
                if flush_effects:
                    await self._async._flush_effects()
            except BaseException as exc:
                handoff["error"] = exc
            finally:
                done.set()

        fut = asyncio.run_coroutine_threadsafe(_run(), loop)
        if not done.wait(timeout=_LOOP_HANDOFF_TIMEOUT):
            fut.cancel()
            raise RuntimeError(f"{what} handoff to event loop timed out")
        error = handoff.get("error")
        if error is not None:
            raise error
        return handoff.get("result")

    def _run_loop_mutation(self, mutation: Callable[[], Any]) -> Any:
        """Apply a configuration mutation, on the network loop when one exists.

        Before ``loop_start`` there is no loop to protect, so the mutation runs
        directly on the caller's thread.
        """
        loop = self._loop
        thread = self._thread
        if loop is None or not loop.is_running() or thread is None:
            return mutation()
        if threading.current_thread() is thread:
            return mutation()
        return self._await_on_loop(mutation, flush_effects=False, what="mutation")

    def _queue_loop_command(self, command: Callable[[], Any]) -> Any:
        """Run an engine command on the dedicated loop and flush its effects.

        Callback code already executes on that loop, so it may run the short
        synchronous engine command directly.
        """
        if self._loop is None:
            self.loop_start()
        assert self._loop is not None

        if self._on_network_thread():
            result = command()
            self._async._finalize_loop_commands()
            return result

        return self._await_on_loop(command, flush_effects=True, what="command")

    def connect(self, host: str, port: int = 1883, keepalive: int = 60) -> int:
        self._run_loop_mutation(lambda: self._async._reconfigure(keepalive=keepalive))
        self._submit(self._async.connect(host, port))
        return 0

    def reconnect(self) -> int:
        host, port, keepalive = self._run_loop_mutation(
            lambda: self._async._compat_connection_settings
        )
        if not host:
            raise RuntimeError("reconnect() called before connect()")
        return self.connect(host, port, keepalive=keepalive)

    def disconnect(self) -> int:
        if self._on_network_thread():
            raise RuntimeError("disconnect() cannot block the network event loop")
        try:
            self._submit(self._async.disconnect(), timeout=10.0)
        except Exception:
            return 1
        return 0

    def _on_network_thread(self) -> bool:
        return threading.current_thread() is self._thread

    def _enqueue_publish_request(self, request: _PendingPublish) -> bool:
        """Reserve bounded cross-thread ingress capacity and schedule one drain."""
        loop = self._loop
        if self._stopping.is_set() or loop is None or not loop.is_running():
            raise RuntimeError("network loop is not running")

        schedule = False
        with self._publish_schedule_lock:
            if self._stopping.is_set() or not loop.is_running():
                raise RuntimeError("network loop is not running")
            if (
                self._pending_publish_requests >= self._max_pending_publish_requests
                or self._pending_publish_bytes + request.logical_size
                > self._max_pending_publish_bytes
            ):
                return False
            self._pending_publish_requests += 1
            self._pending_publish_bytes += request.logical_size
            self._publish_pending.put(request)
            if not self._publish_drain_scheduled:
                self._publish_drain_scheduled = True
                schedule = True
        if schedule:
            try:
                loop.call_soon_threadsafe(self._drain_publish_requests)
            except RuntimeError:
                self._fail_pending_publish_requests(
                    RuntimeError("network loop stopped before publish admission")
                )
                raise
        return True

    def _release_publish_request_locked(self, request: _PendingPublish) -> None:
        self._pending_publish_requests -= 1
        self._pending_publish_bytes -= request.logical_size
        if self._pending_publish_requests < 0 or self._pending_publish_bytes < 0:
            raise RuntimeError("compat publish ingress accounting underflow")

    def _release_publish_requests(self, requests: list[_PendingPublish]) -> None:
        with self._publish_schedule_lock:
            for request in requests:
                self._release_publish_request_locked(request)

    def _take_publish_batch(self) -> tuple[list[_PendingPublish], bool]:
        batch: list[_PendingPublish] = []
        batch_bytes = 0
        with self._publish_schedule_lock:
            if self._publish_spillover is not None:
                batch.append(self._publish_spillover)
                batch_bytes = self._publish_spillover.logical_size
                self._publish_spillover = None
            while len(batch) < _PUBLISH_BATCH_MAX_MESSAGES:
                try:
                    request = self._publish_pending.get_nowait()
                except Empty:
                    break
                if batch and batch_bytes + request.logical_size > _PUBLISH_BATCH_MAX_BYTES:
                    self._publish_spillover = request
                    break
                batch.append(request)
                batch_bytes += request.logical_size
            if self._publish_spillover is None and len(batch) == _PUBLISH_BATCH_MAX_MESSAGES:
                try:
                    self._publish_spillover = self._publish_pending.get_nowait()
                except Empty:
                    pass
            has_more = self._publish_spillover is not None
        return batch, has_more

    def _fail_pending_publish_requests(self, error: BaseException) -> None:
        pending: list[_PendingPublish] = []
        with self._publish_schedule_lock:
            if self._publish_spillover is not None:
                pending.append(self._publish_spillover)
                self._publish_spillover = None
            while True:
                try:
                    pending.append(self._publish_pending.get_nowait())
                except Empty:
                    break
            for request in pending:
                self._release_publish_request_locked(request)
            self._publish_drain_scheduled = False
        for request in pending:
            request.discard_payload()
            if request.info is not None:
                self._release_facade_mid(request.info.mid)
            if not request.completion.done():
                request.completion.set_exception(error)

    def _report_qos0_publish_error(self, error: BaseException) -> None:
        try:
            self._async._spawn_callback(self._dispatch_publish, None, error)
        except BaseException as callback_error:
            loop = self._loop
            if loop is not None:
                loop.call_exception_handler(
                    {
                        "message": "mqttium QoS 0 publish error callback failed",
                        "exception": callback_error,
                    }
                )

    def _commit_qosn_publish_on_loop(self, request: _PendingPublish) -> None:
        info = request.info
        assert info is not None
        receipt = self._queue_qosn_on_loop(
            request.topic,
            request.payload if request.payload is not None else b"",
            qos=request.qos,
            retain=request.retain,
        )
        assert receipt.mid is not None
        self._register_facade_mid(receipt, info.mid)
        # The producer thread reads `_receipt` only after the handoff future
        # resolves, and that future is settled below. Publishing the receipt
        # first is what makes the read safe without a lock.
        info._receipt = receipt

    def _finalize_publish_effects(self) -> None:
        self._finalize_async_commands()

    def _commit_publish_request(self, request: _PendingPublish) -> None:
        """Admit one request on the loop. Raises when admission is refused."""
        if request.qos is QoS.AT_MOST_ONCE:
            payload = request.payload if request.payload is not None else b""
            # Same writer-direct path the native client takes. It declines
            # whenever effects are already pending — which is exactly the case
            # after a QoS 1/2 commit earlier in this batch, whose SEND is not
            # flushed until _finalize_publish_effects(). That is what keeps
            # ordering across QoS levels (COMPAT.md).
            #
            # nowait=False deliberately: a full writer queue must fall back to
            # the effect path, which defers the SEND rather than refusing it.
            # The direct path is an optimisation, never a new failure mode.
            if (
                self._try_direct_qos0_publish(
                    request.topic,
                    payload,
                    qos=QoS.AT_MOST_ONCE,
                    retain=request.retain,
                    properties=None,
                    nowait=False,
                )
                is None
            ):
                self._queue_qos0_on_loop(request.topic, payload, retain=request.retain)
        else:
            self._commit_qosn_publish_on_loop(request)

    def _reject_publish_request(
        self, request: _PendingPublish, error: BaseException, *, refused: bool
    ) -> bool:
        """Record one failed admission; True when the caller must still settle it.

        QoS 0 has no handle to carry an rc, so it is settled here and reported
        through ``on_publish`` instead.
        """
        info = request.info
        if info is None:
            if not request.completion.done():
                request.completion.set_exception(error)
            self._report_qos0_publish_error(error)
            return False
        # No native receipt exists when loop-side admission raises, so the
        # façade reservation has no later settlement hook that could retire it.
        self._release_facade_mid(info.mid)
        if refused:
            # QoS 1/2 keeps Paho's shape: a refusal is an rc on the handle,
            # which wait_for_publish()/is_published() check before they ever
            # look at the handoff.
            info.rc = MQTT_ERR_QUEUE_SIZE
        return True

    def _drain_publish_requests(self) -> None:
        """Commit a bounded mixed-QoS batch on the owning network loop."""
        batch, has_more = self._take_publish_batch()
        # (request, admission error). Settled only after the batch's effects are
        # finalized, so a caller never observes a completion the writer has not
        # yet accepted.
        settled: list[tuple[_PendingPublish, BaseException | None]] = []
        committed = False

        try:
            for request in batch:
                if self._stopping.is_set():
                    stop_error = RuntimeError("network loop stopped before publish admission")
                    if request.info is not None:
                        self._release_facade_mid(request.info.mid)
                    if not request.completion.done():
                        request.completion.set_exception(stop_error)
                    continue
                try:
                    self._commit_publish_request(request)
                except FlowControlError as exc:
                    if self._reject_publish_request(request, exc, refused=True):
                        settled.append((request, None))
                except BaseException as exc:
                    if self._reject_publish_request(request, exc, refused=False):
                        settled.append((request, exc))
                else:
                    committed = True
                    settled.append((request, None))
        finally:
            for request in batch:
                request.discard_payload()
            self._release_publish_requests(batch)

        finalize_error: BaseException | None = None
        if committed:
            try:
                self._finalize_publish_effects()
            except BaseException as exc:
                finalize_error = exc
                loop = self._loop
                if loop is not None:
                    loop.call_exception_handler(
                        {
                            "message": "mqttium compat publish effect finalization failed",
                            "exception": exc,
                        }
                    )

        self._settle_publish_batch(settled, finalize_error)
        self._reschedule_drain(has_more)

    def _settle_publish_batch(
        self,
        settled: list[tuple[_PendingPublish, BaseException | None]],
        finalize_error: BaseException | None,
    ) -> None:
        for request, error in settled:
            completion = request.completion
            if completion.done():
                continue
            if error is not None:
                completion.set_exception(error)
            elif finalize_error is None:
                completion.set_result(None)
            else:
                completion.set_exception(finalize_error)
                if request.qos is QoS.AT_MOST_ONCE:
                    self._report_qos0_publish_error(finalize_error)

    def _reschedule_drain(self, has_more: bool) -> None:
        if not has_more:
            # Close the producer race atomically with the scheduled flag. Queue
            # insertion uses the same lock, so an empty queue is authoritative here.
            with self._publish_schedule_lock:
                has_more = not self._publish_pending.empty()
                if not has_more:
                    self._publish_drain_scheduled = False
            if not has_more:
                return
        if self._stopping.is_set():
            self._fail_pending_publish_requests(
                RuntimeError("network loop stopped before publish admission")
            )
            return
        loop = self._loop
        if loop is not None and loop.is_running():
            loop.call_soon(self._drain_publish_requests)

    def publish(
        self,
        topic: str,
        payload: bytes | str | None = None,
        qos: int = 0,
        retain: bool = False,
    ) -> MQTTMessageInfo:
        """Queue a publish without waiting for the network loop.

        Every QoS shares one coalesced façade queue and returns as soon as the
        request is accepted, like Paho. ``mid`` is a façade correlation
        identifier, not the wire packet identifier. Ingress saturation returns
        ``MQTT_ERR_QUEUE_SIZE`` synchronously; a later admission refusal
        surfaces through ``wait_for_publish()`` / ``is_published()``.
        """
        if payload is None:
            data = b""
        elif isinstance(payload, str):
            data = payload.encode("utf-8")
        else:
            data = payload
        requested_qos = QoS(qos)
        if self._loop is None:
            self.loop_start()
        assert self._loop is not None

        if self._on_network_thread():
            facade_mid: int | None = None
            if requested_qos is not QoS.AT_MOST_ONCE:
                facade_mid, reserved = self._reserve_next_facade_mid()
                if not reserved:
                    return MQTTMessageInfo(mid=facade_mid, rc=MQTT_ERR_QUEUE_SIZE)
            try:
                if requested_qos is QoS.AT_MOST_ONCE:
                    receipt = self._async._queue_publish_on_loop(
                        topic,
                        data,
                        qos=requested_qos,
                        retain=retain,
                    )
                    info = MQTTMessageInfo(mid=None, _receipt=receipt, _loop=self._loop)
                else:
                    receipt = self._queue_qosn_on_loop(
                        topic,
                        data,
                        qos=requested_qos,
                        retain=retain,
                    )
                    assert facade_mid is not None
                    info = MQTTMessageInfo(
                        mid=facade_mid,
                        _receipt=receipt,
                        _loop=self._loop,
                    )
                    self._register_facade_mid(receipt, facade_mid)
            except FlowControlError:
                self._release_facade_mid(facade_mid)
                return MQTTMessageInfo(mid=facade_mid, rc=MQTT_ERR_QUEUE_SIZE)
            except BaseException:
                self._release_facade_mid(facade_mid)
                raise
            self._finalize_publish_effects()
            return info

        completion: Future[None] = Future()
        if requested_qos is QoS.AT_MOST_ONCE:
            info = MQTTMessageInfo(mid=None, _loop=self._loop, _handoff=completion)
            request = _PendingPublish(topic, data, retain, requested_qos, None, completion)
        else:
            facade_mid, reserved = self._reserve_next_facade_mid()
            if not reserved:
                return MQTTMessageInfo(mid=facade_mid, rc=MQTT_ERR_QUEUE_SIZE)
            info = MQTTMessageInfo(mid=facade_mid, _loop=self._loop, _handoff=completion)
            request = _PendingPublish(topic, data, retain, requested_qos, info, completion)
        try:
            accepted = self._enqueue_publish_request(request)
        except BaseException:
            if request.info is not None:
                self._release_facade_mid(request.info.mid)
            raise
        if not accepted:
            request.discard_payload()
            if request.info is not None:
                self._release_facade_mid(request.info.mid)
            return MQTTMessageInfo(mid=None, rc=MQTT_ERR_QUEUE_SIZE)
        return info

    def _next_facade_mid_locked(self) -> int:
        mid = self._last_facade_mid + 1
        if mid > _MAX_FACADE_MID:
            mid = 1
        self._last_facade_mid = mid
        return mid

    def _next_facade_mid(self) -> int:
        """Advance the façade MID generator without reserving the value."""
        with self._facade_mid_lock:
            return self._next_facade_mid_locked()

    def _reserve_next_facade_mid(self) -> tuple[int, bool]:
        """Generate Paho's next wrapping MID and reserve it if it is free.

        Paho does not search for another free identifier after a wrap collision:
        the generated MID is returned with ``MQTT_ERR_QUEUE_SIZE``. Mirroring
        that detail keeps overload/collision behaviour deterministic and bounds
        this calling-thread operation to one lock acquisition and one set lookup.
        """
        with self._facade_mid_lock:
            mid = self._next_facade_mid_locked()
            if mid in self._active_facade_mids:
                return mid, False
            self._active_facade_mids.add(mid)
            return mid, True

    def _release_facade_mid(self, facade_mid: int | None) -> None:
        if facade_mid is None:
            return
        with self._facade_mid_lock:
            self._active_facade_mids.discard(facade_mid)

    def _register_facade_mid(self, receipt: PublishReceipt, facade_mid: int | None) -> None:
        """Bind one committed wire MID to the façade MID and its lifetime.

        Correlation is registered even while the user callback is ``None`` so a
        callback installed before the ACK still sees the MID returned by
        ``publish()``. Receipt identity is authoritative; the fast wire-MID map
        is populated only while a dispatcher is installed. The receipt's
        one-shot settle hook retires immediately when no publish dispatcher
        exists; otherwise the reservation survives until that dispatcher has
        delivered completion, matching Paho's active-MID lifetime.
        """
        real_mid = receipt.mid
        if real_mid is None or facade_mid is None:
            return
        receipt_id = id(receipt)
        entry = (receipt_id, facade_mid)
        self._facade_receipts[receipt_id] = (real_mid, facade_mid)
        if self._async.on_publish is not None:
            pending = self._facade_mid_map.get(real_mid)
            if pending is None:
                self._facade_mid_map[real_mid] = deque((entry,))
            else:
                pending.append(entry)
        receipt._on_settle = self._settle_facade_receipt

    def _receipt_still_registered(self, receipt: PublishReceipt) -> bool:
        """Whether AsyncClient is bulk-settling this receipt without a callback."""
        real_mid = receipt.mid
        if real_mid is None:
            return False
        current = self._async._receipts.get(real_mid)
        if current is receipt:
            return True
        return isinstance(current, deque) and any(item is receipt for item in current)

    def _facade_receipt_settled(self, receipt: PublishReceipt) -> None:
        """Retire a façade MID unless an on_publish dispatcher still owns it."""
        receipt_id = id(receipt)
        binding = self._facade_receipts.get(receipt_id)
        if binding is None:
            return
        real_mid, facade_mid = binding

        # Normal completion pops the receipt before settling it. AsyncClient's
        # final bulk-failure path instead settles receipts while they are still
        # registered and does not emit one on_publish callback per receipt. Keep
        # a reservation only for the former case, when an inner dispatcher is
        # actually installed and can consume the authoritative binding.
        callback_owns_mid = (
            self._async.on_publish is not None and not self._receipt_still_registered(receipt)
        )
        if callback_owns_mid:
            return

        self._facade_receipts.pop(receipt_id, None)
        self._release_facade_mid(facade_mid)
        pending = self._facade_mid_map.get(real_mid)
        if pending is None:
            return
        entry = (receipt_id, facade_mid)
        with suppress(ValueError):
            pending.remove(entry)
        if not pending:
            self._facade_mid_map.pop(real_mid, None)

    def _take_facade_mid(self, real_mid: int) -> tuple[int, int | None]:
        """Consume one wire-to-façade correlation; return MID and release token."""
        pending = self._facade_mid_map.get(real_mid)
        if pending:
            receipt_id, facade_mid = pending.popleft()
            self._facade_receipts.pop(receipt_id, None)
            if not pending:
                del self._facade_mid_map[real_mid]
            return facade_mid, facade_mid

        # The callback may have been removed after AsyncClient queued this
        # dispatcher, which deliberately clears the fast map. The authoritative
        # receipt bindings survive that toggle, so consume the oldest matching
        # binding directly. Dict insertion order preserves reuse FIFO.
        for receipt_id, binding in self._facade_receipts.items():
            bound_real_mid, facade_mid = binding
            if bound_real_mid == real_mid:
                del self._facade_receipts[receipt_id]
                return facade_mid, facade_mid

        # A publication committed before this façade bound the identifier
        # (direct AsyncClient use). Report what the engine reported.
        return real_mid, None

    def _resolve_facade_mid(self, real_mid: int) -> int:
        """Consume one correlation outside the callback path (test/debug helper)."""
        facade_mid, release_mid = self._take_facade_mid(real_mid)
        self._release_facade_mid(release_mid)
        return facade_mid

    def _dispatch_publish(self, mid: int | None, error: BaseException | None) -> None:
        release_mid: int | None = None
        if mid is not None:
            mid, release_mid = self._take_facade_mid(mid)
        try:
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
        finally:
            # Paho removes the outbound message/MID after on_publish returns.
            # Keep the same active-ID lifetime even though mqttium dispatches
            # callbacks through a separate worker.
            self._release_facade_mid(release_mid)

    def subscribe(self, topic: str, qos: int = 0) -> tuple[int, int]:
        """Paho-style: return ``(0, mid)`` without awaiting SUBACK."""
        mid = self._queue_loop_command(lambda: self._async._queue_subscribe_on_loop(topic, qos=qos))
        return (0, mid)

    def unsubscribe(self, topic: str) -> tuple[int, int]:
        mid = self._queue_loop_command(lambda: self._async._queue_unsubscribe_on_loop(topic))
        return (0, mid)

    def _safe_callback(self, cb: Callable[..., Any], *args: Any) -> None:
        on_loop = threading.current_thread() is self._thread
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

    def _dispatch_connect(self, connack: ConnAckPacket) -> None:
        cb = self.on_connect
        if cb is None:
            return
        flags = ConnectFlags(session_present=bool(connack.session_present))
        props = connack.properties or Properties()
        self._safe_callback(cb, self, self._userdata, flags, connack.reason_code, props)

    def _dispatch_disconnect(self, exc: BaseException | None) -> None:
        cb = self.on_disconnect
        if cb is None:
            return
        info = self._async._last_disconnect_info
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
