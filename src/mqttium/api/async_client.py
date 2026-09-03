"""Async-native MQTT client.

Owns the transport + IncrementalDecoder + ProtocolEngine loop.

Concurrency invariants (see docs/implementation-guide.md §1):
- A single writer task drains the outbound queue.
- Publish receipts / SUBACK futures are registered *before* bytes can reach
  the wire.
- User callbacks run outside the engine's synchronous critical section.
"""

from __future__ import annotations

import asyncio
import ssl
import time
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from itertools import islice
from typing import Any, Literal, Never, TypeVar

from mqttium.api._delivery import ApplicationDelivery, MessageDelivery
from mqttium.api._effects import EffectPump, StaleConnectionEffect
from mqttium.api._writer import WritePump
from mqttium.api.models import (
    PublishBatchReceipt,
    PublishMessage,
    PublishReceipt,
    SubscribeResult,
    UnsubscribeResult,
)
from mqttium.api.stats import (
    ClientStats,
    DecoderStats,
    ReceiptStats,
    TaskStats,
    TransportStats,
)
from mqttium.codec.buffer import DEFAULT_MAX_PACKET_SIZE, IncrementalDecoder
from mqttium.dispatch.matcher import TopicMatcher
from mqttium.enums import ConnectionState, MQTTProtocolVersion, PacketType, QoS
from mqttium.errors import (
    FlowControlError,
    MQTTError,
    MQTTTimeoutError,
    MalformedPacketError,
    MandatoryResponseTooLargeError,
    MessageDeliveryError,
    PublishBatchError,
    PacketTooLargeError,
    ProtocolError,
)
from mqttium.packets import (
    AuthPacket,
    ConnAckPacket,
    SubAckPacket,
    SubscribeOptions,
    UnsubAckPacket,
    encode_disconnect,
)
from mqttium.packets._publish import (
    decode_qos0_message_v311_borrowed,
    decode_qos0_message_v5_borrowed,
)
from mqttium.protocol.engine import (
    DisconnectInfo,
    EffectKind,
    EngineConfig,
    EngineEffect,
    ProtocolEngine,
    PublishFailure,
)
from mqttium.protocol.negotiated import NegotiatedSettings
from mqttium.protocol.outbound import _PreparedPublish
from mqttium.protocol.reconnect import ReconnectPolicy
from mqttium.persistence.memory import InflightStore
from mqttium.topics import validate_subscribe_filter
from mqttium.transport._stream import AsyncTransport
from mqttium.transport.tcp import TcpTransport
from mqttium.transport.unix import UnixSocketTransport
from mqttium.transport.websocket import WebSocketTransport
from mqttium.transport.writes import WriteItem, item_size
from mqttium.types import Message, Properties

OnMessage = Callable[[Message], Any]
OnConnect = Callable[[ConnAckPacket], Any]
OnDisconnect = Callable[[BaseException | None], Any]
OnPublish = Callable[[int | None, BaseException | None], Any]
OnAuth = Callable[[AuthPacket], Any]
PublishBackpressure = Literal["wait", "error"]

_GRACEFUL_DISCONNECT_DRAIN_TIMEOUT = 5.0
_FATAL_DISCONNECT_DRAIN_TIMEOUT = 0.25

_ReceiptT = TypeVar("_ReceiptT", "PublishReceipt", "PublishBatchReceipt")
_AckResultT = TypeVar("_AckResultT", "SubscribeResult", "UnsubscribeResult")


def _fifo_register(
    registry: dict[int, _ReceiptT | deque[_ReceiptT]],
    mid: int,
    entry: _ReceiptT,
) -> None:
    """Register one receipt under a packet identifier, FIFO on reuse."""
    current = registry.get(mid)
    if current is None:
        registry[mid] = entry
    elif isinstance(current, deque):
        current.append(entry)
    else:
        registry[mid] = deque((current, entry))


def _fifo_pop(
    registry: dict[int, _ReceiptT | deque[_ReceiptT]],
    mid: int,
) -> _ReceiptT | None:
    """Pop the oldest receipt registered under a packet identifier."""
    current = registry.pop(mid, None)
    if current is None or not isinstance(current, deque):
        return current
    entry = current.popleft()
    if len(current) == 1:
        registry[mid] = current[0]
    elif current:
        registry[mid] = current
    return entry


def _extend_message_effects(
    sink: list[EngineEffect],
    messages: list[Message],
    property_sizes: list[int | None] | None,
) -> None:
    """Materialise borrowed-decode QoS 0 messages as ordinary engine effects."""
    if property_sizes is None:
        sink.extend(EngineEffect(EffectKind.MESSAGE, message, False, None) for message in messages)
    else:
        sink.extend(
            EngineEffect(
                EffectKind.DECODED_MESSAGE if wire_size is not None else EffectKind.MESSAGE,
                message,
                False,
                wire_size,
            )
            for message, wire_size in zip(messages, property_sizes, strict=True)
        )


def _terminal_publish_result(effect: EngineEffect) -> tuple[int | None, BaseException | None]:
    """Extract the (mid, reason) outcome of a terminal publish effect."""
    if effect.kind is EffectKind.PUBLISH_COMPLETE:
        return effect.data, None
    failure: PublishFailure = effect.data
    return failure.mid, failure.reason


def _positive(name: str, value: float) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be greater than 0")


def _non_negative_optional(name: str, value: int | None) -> None:
    if value is not None and value < 0:
        raise ValueError(f"{name} must be non-negative or None")


def _validate_client_arguments(
    *,
    client_id: object,
    username: object,
    password: object,
    message_delivery: str,
    publish_backpressure: str,
    optional_bounds: tuple[tuple[str, int | None], ...],
    positive_bounds: tuple[tuple[str, float], ...],
    ping_timeout: float | None,
) -> None:
    if message_delivery not in ("auto", "iterator", "callback", "both"):
        raise ValueError("message_delivery must be 'auto', 'iterator', 'callback', or 'both'")
    if publish_backpressure not in ("wait", "error"):
        raise ValueError("publish_backpressure must be 'wait' or 'error'")
    for name, optional_value in optional_bounds:
        _non_negative_optional(name, optional_value)
    for name, positive_value in positive_bounds:
        _positive(name, positive_value)
    if ping_timeout is not None:
        _positive("ping_timeout", ping_timeout)
    if not isinstance(client_id, str):
        raise ValueError("client_id must be a string")
    if username is not None and not isinstance(username, str):
        raise ValueError("username must be a string or None")
    if password is not None and not isinstance(password, (bytes, str)):
        raise ValueError("password must be bytes, str, or None")


class _DirectQos0Fallback(Exception):
    """Use the historical engine path for a valid stateful QoS0 packet."""


class AsyncClient:
    """Asyncio-native MQTT 3.1.1 and MQTT 5 client.

    The client owns one event loop, one protocol engine, and at most one active
    transport. It provides bounded outbound, inbound, writer, callback, and
    delivery queues; limits can be tuned explicitly through the constructor.

    Instances are loop-confined and are not thread-safe. Use the native async
    methods from the owning loop. The Provisional Paho compatibility façade is
    available for synchronous migration code that requires a thread handoff.

    Args:
        client_id: MQTT client identifier. An empty identifier lets an MQTT 5
            broker assign one when the connection settings allow it.
        protocol: MQTT protocol version used for encoding and negotiation.
        clean_start: Whether to start without a previous broker session.
        keepalive: Keep-alive interval in seconds; zero disables keep-alive.
        username: Optional CONNECT username.
        password: Optional CONNECT password. Strings are encoded as UTF-8.
        local_receive_maximum: Maximum concurrent inbound QoS 1/2 exchanges.
        max_outbound_inflight: Optional local cap on concurrent outbound QoS
            1/2 exchanges, additionally bounded by broker negotiation.
        publish_backpressure: ``"wait"`` to suspend producers or ``"error"``
            to raise :class:`~mqttium.FlowControlError` at logical capacity.
        reconnect: Reconnection policy. The default disables reconnection.
        message_delivery: ``"iterator"``, ``"callback"``, ``"both"``, or
            ``"auto"`` delivery selection.
        manual_ack: Defer terminal acknowledgement of inbound QoS messages
            until :meth:`ack` is called.
        store: Optional inflight store used for durable QoS state.
        auth_handler: Optional MQTT 5 enhanced-authentication callback. A
            callback-raised :class:`asyncio.CancelledError` is treated as an
            authentication failure; cancellation requested on MQTTium's
            owning task still propagates normally.
        auth_timeout: Maximum seconds allowed for one enhanced-authentication
            callback invocation.

    Raises:
        ValueError: If a limit or constructor option is invalid.

    Note:
        Remaining ``max_*`` arguments are explicit memory and queue bounds.
        See the configuration guide for sizing rules and interactions.
    """

    def __init__(
        self,
        client_id: str = "",
        *,
        protocol: MQTTProtocolVersion = MQTTProtocolVersion.MQTTv311,
        clean_start: bool = True,
        keepalive: int = 60,
        username: str | None = None,
        password: bytes | str | None = None,
        local_receive_maximum: int = 100,
        max_outbound_inflight: int | None = None,
        max_pending_outbound_messages: int | None = 10_000,
        max_pending_outbound_bytes: int | None = 64 * 1024 * 1024,
        max_pending_inbound_bytes: int | None = 64 * 1024 * 1024,
        publish_backpressure: PublishBackpressure = "wait",
        connect_properties: Properties | None = None,
        will: Message | None = None,
        will_properties: Properties | None = None,
        maximum_packet_size: int | None = None,
        topic_alias_maximum: int = 0,
        reconnect: ReconnectPolicy | None = None,
        ping_timeout: float | None = None,
        ack_timeout: float = 30.0,
        max_outbound_bytes: int = 1 * 1024 * 1024,
        max_outbound_messages: int = 10_000,
        max_ingress_batch_bytes: int = 1 * 1024 * 1024,
        max_pending_messages: int = 65_536,
        max_pending_callbacks: int = 1_024,
        max_pending_delivery_bytes: int | None = 64 * 1024 * 1024,
        delivery_timeout: float = 1.0,
        callback_shutdown_timeout: float = 5.0,
        message_delivery: MessageDelivery = "auto",
        manual_ack: bool = False,
        store: InflightStore | None = None,
        auth_handler: OnAuth | None = None,
        auth_timeout: float = 10.0,
    ) -> None:
        _validate_client_arguments(
            client_id=client_id,
            username=username,
            password=password,
            message_delivery=message_delivery,
            publish_backpressure=publish_backpressure,
            optional_bounds=(
                ("max_pending_outbound_messages", max_pending_outbound_messages),
                ("max_pending_outbound_bytes", max_pending_outbound_bytes),
                ("max_pending_inbound_bytes", max_pending_inbound_bytes),
                ("max_pending_delivery_bytes", max_pending_delivery_bytes),
            ),
            positive_bounds=(
                ("max_pending_messages", max_pending_messages),
                ("max_pending_callbacks", max_pending_callbacks),
                ("delivery_timeout", delivery_timeout),
                ("callback_shutdown_timeout", callback_shutdown_timeout),
                ("max_outbound_messages", max_outbound_messages),
                ("max_outbound_bytes", max_outbound_bytes),
                ("max_ingress_batch_bytes", max_ingress_batch_bytes),
                ("ack_timeout", ack_timeout),
                ("auth_timeout", auth_timeout),
            ),
            ping_timeout=ping_timeout,
        )
        self._publish_backpressure = publish_backpressure
        effective_max_packet_size = (
            maximum_packet_size if maximum_packet_size is not None else DEFAULT_MAX_PACKET_SIZE
        )
        configured_max_packet_size = (
            connect_properties.get("maximum_packet_size")
            if protocol == MQTTProtocolVersion.MQTTv5 and connect_properties is not None
            else None
        )
        initial_decoder_max_packet_size = (
            configured_max_packet_size
            if isinstance(configured_max_packet_size, int)
            else effective_max_packet_size
        )
        pwd = password.encode("utf-8") if isinstance(password, str) else password
        self._engine = ProtocolEngine(
            EngineConfig(
                client_id=client_id,
                protocol=protocol,
                clean_start=clean_start,
                keepalive=keepalive,
                username=username,
                password=pwd,
                local_receive_maximum=local_receive_maximum,
                max_outbound_inflight=max_outbound_inflight,
                max_pending_outbound_messages=max_pending_outbound_messages,
                max_pending_outbound_bytes=max_pending_outbound_bytes,
                max_pending_inbound_bytes=max_pending_inbound_bytes,
                connect_properties=connect_properties,
                will=will,
                will_properties=will_properties,
                maximum_packet_size=effective_max_packet_size,
                topic_alias_maximum=topic_alias_maximum,
                manual_ack=manual_ack,
                accept_auth=auth_handler is not None,
            ),
            store=store,
        )
        self._decoder = IncrementalDecoder(max_packet_size=initial_decoder_max_packet_size)
        self._max_ingress_batch_bytes = max_ingress_batch_bytes
        self._transport: AsyncTransport | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._keepalive_task: asyncio.Task[None] | None = None
        self._reconnect_task: asyncio.Task[None] | None = None
        self._lifecycle_lock = asyncio.Lock()
        self._engine_lock = asyncio.Lock()
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._connection_epoch = 0
        self._effect_pump = EffectPump(self)
        # Bind the pump operations directly on the client. This keeps existing
        # internal/Paho call sites stable and avoids an extra wrapper frame on
        # the single-effect hot path.
        self._collect_effects_locked = self._effect_pump.collect_from_engine
        self._drain_effects_inline = self._effect_pump.drain_inline
        self._schedule_effect_flush = self._effect_pump.schedule
        self._drain_effects = self._effect_pump.drain
        self._discard_connection_effects = self._effect_pump.discard_connection_effects
        self._write_pump = WritePump(
            max_bytes=max_outbound_bytes,
            max_messages=max_outbound_messages,
            on_failure=self._writer_failed,
        )
        # Bind the queue operations directly, preserving the existing SEND and
        # async enqueue call cost while moving their state to one owner.
        self._can_enqueue_outbound_size = self._write_pump.can_enqueue_size
        self._try_enqueue_outbound = self._write_pump.try_enqueue
        self._try_enqueue_outbound_ack = self._write_pump.try_enqueue_ack
        self._try_enqueue_outbound_many = self._write_pump.try_enqueue_many
        self._enqueue_outbound = self._write_pump.enqueue
        self._enqueue_outbound_ack = self._write_pump.enqueue_ack
        self._delivery = ApplicationDelivery(
            mode=message_delivery,
            protocol=protocol,
            max_pending_messages=max_pending_messages,
            max_pending_callbacks=max_pending_callbacks,
            max_pending_delivery_bytes=max_pending_delivery_bytes,
            maximum_packet_size=initial_decoder_max_packet_size,
            delivery_timeout=delivery_timeout,
            callback_shutdown_timeout=callback_shutdown_timeout,
        )
        # Keep established private test/compatibility seams as direct bound
        # operations while the controller remains the sole state owner.
        self._try_reserve_delivery = self._delivery.try_reserve
        self._release_delivery_reference_nowait = self._delivery.release_nowait
        self._release_delivery_reference = self._delivery.release
        self._delivery_logical_size = self._delivery.logical_size
        self._put_message = self._delivery.put_message
        self._accept_message = self._delivery.acceptor()
        self._accept_decoded_message = self._delivery.decoded_acceptor()
        self._spawn_callback = self._delivery.spawn_callback
        self._try_enqueue_callback = self._delivery.try_enqueue_callback
        self._try_dispatch_callback_inline = self._delivery.try_dispatch_callback_inline
        self._dispatch_callback_inline = self._delivery.dispatch_callback_inline
        self._can_dispatch_callback_inline = self._delivery.can_dispatch_callback_inline
        self._has_callback_capacity = self._delivery.has_callback_capacity
        self._enqueue_callback_repeated_nowait = self._delivery.enqueue_callback_repeated_nowait
        self._enqueue_callback = self._delivery.enqueue_callback
        self._report_callback_error = self._delivery.report_callback_error
        self._shutdown_callback_worker = self._delivery.shutdown_callbacks
        self._invoke = self._delivery.invoke
        self._publish_waiters = 0
        self._publish_waiter_futs: deque[asyncio.Future[None]] = deque()
        self._publish_wakeups = 0
        self._publish_wait_retries = 0
        self._connack_fut: asyncio.Future[ConnAckPacket] | None = None
        self._connect_disconnect_fut: asyncio.Future[int] | None = None
        self._receipts: dict[int, PublishReceipt | deque[PublishReceipt]] = {}
        self._batch_receipts: dict[int, PublishBatchReceipt | deque[PublishBatchReceipt]] = {}
        self._sub_futs: dict[int, asyncio.Future[SubscribeResult]] = {}
        self._unsub_futs: dict[int, asyncio.Future[UnsubscribeResult]] = {}
        self._disconnect_exc: BaseException | None = None
        # Set once by a local-terminal ingress failure (store/persistence error
        # escaping the read-loop batch). While set, the protocol/persistence
        # state is unfit for automatic reconnect or silent reuse: reconnect is
        # suppressed and explicit connect() is refused. Never cleared; the
        # application must create a new client.
        self._local_terminal_failure: BaseException | None = None
        # Set once a connection is torn down, cleared by the next connect. Read
        # by _publish_wait_failure(); _closed is set too late in teardown to use.
        self._teardown_final = False
        self._ping_pending = False
        self._ping_deadline = 0.0
        self._host = ""
        self._port = 1883
        self._ssl: ssl.SSLContext | bool | None = None
        self._unix_path: str | None = None
        self._ws_url: str | None = None
        self._ws_headers: dict[str, str] | None = None
        self._reconnect = reconnect if reconnect is not None else ReconnectPolicy(enabled=False)
        self._ping_timeout = ping_timeout
        self._ack_timeout = ack_timeout
        self._auth_timeout = auth_timeout
        self._intentional_disconnect = False
        self._transport_factory: Callable[..., Awaitable[AsyncTransport]] = TcpTransport.connect
        self._last_disconnect: DisconnectInfo | None = None
        self._last_connack_reason: int | None = None

        self._on_message: OnMessage | None = None
        self._message_callback: OnMessage | None = None
        self._topic_callbacks: TopicMatcher | None = None
        self.on_connect: OnConnect | None = None
        self.on_disconnect: OnDisconnect | None = None
        self.on_publish: OnPublish | None = None
        self.auth_handler: OnAuth | None = auth_handler

    # Diagnostic compatibility views. Effect state has one owner in EffectPump;
    # these historical read-only names remain for tests and instrumentation.
    # Runtime code goes through `self._effect_pump` directly — a descriptor call
    # per publish and per ingress batch is not worth the shorter spelling.
    @property
    def _pending_effects(self) -> deque[EngineEffect]:
        return self._effect_pump.pending

    @property
    def _pending_effect_epoch(self) -> int | None:
        return self._effect_pump.pending_epoch

    @property
    def _effect_enqueued(self) -> int:
        return self._effect_pump.enqueued

    @property
    def _effect_applied(self) -> int:
        return self._effect_pump.applied

    @property
    def _effect_flush_task(self) -> asyncio.Task[None] | None:
        return self._effect_pump.task

    # Historical private delivery views. ApplicationDelivery owns the mutable
    # values; these properties preserve focused tests without duplicating state.
    @property
    def _callback_worker_task(self) -> asyncio.Task[None] | None:
        return self._delivery.callback_task

    @property
    def _messages(self) -> asyncio.Queue[Any]:
        return self._delivery.messages_queue

    @property
    def _callback_queue(self) -> asyncio.Queue[Any]:
        return self._delivery.callback_queue

    @property
    def _message_ready(self) -> asyncio.Event:
        return self._delivery.message_ready

    @property
    def _closed(self) -> asyncio.Event:
        return self._delivery.closed

    @property
    def _pending_delivery_bytes(self) -> int:
        return self._delivery.pending_bytes

    @property
    def _pending_delivery_high_water_bytes(self) -> int:
        return self._delivery.pending_high_water_bytes

    @property
    def _delivery_accounted_limit(self) -> int | None:
        return self._delivery.accounted_limit

    @property
    def _delivery_small_budget_bytes(self) -> int:
        return self._delivery.small_budget_bytes

    @property
    def _delivery_small_message_limit(self) -> int | None:
        return self._delivery.small_message_limit

    # Historical private writer views remain for tests and instrumentation.
    # WritePump is the sole state owner; runtime code does not use these views.
    # `_outbound` is the live asyncio.Queue (`qsize()`, not the resident
    # admission count). Occupying writer capacity must go through try_enqueue.
    @property
    def _writer_task(self) -> asyncio.Task[None] | None:
        return self._write_pump.task

    @property
    def _outbound(self) -> asyncio.Queue[WriteItem]:
        return self._write_pump.queue

    @property
    def _outbound_bytes(self) -> int:
        return self._write_pump.queued_bytes

    @_outbound_bytes.setter
    def _outbound_bytes(self, value: int) -> None:
        self._write_pump.queued_bytes = value

    @property
    def _max_outbound_bytes(self) -> int:
        return self._write_pump.max_bytes

    @_max_outbound_bytes.setter
    def _max_outbound_bytes(self, value: int) -> None:
        self._write_pump.max_bytes = value

    @property
    def _max_outbound_messages(self) -> int:
        return self._write_pump.max_messages

    @_max_outbound_messages.setter
    def _max_outbound_messages(self, value: int) -> None:
        self._write_pump.max_messages = value

    @property
    def _outbound_space(self) -> asyncio.Condition:
        return self._write_pump.space

    @property
    def _outbound_waiters(self) -> int:
        return self._write_pump.waiters

    def stats(self) -> ClientStats:
        """Return an immutable point-in-time runtime snapshot.

        The method only reads already-maintained counters and queue sizes. It
        does not enable sampling, emit logs, or mutate protocol state. Like the
        rest of ``AsyncClient``'s synchronous surface, it is intended for the
        owning event-loop thread.
        """

        def running(task: asyncio.Task[Any] | None) -> bool:
            return task is not None and not task.done()

        publish_receipts = sum(
            len(current) if isinstance(current, deque) else 1 for current in self._receipts.values()
        )
        batch_ids: set[int] = set()
        for current in self._batch_receipts.values():
            batches = current if isinstance(current, deque) else (current,)
            batch_ids.update(id(batch) for batch in batches)

        transport = self._transport
        report = getattr(transport, "stats", None)
        transport_stats = report() if report is not None else TransportStats.unavailable(transport)
        engine = self._engine
        outbound_stats = engine.outbound.stats()
        inbound_stats = engine.inbound.stats()
        effect_pump = self._effect_pump
        write_pump = self._write_pump
        return ClientStats(
            state=self._engine.state,
            connection_epoch=self._connection_epoch,
            reconnect_attempt=self._reconnect.attempt,
            tasks=TaskStats(
                reader=running(self._reader_task),
                writer=running(write_pump.task),
                keepalive=running(self._keepalive_task),
                reconnect=running(self._reconnect_task),
                effect_flush=running(effect_pump.task),
                callback_worker=running(self._callback_worker_task),
            ),
            outbound=outbound_stats,
            inbound=inbound_stats,
            effects=effect_pump.stats(),
            writer=write_pump.stats(),
            decoder=DecoderStats(
                buffered_bytes=self._decoder.buffered,
                high_water_bytes=self._decoder.high_water,
                max_packet_size=self._decoder.max_packet_size,
                ingress_batch_limit_bytes=self._max_ingress_batch_bytes,
            ),
            delivery=self._delivery.stats(),
            receipts=ReceiptStats(
                publish=publish_receipts,
                publish_batches=len(batch_ids),
                subscribe=len(self._sub_futs),
                unsubscribe=len(self._unsub_futs),
                publish_waiters=self._publish_waiters,
            ),
            transport=transport_stats,
        )

    @property
    def state(self) -> ConnectionState:
        """Current protocol connection state."""
        return self._engine.state

    @property
    def is_connected(self) -> bool:
        """Whether a successful CONNACK established the current connection."""
        return self._engine.state == ConnectionState.CONNECTED

    @property
    def negotiated(self) -> NegotiatedSettings:
        """Settings negotiated for the active or most recent connection."""
        return self._engine.negotiated

    @property
    def effective_client_id(self) -> str:
        """Configured client id, or the broker-assigned id when one was supplied."""
        return self._engine.effective_client_id

    @property
    def _last_disconnect_info(self) -> DisconnectInfo | None:
        """Loop-confined disconnect metadata used by compatibility adapters."""
        return self._last_disconnect

    @property
    def _compat_connection_settings(self) -> tuple[str, int, int]:
        """Loop-confined connection target used by the Paho adapter."""
        return self._host, self._port, self._engine.config.keepalive

    def _reconfigure(self, **changes: Any) -> None:
        """Internal loop-confined configuration boundary for adapters."""
        self._engine.reconfigure(**changes)

    def _queue_publish_on_loop(
        self,
        topic: str,
        payload: bytes,
        *,
        qos: int | QoS,
        retain: bool,
        properties: Properties | None = None,
        _prepared: _PreparedPublish | None = None,
    ) -> PublishReceipt:
        """Admit one publish and register its receipt without flushing.

        The caller must already execute synchronously on the client's event loop.
        Keeping finalization separate lets adapters commit a bounded batch and
        collect/drain effects once.
        """
        handle = self._engine.outbound.queue_publish(
            topic,
            payload,
            qos=qos,
            retain=retain,
            properties=properties,
            _prepared=_prepared,
        )
        if handle.qos == QoS.AT_MOST_ONCE:
            return PublishReceipt(mid=None, qos=handle.qos)
        assert handle.mid is not None
        receipt = PublishReceipt(mid=handle.mid, qos=handle.qos)
        _fifo_register(self._receipts, handle.mid, receipt)
        return receipt

    def _direct_qos0_ready(self, callback_count: int = 1) -> bool:
        """True when a native QoS 0 write can bypass the effect adapter safely."""
        if self.on_publish is None:
            return not self._effect_pump.pending and not self._engine.has_pending_effects
        callback = self.on_publish
        assert callback is not None
        if not (
            (
                callback_count == 1
                and not self._engine_lock.locked()
                and self._can_dispatch_callback_inline(callback)
            )
            or self._has_callback_capacity(callback_count)
        ):
            return False
        return not self._effect_pump.pending and not self._engine.has_pending_effects

    def _try_direct_qos0_publish(
        self,
        topic: str,
        payload: bytes,
        *,
        qos: QoS | int,
        retain: bool,
        properties: Properties | None,
        nowait: bool,
    ) -> PublishReceipt | None:
        # Compare before converting: every QoS 1/2 publish reaches this line and
        # would otherwise construct an enum only to be rejected. Invalid values
        # still raise ValueError from _prepare_publish_request downstream.
        if qos != QoS.AT_MOST_ONCE or not self._direct_qos0_ready():
            return None
        callback = self.on_publish
        item = self._engine.outbound.prepare_qos0(
            topic,
            payload,
            retain=retain,
            properties=properties,
        )
        if not self._try_enqueue_outbound(
            item,
            epoch=self._connection_epoch,
        ):
            if nowait:
                raise FlowControlError(self._write_pump.refusal(item_size(item)))
            return None
        if properties is not None and properties.get("topic_alias") is not None:
            self._engine.outbound.commit_topic_alias(topic, properties)
        if callback is not None:
            if self._engine_lock.locked() or not self._try_dispatch_callback_inline(
                callback, None, None
            ):
                self._enqueue_callback_repeated_nowait(callback, (None, None), 1)
        return PublishReceipt(mid=None, qos=QoS.AT_MOST_ONCE)

    def _try_direct_qos0_many(
        self,
        requests: list[tuple[str, bytes, QoS | int, bool, Properties | None]],
        receipt: PublishBatchReceipt,
        *,
        nowait: bool,
    ) -> bool:
        if not requests or not self._direct_qos0_ready(len(requests)):
            return False
        for _topic, _payload, qos, _retain, _properties in requests:
            if qos != QoS.AT_MOST_ONCE:
                return False
            if _properties is not None and _properties.get("topic_alias") is not None:
                # Alias mappings are established in request order. Reuse the
                # ordinary atomic engine batch rather than staging a second
                # connection-scoped mapping beside OutboundSession's owner.
                return False

        items: list[WriteItem] = []
        for topic, payload, _qos, retain, properties in requests:
            items.append(
                self._engine.outbound.prepare_qos0(
                    topic,
                    payload,
                    retain=retain,
                    properties=properties,
                )
            )
        if not self._try_enqueue_outbound_many(items, epoch=self._connection_epoch):
            if nowait:
                raise FlowControlError(self._write_pump.refusal_many(items))
            return False
        callback = self.on_publish
        if callback is not None:
            self._enqueue_callback_repeated_nowait(
                callback,
                (None, None),
                len(requests),
            )
        for _ in requests:
            receipt._register(None)
        return True

    def _queue_qosn_on_loop(
        self,
        topic: str,
        payload: bytes,
        *,
        qos: QoS,
        retain: bool,
        properties: Properties | None = None,
    ) -> PublishReceipt:
        """Admit QoS 1/2 and register its receipt for loop-bound adapters."""
        handle = self._engine.outbound.queue_publish(
            topic,
            payload,
            qos=qos,
            retain=retain,
            properties=properties,
        )
        assert handle.mid is not None
        receipt = PublishReceipt(mid=handle.mid, qos=handle.qos)
        _fifo_register(self._receipts, handle.mid, receipt)
        return receipt

    def _queue_qos0_on_loop(
        self,
        topic: str,
        payload: bytes,
        *,
        retain: bool,
        properties: Properties | None = None,
    ) -> None:
        """Admit QoS 0 without allocating a receipt, for batched adapters."""
        self._engine.outbound.queue_publish(
            topic,
            payload,
            qos=QoS.AT_MOST_ONCE,
            retain=retain,
            properties=properties,
        )

    def _finalize_loop_commands(self) -> None:
        """Collect engine effects and apply/schedule them without suspending."""
        self._collect_effects_locked()
        self._drain_effects_inline()

    def _queue_subscribe_on_loop(
        self,
        topics: str | Iterable[str | tuple[str, SubscribeOptions | int | QoS]],
        *,
        qos: int | QoS = 0,
        properties: Properties | None = None,
    ) -> int:
        return self._engine.queue_subscribe(topics, qos=qos, properties=properties)

    def _queue_unsubscribe_on_loop(self, topics: str | Iterable[str]) -> int:
        return self._engine.queue_unsubscribe(topics)

    async def connect(
        self,
        host: str,
        port: int = 1883,
        *,
        ssl: ssl.SSLContext | bool | None = None,
        timeout: float | None = None,
    ) -> ConnAckPacket:
        """Connect to an MQTT broker over TCP or TLS.

        Args:
            host: Broker hostname or IP address.
            port: Broker TCP port.
            ssl: TLS context, ``True`` for a default context, or ``None`` for
                clear-text TCP.
            timeout: Transport and CONNACK deadline. The reconnect policy's
                connection timeout is used when omitted.

        Returns:
            The successful CONNACK packet and its negotiated properties.

        Raises:
            MQTTTimeoutError: If transport setup or CONNACK exceeds the deadline.
            ProtocolError: If the client is already connecting/connected or the
                broker refuses or violates the protocol.
            MQTTError: If :meth:`disconnect` cancels connection setup.
            MQTTError: If a previous local persistence failure fail-stopped
                this client; create a new one instead of reusing it.
            OSError: If TCP/TLS setup or the initial MQTT CONNECT write fails.
            asyncio.CancelledError: If the calling task is cancelled.
        """
        return await self._connect_explicit(host, port, ssl=ssl, timeout=timeout)

    async def connect_unix(
        self,
        path: str,
        *,
        timeout: float | None = None,
    ) -> ConnAckPacket:
        """Connect over a Unix domain socket.

        Args:
            path: Filesystem path of the broker's AF_UNIX socket.
            timeout: Transport and CONNACK deadline.

        Returns:
            The successful CONNACK packet.

        Raises:
            MQTTTimeoutError: If connection or CONNACK exceeds the deadline.
            ProtocolError: If the broker refuses or violates the protocol.
            MQTTError: If a previous local persistence failure fail-stopped
                this client; create a new one instead of reusing it.
            OSError: If Unix socket setup or the initial MQTT CONNECT write fails.
            asyncio.CancelledError: If the calling task is cancelled.
        """

        async def _factory(
            host: str,
            port: int,
            *,
            ssl: object | None = None,
        ) -> AsyncTransport:
            return await UnixSocketTransport.connect(self._unix_path or host)

        return await self._connect_explicit(
            path, 0, ssl=None, timeout=timeout, unix_path=path, factory=_factory
        )

    async def connect_ws(
        self,
        url: str,
        *,
        ssl: ssl.SSLContext | bool | None = None,
        extra_headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> ConnAckPacket:
        """Connect over MQTT-over-WebSocket.

        Args:
            url: ``ws://`` or ``wss://`` broker endpoint, including its path.
            ssl: TLS context or default-context selector for ``wss://``.
            extra_headers: Additional HTTP headers for the upgrade request.
            timeout: Transport and CONNACK deadline.

        Returns:
            The successful CONNACK packet.

        Raises:
            MQTTTimeoutError: If connection or CONNACK exceeds the deadline.
            ProtocolError: If the broker refuses or violates the MQTT protocol.
            MQTTError: If a previous local persistence failure fail-stopped
                this client; create a new one instead of reusing it.
            ConnectionError: If the WebSocket upgrade, transport, or initial
                MQTT CONNECT write fails.
            ValueError: If the URL or WebSocket options are invalid.
            asyncio.CancelledError: If the calling task is cancelled.
        """

        async def _factory(
            host: str,
            port: int,
            *,
            ssl: object | None = None,
        ) -> AsyncTransport:
            return await WebSocketTransport.connect(
                self._ws_url or host,
                ssl=ssl if ssl is not None else self._ssl,
                extra_headers=self._ws_headers,
            )

        return await self._connect_explicit(
            url,
            0,
            ssl=ssl,
            timeout=timeout,
            ws_url=url,
            ws_headers=extra_headers,
            factory=_factory,
        )

    async def _connect_explicit(
        self,
        host: str,
        port: int,
        *,
        ssl: ssl.SSLContext | bool | None,
        timeout: float | None,
        unix_path: str | None = None,
        ws_url: str | None = None,
        ws_headers: dict[str, str] | None = None,
        factory: Callable[..., Awaitable[AsyncTransport]] | None = None,
    ) -> ConnAckPacket:
        """Shared body of the explicit connect entry points.

        Records the endpoint for later reconnects, replaces any automatic
        generation, and performs one connection attempt under the lifecycle
        lock.
        """
        if self._local_terminal_failure is not None:
            raise MQTTError(
                "Client is unusable after a local persistence failure; "
                "create a new AsyncClient instead of reusing this one"
            )
        async with self._lifecycle_lock:
            await self._prepare_explicit_connect()
            was_alt = self._unix_path is not None or self._ws_url is not None
            self._unix_path = unix_path
            self._ws_url = ws_url
            self._ws_headers = ws_headers
            self._host = host
            self._port = port
            self._ssl = ssl
            if factory is not None:
                self._transport_factory = factory
            elif was_alt:
                # Reclaim the default TCP factory only when leaving an
                # alternative endpoint: an injected factory must survive a
                # plain TCP connect (custom transports rely on this seam).
                self._transport_factory = TcpTransport.connect
            self._intentional_disconnect = False
            timeout = timeout if timeout is not None else self._reconnect.connect_timeout
            self._reconnect.reset()
            return await self._connect_once_locked(host, port, ssl=ssl, timeout=timeout)

    async def _connect_once_locked(
        self,
        host: str,
        port: int,
        *,
        ssl: ssl.SSLContext | bool | None = None,
        timeout: float = 30.0,
        reconnect_attempt: bool = False,
    ) -> ConnAckPacket:
        loop = asyncio.get_running_loop()
        owner_loop = self._owner_loop
        if owner_loop is None:
            self._owner_loop = loop
        elif owner_loop is not loop:
            raise RuntimeError("AsyncClient is bound to a different event loop")
        if self._engine.state in (ConnectionState.CONNECTED, ConnectionState.CONNECTING):
            raise ProtocolError("Already connected or connecting")
        self._last_connack_reason = None
        # Effects belong to a protocol/transport epoch. QoS replay and
        # inbound redelivery are rebuilt from the engine/store after
        # CONNACK; no old effect may cross into the new connection.
        await self._invalidate_connection_epoch()
        self._discard_connection_effects()
        try:
            try:
                transport = await asyncio.wait_for(
                    self._transport_factory(host, port, ssl=ssl), timeout=timeout
                )
            except TimeoutError as exc:
                raise MQTTTimeoutError("Transport connection timed out") from exc
            self._transport = transport
            self._delivery.reopen()
            self._disconnect_exc = None
            self._teardown_final = False
            self._last_disconnect = None
            self._decoder.clear()
            self._write_pump.reset()
            self._ping_pending = False
            connect_packet = self._engine.begin_connect()
            sent_maximum_packet_size = self._engine._sent_maximum_packet_size
            if sent_maximum_packet_size is not None:
                self._decoder.max_packet_size = sent_maximum_packet_size
            self._connack_fut = loop.create_future()
            self._connect_disconnect_fut = loop.create_future()
            self._write_pump.start(transport)
            await self._enqueue_outbound(connect_packet)
            self._reader_task = asyncio.create_task(self._read_loop(), name="mqttium-reader")
            try:
                connack = await self._await_connack_or_disconnect(timeout)
            finally:
                connect_disconnect_fut = self._connect_disconnect_fut
                self._connect_disconnect_fut = None
                if connect_disconnect_fut is not None and not connect_disconnect_fut.done():
                    connect_disconnect_fut.cancel()
            if connack.reason_code != 0:
                refusal = self._disconnect_exc
                if not isinstance(refusal, ProtocolError):
                    refusal = ProtocolError(
                        f"Connection refused: reason_code={connack.reason_code}"
                    )
                    self._disconnect_exc = refusal
                raise refusal
            self._write_pump.last_outbound = time.monotonic()
            self._keepalive_task = asyncio.create_task(
                self._keepalive_loop(), name="mqttium-keepalive"
            )
            return connack
        except BaseException:
            if not reconnect_attempt:
                self._intentional_disconnect = True
            if self._connack_fut is not None and not self._connack_fut.done():
                self._connack_fut.cancel()
            try:
                # A failed attempt inside an active retry loop is transient:
                # keep the application stream and callback worker alive.
                await self._force_close(preserve_reconnect=reconnect_attempt)
            except BaseException:
                pass
            self._retire_engine_connection()
            raise

    async def _await_connack_or_disconnect(self, timeout: float) -> ConnAckPacket:
        connack_fut = self._connack_fut
        disconnect_fut = self._connect_disconnect_fut
        if connack_fut is None or disconnect_fut is None:
            raise RuntimeError("CONNECT wait futures are not initialized")
        done, _ = await asyncio.wait(
            (connack_fut, disconnect_fut),
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if not done:
            raise MQTTTimeoutError("CONNACK timed out")
        if disconnect_fut in done:
            await self._send_connecting_disconnect(disconnect_fut.result())
            raise MQTTError("Connection cancelled by disconnect()")
        return connack_fut.result()

    async def _flush_terminal_packet(self, packet: WriteItem, timeout: float) -> None:
        """Admit a terminal packet and wait, bounded, for the writer to drain it.

        Shutdown must not wait for application traffic to free a bounded queue,
        so a packet that cannot be admitted immediately is dropped rather than
        parked. StaleConnectionEffect is deliberately not caught here: whether a
        dead epoch is an error depends on the caller.
        """
        if not self._try_enqueue_outbound(packet, epoch=self._connection_epoch):
            return
        writer_task = self._write_pump.task
        if writer_task is None or writer_task.done():
            return
        try:
            await asyncio.wait_for(self._write_pump.join(), timeout=timeout)
        except TimeoutError:
            pass

    async def _send_connecting_disconnect(self, reason_code: int) -> None:
        packet = self._engine.begin_disconnect(reason_code)
        await self._flush_terminal_packet(packet, _GRACEFUL_DISCONNECT_DRAIN_TIMEOUT)

    async def disconnect(self, reason_code: int = 0) -> None:
        """Disconnect gracefully and stop connection-scoped tasks.

        The method is idempotent when no transport exists. A legal MQTT
        DISCONNECT is sent when possible; shutdown remains bounded when the
        writer is congested or the peer's packet limit cannot admit it.

        Args:
            reason_code: MQTT 5 DISCONNECT reason code. MQTT 3 clients must use
                the success value.

        Raises:
            ProtocolError: If the reason code is invalid for the protocol.
        """
        self._intentional_disconnect = True
        connect_disconnect_fut = self._connect_disconnect_fut
        disconnecting_connect = (
            self._engine.state is ConnectionState.CONNECTING
            and connect_disconnect_fut is not None
            and not connect_disconnect_fut.done()
        )
        if disconnecting_connect:
            assert connect_disconnect_fut is not None
            self._engine.codec.encode_disconnect(reason_code)
            connect_disconnect_fut.set_result(reason_code)
        else:
            reconnect_task = self._reconnect_task
            if reconnect_task is not None and reconnect_task is not asyncio.current_task():
                reconnect_task.cancel()
                try:
                    await reconnect_task
                except asyncio.CancelledError:
                    pass
                self._reconnect_task = None
        should_close = False
        packet_failure = False
        async with self._lifecycle_lock:
            if disconnecting_connect:
                return
            if self._transport is None:
                # No live transport (e.g. called inside a reconnect gap): the
                # intentional shutdown is still terminal for receipts and the
                # application stream, which the reconnect loop would otherwise
                # keep alive.
                if not self._will_reconnect():
                    await self._terminal_shutdown(self._disconnect_exc or MQTTError("Disconnected"))
                return
            # Preserve validation semantics: an invalid reason code must fail
            # before teardown, just as it did before shutdown became bounded.
            try:
                packet = self._engine.begin_disconnect(reason_code) if self.is_connected else None
            except PacketTooLargeError:
                # The peer's packet limit makes a legal DISCONNECT impossible.
                # Closing the transport is the only conforming shutdown left.
                packet_failure = True
            else:
                should_close = True
                if packet is not None:
                    try:
                        await self._flush_terminal_packet(
                            packet, _GRACEFUL_DISCONNECT_DRAIN_TIMEOUT
                        )
                    except StaleConnectionEffect:
                        pass
        if packet_failure:
            await self._force_close_after_local_packet_failure()
            return
        if should_close:
            # The reader invokes on_disconnect while it terminates. Joining it
            # under the lifecycle lock would deadlock callbacks that reconnect.
            await self._force_close()

    def publish_nowait(
        self,
        topic: str,
        payload: bytes | str = b"",
        *,
        qos: int | QoS = 0,
        retain: bool = False,
        properties: Properties | None = None,
    ) -> PublishReceipt:
        """Queue a publication synchronously on the owning event-loop thread.

        This is the non-suspending counterpart to ``publish(..., nowait=True)``.
        It never waits for engine or writer capacity and raises ``FlowControlError``
        immediately when either bound is full. Like ``asyncio.Queue.put_nowait()``,
        it is loop-bound rather than thread-safe; cross-thread producers need an
        adapter that hands work to the owning loop.

        Args:
            topic: MQTT topic name.
            payload: Bytes or UTF-8 text payload.
            qos: Requested QoS 0, 1, or 2. MQTTium never silently downgrades it.
            retain: Set the MQTT RETAIN flag.
            properties: MQTT 5 PUBLISH properties.

        Returns:
            A receipt with the same completion semantics as :meth:`publish`.

        Raises:
            RuntimeError: If called without a running event loop or from an
                event loop other than the client's owning loop.
            FlowControlError: If logical or writer capacity is unavailable.
            NotConnectedError: If the publication is unavailable in the current
                connection state.
            PacketTooLargeError: If the encoded PUBLISH exceeds a local or
                negotiated packet-size limit.
            ProtocolError: If the topic, properties, or request violates MQTT or
                negotiated broker capabilities.
            ValueError: If ``qos`` is not 0, 1, or 2.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError as exc:
            raise RuntimeError(
                "publish_nowait() must be called from the client's event-loop thread"
            ) from exc
        owner_loop = self._owner_loop
        if owner_loop is None:
            self._owner_loop = loop
        elif owner_loop is not loop:
            raise RuntimeError("AsyncClient is bound to a different event loop")
        data = payload.encode("utf-8") if isinstance(payload, str) else payload
        direct = self._try_direct_qos0_publish(
            topic,
            data,
            qos=qos,
            retain=retain,
            properties=properties,
            nowait=True,
        )
        if direct is not None:
            return direct
        prepared = self._check_nowait_publish_capacity(topic, data, qos, retain, properties)
        receipt = self._queue_publish_on_loop(
            topic,
            data,
            qos=qos,
            retain=retain,
            properties=properties,
            _prepared=prepared,
        )
        self._finalize_loop_commands()
        return receipt

    async def publish(
        self,
        topic: str,
        payload: bytes | str = b"",
        *,
        qos: int | QoS = 0,
        retain: bool = False,
        properties: Properties | None = None,
        nowait: bool = False,
    ) -> PublishReceipt:
        """Publish one application message.

        Args:
            topic: MQTT topic name.
            payload: Bytes or UTF-8 text payload.
            qos: Requested QoS 0, 1, or 2. MQTTium never silently downgrades it.
            retain: Set the MQTT RETAIN flag.
            properties: MQTT 5 PUBLISH properties.
            nowait: Reject immediately instead of waiting for logical or writer
                capacity.

        Returns:
            A receipt. QoS 0 receipts are complete on successful handoff; QoS
            1/2 receipts complete after the protocol exchange. Await
            :meth:`PublishReceipt.wait` when acknowledgement is required.

        Raises:
            FlowControlError: If capacity is unavailable in non-waiting mode,
                or the request can never fit configured bounds.
            NotConnectedError: If publication is unavailable in the current state.
            ProtocolError: If the request violates protocol or negotiated limits.
            asyncio.CancelledError: If a waiting producer is cancelled; no new
                publication state is retained for the cancelled request.
        """
        data = payload.encode("utf-8") if isinstance(payload, str) else payload
        while True:
            waiter: asyncio.Future[None] | None = None
            async with self._engine_lock:
                try:
                    direct = self._try_direct_qos0_publish(
                        topic,
                        data,
                        qos=qos,
                        retain=retain,
                        properties=properties,
                        nowait=nowait,
                    )
                    if direct is not None:
                        return direct
                    prepared = None
                    if nowait:
                        prepared = self._check_nowait_publish_capacity(
                            topic, data, qos, retain, properties
                        )
                    # Keep the native async hot path inline. Routing these
                    # operations through the adapter boundary measured 2.36% slower.
                    handle = self._engine.outbound.queue_publish(
                        topic,
                        data,
                        qos=qos,
                        retain=retain,
                        properties=properties,
                        _prepared=prepared,
                    )
                    if handle.qos == QoS.AT_MOST_ONCE:
                        receipt = PublishReceipt(mid=None, qos=handle.qos)
                    else:
                        assert handle.mid is not None
                        receipt = PublishReceipt(mid=handle.mid, qos=handle.qos)
                        _fifo_register(self._receipts, handle.mid, receipt)
                except FlowControlError as flow_exc:
                    if (
                        nowait
                        or self._publish_backpressure == "error"
                        or not self._engine.can_ever_admit_publish(topic, data, qos, properties)
                    ):
                        raise
                    terminal = self._publish_wait_failure()
                    if terminal is not None:
                        raise terminal from flow_exc
                    waiter = self._register_publish_waiter()
                else:
                    self._collect_effects_locked()
                    self._drain_effects_inline()
            if waiter is None:
                if self._effect_pump.pending:
                    if nowait:
                        self._schedule_effect_flush()
                    else:
                        await self._drain_effects()
                if not nowait and receipt.qos != QoS.AT_MOST_ONCE:
                    self._write_pump._try_flush_latency_batch()
                return receipt
            await self._wait_publish_space(waiter)

    async def _admit_publish_many(
        self,
        requests: list[tuple[str, bytes, QoS | int, bool, Properties | None]],
        receipt: PublishBatchReceipt,
        *,
        nowait: bool,
    ) -> None:
        while True:
            waiter: asyncio.Future[None] | None = None
            async with self._engine_lock:
                try:
                    if self._try_direct_qos0_many(requests, receipt, nowait=nowait):
                        return
                    if nowait:
                        self._check_nowait_publish_many_capacity(requests)
                    handles = self._engine.outbound.queue_publish_many(requests)
                except FlowControlError as flow_exc:
                    if (
                        nowait
                        or self._publish_backpressure == "error"
                        or not self._engine.can_ever_admit_publish_many(requests)
                    ):
                        raise
                    terminal = self._publish_wait_failure()
                    if terminal is not None:
                        raise terminal from flow_exc
                    waiter = self._register_publish_waiter()
                else:
                    for handle in handles:
                        receipt._register(handle.mid)
                        if handle.mid is not None:
                            _fifo_register(self._batch_receipts, handle.mid, receipt)
                    self._collect_effects_locked()
                    self._drain_effects_inline()
                    return
            if waiter is not None:
                await self._wait_publish_space(waiter)

    async def publish_many(
        self,
        messages: Iterable[PublishMessage],
        *,
        chunk_size: int = 256,
        nowait: bool = False,
        max_failure_details: int | None = 128,
        failure_sink: Callable[[int, BaseException], None] | None = None,
    ) -> PublishBatchReceipt:
        """Publish a batch with bounded memory and aggregate completion.

        QoS 0 avoids one lock/effect flush per message. QoS 1/2 use one shared
        receipt and continuously refill the negotiated inflight window without
        creating waiter tasks for individual packet identifiers.

        Args:
            messages: Iterable of :class:`PublishMessage` values.
            chunk_size: Maximum number of input entries admitted as one atomic
                chunk.
            nowait: Reject a chunk immediately instead of waiting for capacity.
            max_failure_details: Maximum individual completion failures retained
                by the receipt, or ``None`` for no limit.
            failure_sink: Optional synchronous observer called for every
                completion failure with its zero-based input index.

        Returns:
            A bounded aggregate receipt after the input iterable has been
            consumed and admitted. Await :meth:`PublishBatchReceipt.wait` for
            protocol completion.

        Raises:
            ValueError: If ``chunk_size`` is not positive or
                ``max_failure_details`` is negative.
            TypeError: If ``messages`` is not iterable.
            PublishBatchError: If iterating or admitting the batch fails. Its
                ``cause`` is the original exception, and ``receipt`` describes
                work admitted before that failure.
            asyncio.CancelledError: If submission is cancelled. Publications
                already admitted remain active and are not rolled back.
        """
        if chunk_size <= 0:
            raise ValueError("chunk_size must be greater than 0")
        receipt = PublishBatchReceipt(
            max_failure_details=max_failure_details,
            failure_sink=failure_sink,
        )
        iterator = iter(messages)
        flow_limit = self._engine.flow.limit
        # Bound retained QoS state to one active protocol window plus one
        # submission chunk. This keeps memory independent of total iterable size
        # while allowing the next chunk to refill the window continuously.
        pending_limit = flow_limit + chunk_size

        try:
            while True:
                chunk = list(islice(iterator, chunk_size))
                if not chunk:
                    break
                target = max(flow_limit, pending_limit - len(chunk))
                await receipt._wait_pending_at_most(target)

                requests: list[tuple[str, bytes, QoS | int, bool, Properties | None]] = []
                for message in chunk:
                    if not isinstance(message, PublishMessage):
                        raise TypeError("publish_many entries must be PublishMessage instances")
                    payload = (
                        message.payload.encode("utf-8")
                        if isinstance(message.payload, str)
                        else message.payload
                    )
                    requests.append(
                        (
                            message.topic,
                            payload,
                            message.qos,
                            message.retain,
                            message.properties,
                        )
                    )

                await self._admit_publish_many(
                    requests,
                    receipt,
                    nowait=nowait,
                )
                if nowait:
                    self._schedule_effect_flush()
                else:
                    await self._drain_effects()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            receipt._seal()
            raise PublishBatchError(
                receipt.failures,
                failure_count=receipt.failure_count,
                failure_counts=dict(receipt.failure_counts),
                cause=exc,
                receipt=receipt,
            ) from exc

        receipt._seal()
        return receipt

    async def auth(
        self,
        reason_code: int = 0x19,
        properties: Properties | None = None,
    ) -> None:
        """Send a client AUTH packet (MQTT 5 continue / re-authenticate)."""
        async with self._engine_lock:
            if self.auth_handler is None:
                raise MQTTError("auth() requires an auth_handler")
            self._engine.queue_auth(reason_code=reason_code, properties=properties)
            self._collect_effects_locked()
        await self._drain_effects()

    def set_auth_handler(self, handler: OnAuth | None) -> None:
        """Register or clear the enhanced-authentication handler.

        A handler-raised :class:`asyncio.CancelledError` is treated as an
        authentication failure. Cancellation requested on MQTTium's owning
        task still propagates normally.
        """
        self.auth_handler = handler
        self._engine.reconfigure(accept_auth=handler is not None)

    @property
    def on_message(self) -> OnMessage | None:
        """Default callback used when no topic-specific callback matches."""
        return self._on_message

    @on_message.setter
    def on_message(self, callback: OnMessage | None) -> None:
        self._on_message = callback
        if self._topic_callbacks is None:
            self._message_callback = callback

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
            matcher[topic_filter] = callback
            self._topic_callbacks = matcher
            self._message_callback = self._dispatch_topic_message
            return
        matcher[topic_filter] = callback

    def message_callback_remove(self, topic_filter: str) -> None:
        """Remove the callback registered for ``topic_filter``, if any."""
        matcher = self._topic_callbacks
        if matcher is None:
            return
        try:
            del matcher[topic_filter]
        except KeyError:
            return
        if not matcher:
            self._topic_callbacks = None
            self._message_callback = self._on_message

    def _dispatch_topic_message(self, message: Message) -> Any:
        """Dispatch matching callbacks, or the current default callback."""
        matcher = self._topic_callbacks
        if matcher is not None:
            callbacks = matcher.iter_match(message.topic)
            matched = False
            for callback in callbacks:
                matched = True
                try:
                    result = callback(message)
                except asyncio.CancelledError as exc:
                    self._delivery._propagate_callback_cancellation(callback, exc)
                    continue
                except Exception as exc:
                    self._report_callback_error(callback, exc)
                    continue
                if isinstance(result, Awaitable):
                    return self._continue_topic_callbacks(
                        callback, result, tuple(callbacks), message
                    )
            if matched:
                return None

        callback = self._on_message
        if callback is None:
            return None
        try:
            result = callback(message)
        except asyncio.CancelledError as exc:
            self._delivery._propagate_callback_cancellation(callback, exc)
            return None
        except Exception as exc:
            self._report_callback_error(callback, exc)
            return None
        if isinstance(result, Awaitable):
            return self._continue_topic_callbacks(callback, result, (), message)
        return None

    async def _continue_topic_callbacks(
        self,
        callback: OnMessage,
        result: Awaitable[Any],
        callbacks: Iterable[OnMessage],
        message: Message,
    ) -> None:
        try:
            await result
        except asyncio.CancelledError as exc:
            self._delivery._propagate_callback_cancellation(callback, exc)
        except Exception as exc:
            self._report_callback_error(callback, exc)
        for callback in callbacks:
            try:
                result = callback(message)
                if isinstance(result, Awaitable):
                    await result
            except asyncio.CancelledError as exc:
                self._delivery._propagate_callback_cancellation(callback, exc)
            except Exception as exc:
                self._report_callback_error(callback, exc)

    async def subscribe(
        self,
        topics: str | Iterable[str | tuple[str, SubscribeOptions | int | QoS]],
        *,
        qos: int | QoS = 0,
        properties: Properties | None = None,
        timeout: float | None = None,
    ) -> SubscribeResult:
        """Subscribe to one or more topic filters and wait for SUBACK.

        Args:
            topics: One filter, an iterable of filters, or filter/options pairs.
            qos: Default maximum QoS for plain string filters.
            properties: MQTT 5 SUBSCRIBE properties.
            timeout: SUBACK deadline; ``ack_timeout`` is used when omitted.

        Returns:
            Packet identifier and broker reason codes in request order.

        Raises:
            MQTTTimeoutError: If SUBACK does not arrive before the deadline.
            ProtocolError: If a filter, option, property, or negotiated limit is
                invalid.
            NotConnectedError: If the client cannot submit the request.
        """
        loop = asyncio.get_running_loop()
        async with self._engine_lock:
            mid = self._engine.queue_subscribe(
                topics,
                qos=qos,
                properties=properties,
            )
            fut: asyncio.Future[SubscribeResult] = loop.create_future()
            self._sub_futs[mid] = fut
            self._collect_effects_locked()
        return await self._await_request_ack(fut, self._sub_futs, mid, timeout, "SUBACK")

    async def unsubscribe(
        self,
        topics: str | Iterable[str],
        *,
        timeout: float | None = None,
    ) -> UnsubscribeResult:
        """Unsubscribe from one or more topic filters and wait for UNSUBACK.

        Args:
            topics: One topic filter or an iterable of filters.
            timeout: UNSUBACK deadline; ``ack_timeout`` is used when omitted.

        Returns:
            Packet identifier and MQTT 5 reason codes. MQTT 3.1.1 returns an
            empty reason-code tuple.

        Raises:
            MQTTTimeoutError: If UNSUBACK does not arrive before the deadline.
            ProtocolError: If a filter is invalid.
            NotConnectedError: If the client cannot submit the request.
        """
        loop = asyncio.get_running_loop()
        async with self._engine_lock:
            mid = self._engine.queue_unsubscribe(topics)
            fut: asyncio.Future[UnsubscribeResult] = loop.create_future()
            self._unsub_futs[mid] = fut
            self._collect_effects_locked()
        return await self._await_request_ack(fut, self._unsub_futs, mid, timeout, "UNSUBACK")

    async def _await_request_ack(
        self,
        fut: asyncio.Future[_AckResultT],
        futs: dict[int, asyncio.Future[_AckResultT]],
        mid: int,
        timeout: float | None,
        ack_name: str,
    ) -> _AckResultT:
        """Flush effects and await one registered SUBACK/UNSUBACK future."""
        await self._drain_effects()
        try:
            return await asyncio.wait_for(
                fut, timeout=timeout if timeout is not None else self._ack_timeout
            )
        except TimeoutError as exc:
            futs.pop(mid, None)
            raise MQTTTimeoutError(f"{ack_name} timed out for mid={mid}") from exc

    def messages(self) -> AsyncIterator[Message]:
        """Return the delivered-message iterator for the current generation.

        Automatic reconnect keeps the current iterator alive on the replacement
        transport. A terminal disconnect ends it. A later explicit
        :meth:`connect`, :meth:`connect_unix`, or :meth:`connect_ws` starts a new
        generation; an iterator from the previous generation remains terminal,
        so call ``messages()`` again after that explicit connection.

        The delivery controller's iterator is returned directly rather than
        re-yielded, avoiding one generator resume and suspend per message.
        """
        return self._delivery.messages()

    async def ack(self, message: Message) -> None:
        """Acknowledge an inbound QoS>0 message when ``manual_ack=True``.

        Defers PUBACK (QoS 1) or PUBCOMP (QoS 2). PUBREC is always immediate.

        Args:
            message: Message previously delivered by this client's current
                session. A QoS 0 message has no packet identifier and is a no-op.

        Raises:
            NotConnectedError: If a message with a packet identifier is
                acknowledged without an active connection.
            ProtocolError: If manual acknowledgement is disabled or the packet
                identifier is not awaiting application acknowledgement.
            PacketTooLargeError: If the broker's negotiated packet limit cannot
                carry the mandatory acknowledgement. The connection is closed.
            asyncio.CancelledError: If the caller is cancelled while effects are
                flushing; already accepted acknowledgement state is not rolled
                back.
        """
        if message.mid is None:
            return
        try:
            async with self._engine_lock:
                self._engine.ack(message.mid)
                self._collect_effects_locked()
        except PacketTooLargeError as exc:
            # A broker limit below the mandatory ACK size makes this QoS
            # exchange impossible to complete without violating negotiation.
            self._disconnect_exc = exc
            self._intentional_disconnect = True
            await self._force_close_after_local_packet_failure()
            raise
        await self._drain_effects()

    def _process_ingress_batch(self) -> tuple[int, int, bool]:
        """Decode until a byte/count bound or an auto-PUBACK handoff boundary."""
        decoder = self._decoder
        engine = self._engine
        handle_raw = engine.handle_raw
        inbound = engine.inbound
        max_bytes = self._max_ingress_batch_bytes
        count = 0
        decoded_bytes = 0
        for _ in range(256):
            packet = decoder.next_packet()
            if packet is None:
                break
            handle_raw(packet)
            count += 1
            decoded_bytes += len(packet.remaining) + 5
            # Auto-PUBACK slots remain owned until take_effects(). Stop exactly
            # when their batch fills the remaining Receive Maximum window, so
            # the effect handoff below can release them before another PUBLISH.
            # Control packets and QoS 0 traffic retain the full 256-packet batch.
            if inbound._autoack_handoff_required:
                return count, decoded_bytes, True
            if decoded_bytes >= max_bytes:
                break
        return count, decoded_bytes, False

    def _process_direct_qos0_batch(  # noqa: C901
        self,
    ) -> tuple[int, int, bool, list[Message], list[int | None] | None]:
        decoder = self._decoder
        peek_packet_bounds = decoder.peek_packet_bounds
        consume_peeked_packet = decoder.consume_peeked_packet
        decoder_buffer = decoder._buf
        engine = self._engine
        inbound = engine.inbound
        protocol = engine.config.protocol
        captured: list[Message] = []
        captured_property_sizes: list[int | None] | None = (
            [] if protocol is MQTTProtocolVersion.MQTTv5 else None
        )
        max_bytes = self._max_ingress_batch_bytes
        count = 0
        decoded_bytes = 0
        handoff_required = False

        def materialize_captured() -> None:
            if not captured:
                return
            _extend_message_effects(engine._effects, captured, captured_property_sizes)
            if captured_property_sizes is not None:
                captured_property_sizes.clear()
            captured.clear()

        for _ in range(256):
            try:
                bounds = peek_packet_bounds()
            except (MalformedPacketError, PacketTooLargeError):
                materialize_captured()
                raise
            if bounds is None:
                break
            header, body_start, body_end = bounds
            is_qos0_publish = (header & 0xF0) == PacketType.PUBLISH and ((header >> 1) & 0x03) == 0
            if is_qos0_publish and engine.state is ConnectionState.CONNECTED:
                flags = header & 0x0F
                body_size = body_end - body_start
                try:
                    if protocol is MQTTProtocolVersion.MQTTv311:
                        message = decode_qos0_message_v311_borrowed(
                            decoder_buffer, body_start, body_end, flags
                        )
                        wire_size = None
                    else:
                        message, property_wire_size = decode_qos0_message_v5_borrowed(
                            decoder_buffer, body_start, body_end, flags
                        )
                        properties = message.properties
                        assert properties is not None
                        if not message.topic or "topic_alias" in properties.values:
                            raise _DirectQos0Fallback
                        wire_size = property_wire_size if properties.values else None
                except _DirectQos0Fallback:
                    materialize_captured()
                    packet = decoder.next_packet()
                    assert packet is not None
                    engine.handle_raw(packet)
                except MalformedPacketError:
                    materialize_captured()
                    packet = decoder.next_packet()
                    assert packet is not None
                    engine.handle_raw(packet)
                else:
                    consume_peeked_packet(body_end)
                    captured.append(message)
                    if captured_property_sizes is not None:
                        captured_property_sizes.append(wire_size)
                count += 1
                decoded_bytes += body_size + 5
            else:
                materialize_captured()
                packet = decoder.next_packet()
                if packet is None:
                    break
                engine.handle_raw(packet)
                count += 1
                decoded_bytes += len(packet.remaining) + 5
            if inbound._autoack_handoff_required:
                handoff_required = True
                break
            if decoded_bytes >= max_bytes:
                break
        if engine._effects:
            materialize_captured()
        return count, decoded_bytes, handoff_required, captured, captured_property_sizes

    async def _read_loop(self) -> None:  # noqa: C901
        assert self._transport is not None
        direct_qos0_mode = self._delivery.mode in (
            "auto",
            "callback",
        ) and self._engine.config.protocol in (
            MQTTProtocolVersion.MQTTv311,
            MQTTProtocolVersion.MQTTv5,
        )
        try:
            while not self._transport.is_closing():
                data = await self._transport.read(256 * 1024)
                if not data:
                    break
                self._decoder.feed(data)
                # Process one bounded packet batch at a time. Applying its
                # effects before decoding the next batch propagates delivery
                # byte backpressure all the way to transport.read().
                while True:
                    async with self._engine_lock:
                        captured: list[Message] = []
                        captured_property_sizes: list[int | None] | None = None
                        with self._engine.store.batch():
                            effect_start = len(self._engine._effects)
                            try:
                                header = getattr(self._decoder, "next_header_byte", None)
                                if (
                                    direct_qos0_mode
                                    and (
                                        self._delivery.mode == "callback"
                                        or self._message_callback is not None
                                    )
                                    and not self._effect_pump.pending
                                    and self._engine.state is ConnectionState.CONNECTED
                                    and header is not None
                                    and (header & 0xF0) == PacketType.PUBLISH
                                    and ((header >> 1) & 0x03) == 0
                                ):
                                    (
                                        handled,
                                        handled_bytes,
                                        handoff_required,
                                        captured,
                                        captured_property_sizes,
                                    ) = self._process_direct_qos0_batch()
                                else:
                                    handled, handled_bytes, handoff_required = (
                                        self._process_ingress_batch()
                                    )
                            except (
                                MandatoryResponseTooLargeError,
                                PacketTooLargeError,
                                MalformedPacketError,
                                ProtocolError,
                            ):
                                raise
                            except Exception as exc:
                                # Local failure (store/persistence error): fail-stop.
                                # Latch first so reconnect and reuse decisions see
                                # it, then keep only terminal publish outcomes
                                # among this lot's new effects — anything else was
                                # never durably committed and must not be exposed —
                                # and let the original exception propagate.
                                self._local_terminal_failure = exc
                                effects = self._engine._effects
                                kept = [
                                    effect
                                    for effect in effects[effect_start:]
                                    if effect.kind
                                    in (
                                        EffectKind.PUBLISH_COMPLETE,
                                        EffectKind.PUBLISH_FAILED,
                                    )
                                ]
                                del effects[effect_start:]
                                effects.extend(kept)
                                raise
                        if captured:
                            if self._delivery.deliver_callback_messages_inline(
                                captured, self._message_callback, captured_property_sizes
                            ):
                                self._effect_pump.record_inline_batch(len(captured))
                            else:
                                _extend_message_effects(
                                    self._engine._effects, captured, captured_property_sizes
                                )
                        if handled and self._engine.has_pending_effects:
                            self._collect_effects_locked()
                    if self._effect_pump.pending:
                        await self._drain_effects()
                    # A batch that stopped short of both bounds emptied the
                    # buffer, so there is nothing to decode until the next
                    # read(). Re-entering only to observe handled == 0 cost a
                    # second lock acquisition and bounded decode per read.
                    if (
                        not handoff_required
                        and handled < 256
                        and handled_bytes < self._max_ingress_batch_bytes
                    ):
                        break
                    await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise
        except MandatoryResponseTooLargeError as exc:
            # The broker negotiated a legal limit, but mqttium cannot produce
            # the mandatory automatic response within it. This is a local
            # terminal capability failure, not a peer protocol violation.
            self._disconnect_exc = exc
            self._fail_pending(exc)
        except (PacketTooLargeError, MalformedPacketError, ProtocolError) as exc:
            # Fatal wire/protocol error: send a normative DISCONNECT (v5) before
            # tearing down, so a strict broker sees *why* we left.
            await self._send_fatal_disconnect(exc)
            self._disconnect_exc = exc
            if not self._will_reconnect():
                self._fail_pending(exc)
        except Exception as exc:
            self._disconnect_exc = exc
            if self._local_terminal_failure is None and not self._will_reconnect():
                # Terminal publish effects preserved from a failed lot are
                # applied by the finally drain below; failing receipts here
                # would poison them first. Latched local failures therefore
                # defer to that terminal settlement.
                self._fail_pending(exc)
        finally:
            clean_disconnect = (
                self._intentional_disconnect
                and self._engine.state in (ConnectionState.CONNECTED, ConnectionState.DISCONNECTING)
                and self._disconnect_exc is None
            )
            await self._invalidate_connection_epoch()
            # Retire protocol-visible ownership before joining any child task.
            # Keepalive cancellation can suspend, and while it does callers
            # must not observe the dead transport's engine as CONNECTED and
            # admit work that a following clean reconnect will discard.
            async with self._engine_lock:
                self._engine.notify_transport_closed()
                self._collect_effects_locked()
            # The keepalive loop belongs to this reader's transport epoch. An
            # EOF or reader-side failure can end the reader without entering
            # _force_close(), so retire the task here before a reconnect can
            # replace its reference with a new epoch's keepalive owner.
            keepalive = self._keepalive_task
            if keepalive is not None and keepalive is not asyncio.current_task():
                if not keepalive.done():
                    keepalive.cancel()
                try:
                    await keepalive
                except (asyncio.CancelledError, Exception):
                    pass
                if self._keepalive_task is keepalive:
                    self._keepalive_task = None
            try:
                await self._drain_effects()
            except (Exception, asyncio.CancelledError):
                pass
            if self._disconnect_exc is None:
                self._disconnect_exc = MQTTError("Connection closed")
            self._fail_non_replayable(self._disconnect_exc)
            will_reconnect = self._will_reconnect()
            if not will_reconnect:
                self._fail_pending(self._disconnect_exc)
                # Wake any publish() parked on outbound backpressure.
                await self._write_pump.wake_waiters()
                # Cancel writer + close transport so no task/fd leaks.
                await self._write_pump.stop()
                if self._transport is not None:
                    try:
                        await self._transport.close()
                    except Exception:
                        pass
            if not will_reconnect:
                # A reconnectable loss must not terminate the application
                # message stream: the same iterator resumes after reconnect.
                self._delivery.close()
            try:
                callback_error = None if clean_disconnect else self._disconnect_exc
                await self._invoke(self.on_disconnect, callback_error)
            except Exception as exc:
                self._report_callback_error(self.on_disconnect, exc)
            # The callback may have disconnected or installed an explicit
            # replacement connection. Do not apply the pre-callback reconnect
            # decision to state now owned by the application.
            user_took_over = self._intentional_disconnect or (
                self.is_connected and self._reader_task is not asyncio.current_task()
            )
            if not user_took_over:
                if not will_reconnect:
                    await self._shutdown_callback_worker(drain=True)
                if will_reconnect and (self._reconnect_task is None or self._reconnect_task.done()):
                    self._reconnect_task = asyncio.create_task(
                        self._reconnect_loop(), name="mqttium-reconnect"
                    )

    async def _terminal_shutdown(self, exc: BaseException) -> None:
        """Fail pending work and close the application stream, terminally.

        ``_fail_pending`` already marks teardown final and wakes parked
        publishers; this adds the callback-worker drain and stream close that
        every terminal path shares.
        """
        self._fail_pending(exc)
        await self._shutdown_callback_worker(drain=True)
        self._delivery.close()

    def _retry_reason(self) -> int | None:
        """The reason code the reconnect policy judges: broker DISCONNECT, else CONNACK."""
        if self._last_disconnect is not None and self._last_disconnect.from_broker:
            return self._last_disconnect.reason_code
        return self._last_connack_reason

    def _will_reconnect(self) -> bool:
        reason = self._retry_reason()
        return (
            self._local_terminal_failure is None
            and not isinstance(
                self._disconnect_exc,
                (MessageDeliveryError, MandatoryResponseTooLargeError, AssertionError),
            )
            and not self._intentional_disconnect
            and self._reconnect.enabled
            and self._reconnect.should_retry(reason, self._engine.config.protocol)
        )

    async def _close_transport_after_connection_failure(self) -> None:
        """Unblock the reader without letting teardown errors replace the cause."""
        transport = self._transport
        if transport is None:
            return
        try:
            await transport.close()
        except Exception:
            # Some transports may fail while closing before read() is released.
            # The reader owns connection teardown, so cancellation is the safe
            # fallback. Never await it here: the reader can in turn stop the
            # writer task that is executing this failure handler.
            reader = self._reader_task
            if reader is not None and reader is not asyncio.current_task() and not reader.done():
                reader.cancel()

    async def _writer_failed(self, exc: BaseException) -> None:
        """Hand a writer failure to the reader-owned connection lifecycle."""
        self._disconnect_exc = exc
        connack_fut = self._connack_fut
        if connack_fut is not None and not connack_fut.done():
            connack_fut.set_exception(exc)
        await self._close_transport_after_connection_failure()

    def _preview_publish_size(
        self,
        topic: str,
        payload: bytes,
        qos: QoS | int,
        properties: Properties | None,
    ) -> int:
        return self._engine.outbound.publish_wire_size(topic, len(payload), qos, properties)

    def _check_nowait_publish_capacity(
        self,
        topic: str,
        payload: bytes,
        qos: QoS | int,
        retain: bool,
        properties: Properties | None,
    ) -> _PreparedPublish | None:
        if self._engine.state != ConnectionState.CONNECTED:
            # Preserve validation order: invalid QoS raises before the later
            # connection-state guard, as it did when every preflight converted.
            QoS(qos)
            return None
        if qos != QoS.AT_MOST_ONCE and self._engine.flow.available <= 0:
            QoS(qos)
            return None
        # An empty writer (no resident frames, no charged bytes) admits a
        # single item of any size, so its size cannot change the answer.
        # Sizing it anyway measured the topic and, on MQTT 5 with properties,
        # encoded the property table a second time -- queue_publish encodes it
        # again immediately afterwards. qsize()==0 is not enough: an in-flight
        # writer batch has already left the queue but still occupies the bound.
        if not self._write_pump.resident_messages and not self._write_pump.queued_bytes:
            return None
        if qos == QoS.AT_MOST_ONCE:
            size = self._preview_publish_size(topic, payload, qos, properties)
            if not self._can_enqueue_outbound_size(size):
                raise FlowControlError(self._write_pump.refusal(size))
            return None
        prepared = self._engine.outbound._prepare_publish_request(
            topic,
            payload,
            qos,
            retain,
            properties,
            include_wire_size=True,
        )
        prepared_size = prepared[5]
        assert prepared_size is not None
        if not self._can_enqueue_outbound_size(prepared_size):
            raise FlowControlError(self._write_pump.refusal(prepared_size))
        return prepared

    def _check_nowait_publish_many_capacity(
        self,
        requests: list[tuple[str, bytes, QoS | int, bool, Properties | None]],
    ) -> None:
        if self._engine.state != ConnectionState.CONNECTED:
            return
        messages = self._write_pump.resident_messages
        bytes_used = self._write_pump.queued_bytes
        flow_available = self._engine.flow.available
        for topic, payload, qos, _retain, properties in requests:
            level = QoS(qos)
            if level != QoS.AT_MOST_ONCE:
                if flow_available <= 0:
                    continue
                flow_available -= 1
            size = self._preview_publish_size(topic, payload, level, properties)
            if not self._can_enqueue_outbound_size(
                size,
                queued_messages=messages,
                queued_bytes=bytes_used,
            ):
                raise FlowControlError(
                    self._write_pump.refusal(
                        size,
                        queued_messages=messages,
                        queued_bytes=bytes_used,
                    )
                )
            messages += 1
            bytes_used += size

    async def _keepalive_loop(self) -> None:
        try:
            while not self._closed.is_set():
                k = self._effective_keepalive()
                if k <= 0:
                    await asyncio.sleep(1.0)
                    continue
                now = time.monotonic()
                if self._ping_pending:
                    if now >= self._ping_deadline:
                        self._disconnect_exc = MQTTTimeoutError("PINGRESP timed out")
                        # The reader is the single owner of connection teardown:
                        # it emits on_disconnect once and decides reconnect vs
                        # terminal delivery shutdown after the transport breaks.
                        await self._close_transport_after_connection_failure()
                        return
                    await asyncio.sleep(min(0.5, self._ping_deadline - now))
                    continue
                due = self._write_pump.last_outbound + k
                if now >= due:
                    try:
                        async with self._engine_lock:
                            self._engine.queue_ping()
                            self._collect_effects_locked()
                    except PacketTooLargeError as exc:
                        # A broker limit below the two-byte PINGREQ leaves no
                        # conforming keepalive packet to send.
                        self._disconnect_exc = exc
                        self._intentional_disconnect = True
                        await self._close_transport_after_connection_failure()
                        return
                    # A lost PINGREQ beats a wedged keepalive under backpressure.
                    try:
                        await self._drain_effects(nowait=True)
                    except FlowControlError:
                        pass
                    self._ping_pending = True
                    ping_to = self._ping_timeout
                    if ping_to is None:
                        ping_to = max(k / 2, 5.0)
                    self._ping_deadline = now + ping_to
                else:
                    await asyncio.sleep(min(1.0, due - now))
        except asyncio.CancelledError:
            raise

    async def _reconnect_loop(self) -> None:
        try:
            while self._reconnect.enabled and not self._intentional_disconnect:
                reason = self._retry_reason()
                if not self._reconnect.should_retry(reason, self._engine.config.protocol):
                    # Retry budget exhausted: the stream must terminate, not
                    # park forever now that transient paths keep it open.
                    await self._terminal_shutdown(
                        self._disconnect_exc or MQTTError("Reconnect exhausted")
                    )
                    return
                delay = self._reconnect.next_delay()
                await asyncio.sleep(delay)
                try:
                    async with self._lifecycle_lock:
                        if self._intentional_disconnect:
                            return
                        await self._force_close(preserve_reconnect=True)
                        await self._connect_once_locked(
                            self._host,
                            self._port,
                            ssl=self._ssl,
                            timeout=self._reconnect.connect_timeout,
                            reconnect_attempt=True,
                        )
                    # Only clear backoff after the connection stays up.
                    await asyncio.sleep(self._reconnect.stable_after)
                    if self.is_connected:
                        self._reconnect.reset()
                        return
                    # Dropped again during the stability window — keep retrying.
                    continue
                except Exception as exc:
                    self._disconnect_exc = exc
                    if isinstance(exc, AssertionError):
                        # Never reuse an engine after a proven local invariant
                        # violation, including one raised during reconnect.
                        await self._terminal_shutdown(exc)
                        return
                    continue
        except asyncio.CancelledError:
            raise
        finally:
            self._reconnect_task = None

    def _effective_keepalive(self) -> int:
        negotiated = self._engine.negotiated.server_keep_alive
        if negotiated is not None:
            return negotiated
        return self._engine.config.keepalive

    def _apply_effect_inline(self, effect: EngineEffect, epoch: int) -> bool:
        if epoch != self._connection_epoch:
            return True
        kind = effect.kind
        if kind is EffectKind.SEND:
            return self._try_enqueue_outbound(effect.data, epoch=epoch)
        if kind is EffectKind.SEND_ACK:
            return self._try_enqueue_outbound_ack(effect.data, epoch=epoch)
        if kind is EffectKind.CONNACK and self.on_connect is None:
            connack: ConnAckPacket = effect.data
            self._resolve_connack(connack)
            return True
        if kind is EffectKind.PUBLISH_COMPLETE:
            mid: int | None = effect.data
            callback = self.on_publish
            if callback is not None:
                return self._apply_terminal_callback_inline(callback, mid, None)
            self._settle_publish(mid, None)
            return True
        if kind is EffectKind.PUBLISH_FAILED:
            failure: PublishFailure = effect.data
            callback = self.on_publish
            if callback is not None:
                return self._apply_terminal_callback_inline(callback, failure.mid, failure.reason)
            self._settle_publish(failure.mid, failure.reason)
            return True
        if kind is EffectKind.SUBACK:
            self._resolve_suback(effect.data)
            return True
        if kind is EffectKind.UNSUBACK:
            self._resolve_unsuback(effect.data)
            return True
        if kind is EffectKind.PINGRESP:
            self._ping_pending = False
            return True
        if kind is EffectKind.PROTOCOL_ERROR:
            self._raise_protocol_effect(effect.data)
        return False

    def _raise_protocol_effect(self, data: object) -> Never:
        error = (
            data
            if isinstance(data, (MalformedPacketError, PacketTooLargeError, ProtocolError))
            else ProtocolError(str(data))
        )
        if self._engine.state is ConnectionState.DISCONNECTED:
            self._disconnect_exc = error
        raise error

    def _apply_terminal_callback_inline(
        self,
        callback: Callable[[int | None, BaseException | None], object],
        mid: int | None,
        reason: BaseException | None,
    ) -> bool:
        """Settle and dispatch one terminal callback outside the engine lock."""
        if self._engine_lock.locked():
            return False
        if self._can_dispatch_callback_inline(callback):
            self._settle_publish(mid, reason)
            self._dispatch_callback_inline(callback, mid, reason)
            return True
        if not self._try_enqueue_callback(callback, mid, reason):
            return False
        self._settle_publish(mid, reason)
        return True

    def _resolve_suback(self, packet: SubAckPacket) -> None:
        sub_result = SubscribeResult.from_packet(packet)
        sub_fut = self._sub_futs.pop(sub_result.mid, None)
        if sub_fut is not None and not sub_fut.done():
            sub_fut.set_result(sub_result)

    def _resolve_unsuback(self, packet: UnsubAckPacket) -> None:
        unsub_result = UnsubscribeResult.from_packet(packet)
        unsub_fut = self._unsub_futs.pop(unsub_result.mid, None)
        if unsub_fut is not None and not unsub_fut.done():
            unsub_fut.set_result(unsub_result)

    def _apply_message_effect_batch_inline(
        self,
        effects: deque[EngineEffect],
        epoch: int,
    ) -> int:
        if epoch != self._connection_epoch or self._engine_lock.locked():
            return 0
        return self._delivery.deliver_message_batch_inline(effects, self._message_callback)

    async def _flush_effects(self) -> None:
        async with self._engine_lock:
            self._collect_effects_locked()
        await self._drain_effects()

    async def _apply_effect(  # noqa: C901 -- reduced from 44; remaining branches own lifecycle
        self,
        effect: EngineEffect,
        *,
        nowait: bool,
        epoch: int | None = None,
    ) -> None:
        kind = effect.kind
        if kind is EffectKind.SEND:
            await self._enqueue_outbound(effect.data, nowait=nowait, epoch=epoch)
        elif kind is EffectKind.SEND_ACK:
            await self._enqueue_outbound_ack(effect.data, nowait=nowait, epoch=epoch)
        elif kind is EffectKind.CONNACK:
            connack: ConnAckPacket = effect.data
            self._resolve_connack(connack)
            if self.on_connect is not None:
                await self._enqueue_callback(self.on_connect, connack)
        elif kind is EffectKind.AUTH:
            challenge: AuthPacket = effect.data
            handler = self.auth_handler
            if handler is None:
                # set_auth_handler(None) updates EngineConfig, so the engine no
                # longer emits AUTH. Direct attribute mutation can still race an
                # already-produced effect; surface that as an application failure.
                raise MQTTError("AUTH handler is no longer available")
            try:
                response = await asyncio.wait_for(
                    self._invoke(handler, challenge), timeout=self._auth_timeout
                )
            except TimeoutError as exc:
                raise MQTTTimeoutError("AUTH handler timed out") from exc
            except asyncio.CancelledError as exc:
                task = asyncio.current_task()
                if task is None or task.cancelling():
                    raise
                raise MQTTError("AUTH handler cancelled") from exc
            if isinstance(response, AuthPacket):
                async with self._engine_lock:
                    self._engine.queue_auth(
                        reason_code=response.reason_code,
                        properties=response.properties,
                    )
                    self._collect_effects_locked()
        elif kind is EffectKind.MESSAGE or kind is EffectKind.DECODED_MESSAGE:
            message: Message = effect.data
            property_wire_size = effect.decoded_property_wire_size
            # Producers pair the decoded size with DECODED_MESSAGE and leave it
            # None on MESSAGE, so the size selects the admission entry point.
            if property_wire_size is None:
                pending_delivery = self._accept_message(message, self._message_callback)
            else:
                pending_delivery = self._accept_decoded_message(
                    message, self._message_callback, property_wire_size
                )
            if pending_delivery is not None:
                await pending_delivery
            if effect.requires_delivery_mark and message.mid is not None:
                async with self._engine_lock:
                    self._engine.inbound.mark_delivered(message.mid)
        elif kind is EffectKind.PUBLISH_COMPLETE or kind is EffectKind.PUBLISH_FAILED:
            mid, reason = _terminal_publish_result(effect)
            self._settle_publish(mid, reason)
            if self.on_publish is not None:
                await self._enqueue_callback(self.on_publish, mid, reason)
        elif kind is EffectKind.SUBACK:
            self._resolve_suback(effect.data)
        elif kind is EffectKind.UNSUBACK:
            self._resolve_unsuback(effect.data)
        elif kind is EffectKind.PINGRESP:
            self._ping_pending = False
        elif kind is EffectKind.DISCONNECTED:
            info = effect.data
            if isinstance(info, DisconnectInfo):
                self._last_disconnect = info
                if self._transport is not None and not self._transport.is_closing():
                    if not info.from_broker:
                        try:
                            await asyncio.wait_for(
                                self._write_pump.join(),
                                timeout=_FATAL_DISCONNECT_DRAIN_TIMEOUT,
                            )
                        except TimeoutError:
                            pass
                    try:
                        await self._transport.close()
                    except Exception:
                        pass
        elif kind is EffectKind.CONTINUE_INBOUND_REPLAY:
            # The previous batch of redeliveries has been applied — and waited
            # on, if delivery backpressure kicked in — so the engine may produce
            # the next one. Re-entering under the lock is what keeps peak memory
            # proportional to one batch rather than to the whole session.
            if epoch is not None and epoch != self._connection_epoch:
                return
            async with self._engine_lock:
                self._engine.continue_inbound_replay()
                self._collect_effects_locked()
        elif kind is EffectKind.PROTOCOL_ERROR:
            self._raise_protocol_effect(effect.data)
        else:
            never: Never = kind
            raise MQTTError(f"Unhandled effect {never!r}")

    @property
    def pending_delivery_bytes(self) -> int:
        """Bytes currently using the exact-accounted large-message pool.

        Small messages may instead be covered by ``delivery_small_budget_bytes``;
        that static count-derived reserve deliberately has no per-message hot-path
        accounting.
        """
        return self._pending_delivery_bytes

    @property
    def pending_delivery_high_water_bytes(self) -> int:
        """High-water mark of the exact-accounted large-message pool."""
        return self._pending_delivery_high_water_bytes

    @property
    def delivery_small_budget_bytes(self) -> int:
        """Static byte-budget slice reserved for count-bounded small messages."""
        return self._delivery_small_budget_bytes

    @property
    def delivery_small_message_limit(self) -> int | None:
        """Maximum logical size eligible for the zero-accounting fast path."""
        return self._delivery_small_message_limit

    def _settle_publish(self, mid: int | None, reason: BaseException | None) -> None:
        """Retire the receipt and batch entry for one publication.

        Receipts are keyed FIFO per identifier and the engine emits completion
        before releasing the identifier, which is what keeps a reused MID from
        settling a stale receipt. Both effect application paths go through here
        so those two rules cannot drift apart.
        """
        # QoS 0 carries no packet identifier: its receipt is never registered
        # and its batch entry completes at submission.
        if mid is not None:
            receipt = _fifo_pop(self._receipts, mid)
            if receipt is not None:
                if reason is not None:
                    receipt._error = reason
                receipt._settle()
            # Most clients never call publish_many, so skip the lookup rather
            # than hashing every acknowledged identifier against an empty table.
            if self._batch_receipts:
                batch = _fifo_pop(self._batch_receipts, mid)
                if batch is not None:
                    batch._complete(mid, reason)
        # Inlined check: _settle_publish runs per acknowledgement, and the
        # common case has no waiter at all. One completion wakes one waiter;
        # teardown callers use _notify_publish_space() to wake everyone.
        if self._publish_waiters:
            self._wake_publish_waiters(1)

    def _register_publish_waiter(self) -> asyncio.Future[None]:
        """Park one producer. Must run while still holding ``_engine_lock``."""
        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._publish_waiter_futs.append(waiter)
        self._publish_waiters += 1
        return waiter

    async def _wait_publish_space(self, waiter: asyncio.Future[None]) -> None:
        try:
            await waiter
            self._publish_wait_retries += 1
        except asyncio.CancelledError:
            if waiter.done() and not waiter.cancelled():
                # The wakeup was delivered, but this producer will not retry.
                self._wake_publish_waiters(1)
            else:
                self._discard_publish_waiter(waiter)
            raise
        finally:
            self._publish_waiters -= 1

    def _discard_publish_waiter(self, waiter: asyncio.Future[None]) -> None:
        if waiter.done() and not waiter.cancelled():
            return
        try:
            self._publish_waiter_futs.remove(waiter)
        except ValueError:
            pass

    def _wake_publish_waiters(self, n: int = 1) -> None:
        """Complete up to ``n`` pending waiter futures (one ACK → one waiter)."""
        waiters = self._publish_waiter_futs
        remaining = n
        while remaining > 0 and waiters:
            fut = waiters.popleft()
            if fut.done():
                continue
            fut.set_result(None)
            self._publish_wakeups += 1
            remaining -= 1

    def _notify_publish_space(self) -> None:
        """Wake every parked publisher (teardown / reconnect)."""
        self._wake_publish_waiters(len(self._publish_waiter_futs))

    def _resolve_connack(self, connack: ConnAckPacket) -> None:
        if connack.reason_code != 0:
            self._last_connack_reason = connack.reason_code
            self._disconnect_exc = ProtocolError(
                f"Connection refused: reason_code={connack.reason_code}"
            )
        if self._connack_fut is not None and not self._connack_fut.done():
            self._connack_fut.set_result(connack)

    def _publish_wait_failure(self) -> BaseException | None:
        """Why a producer must not park on outbound admission capacity.

        Admission capacity is only released by an ACK, so once the connection is
        gone for good nothing can ever wake a parked ``publish()``. Reconnect in
        progress is not terminal: the replayed session still settles the budget.
        """
        if not self._teardown_final or self._will_reconnect():
            return None
        return self._disconnect_exc or MQTTError("Connection closed")

    async def _cancel_automatic_reconnect(self) -> None:
        reconnect_task = self._reconnect_task
        if reconnect_task is None or reconnect_task is asyncio.current_task():
            return
        reconnect_task.cancel()
        try:
            await reconnect_task
        except asyncio.CancelledError:
            pass
        self._reconnect_task = None

    async def _prepare_explicit_connect(self) -> None:
        """Replace any automatic-reconnect generation before explicit connect."""
        reconnect_task = self._reconnect_task
        automatic_generation = (
            reconnect_task is not None
            and reconnect_task is not asyncio.current_task()
            and not reconnect_task.done()
        )
        transport_closing = self._transport is not None and self._transport.is_closing()
        if (
            self._engine.state in (ConnectionState.CONNECTED, ConnectionState.CONNECTING)
            and not automatic_generation
            and not transport_closing
        ):
            return
        replacing = (
            self._reconnect_task is not None
            or self._transport is not None
            or self._delivery.closed.is_set()
            or (self._write_pump.task is not None and not self._write_pump.task.done())
        )
        await self._cancel_automatic_reconnect()
        await self._force_close(preserve_reconnect=True)
        # Joining the automatically established reader can run its reconnect
        # decision before the explicit caller regains ownership. Cancel that
        # successor as part of the same takeover boundary as well.
        await self._cancel_automatic_reconnect()
        if replacing:
            await self._reset_message_stream()

    async def _reset_message_stream(self) -> None:
        self._delivery.close()
        self._delivery.reset_stream()

    async def _invalidate_connection_epoch(self) -> None:
        self._connection_epoch += 1
        await self._write_pump.advance_epoch(self._connection_epoch)

    def _settle_terminal_effect(self, effect: EngineEffect) -> None:
        """Settle one terminal publish effect during final teardown."""
        mid, reason = _terminal_publish_result(effect)
        self._settle_publish(mid, reason)
        if self.on_publish is not None:
            try:
                self._spawn_callback(self.on_publish, mid, reason)
            except MessageDeliveryError as exc:
                self._report_callback_error(self.on_publish, exc)

    # Established internal test/benchmark seams; runtime call sites use the
    # module-level FIFO helpers directly to avoid a wrapper frame per publish.
    def _register_publish_receipt(self, mid: int, receipt: PublishReceipt) -> None:
        _fifo_register(self._receipts, mid, receipt)

    def _pop_publish_receipt(self, mid: int) -> PublishReceipt | None:
        return _fifo_pop(self._receipts, mid)

    def _register_batch_receipt(self, mid: int, receipt: PublishBatchReceipt) -> None:
        _fifo_register(self._batch_receipts, mid, receipt)

    def _pop_batch_receipt(self, mid: int) -> PublishBatchReceipt | None:
        return _fifo_pop(self._batch_receipts, mid)

    def _fail_non_replayable(self, exc: BaseException) -> None:
        for sub_fut in self._sub_futs.values():
            if not sub_fut.done():
                sub_fut.set_exception(exc)
        self._sub_futs.clear()
        for unsub_fut in self._unsub_futs.values():
            if not unsub_fut.done():
                unsub_fut.set_exception(exc)
        self._unsub_futs.clear()

    def _fail_pending(self, exc: BaseException) -> None:
        for current in self._receipts.values():
            receipts = current if isinstance(current, deque) else (current,)
            for receipt in receipts:
                receipt._error = exc
                receipt._settle()
        self._receipts.clear()
        batches = {
            batch
            for current in self._batch_receipts.values()
            for batch in (current if isinstance(current, deque) else (current,))
        }
        self._batch_receipts.clear()
        for batch in batches:
            batch._fail_remaining(exc)
        self._fail_non_replayable(exc)
        # Producers parked on outbound admission hold no receipt, so the loops
        # above cannot reach them. Wake them to re-check _publish_wait_failure().
        self._teardown_final = True
        self._notify_publish_space()

    async def _send_fatal_disconnect(self, exc: BaseException) -> None:
        """Best-effort normative DISCONNECT before a fatal close (MQTT 5).

        Maps the error to its spec reason code; no-op on v3.1.1 or if the
        transport is already unusable. Never raises.
        """
        if self._engine.config.protocol != MQTTProtocolVersion.MQTTv5:
            return
        if self._transport is None or self._transport.is_closing():
            return
        reason = 0x82  # Protocol Error (generic)
        if isinstance(exc, PacketTooLargeError):
            reason = 0x95  # Packet too large
        elif isinstance(exc, MalformedPacketError):
            reason = 0x81  # Malformed Packet
        try:
            packet = encode_disconnect(reason, MQTTProtocolVersion.MQTTv5)
            self._engine._check_outbound_size(packet)
            await self._flush_terminal_packet(packet, _FATAL_DISCONNECT_DRAIN_TIMEOUT)
        except Exception:
            pass

    def _retire_engine_connection(self) -> None:
        """Retire engine connection state after a locally failed connection."""
        if self._engine.state not in (
            ConnectionState.NEW,
            ConnectionState.DISCONNECTED,
        ):
            self._engine.notify_transport_closed()
            self._engine.take_effects()

    async def _force_close_after_local_packet_failure(self) -> None:
        """Close transport and finalize engine state for local packet-size failures."""
        await self._force_close()
        async with self._engine_lock:
            self._retire_engine_connection()

    async def _force_close(self, *, preserve_reconnect: bool = False) -> None:
        await self._invalidate_connection_epoch()
        current = asyncio.current_task()
        old_reader = self._reader_task
        tasks = [
            self._reader_task,
            self._keepalive_task,
        ]
        if not preserve_reconnect:
            tasks.append(self._reconnect_task)
        # Quiesce suspended work before the reader enters its finally block and
        # waits for the same EffectPump. Terminal publish results remain queued
        # for settlement after the task owners have stopped.
        self._discard_connection_effects()
        tasks_to_stop = [
            task
            for task in (self._effect_flush_task, *tasks)
            if task is not None and task is not current
        ]
        for task in tasks_to_stop:
            task.cancel()
        for task in tasks_to_stop:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        if (
            old_reader is not None
            and old_reader is not current
            and self._reader_task is not None
            and self._reader_task is not old_reader
        ):
            # on_disconnect established a replacement while the old reader was
            # being joined. Its writer and transport belong to the new epoch.
            return
        await self._write_pump.stop()
        self._discard_connection_effects(settle_publish=True)
        self._write_pump.discard()
        self._decoder.clear()
        await self._write_pump.wake_waiters()
        if not preserve_reconnect:
            # A reconnectable loss must not terminate the application
            # message stream: the same iterator resumes after reconnect.
            self._teardown_final = True
            self._notify_publish_space()
        if self._reader_task is not current:
            self._reader_task = None
        if self._keepalive_task is not current:
            self._keepalive_task = None
        if self._effect_pump.task is not current:
            self._effect_pump.task = None
            self._effect_pump.flush_requested = False
        self._effect_pump.draining_inline = False
        if not preserve_reconnect and self._reconnect_task is not current:
            self._reconnect_task = None
        if self._transport is not None:
            try:
                await self._transport.close()
            except Exception:
                pass
            self._transport = None
        if not preserve_reconnect:
            await self._shutdown_callback_worker(drain=True)
            # Only the really-terminal teardown closes the application stream.
            self._delivery.close()
