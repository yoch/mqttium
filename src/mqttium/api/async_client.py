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
import math
import ssl
import time
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Iterator
from typing import Any, Never, TypeVar

from mqttium.api._auth import AuthExchange
from mqttium.api._cancel import (
    dependency_failure,
    failure_for,
    ignoring_dependency_failures,
    owner_cancelled,
)
from mqttium.api._delivery import (
    ApplicationDelivery,
    MessageDelivery,
    CallbackTarget,
    MessageRoute,
)
from mqttium.api._effects import IMMEDIATE_EFFECTS, EffectPump, StaleConnectionEffect
from mqttium.api._lifecycle import LifecycleHooks
from mqttium.api._delivery_lane import DeliveryLane
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
    TransportStats,
)
from mqttium.codec.buffer import DEFAULT_MAX_PACKET_SIZE, IncrementalDecoder
from mqttium.dispatch.matcher import TopicMatcher
from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
from mqttium.errors import (
    BrokerDisconnectError,
    FlowControlError,
    MQTTError,
    MQTTTimeoutError,
    MalformedPacketError,
    MandatoryResponseTooLargeError,
    MessageDeliveryError,
    PublishBatchError,
    PacketTooLargeError,
    ProtocolError,
    SessionReplayError,
)
from mqttium.packets import (
    AuthPacket,
    ConnAckPacket,
    SubAckPacket,
    SubscribeOptions,
    UnsubAckPacket,
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
from mqttium.protocol.reconnect import ReconnectPolicy, _ReconnectState
from mqttium.persistence.memory import InflightStore
from mqttium.topics import validate_subscribe_filter
from mqttium.transport._stream import AsyncTransport, DecoderPushTransport, PullTransport
from mqttium.transport.tcp import TcpTransport
from mqttium.transport.unix import UnixSocketTransport
from mqttium.transport.websocket import WebSocketTransport
from mqttium.transport.writes import WriteItem, item_size
from mqttium.types import Message, Properties, _owned_payload

OnMessage = Callable[[Message], None]
OnConnect = Callable[[ConnAckPacket], Any]
OnDisconnect = Callable[[BaseException | None], Any]
OnAuth = Callable[[AuthPacket], Any]

_GRACEFUL_DISCONNECT_DRAIN_TIMEOUT = 5.0
_FATAL_DISCONNECT_DRAIN_TIMEOUT = 0.25
# Reader fairness quantum: decoded bytes handled between two effect handoffs.
# Together with the 256-packet bound it caps the size of one delivery lot; it is
# not an application memory bound and is deliberately not configurable.
_MAX_INGRESS_BATCH_BYTES = 1 * 1024 * 1024
_TERMINAL_ENGINE_STATES = (ConnectionState.DISCONNECTING, ConnectionState.DISCONNECTED)

# Terminal-cause precedence for one connection. Real causes are first-wins:
# the fact observed first ended the connection, and failures caused by
# retiring it afterwards (a writer error while closing after a broker
# DISCONNECT) cannot replace it (#543). The broker's verdict is therefore
# latched as soon as its DISCONNECT is applied, not derived at teardown. A peer
# protocol violation outranks them, since a malformed DISCONNECT is no valid
# verdict, and a local capability failure outranks everything.
_CAUSE_SYNTHETIC = 0  # "Connection closed" with nothing better known
_CAUSE_TRANSPORT = 1  # transport, writer, keepalive and effect failures
_CAUSE_BROKER = 1  # broker DISCONNECT or refused CONNACK (first-wins with transport)
_CAUSE_PROTOCOL = 2  # peer protocol or decoding violations
_CAUSE_LOCAL = 3  # local capability failures that make the session unusable
_DEFAULT_MAX_ITERATOR_MESSAGES = 65_536
_DEFAULT_MAX_ITERATOR_BYTES = 64 * 1024 * 1024

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


def _terminal_publish_result(effect: EngineEffect) -> tuple[int | None, BaseException | None]:
    """Extract the (mid, reason) outcome of a terminal publish effect."""
    if effect.kind is EffectKind.PUBLISH_COMPLETE:
        return effect.data, None
    failure: PublishFailure = effect.data
    return failure.mid, failure.reason


def _positive(name: str, value: float) -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
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
    optional_bounds: tuple[tuple[str, int | None], ...],
    positive_bounds: tuple[tuple[str, float], ...],
    ping_timeout: float | None,
) -> None:
    if message_delivery not in ("iterator", "callback"):
        raise ValueError("message_delivery must be 'iterator' or 'callback'")
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


class AsyncClient:
    """Asyncio-native MQTT 3.1.1 and MQTT 5 client.

    The client owns one event loop, one protocol engine, and at most one active
    transport. It provides bounded outbound, inbound, writer, and iterator
    delivery queues; limits can be tuned explicitly through the constructor.
    Synchronous message callbacks run directly on the delivering reader.

    Instances are loop-confined and are not thread-safe. Use the native async
    methods from the owning loop.

    Args:
        client_id: MQTT client identifier. An empty identifier lets an MQTT 5
            broker assign one when the connection settings allow it.
        protocol: MQTT protocol version used for encoding and negotiation.
        clean_start: Whether to start without a previous broker session.
        keepalive: Keep-alive interval in seconds; zero disables keep-alive.
        username: Optional CONNECT username.
        password: Optional CONNECT password. Strings are encoded as UTF-8.
        connect_properties: MQTT 5 CONNECT properties.
        will: Last Will message.
        will_properties: MQTT 5 Will properties.
        maximum_packet_size: Largest inbound packet accepted by the decoder;
            advertised to an MQTT 5 broker. Larger packets end the connection.
        topic_alias_maximum: Inbound topic aliases accepted from an MQTT 5
            broker.
        max_inbound_inflight: Concurrent inbound QoS 1/2 exchanges accepted;
            advertised as Receive Maximum on MQTT 5. A broker that exceeds it
            is disconnected.
        max_inbound_inflight_bytes: Logical bytes retained for inbound QoS 1/2
            exchanges; ``None`` disables the bound. A broker that exceeds it is
            disconnected with reason 0x97.
        max_outbound_inflight: Optional local cap on concurrent outbound QoS
            1/2 exchanges, additionally bounded by broker negotiation.
        max_unacknowledged_messages: Outbound QoS 1/2 publications admitted
            and not yet completed, including those waiting for an inflight
            slot; ``None`` disables the bound.
        max_unacknowledged_bytes: Logical bytes of those publications; ``None``
            disables the bound.
        max_write_queue_messages: Encoded frames resident in the writer.
        max_write_queue_bytes: Encoded bytes resident in the writer.
        message_delivery: Explicit ``"iterator"`` (default) or ``"callback"`` delivery.
            Callback delivery notifies synchronously and acknowledges
            automatically; iterator delivery is the asynchronous processing
            mode and the only one that supports ``manual_ack``.
        manual_ack: Defer terminal acknowledgement of inbound QoS messages
            until :meth:`ack` is called. Requires iterator delivery.
        max_iterator_messages: Iterator queue count bound.
        max_iterator_bytes: Logical bytes retained in the iterator queue;
            ``None`` disables the bound.
        iterator_admission_timeout: Optional deadline for admitting one
            message into the iterator queue; ``None`` waits without deadline.
        store: Optional inflight store used for durable QoS state. Combine it
            with ``clean_start=False`` to resume the session it holds.
        reconnect: Reconnection policy. ``None`` disables reconnection.
        connect_timeout: Transport and CONNACK deadline for explicit and
            automatic connection attempts when a call does not override it.
        ping_timeout: PINGRESP deadline; derived from ``keepalive`` when omitted.
        subscribe_timeout: SUBACK and UNSUBACK deadline when a call does not
            override it.
        auth_handler: Optional MQTT 5 enhanced-authentication callback. A
            callback-raised :class:`asyncio.CancelledError` is treated as an
            authentication failure; cancellation requested on MQTTium's
            owning task still propagates normally.
        auth_timeout: Maximum seconds allowed for one enhanced-authentication
            callback invocation.

    Raises:
        ValueError: If a limit or constructor option is invalid, or an
            iterator bound or ``manual_ack`` is given with callback delivery.
        ProtocolError: If an MQTT 5 option is given with MQTT 3.1.1.

    Note:
        The iterator bounds describe the only queue kept on the application's
        behalf; callback delivery retains nothing and refuses them. See the
        configuration guide for sizing rules and interactions.
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
        connect_properties: Properties | None = None,
        will: Message | None = None,
        will_properties: Properties | None = None,
        maximum_packet_size: int | None = None,
        topic_alias_maximum: int = 0,
        max_inbound_inflight: int = 100,
        max_inbound_inflight_bytes: int | None = 64 * 1024 * 1024,
        max_outbound_inflight: int | None = None,
        max_unacknowledged_messages: int | None = 10_000,
        max_unacknowledged_bytes: int | None = 64 * 1024 * 1024,
        max_write_queue_messages: int = 10_000,
        max_write_queue_bytes: int = 1 * 1024 * 1024,
        message_delivery: MessageDelivery = "iterator",
        manual_ack: bool = False,
        max_iterator_messages: int = _DEFAULT_MAX_ITERATOR_MESSAGES,
        max_iterator_bytes: int | None = _DEFAULT_MAX_ITERATOR_BYTES,
        iterator_admission_timeout: float | None = None,
        store: InflightStore | None = None,
        reconnect: ReconnectPolicy | None = None,
        connect_timeout: float = 30.0,
        ping_timeout: float | None = None,
        subscribe_timeout: float = 30.0,
        auth_handler: OnAuth | None = None,
        auth_timeout: float = 10.0,
    ) -> None:
        _validate_client_arguments(
            client_id=client_id,
            username=username,
            password=password,
            message_delivery=message_delivery,
            optional_bounds=(
                ("max_unacknowledged_messages", max_unacknowledged_messages),
                ("max_unacknowledged_bytes", max_unacknowledged_bytes),
                ("max_inbound_inflight_bytes", max_inbound_inflight_bytes),
                ("max_iterator_bytes", max_iterator_bytes),
            ),
            positive_bounds=(
                ("max_iterator_messages", max_iterator_messages),
                ("max_write_queue_messages", max_write_queue_messages),
                ("max_write_queue_bytes", max_write_queue_bytes),
                ("connect_timeout", connect_timeout),
                ("subscribe_timeout", subscribe_timeout),
                ("auth_timeout", auth_timeout),
            ),
            ping_timeout=ping_timeout,
        )
        if iterator_admission_timeout is not None:
            _positive("iterator_admission_timeout", iterator_admission_timeout)
        if message_delivery == "callback" and (
            max_iterator_messages != _DEFAULT_MAX_ITERATOR_MESSAGES
            or max_iterator_bytes != _DEFAULT_MAX_ITERATOR_BYTES
            or iterator_admission_timeout is not None
        ):
            # Callback delivery retains nothing on the application's behalf, so
            # an iterator bound would describe a queue that does not exist.
            raise ValueError(
                "max_iterator_messages, max_iterator_bytes and "
                "iterator_admission_timeout apply to iterator delivery only"
            )
        if message_delivery == "callback" and manual_ack:
            # Message callbacks are synchronous and ack() is awaited, so the
            # only way to acknowledge from a callback would be a detached task;
            # messages() is the delivery mode built for that processing shape.
            raise ValueError("manual_ack requires iterator delivery; use messages() and ack()")
        effective_max_packet_size = (
            maximum_packet_size if maximum_packet_size is not None else DEFAULT_MAX_PACKET_SIZE
        )
        initial_decoder_max_packet_size = effective_max_packet_size
        pwd = password.encode("utf-8") if isinstance(password, str) else password
        self._engine = ProtocolEngine(
            EngineConfig(
                client_id=client_id,
                protocol=protocol,
                clean_start=clean_start,
                keepalive=keepalive,
                username=username,
                password=pwd,
                max_inbound_inflight=max_inbound_inflight,
                max_inbound_inflight_bytes=max_inbound_inflight_bytes,
                max_outbound_inflight=max_outbound_inflight,
                max_unacknowledged_messages=max_unacknowledged_messages,
                max_unacknowledged_bytes=max_unacknowledged_bytes,
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
        self._transport: AsyncTransport | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._keepalive_task: asyncio.Task[None] | None = None
        self._reconnect_task: asyncio.Task[None] | None = None
        self._explicit_connect_task: asyncio.Task[Any] | None = None
        self._lifecycle_lock = asyncio.Lock()
        self._lifecycle_hooks = LifecycleHooks(self)
        self._disconnect_hook_origin: asyncio.Task[None] | None = None
        self._engine_lock = asyncio.Lock()
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._connection_epoch = 0
        self._effect_pump = EffectPump(self)
        self._delivery_lane = DeliveryLane(self)
        self._write_pump = WritePump(
            max_bytes=max_write_queue_bytes,
            max_messages=max_write_queue_messages,
            on_failure=self._writer_failed,
        )
        self._delivery = ApplicationDelivery(
            mode=message_delivery,
            protocol=protocol,
            max_iterator_messages=max_iterator_messages,
            max_iterator_bytes=max_iterator_bytes,
            iterator_admission_timeout=iterator_admission_timeout,
        )
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
        self._disconnect_rank = _CAUSE_TRANSPORT
        # Set once by a local-terminal ingress failure (inside the read-loop
        # batch or while applying an effect that touches persistence). While
        # set, the protocol/persistence state is unfit for automatic
        # reconnect or silent reuse: reconnect is suppressed and explicit
        # connect() is refused. Never cleared; the application must create
        # a new client.
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
        self._reconnect = _ReconnectState(reconnect)
        self._connect_timeout = connect_timeout
        self._ping_timeout = ping_timeout
        self._subscribe_timeout = subscribe_timeout
        self._auth_timeout = auth_timeout
        self._auth_exchange = AuthExchange(self)
        self._intentional_disconnect = False
        self._transport_factory: Callable[..., Awaitable[AsyncTransport]] = TcpTransport.connect
        self._last_disconnect: DisconnectInfo | None = None
        self._last_connack_reason: int | None = None

        self._on_message: OnMessage | None = None
        self._message_callback: CallbackTarget | None = None
        self._topic_callbacks: TopicMatcher | None = None
        self.on_connect: OnConnect | None = None
        self.on_disconnect: OnDisconnect | None = None
        self._auth_handler = auth_handler
        self._routes_frozen = False

    def stats(self) -> ClientStats:
        """Return an immutable point-in-time runtime snapshot.

        The method only reads already-maintained counters and queue sizes. It
        does not enable sampling, emit logs, or mutate protocol state. Like the
        rest of ``AsyncClient``'s synchronous surface, it is intended for the
        owning event-loop thread.
        """

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
        return ClientStats(
            state=engine.state,
            connection_epoch=self._connection_epoch,
            reconnect_attempt=self._reconnect.attempt,
            outbound=engine.outbound.stats(),
            inbound=engine.inbound.stats(),
            writer=self._write_pump.stats(),
            decoder=DecoderStats(
                buffered_bytes=self._decoder.buffered,
                high_water_bytes=self._decoder.high_water,
                max_packet_size=self._decoder.max_packet_size,
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

    def _running_tasks(self) -> dict[str, bool]:
        """Which client-owned background tasks are alive; maintainer diagnostics."""

        def running(task: asyncio.Task[Any] | None) -> bool:
            return task is not None and not task.done()

        return {
            "reader": running(self._reader_task),
            "writer": running(self._write_pump.task),
            "keepalive": running(self._keepalive_task),
            "reconnect": running(self._reconnect_task),
            "effect_flush": running(self._effect_pump.task),
            "lifecycle": running(self._lifecycle_hooks.task),
            "auth": running(self._auth_exchange.task),
        }

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

    def _try_direct_qos0_publish(
        self,
        topic: str,
        payload: bytes,
        *,
        retain: bool,
        properties: Properties | None,
        nowait: bool = False,
        batch: PublishBatchReceipt | None = None,
    ) -> PublishReceipt | bool:
        """Hand ready QoS 0 to the writer; False declines, True accepts a batch item."""
        pump = self._effect_pump
        writer = self._write_pump
        epoch = self._connection_epoch
        if (
            self._engine.state is not ConnectionState.CONNECTED
            or self._local_terminal_failure is not None
            or writer.epoch != epoch
            or self._engine_lock.locked()
            or pump.lock.locked()
            or pump.draining_inline
            or pump.pending
            or self._engine.has_pending_effects
        ):
            return False
        item = self._engine.outbound.prepare_qos0(
            topic, payload, retain=retain, properties=properties
        )
        receipt: PublishReceipt | bool
        if batch is None:
            receipt = PublishReceipt(mid=None, qos=QoS.AT_MOST_ONCE)
        else:
            batch._register(None)
            receipt = True
        # No await or user callback separates preflight and handoff. Writer
        # exceptions propagate: an eager write may already have reached wire.
        if not writer.try_enqueue(item, epoch=epoch):
            if batch is not None:
                batch._rollback_qos0_registration()
            if nowait:
                raise FlowControlError(writer.refusal(item_size(item)))
            return False
        if properties is not None and properties.get("topic_alias") is not None:
            self._engine.outbound.commit_topic_alias(topic, properties)
        return receipt

    def _commit_publish(
        self,
        topic: str,
        payload: bytes,
        *,
        qos: int | QoS,
        retain: bool,
        properties: Properties | None,
        batch: PublishBatchReceipt | None = None,
        prepared: _PreparedPublish | None = None,
    ) -> PublishReceipt | None:
        """Admit one operation and register its completion before collecting SEND."""
        if qos != QoS.AT_MOST_ONCE and self._local_terminal_failure is not None:
            raise MQTTError(
                "Client is unusable after a local terminal failure; create a new AsyncClient"
            )
        handle = self._engine.outbound.queue_publish(
            topic,
            payload,
            qos=qos,
            retain=retain,
            properties=properties,
            _prepared=prepared,
        )
        if batch is not None:
            batch._register(handle.mid)
            if handle.mid is not None:
                _fifo_register(self._batch_receipts, handle.mid, batch)
            return None
        receipt = PublishReceipt(mid=handle.mid, qos=handle.qos)
        if handle.mid is not None:
            _fifo_register(self._receipts, handle.mid, receipt)
        return receipt

    def _finalize_loop_commands(self) -> None:
        """Collect engine effects and apply/schedule them without suspending."""
        self._effect_pump.collect_from_engine()
        self._effect_pump.drain_inline()

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
            timeout: Transport and CONNACK deadline; ``connect_timeout`` is
                used when omitted.

        Returns:
            The successful CONNACK packet and its negotiated properties.

        Raises:
            MQTTTimeoutError: If transport setup or CONNACK exceeds the deadline.
            ValueError: If ``timeout`` is not a finite positive number.
            ProtocolError: If the client is already connecting/connected or the
                broker refuses or violates the protocol.
            MQTTError: If :meth:`disconnect` cancels connection setup.
            MQTTError: If a previous local terminal failure fail-stopped
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
            ValueError: If ``timeout`` is not a finite positive number.
            ProtocolError: If the broker refuses or violates the protocol.
            MQTTError: If a previous local terminal failure fail-stopped
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
            ValueError: If ``timeout`` is not a finite positive number.
            ProtocolError: If the broker refuses or violates the MQTT protocol.
            MQTTError: If a previous local terminal failure fail-stopped
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
                timeout=None,  # The connection attempt owns the complete deadline.
                max_frame_size=max(DEFAULT_MAX_PACKET_SIZE, self._decoder.max_packet_size),
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
        if timeout is not None:
            _positive("timeout", timeout)
        if self._local_terminal_failure is not None:
            raise MQTTError(
                "Client is unusable after a local terminal failure; "
                "create a new AsyncClient instead of reusing this one"
            )
        if self._explicit_connect_task is not None or (
            self.is_connected and self._has_active_explicit_connection()
        ):
            raise ProtocolError("Already connected or connecting")
        task = asyncio.current_task()
        assert task is not None
        self._explicit_connect_task = task
        try:
            self._freeze_message_routes()
            # Cancel obsolete hook work immediately, but keep the live reader's
            # token valid if this caller is cancelled while waiting for the lock.
            self._lifecycle_hooks.begin_operation(replace_connection=False)
            async with self._lifecycle_lock:
                await self._prepare_explicit_connect()
                # Rechecked, not just entry-checked: the latch is set-once, so a
                # failure that landed while waiting on the lock is observed here.
                if self._local_terminal_failure is not None:
                    raise MQTTError(
                        "Client is unusable after a local terminal failure; "
                        "create a new AsyncClient instead of reusing this one"
                    )
                self._lifecycle_hooks.begin_operation()
                lifecycle_token = self._lifecycle_hooks.token
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
                timeout = timeout if timeout is not None else self._connect_timeout
                self._reconnect.reset()
                connack = await self._connect_once_locked(host, port, ssl=ssl, timeout=timeout)
            if self.is_connected:
                self._lifecycle_hooks.connected(connack, lifecycle_token)
            return connack
        finally:
            if self._explicit_connect_task is task:
                self._explicit_connect_task = None

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
        self._effect_pump.discard_connection_effects()
        deadline = loop.time() + timeout
        try:
            try:
                transport = await asyncio.wait_for(
                    self._transport_factory(host, port, ssl=ssl), timeout=timeout
                )
            except TimeoutError as exc:
                raise MQTTTimeoutError("Transport connection timed out") from exc
            self._transport = transport
            # Reject a misconfigured factory before CONNECT or background tasks.
            # Assign first so the normal failure cleanup closes the transport.
            push = isinstance(transport, DecoderPushTransport)
            pull = isinstance(transport, PullTransport)
            if push == pull:
                raise TypeError(
                    f"{type(transport).__name__} must offer exactly one receive capability: "
                    "PullTransport or DecoderPushTransport"
                )
            self._delivery.reopen()
            self._disconnect_hook_origin = None
            self._disconnect_exc = None
            self._disconnect_rank = _CAUSE_TRANSPORT
            self._teardown_final = False
            self._last_disconnect = None
            self._decoder.clear()
            if isinstance(transport, DecoderPushTransport):
                # This transport receives straight into the decoder's storage.
                # It stays paused until attached, so nothing is read before the
                # decoder is ready for the new connection.
                transport.attach_decoder(self._decoder)
            self._write_pump.reset()
            self._ping_pending = False
            connect_packet = self._engine.begin_connect()
            sent_maximum_packet_size = self._engine._sent_maximum_packet_size
            if sent_maximum_packet_size is not None:
                self._decoder.max_packet_size = sent_maximum_packet_size
            self._connack_fut = loop.create_future()
            self._connect_disconnect_fut = loop.create_future()
            self._write_pump.start(transport)
            await self._write_pump.enqueue(connect_packet)
            self._reader_task = asyncio.create_task(self._read_loop(), name="mqttium-reader")
            try:
                connack = await self._await_connack_or_disconnect(max(0.0, deadline - loop.time()))
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
                    self._propose_disconnect_cause(refusal, _CAUSE_BROKER)
                raise refusal
            self._write_pump.last_outbound = time.monotonic()
            self._keepalive_task = asyncio.create_task(
                self._keepalive_loop(), name="mqttium-keepalive"
            )
            return connack
        except BaseException as exc:
            # The transport factory or CONNECT write can raise CancelledError
            # although the caller was not cancelled (#529).
            failure = failure_for(exc, "connection setup")
            if not reconnect_attempt:
                self._intentional_disconnect = True
            if self._connack_fut is not None and not self._connack_fut.done():
                self._connack_fut.cancel()
            try:
                # A failed attempt inside an active retry loop is transient:
                # keep the application stream alive.
                await self._force_close(preserve_reconnect=reconnect_attempt)
            except BaseException:
                pass
            self._retire_engine_connection()
            raise failure from failure.__cause__

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
        if not self._write_pump.try_enqueue_terminal(packet, epoch=self._connection_epoch):
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
        Cancellation during terminal drainage still closes the connection before
        propagating to the caller.

        Args:
            reason_code: MQTT 5 DISCONNECT reason code. MQTT 3 clients must use
                the success value.

        Raises:
            ProtocolError: If the reason code is invalid for the protocol.
        """
        if self._transport is not None and self._engine.state in (
            ConnectionState.CONNECTING,
            ConnectionState.CONNECTED,
        ):
            # Reject an invalid packet before superseding hooks or stopping
            # automatic retry. Negotiated size remains a shutdown fallback.
            self._engine.codec.encode_disconnect(reason_code)
        origin = self._lifecycle_hooks.begin_operation(replace_connection=False)
        self._disconnect_hook_origin = origin
        self._intentional_disconnect = True
        connect_disconnect_fut = self._connect_disconnect_fut
        disconnecting_connect = (
            self._engine.state is ConnectionState.CONNECTING
            and connect_disconnect_fut is not None
            and not connect_disconnect_fut.done()
        )
        if disconnecting_connect:
            assert connect_disconnect_fut is not None
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
        try:
            async with self._lifecycle_lock:
                if disconnecting_connect:
                    return
                if self._transport is None:
                    # No live transport (e.g. called inside a reconnect gap): the
                    # intentional shutdown is still terminal for receipts and the
                    # application stream, which the reconnect loop would otherwise
                    # keep alive.
                    if not self._will_reconnect():
                        self._terminal_shutdown(self._disconnect_exc or MQTTError("Disconnected"))
                    return
                # Preserve validation semantics: an invalid reason code must fail
                # before teardown, just as it did before shutdown became bounded.
                try:
                    packet = (
                        self._engine.begin_disconnect(reason_code) if self.is_connected else None
                    )
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
        finally:
            if packet_failure:
                await self._force_close_after_local_packet_failure()
            elif should_close:
                # Transport cleanup completes before the separate lifecycle owner
                # can notify user code; hooks never become a prerequisite here.
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

        This is the non-suspending counterpart to :meth:`publish`.
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
            MQTTError: If a previous local terminal failure fail-stopped
                this client; create a new one instead of reusing it.
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
        data = _owned_payload(payload)
        # Ready QoS 0 goes to the writer first: it validates and encodes the
        # real frame once and lets the writer admit its exact size. The
        # generic preflight below previewed that size a second time on this
        # path; it remains for QoS 1/2 and for QoS 0 the direct path declines.
        if qos == QoS.AT_MOST_ONCE:
            direct = self._try_direct_qos0_publish(
                topic, data, retain=retain, properties=properties, nowait=True
            )
            if direct is not False:
                assert direct is not True
                return direct
        prepared = self._check_nowait_publish_capacity(topic, data, qos, retain, properties)
        receipt = self._commit_publish(
            topic,
            data,
            qos=qos,
            retain=retain,
            properties=properties,
            prepared=prepared,
        )
        self._finalize_loop_commands()
        assert receipt is not None
        return receipt

    async def publish(
        self,
        topic: str,
        payload: bytes | str = b"",
        *,
        qos: int | QoS = 0,
        retain: bool = False,
        properties: Properties | None = None,
    ) -> PublishReceipt:
        """Wait for admission and bounded effect transfer, returning a receipt.

        Before commitment, cancellation admits nothing. After commitment it can
        leave an active publication: cancelling this call is not an MQTT undo.
        QoS 0 completes on writer handoff; QoS 1/2 complete on protocol ACK.
        Cancelling receipt.wait() never cancels the exchange or other waiters.
        """
        receipt = await self._publish_one(
            topic, payload, qos=qos, retain=retain, properties=properties
        )
        assert receipt is not None
        return receipt

    async def _publish_one(
        self,
        topic: str,
        payload: bytes | str,
        *,
        qos: int | QoS,
        retain: bool,
        properties: Properties | None,
        batch: PublishBatchReceipt | None = None,
    ) -> PublishReceipt | None:
        data = _owned_payload(payload)
        while True:
            # A broker ACK can free an identifier while its completion effect
            # still waits behind delivery. Settle that old receipt before the
            # identifier can be registered again, including within one batch.
            await self._effect_pump.drain()
            if qos == QoS.AT_MOST_ONCE:
                direct = self._try_direct_qos0_publish(
                    topic, data, retain=retain, properties=properties, batch=batch
                )
                if direct is not False:
                    return None if direct is True else direct
            waiter: asyncio.Future[None] | None = None
            async with self._engine_lock:
                if self._effect_pump.pending or self._engine.has_pending_effects:
                    self._effect_pump.collect_from_engine()
                    continue
                try:
                    receipt = self._commit_publish(
                        topic,
                        data,
                        qos=qos,
                        retain=retain,
                        properties=properties,
                        batch=batch,
                    )
                except FlowControlError as exc:
                    if not self._engine.outbound.can_ever_admit(topic, data, qos, properties):
                        raise
                    terminal = self._publish_wait_failure()
                    if terminal is not None:
                        raise terminal from exc
                    waiter = self._register_publish_waiter()
                else:
                    self._effect_pump.collect_from_engine()
                    self._effect_pump.drain_inline()
            if waiter is None:
                # Deferred SEND payloads are outside the writer's budget until
                # transfer. Await it before admitting another batch element.
                if self._effect_pump.pending:
                    await self._effect_pump.drain()
                if qos != QoS.AT_MOST_ONCE:
                    self._write_pump._try_flush_latency_batch()
                return receipt
            await self._wait_publish_space(waiter)
            terminal = self._publish_wait_failure()
            if terminal is not None:
                # Parked through a final teardown, which failed every pending
                # publication: this one fails with them instead of queueing on
                # the budget their abandonment freed (#521).
                raise terminal

    def _publish_ready_prefix(
        self,
        source: Iterator[PublishMessage],
        first: PublishMessage,
        batch: PublishBatchReceipt,
        limit: int,
    ) -> tuple[PublishMessage | None, bool]:
        """Transfer ready QoS 0; return the fallback and whether source ended.

        Each item has writer ownership before next(source), which is user code.
        Pressure or a different QoS returns to the ordinary admission path.
        """
        message = first
        for index in range(limit):
            if not isinstance(message, PublishMessage):
                raise TypeError("publish_many entries must be PublishMessage instances")
            # Only an exact QoS 0 stays on the ready path; anything else,
            # including an invalid level, is validated by ordinary admission.
            if message.qos != 0:
                return message, False
            payload = message.payload
            if not self._try_direct_qos0_publish(
                message.topic,
                payload if type(payload) is bytes else _owned_payload(payload),
                retain=message.retain,
                properties=message.properties,
                batch=batch,
            ):
                return message, False
            if index + 1 == limit:
                return None, False
            try:
                message = next(source)
            except StopIteration:
                return None, True
        return None, False

    async def publish_many(
        self,
        messages: Iterable[PublishMessage],
        *,
        max_failure_details: int = 128,
    ) -> PublishBatchReceipt:
        """Admit an iterable progressively, with bounded aggregate completion.

        Each publication commits independently. An iteration/admission failure
        raises PublishBatchError carrying the receipt for the committed prefix.
        Cancellation propagates unchanged and leaves that prefix active.
        No input chunk or per-publication task is created.
        """
        receipt = PublishBatchReceipt(max_failure_details=max_failure_details)
        try:
            source = iter(messages)
            for message in source:
                if not isinstance(message, PublishMessage):
                    raise TypeError("publish_many entries must be PublishMessage instances")
                pending: PublishMessage | None = message
                qos = QoS(message.qos)
                if qos == QoS.AT_MOST_ONCE:
                    pending, exhausted = self._publish_ready_prefix(
                        source, message, receipt, min(64, 256 - receipt.submitted % 256)
                    )
                    if exhausted:
                        break
                    if pending is not None:
                        qos = QoS(pending.qos)
                if pending is not None:
                    if qos != QoS.AT_MOST_ONCE:
                        await receipt._wait_pending_at_most(max(1, self._engine.flow.limit) - 1)
                    await self._publish_one(
                        pending.topic,
                        pending.payload,
                        qos=pending.qos,
                        retain=pending.retain,
                        properties=pending.properties,
                        batch=receipt,
                    )
                if receipt.submitted % 256 == 0:
                    await asyncio.sleep(0)
        except (Exception, asyncio.CancelledError) as caught:
            # Caller cancellation propagates unchanged and leaves the committed
            # prefix active. A dependency raising CancelledError is an admission
            # failure: the caller still needs the prefix receipt.
            if isinstance(caught, asyncio.CancelledError) and owner_cancelled():
                raise
            exc = failure_for(caught, "publication admission")
            raise PublishBatchError(
                receipt.failures,
                failure_count=receipt.failure_count,
                failure_counts=dict(receipt.failure_counts),
                cause=exc,
                receipt=receipt,
            ) from exc
        finally:
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
            self._effect_pump.collect_from_engine()
        await self._effect_pump.drain()

    @property
    def auth_handler(self) -> OnAuth | None:
        """Enhanced-authentication handler fixed at construction."""
        return self._auth_handler

    def _check_routes_mutable(self) -> None:
        if self._routes_frozen:
            raise MQTTError("Message routing is frozen after the first connection attempt")

    @property
    def on_message(self) -> OnMessage | None:
        """Synchronous callback used when no topic-specific callback matches."""
        return self._on_message

    @on_message.setter
    def on_message(self, callback: OnMessage | None) -> None:
        self._check_routes_mutable()
        if callback is not None:
            self._delivery.validate_message_callback(callback)
        self._on_message = callback
        self._refresh_message_callback()

    def _freeze_message_routes(self) -> None:
        if self._routes_frozen:
            return
        self._routes_frozen = True

    def _refresh_message_callback(self) -> None:
        self._message_callback = (
            MessageRoute(self._dispatch_topic_message)
            if self._topic_callbacks
            else self._on_message
        )

    def message_callback_add(self, topic_filter: str, callback: OnMessage) -> None:
        """Register a synchronous filtered callback before the first connection attempt.

        Matches run in registration order instead of on_message. Replacing a
        filter retains its position. Shared filters match their literal string.
        """
        self._check_routes_mutable()
        validate_subscribe_filter(topic_filter)
        self._delivery.validate_message_callback(callback)
        if self._topic_callbacks is None:
            self._topic_callbacks = TopicMatcher()
        self._topic_callbacks[topic_filter] = callback
        self._refresh_message_callback()

    def message_callback_remove(self, topic_filter: str) -> None:
        """Remove a filtered callback before the first connection attempt."""
        self._check_routes_mutable()
        if self._topic_callbacks is not None:
            try:
                del self._topic_callbacks[topic_filter]
            except KeyError:
                pass
            if not self._topic_callbacks:
                self._topic_callbacks = None
        self._refresh_message_callback()

    def _dispatch_topic_message(self, message: Message) -> Iterator[OnMessage]:
        matcher = self._topic_callbacks
        matched = False
        if matcher:
            for callback in matcher.iter_match(message.topic):
                matched = True
                yield callback
        if not matched and self._on_message is not None:
            yield self._on_message

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
            timeout: SUBACK deadline; ``subscribe_timeout`` is used when omitted.

        Returns:
            Packet identifier and broker reason codes in request order.

        Raises:
            MQTTTimeoutError: If SUBACK does not arrive before the deadline.
            ValueError: If ``timeout`` is not a finite positive number.
            ProtocolError: If a filter, option, property, or negotiated limit is
                invalid.
            NotConnectedError: If the client cannot submit the request.
        """
        if timeout is not None:
            _positive("timeout", timeout)
        # Admission may validate more than once; consume a one-shot iterable once.
        if not isinstance(topics, str):
            topics = tuple(topics)
        loop = asyncio.get_running_loop()
        while True:
            async with self._engine_lock:
                # Reject a terminal or invalid request before waiting on effects.
                request = self._engine.prepare_subscribe(topics, qos=qos, properties=properties)
                if not (self._effect_pump.pending or self._engine.has_pending_effects):
                    mid = self._engine.queue_subscription_request(request)
                    fut: asyncio.Future[SubscribeResult] = loop.create_future()
                    self._sub_futs[mid] = fut
                    self._effect_pump.collect_from_engine()
                    break
                self._effect_pump.collect_from_engine()
            # Settle earlier results before a released identifier can be reused.
            await self._effect_pump.drain()
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
            timeout: UNSUBACK deadline; ``subscribe_timeout`` is used when omitted.

        Returns:
            Packet identifier and MQTT 5 reason codes. MQTT 3.1.1 returns an
            empty reason-code tuple.

        Raises:
            MQTTTimeoutError: If UNSUBACK does not arrive before the deadline.
            ValueError: If ``timeout`` is not a finite positive number.
            ProtocolError: If a filter is invalid.
            NotConnectedError: If the client cannot submit the request.
        """
        if timeout is not None:
            _positive("timeout", timeout)
        # Admission may validate more than once; consume a one-shot iterable once.
        if not isinstance(topics, str):
            topics = tuple(topics)
        loop = asyncio.get_running_loop()
        while True:
            async with self._engine_lock:
                # Reject a terminal or invalid request before waiting on effects.
                request = self._engine.prepare_unsubscribe(topics)
                if not (self._effect_pump.pending or self._engine.has_pending_effects):
                    mid = self._engine.queue_subscription_request(request)
                    fut: asyncio.Future[UnsubscribeResult] = loop.create_future()
                    self._unsub_futs[mid] = fut
                    self._effect_pump.collect_from_engine()
                    break
                self._effect_pump.collect_from_engine()
            # Settle earlier results before a released identifier can be reused.
            await self._effect_pump.drain()
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
        try:
            await self._effect_pump.drain()
            try:
                return await asyncio.wait_for(
                    fut, timeout=timeout if timeout is not None else self._subscribe_timeout
                )
            except TimeoutError as exc:
                raise MQTTTimeoutError(f"{ack_name} timed out for mid={mid}") from exc
        finally:
            # Caller ownership spans transfer as well as ACK waiting. Protocol
            # identifiers remain owned by the engine until ACK or teardown.
            if futs.get(mid) is fut:
                del futs[mid]
            if not fut.done():
                fut.cancel()
            elif not fut.cancelled():
                fut.exception()  # Retrieve failures assigned during a failed drain.

    def messages(self) -> AsyncIterator[Message]:
        """Return an iterator bound to the generation when this method is called.

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
        The handle identifies its logical exchange, even after a transport
        reconnect resumes the same session. It cannot acknowledge a later
        exchange that reuses the packet identifier. Repeated acknowledgements
        are accepted while completion is pending, then rejected after completion.

        Args:
            message: Message previously delivered by this client's current
                session. A QoS 0 message has no packet identifier and is a no-op.

        Raises:
            NotConnectedError: If a message with a packet identifier is
                acknowledged without an active connection.
            ProtocolError: If manual acknowledgement is disabled, or the handle
                is foreign, reconstructed, stale, or no longer awaiting completion.
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
                self._engine.ack(message.mid, message=message)
                self._effect_pump.collect_from_engine()
        except PacketTooLargeError as exc:
            # A broker limit below the mandatory ACK size makes this QoS
            # exchange impossible to complete without violating negotiation.
            self._propose_disconnect_cause(exc, _CAUSE_LOCAL)
            self._intentional_disconnect = True
            await self._force_close_after_local_packet_failure()
            raise
        await self._effect_pump.drain()

    def _process_ingress_batch(self) -> tuple[int, int, bool, MQTTError | None]:
        """Decode until a byte/count bound, an auto-PUBACK handoff boundary,
        or the first peer error.

        A malformed, oversized or protocol-violating packet ends the lot at
        that packet. It is returned, not raised, so the valid prefix before it
        commits and completes like any lot: the peer error must neither roll
        back nor fence packets already observed, and nothing after it is
        processed (#511, #513).
        """
        decoder = self._decoder
        engine = self._engine
        handle_raw = engine.handle_raw
        inbound = engine.inbound
        max_bytes = _MAX_INGRESS_BATCH_BYTES
        count = 0
        decoded_bytes = 0
        for _ in range(256):
            try:
                packet = decoder.next_packet()
            except (MalformedPacketError, PacketTooLargeError) as exc:
                if engine.state in _TERMINAL_ENGINE_STATES:
                    # Bytes after DISCONNECT belong to no connection: the
                    # engine ignores whole packets there, and undecodable
                    # ones must not replace the terminal reason either.
                    return count, decoded_bytes, False, None
                return count, decoded_bytes, False, exc
            if packet is None:
                break
            peer_error = handle_raw(packet)
            if peer_error is not None:
                # handle_raw() emitted it as the lot's last effect; the reader
                # raises it after the prefix instead, so drop that copy.
                engine._effects.pop()
                return count + 1, decoded_bytes, False, peer_error
            count += 1
            decoded_bytes += len(packet.remaining) + 5
            # Auto-PUBACK slots remain owned until take_effects(). Stop exactly
            # when their batch fills the remaining Receive Maximum window, so
            # the effect handoff below can release them before another PUBLISH.
            # Control packets and QoS 0 traffic retain the full 256-packet batch.
            if inbound._autoack_handoff_required:
                return count, decoded_bytes, True, None
            if decoded_bytes >= max_bytes:
                break
        return count, decoded_bytes, False, None

    async def _read_loop(self) -> None:  # noqa: C901
        assert self._transport is not None
        lifecycle_token = self._lifecycle_hooks.token
        reader_transport = self._transport
        reader_connack = self._connack_fut
        # Receiving is a capability, and the two are exclusive: a push
        # transport has already placed the bytes in the decoder by the time it
        # reports them, so there is nothing to feed.
        push: DecoderPushTransport | None = None
        pull: PullTransport | None = None
        if isinstance(self._transport, DecoderPushTransport):
            push = self._transport
        elif isinstance(self._transport, PullTransport):
            pull = self._transport
        else:  # pragma: no cover - a transport must offer one of the two
            raise TypeError(f"{type(self._transport).__name__} offers no receive capability")
        try:
            while not self._transport.is_closing():
                if push is not None:
                    if not await push.receive():
                        break
                else:
                    assert pull is not None
                    data = await pull.read(256 * 1024)
                    if not data:
                        break
                    self._decoder.feed(data)
                # Process one bounded packet batch at a time. Applying its
                # effects before decoding the next batch propagates delivery
                # byte backpressure all the way to transport.read().
                while True:
                    async with self._engine_lock:
                        # The try wraps the whole batch statement, including its
                        # exit: a commit failure at batch close is a local
                        # failure like any store error inside the body.
                        effect_start = len(self._engine._effects)
                        try:
                            with self._engine.store.batch():
                                handled, handled_bytes, handoff_required, peer_error = (
                                    self._process_ingress_batch()
                                )
                        except (
                            MandatoryResponseTooLargeError,
                            SessionReplayError,
                            PacketTooLargeError,
                            MalformedPacketError,
                            ProtocolError,
                        ):
                            raise
                        except Exception as exc:
                            # Local failure (store/persistence error, including
                            # a batch-exit commit failure): fail-stop.
                            # Everything here is synchronous under the engine
                            # lock with no await between latch and retire, so
                            # no admission can slip into the window. Latch
                            # first so reconnect and reuse decisions see it.
                            # A pending explicit connect() must fail with the
                            # original cause instead of timing out on CONNACK.
                            # First cause wins: a later local failure must not
                            # replace it.
                            if self._local_terminal_failure is None:
                                self._local_terminal_failure = exc
                            connack_fut = self._connack_fut
                            if connack_fut is not None and not connack_fut.done():
                                connack_fut.set_exception(exc)
                            # Keep only terminal publish outcomes among this
                            # lot's new effects — anything else was never
                            # durably committed and must not be exposed — then
                            # retire connection-visible state immediately via
                            # the idempotent lifecycle boundary (the finally
                            # below re-enters it harmlessly) and let the
                            # original exception propagate.
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
                            self._engine.notify_transport_closed()
                            raise
                        if handled and self._engine.has_pending_effects:
                            self._effect_pump.collect_from_engine()
                        protocol_target = self._effect_pump.enqueued
                        if peer_error is not None:
                            # Raised only after the prefix drains, but observed
                            # now, as its PROTOCOL_ERROR effect would be.
                            self._observe_peer_error(peer_error)
                    if self._write_pump.sealed and self._write_pump.waiters:
                        # A broker DISCONNECT sealed the writer: output parked
                        # for capacity fails now instead of blocking this lot.
                        await self._write_pump.wake_waiters()
                    await self._effect_pump.drain(target=protocol_target)
                    await self._delivery_lane.drain()
                    if peer_error is not None:
                        # The valid prefix is committed, acknowledged and
                        # delivered; only now does the peer error retire the
                        # connection.
                        raise peer_error
                    # A batch that stopped short of both bounds emptied the
                    # buffer, so there is nothing to decode until the next
                    # read(). Re-entering only to observe handled == 0 cost a
                    # second lock acquisition and bounded decode per read.
                    if (
                        not handoff_required
                        and handled < 256
                        and handled_bytes < _MAX_INGRESS_BATCH_BYTES
                    ):
                        break
                    await asyncio.sleep(0)
        except asyncio.CancelledError as exc:
            # The client cancels its reader to retire the connection. A
            # transport read raising CancelledError on its own is a transport
            # failure whose cause must survive teardown.
            if owner_cancelled():
                raise
            failure = dependency_failure(exc, "transport read")
            self._propose_disconnect_cause(failure, _CAUSE_TRANSPORT)
            if self._local_terminal_failure is None and not self._will_reconnect():
                self._fail_pending(failure)
        except (MandatoryResponseTooLargeError, SessionReplayError) as exc:
            connack_fut = self._connack_fut
            if connack_fut is not None and not connack_fut.done():
                connack_fut.set_exception(exc)
            # The broker negotiated legal limits, but mqttium cannot produce a
            # mandatory response (automatic ACK, resumed-session replay) within
            # them. This is a local terminal capability failure, not a peer
            # protocol violation; durable session state is kept.
            self._propose_disconnect_cause(exc, _CAUSE_LOCAL)
            self._fail_pending(exc)
        except (PacketTooLargeError, MalformedPacketError, ProtocolError) as exc:
            # Fatal wire/protocol error: send a normative DISCONNECT (v5) before
            # tearing down, so a strict broker sees *why* we left.
            await self._send_fatal_disconnect(exc)
            self._propose_disconnect_cause(exc, _CAUSE_PROTOCOL)
            if not self._will_reconnect():
                self._fail_pending(exc)
        except Exception as exc:
            self._propose_disconnect_cause(exc, _CAUSE_TRANSPORT)
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
            hook_origin = self._disconnect_hook_origin
            connect_owner = self._explicit_connect_task
            if connect_owner is not None and connect_owner is self._lifecycle_hooks.hook_task:
                # Failure of a directly awaited connect belongs to its caller.
                # Once that call exits, later external loss cancels it normally.
                hook_origin = connect_owner
            self._lifecycle_hooks.retiring(lifecycle_token, hook_origin)
            # One synchronous ownership transition, before the first await:
            # producers must never see the new epoch while the old engine is
            # still CONNECTED, or they admit work into the dead transport
            # (#544). No coroutine suspends while holding the engine lock, so
            # this synchronous step cannot interleave with an engine mutation.
            self._retire_connection_epoch()
            self._engine.notify_transport_closed()
            self._effect_pump.collect_from_engine()
            self._auth_exchange.retire()
            await self._write_pump.wake_waiters()
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
                await self._effect_pump.drain()
            except (Exception, asyncio.CancelledError):
                pass
            if self._disconnect_exc is None:
                info = self._last_disconnect
                if (
                    self._local_terminal_failure is None
                    and info is not None
                    and info.from_broker
                    and info.reason_code != 0
                ):
                    self._propose_disconnect_cause(
                        BrokerDisconnectError(info.reason_code, info.properties), _CAUSE_BROKER
                    )
                else:
                    self._propose_disconnect_cause(MQTTError("Connection closed"), _CAUSE_SYNTHETIC)
            # A latched local-terminal failure is authoritative: a secondary
            # writer/keepalive error that overwrote _disconnect_exc must never
            # replace it for settlement and callbacks. The explicit None test
            # (not truthiness) keeps even a falsey backend exception identical.
            terminal_cause = self._local_terminal_failure
            if terminal_cause is None:
                assert self._disconnect_exc is not None
                terminal_cause = self._disconnect_exc
            if (
                reader_connack is not None
                and not reader_connack.done()
                and not self._intentional_disconnect
            ):
                reader_connack.set_exception(terminal_cause)
            self._fail_non_replayable(terminal_cause)
            will_reconnect = self._will_reconnect()
            if not will_reconnect:
                self._fail_pending(terminal_cause)
            # Retire resources before lifecycle user code can install a
            # replacement. Replayable state remains in the protocol store.
            await self._write_pump.wake_waiters()
            await self._write_pump.stop()
            with ignoring_dependency_failures():
                await reader_transport.close()
            if self._transport is reader_transport:
                self._transport = None
            if not will_reconnect:
                # A reconnectable loss must not terminate the application
                # message stream: the same iterator resumes after reconnect.
                self._delivery.close()
            self._lifecycle_hooks.disconnected(
                None if clean_disconnect else terminal_cause,
                lifecycle_token,
                hook_origin,
            )
            if will_reconnect and (self._reconnect_task is None or self._reconnect_task.done()):
                self._reconnect_task = asyncio.create_task(
                    self._reconnect_loop(), name="mqttium-reconnect"
                )

    def _terminal_shutdown(self, exc: BaseException) -> None:
        """Fail pending work and close the application stream, terminally.

        ``_fail_pending`` already marks teardown final and wakes parked
        publishers; this adds the stream close that every terminal path shares.
        """
        self._fail_pending(exc)
        self._delivery.close()

    def _retry_reason(self) -> int | None:
        """The reason code the reconnect policy judges: broker DISCONNECT, else CONNACK."""
        if self._last_disconnect is not None and self._last_disconnect.from_broker:
            return self._last_disconnect.reason_code
        return self._last_connack_reason

    def _permanent_connection_failure(self) -> bool:
        exc = self._disconnect_exc
        if isinstance(
            exc,
            (
                MessageDeliveryError,
                MandatoryResponseTooLargeError,
                SessionReplayError,
                AssertionError,
                ssl.SSLCertVerificationError,
                MalformedPacketError,
            ),
        ):
            return True
        # A refused CONNACK is also exposed as ProtocolError. Its validated
        # reason remains the policy's decision (server busy/unavailable retry).
        # Each attempt clears this reason before opening the transport.
        return isinstance(exc, ProtocolError) and self._last_connack_reason is None

    def _will_reconnect(self) -> bool:
        reason = self._retry_reason()
        return (
            self._local_terminal_failure is None
            and not self._permanent_connection_failure()
            and not self._intentional_disconnect
            and self._reconnect.should_retry(reason, self._engine.config.protocol)
        )

    async def _close_transport_after_connection_failure(self) -> None:
        """Unblock the reader without letting teardown errors replace the cause."""
        transport = self._transport
        if transport is None:
            return
        try:
            with ignoring_dependency_failures():
                await transport.close()
        finally:
            # The reader can be waiting for application delivery rather than
            # read(). Closing the socket alone cannot wake that wait. Never
            # join here: its teardown can stop this writer/effect task.
            reader = self._reader_task
            if reader is not None and reader is not asyncio.current_task() and not reader.done():
                reader.cancel()

    def _invoke_auth_handler(
        self, handler: Callable[[AuthPacket], Any], packet: AuthPacket
    ) -> Awaitable[Any]:
        return self._delivery.invoke(handler, packet)

    async def _auth_failed(self, exc: BaseException) -> None:
        """An auth_handler failure ends its (still current) connection."""
        connack_fut = self._connack_fut
        if connack_fut is not None and not connack_fut.done():
            connack_fut.set_exception(exc)
        self._propose_disconnect_cause(exc, _CAUSE_TRANSPORT)
        await self._close_transport_after_connection_failure()

    def _propose_disconnect_cause(self, exc: BaseException, rank: int) -> BaseException:
        """Offer a terminal cause for the current connection; return the winner.

        With no cause yet, any proposal wins. A cause recorded without a rank
        counts as a real (transport-tier) cause.
        """
        if self._disconnect_exc is None or rank > self._disconnect_rank:
            self._disconnect_exc = exc
            self._disconnect_rank = rank
        return self._disconnect_exc

    async def _writer_failed(self, exc: BaseException) -> None:
        """Hand a writer failure to the reader-owned connection lifecycle."""
        self._propose_disconnect_cause(exc, _CAUSE_TRANSPORT)
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
        if (
            self._engine_lock.locked()
            or self._effect_pump.pending
            or self._engine.has_pending_effects
        ):
            raise FlowControlError("Pending engine effects prevent immediate publication")
        if self._effect_pump.lock.locked() or self._effect_pump.draining_inline:
            raise FlowControlError("Effect transfer is already active")
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
            if not self._write_pump.can_enqueue_size(size):
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
        if not self._write_pump.can_enqueue_size(prepared_size):
            raise FlowControlError(self._write_pump.refusal(prepared_size))
        return prepared

    async def _keepalive_loop(self) -> None:
        try:
            while not self._delivery.closed.is_set():
                k = self._effective_keepalive()
                if k <= 0:
                    await asyncio.sleep(1.0)
                    continue
                now = time.monotonic()
                if self._ping_pending:
                    if now >= self._ping_deadline:
                        self._propose_disconnect_cause(
                            MQTTTimeoutError("PINGRESP timed out"), _CAUSE_TRANSPORT
                        )
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
                            self._effect_pump.collect_from_engine()
                    except PacketTooLargeError as exc:
                        # A broker limit below the two-byte PINGREQ leaves no
                        # conforming keepalive packet to send.
                        self._propose_disconnect_cause(exc, _CAUSE_LOCAL)
                        self._intentional_disconnect = True
                        await self._close_transport_after_connection_failure()
                        return
                    # A lost PINGREQ beats a wedged keepalive under backpressure.
                    try:
                        await self._effect_pump.drain(nowait=True)
                    except FlowControlError:
                        pass
                    self._ping_pending = True
                    ping_to = self._ping_timeout
                    if ping_to is None:
                        ping_to = max(k / 2, 5.0)
                    self._ping_deadline = now + ping_to
                else:
                    await asyncio.sleep(min(1.0, due - now))
        except asyncio.CancelledError as exc:
            if owner_cancelled():
                raise
            # A PINGREQ dependency raised CancelledError: without keepalive
            # the connection cannot detect a dead peer, so retire it.
            self._propose_disconnect_cause(dependency_failure(exc, "keepalive"), _CAUSE_TRANSPORT)
            await self._close_transport_after_connection_failure()

    async def _reconnect_loop(self) -> None:
        try:
            while self._reconnect.enabled and not self._intentional_disconnect:
                await self._lifecycle_hooks.wait_reconnect()
                if self._intentional_disconnect or self.is_connected:
                    return
                if not self._will_reconnect():
                    # Recheck the cause after the stability window as well as
                    # the retry budget: permanent failures terminate the stream.
                    self._terminal_shutdown(
                        self._disconnect_exc or MQTTError("Reconnect exhausted")
                    )
                    return
                delay = self._reconnect.next_delay()
                await asyncio.sleep(delay)
                cause = self._local_terminal_failure
                if cause is not None:
                    self._terminal_shutdown(cause)
                    return
                try:
                    async with self._lifecycle_lock:
                        if self._intentional_disconnect:
                            return
                        self._lifecycle_hooks.begin_operation()
                        lifecycle_token = self._lifecycle_hooks.token
                        previous_connack = self._connack_fut
                        await self._force_close(preserve_reconnect=True)
                        connack = await self._connect_once_locked(
                            self._host,
                            self._port,
                            ssl=self._ssl,
                            timeout=self._connect_timeout,
                            reconnect_attempt=True,
                        )
                    if self.is_connected:
                        self._lifecycle_hooks.connected(connack, lifecycle_token)
                    # Only clear backoff after the connection stays up.
                    policy = self._reconnect.policy
                    assert policy is not None
                    await asyncio.sleep(policy.stable_after)
                    cause = self._local_terminal_failure
                    if cause is not None:
                        # A local-terminal failure landed while this attempt
                        # was proving itself stable: never start another one.
                        self._terminal_shutdown(cause)
                        return
                    if self.is_connected:
                        self._reconnect.reset()
                        return
                    # Dropped again during the stability window — keep retrying.
                    continue
                except (Exception, asyncio.CancelledError) as caught:
                    # Only disconnect()/explicit connect cancel the supervisor.
                    # A dependency raising CancelledError is one failed attempt.
                    if isinstance(caught, asyncio.CancelledError) and owner_cancelled():
                        raise
                    exc = failure_for(caught, "reconnect attempt")
                    # A failed attempt has no live connection: its outcome
                    # replaces the previous cause outright.
                    self._disconnect_exc = exc
                    self._disconnect_rank = _CAUSE_TRANSPORT
                    cause = self._local_terminal_failure
                    if self._permanent_connection_failure() or cause is not None:
                        # Stop on permanent setup/peer failures as well as
                        # invariant failures. A latched local cause still wins;
                        # peer/security failures alone do not poison the client
                        # for a later explicit connect after configuration repair.
                        terminal = cause if cause is not None else exc
                        self._terminal_shutdown(terminal)
                        # TLS setup can fail before allocating a new CONNACK
                        # waiter/reader, leaving no reader to report its cause.
                        if self._connack_fut is previous_connack:
                            self._lifecycle_hooks.disconnected(terminal, lifecycle_token)
                        return
                    continue
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
            return self._write_pump.try_enqueue(effect.data, epoch=epoch)
        if kind is EffectKind.SEND_ACK:
            return self._write_pump.try_enqueue_ack(effect.data, epoch=epoch)
        if kind is EffectKind.CONNACK:
            connack: ConnAckPacket = effect.data
            self._resolve_connack(connack)
            return True
        if kind is EffectKind.PUBLISH_COMPLETE:
            mid: int | None = effect.data
            self._settle_publish(mid, None)
            return True
        if kind is EffectKind.PUBLISH_FAILED:
            failure: PublishFailure = effect.data
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
        if kind is EffectKind.AUTH:
            self._auth_exchange.hand_off(effect.data, effect.exchange_token, epoch)
            return True
        if kind is EffectKind.PROTOCOL_ERROR:
            self._raise_protocol_effect(effect.data)
        return False

    def _apply_observation(self, effect: EngineEffect) -> bool:
        """Apply a fact the engine has already observed; True when fully done.

        Settling a receipt, resolving CONNACK/SUBACK/UNSUBACK or clearing the
        ping deadline depends on no earlier output. A broker DISCONNECT and a
        peer protocol error establish the connection's cause and unblock their
        waiters now; their ordered remainder (closing the transport, raising
        the error) stays in the effect lane.
        """
        kind = effect.kind
        if kind in IMMEDIATE_EFFECTS:
            return self._apply_effect_inline(effect, self._connection_epoch)
        if kind is EffectKind.DISCONNECTED:
            info = effect.data
            if isinstance(info, DisconnectInfo) and info.from_broker:
                self._observe_broker_disconnect(info)
        elif kind is EffectKind.PROTOCOL_ERROR and isinstance(
            effect.data, (MalformedPacketError, ProtocolError)
        ):
            self._observe_peer_error(effect.data)
        return False

    def _observe_peer_error(self, exc: MQTTError) -> None:
        """Latch a peer violation and fail a pending CONNACK wait with it now.

        Its ordered remainder (normative DISCONNECT, close) may wait for writer
        capacity; connect() must report the violation, not a timeout (#540).
        """
        self._propose_disconnect_cause(exc, _CAUSE_PROTOCOL)
        connack_fut = self._connack_fut
        if connack_fut is not None and not connack_fut.done():
            connack_fut.set_exception(exc)

    def _observe_broker_disconnect(self, info: DisconnectInfo) -> None:
        """Latch the broker's verdict and stop output it will never read."""
        self._last_disconnect = info
        self._propose_disconnect_cause(
            BrokerDisconnectError(info.reason_code, info.properties)
            if info.reason_code != 0
            else MQTTError("Connection closed"),
            _CAUSE_BROKER,
        )
        self._write_pump.seal()

    def _raise_protocol_effect(self, data: object) -> Never:
        if not isinstance(data, (MalformedPacketError, ProtocolError)):
            raise TypeError(
                "PROTOCOL_ERROR effect payload must be MalformedPacketError or ProtocolError"
            )
        if self._engine.state is ConnectionState.DISCONNECTED:
            self._propose_disconnect_cause(data, _CAUSE_PROTOCOL)
        raise data

    def _resolve_suback(self, packet: SubAckPacket) -> None:
        sub_result = SubscribeResult(mid=packet.mid, reason_codes=packet.reason_codes)
        sub_fut = self._sub_futs.pop(sub_result.mid, None)
        if sub_fut is not None and not sub_fut.done():
            sub_fut.set_result(sub_result)

    def _resolve_unsuback(self, packet: UnsubAckPacket) -> None:
        unsub_result = UnsubscribeResult(mid=packet.mid, reason_codes=packet.reason_codes)
        unsub_fut = self._unsub_futs.pop(unsub_result.mid, None)
        if unsub_fut is not None and not unsub_fut.done():
            unsub_fut.set_result(unsub_result)

    async def _apply_effect(  # noqa: C901 -- reduced from 44; remaining branches own lifecycle
        self,
        effect: EngineEffect,
        *,
        nowait: bool,
        epoch: int | None = None,
    ) -> None:
        kind = effect.kind
        if kind is EffectKind.SEND:
            await self._write_pump.enqueue(effect.data, nowait=nowait, epoch=epoch)
        elif kind is EffectKind.SEND_ACK:
            await self._write_pump.enqueue_ack(effect.data, nowait=nowait, epoch=epoch)
        elif kind is EffectKind.CONNACK:
            connack: ConnAckPacket = effect.data
            self._resolve_connack(connack)
        elif kind is EffectKind.AUTH:
            # Never queued in practice (IMMEDIATE_EFFECTS); kept total.
            self._auth_exchange.hand_off(effect.data, effect.exchange_token, self._connection_epoch)
        elif kind is EffectKind.PUBLISH_COMPLETE or kind is EffectKind.PUBLISH_FAILED:
            mid, reason = _terminal_publish_result(effect)
            self._settle_publish(mid, reason)
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
                if info.from_broker:
                    # The broker's verdict is the connection's cause from the
                    # moment it is observed; closing the transport below can
                    # make the writer fail, and that must not replace it (#543).
                    self._observe_broker_disconnect(info)
                if self._transport is not None and not self._transport.is_closing():
                    if not info.from_broker:
                        try:
                            await asyncio.wait_for(
                                self._write_pump.join(),
                                timeout=_FATAL_DISCONNECT_DRAIN_TIMEOUT,
                            )
                        except TimeoutError:
                            pass
                    with ignoring_dependency_failures():
                        await self._transport.close()
        elif kind is EffectKind.PROTOCOL_ERROR:
            self._raise_protocol_effect(effect.data)
        else:
            raise MQTTError(f"Non-protocol effect in protocol pump: {kind!r}")

    def _apply_delivery_effect(self, effect: EngineEffect, epoch: int) -> Awaitable[object] | None:
        """Apply one effect of the reader's delivery lot outside protocol locks.

        The common case -- a message handed to its destination immediately --
        completes synchronously and returns ``None``. Waiting for delivery
        capacity, the fairness yield and replay continuation return the
        awaitable that finishes the effect.
        """
        kind = effect.kind
        if kind is EffectKind.MESSAGE or kind is EffectKind.DECODED_MESSAGE:
            message: Message = effect.data
            pending = self._delivery.accept(
                message, self._message_callback, effect.decoded_property_wire_size
            )
            if not effect.requires_delivery_mark or message.mid is None:
                return pending
            if pending is None or self._delivery.mode == "callback":
                # The application owns the message now: a callback already ran,
                # or the iterator queue accepted it. Mark before any await, so
                # neither teardown nor cancellation can separate the two (#517).
                self._mark_delivered_locked(message.mid, effect.exchange_token)
                return pending
            return self._mark_after_admission(message.mid, effect.exchange_token, pending)
        if kind is EffectKind.CONTINUE_INBOUND_REPLAY:
            return self._continue_inbound_replay(epoch)
        raise MQTTError(f"Non-delivery effect in reader lane: {kind!r}")

    def _mark_delivered_locked(self, mid: int, token: object | None = None) -> None:
        """Record delivery of ``mid`` and apply the completion it releases.

        No coroutine awaits while holding ``_engine_lock``, so a synchronous
        caller can never interleave with another engine mutation. The mark is
        tied to the exchange (``token``), not to the connection: a message
        committed to a stream that survives reconnect stays delivered.
        """
        try:
            self._engine.inbound.mark_delivered(mid, token)
        except Exception as exc:
            # Queue acceptance is observable, but failed durable completion
            # must retire this session before any new admission. Reader
            # teardown preserves this first cause.
            if self._local_terminal_failure is None:
                self._local_terminal_failure = exc
            self._engine.notify_transport_closed()
            raise

    def _flush_released_completions(self) -> None:
        """Hand the PUBCOMP/PUBACKs released by delivery marks to the writer.

        The reader calls this once per delivery lot, so completions released
        by one lot leave as a batch instead of one effect collection per
        message.
        """
        if self._engine.has_pending_effects:
            self._effect_pump.collect_from_engine()
            self._effect_pump.drain_inline()

    async def _mark_after_admission(
        self, mid: int, token: object | None, pending: Awaitable[bool | None]
    ) -> None:
        # The waiting admission commits synchronously with its return, so no
        # suspension separates the commit from the mark. A retired admission
        # (replaced connection or stream) commits nothing and marks nothing.
        if await pending:
            self._mark_delivered_locked(mid, token)

    async def _continue_inbound_replay(self, epoch: int) -> None:
        # This marker follows its messages in the reader-owned lane. Only
        # their completed handoff may hydrate the next bounded replay lot.
        async with self._engine_lock:
            if epoch != self._connection_epoch:
                return
            try:
                self._engine.continue_inbound_replay()
            except Exception as exc:
                if self._local_terminal_failure is None:
                    self._local_terminal_failure = exc
                self._engine.notify_transport_closed()
                raise
            self._effect_pump.collect_from_engine()

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
            self._propose_disconnect_cause(
                ProtocolError(f"Connection refused: reason_code={connack.reason_code}"),
                _CAUSE_BROKER,
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

    def _has_active_explicit_connection(self) -> bool:
        reconnect_task = self._reconnect_task
        automatic_generation = (
            reconnect_task is not None
            and reconnect_task is not asyncio.current_task()
            and not reconnect_task.done()
        )
        transport_closing = self._transport is not None and self._transport.is_closing()
        return (
            self._engine.state in (ConnectionState.CONNECTED, ConnectionState.CONNECTING)
            and not automatic_generation
            and not transport_closing
        )

    async def _prepare_explicit_connect(self) -> None:
        """Replace any automatic-reconnect generation before explicit connect."""
        if self._has_active_explicit_connection():
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

    def _retire_connection_epoch(self) -> None:
        """Publish a new connection epoch to every stale-work guard at once."""
        self._connection_epoch += 1
        self._delivery_lane.discard()
        self._delivery.invalidate_waiting_admissions()
        self._write_pump.set_epoch(self._connection_epoch)

    async def _invalidate_connection_epoch(self) -> None:
        self._retire_connection_epoch()
        await self._write_pump.wake_waiters()

    def _settle_terminal_effect(self, effect: EngineEffect) -> None:
        """Settle one terminal publish effect during final teardown."""
        mid, reason = _terminal_publish_result(effect)
        self._settle_publish(mid, reason)

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
        # A receipt failed here is a final answer: the same client must never
        # send its publication later (#521).
        abandoned = [*self._receipts, *self._batch_receipts]
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
        if abandoned:
            try:
                self._engine.seal_publications(abandoned)
            except Exception as store_exc:
                # A publication this client cannot seal could still be sent
                # later: refuse every later use of this client instead.
                if self._local_terminal_failure is None:
                    self._local_terminal_failure = store_exc
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
        if self._engine.state not in (ConnectionState.CONNECTING, ConnectionState.CONNECTED):
            # The engine already ended the connection and emitted its own
            # normative DISCONNECT (or found none it could send): never a second.
            return
        try:
            # Retire public admission before terminal drainage can suspend.
            packet = self._engine.begin_disconnect(reason)
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
        self._lifecycle_hooks.hold()
        try:
            await self._force_close_transport(preserve_reconnect=preserve_reconnect)
        finally:
            self._lifecycle_hooks.release()

    async def _force_close_transport(self, *, preserve_reconnect: bool) -> None:
        await self._invalidate_connection_epoch()
        self._auth_exchange.retire()
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
        self._effect_pump.discard_connection_effects()
        tasks_to_stop = [
            task
            for task in (self._effect_pump.task, *tasks)
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
            # A concurrent lifecycle operation installed a replacement while
            # the old reader was being joined. Preserve the new ownership.
            return
        await self._write_pump.stop()
        self._effect_pump.discard_connection_effects(settle_publish=True)
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
            with ignoring_dependency_failures():
                await self._transport.close()
            self._transport = None
        if not preserve_reconnect:
            # Only the really-terminal teardown closes the application stream.
            self._delivery.close()
