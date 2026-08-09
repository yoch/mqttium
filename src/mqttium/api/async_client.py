"""Async-native MQTT client.

Owns the transport + IncrementalDecoder + ProtocolEngine loop.

Concurrency invariants (see docs/IMPLEMENTATION-GUIDE.md §1):
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
from typing import Any, Literal, Never

from mqttium.api._effects import EffectPump
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
    DeliveryStats,
    ProtocolStats,
    ReceiptStats,
    TaskStats,
    TransportStats,
)
from mqttium.codec.buffer import DEFAULT_MAX_PACKET_SIZE, IncrementalDecoder
from mqttium.codec.properties import PUBLISH, encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
from mqttium.errors import (
    FlowControlError,
    MQTTError,
    MQTTTimeoutError,
    MalformedPacketError,
    MessageDeliveryError,
    PublishBatchError,
    PacketTooLargeError,
    ProtocolError,
)
from mqttium.packets import (
    AuthPacket,
    ConnAckPacket,
    SubscribeOptions,
    encode_disconnect,
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
from mqttium.protocol.reconnect import ReconnectPolicy
from mqttium.persistence.memory import InflightStore
from mqttium.transport._stream import AsyncTransport
from mqttium.transport.tcp import TcpTransport
from mqttium.transport.unix import UnixSocketTransport
from mqttium.transport.websocket import WebSocketTransport
from mqttium.transport.writes import WriteItem
from mqttium.types import Message, Properties

OnMessage = Callable[[Message], Any]
OnConnect = Callable[[ConnAckPacket], Any]
OnDisconnect = Callable[[BaseException | None], Any]
OnPublish = Callable[[int | None, BaseException | None], Any]
OnAuth = Callable[[AuthPacket], Any]
MessageDelivery = Literal["auto", "iterator", "callback", "both"]
PublishBackpressure = Literal["wait", "error"]


class _SharedDeliveryReservation:
    """One exact byte reservation shared by callback and iterator delivery."""

    __slots__ = ("logical_bytes", "remaining")

    def __init__(self, logical_bytes: int) -> None:
        self.logical_bytes = logical_bytes
        self.remaining = 2


AccountedDeliveryToken = int | _SharedDeliveryReservation
DeliveryToken = AccountedDeliveryToken | None
CallbackJob = tuple[Callable[..., Any], tuple[Any, ...], DeliveryToken]
TrackedIteratorMessage = tuple[Message, AccountedDeliveryToken]
IteratorQueueItem = Message | TrackedIteratorMessage


class _DeliveryQueue(asyncio.Queue[IteratorQueueItem]):
    """Queue for stream delivery without unused ``join()`` bookkeeping."""

    def put_nowait(self, item: IteratorQueueItem) -> None:
        if self.full():
            raise asyncio.QueueFull
        self._put(item)
        self._wakeup_next(self._getters)  # type: ignore[attr-defined]


class AsyncClient:
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
    ) -> None:
        if message_delivery not in ("auto", "iterator", "callback", "both"):
            raise ValueError("message_delivery must be 'auto', 'iterator', 'callback', or 'both'")
        if publish_backpressure not in ("wait", "error"):
            raise ValueError("publish_backpressure must be 'wait' or 'error'")
        if max_pending_outbound_messages is not None and max_pending_outbound_messages < 0:
            raise ValueError("max_pending_outbound_messages must be non-negative or None")
        if max_pending_outbound_bytes is not None and max_pending_outbound_bytes < 0:
            raise ValueError("max_pending_outbound_bytes must be non-negative or None")
        if max_pending_inbound_bytes is not None and max_pending_inbound_bytes < 0:
            raise ValueError("max_pending_inbound_bytes must be non-negative or None")
        if max_pending_messages <= 0:
            raise ValueError("max_pending_messages must be greater than 0")
        if max_pending_callbacks <= 0:
            raise ValueError("max_pending_callbacks must be greater than 0")
        if max_pending_delivery_bytes is not None and max_pending_delivery_bytes < 0:
            raise ValueError("max_pending_delivery_bytes must be non-negative or None")
        if delivery_timeout <= 0:
            raise ValueError("delivery_timeout must be greater than 0")
        if callback_shutdown_timeout <= 0:
            raise ValueError("callback_shutdown_timeout must be greater than 0")
        if max_outbound_messages <= 0:
            raise ValueError("max_outbound_messages must be greater than 0")
        if max_outbound_bytes <= 0:
            raise ValueError("max_outbound_bytes must be greater than 0")
        if max_ingress_batch_bytes <= 0:
            raise ValueError("max_ingress_batch_bytes must be greater than 0")
        if ack_timeout <= 0:
            raise ValueError("ack_timeout must be greater than 0")
        if ping_timeout is not None and ping_timeout <= 0:
            raise ValueError("ping_timeout must be greater than 0")
        if not isinstance(client_id, str):
            raise ValueError("client_id must be a string")
        if username is not None and not isinstance(username, str):
            raise ValueError("username must be a string or None")
        if password is not None and not isinstance(password, (bytes, str)):
            raise ValueError("password must be bytes, str, or None")
        self._message_delivery = message_delivery
        self._publish_backpressure = publish_backpressure
        self._max_pending_messages = max_pending_messages
        self._max_pending_delivery_bytes = max_pending_delivery_bytes
        self._pending_delivery_bytes = 0
        self._pending_delivery_high_water_bytes = 0
        self._delivery_space = asyncio.Event()
        self._delivery_space.set()
        self._delivery_waiters = 0
        effective_max_packet_size = (
            maximum_packet_size if maximum_packet_size is not None else DEFAULT_MAX_PACKET_SIZE
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
        self._decoder = IncrementalDecoder(max_packet_size=effective_max_packet_size)
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
        self._try_enqueue_outbound_many = self._write_pump.try_enqueue_many
        self._enqueue_outbound = self._write_pump.enqueue
        self._callback_queue: asyncio.Queue[CallbackJob] = asyncio.Queue(
            maxsize=max_pending_callbacks
        )
        if max_pending_delivery_bytes is None:
            self._delivery_small_budget_bytes = 0
            self._delivery_small_message_limit = None
            self._delivery_accounted_limit = None
        else:
            if message_delivery == "callback":
                maximum_small_messages = max_pending_callbacks + 2
            elif message_delivery == "iterator":
                maximum_small_messages = max_pending_messages + 1
            else:
                maximum_small_messages = max_pending_messages + max_pending_callbacks + 2
            candidate_small_budget = max_pending_delivery_bytes // 8
            candidate_small_limit = (
                candidate_small_budget // maximum_small_messages if maximum_small_messages else 0
            )
            candidate_accounted_limit = max_pending_delivery_bytes - candidate_small_budget
            # Partition only when it preserves the capacity of every packet that
            # could otherwise fit both the configured delivery budget and the
            # decoder's maximum packet size. Tiny/custom budgets therefore keep
            # exact accounting across their full range.
            minimum_single_message_capacity = min(
                max_pending_delivery_bytes, effective_max_packet_size
            )
            if (
                candidate_small_limit <= 0
                or candidate_accounted_limit < minimum_single_message_capacity
            ):
                self._delivery_small_budget_bytes = 0
                self._delivery_small_message_limit = 0
                self._delivery_accounted_limit = max_pending_delivery_bytes
            else:
                self._delivery_small_budget_bytes = candidate_small_budget
                self._delivery_small_message_limit = candidate_small_limit
                self._delivery_accounted_limit = candidate_accounted_limit
        self._callback_worker_task: asyncio.Task[None] | None = None
        self._publish_waiters = 0
        self._delivery_timeout = delivery_timeout
        self._callback_shutdown_timeout = callback_shutdown_timeout
        self._publish_space = asyncio.Event()
        self._connack_fut: asyncio.Future[ConnAckPacket] | None = None
        self._receipts: dict[int, PublishReceipt | deque[PublishReceipt]] = {}
        self._batch_receipts: dict[int, PublishBatchReceipt | deque[PublishBatchReceipt]] = {}
        self._sub_futs: dict[int, asyncio.Future[SubscribeResult]] = {}
        self._unsub_futs: dict[int, asyncio.Future[UnsubscribeResult]] = {}
        self._messages: asyncio.Queue[IteratorQueueItem] = _DeliveryQueue(
            maxsize=self._max_pending_messages
        )
        self._message_ready = asyncio.Event()
        self._closed = asyncio.Event()
        self._disconnect_exc: BaseException | None = None
        # Set once a connection is torn down, cleared by the next connect. Read
        # by _publish_wait_failure(); _closed is set too late in teardown to use.
        self._teardown_final = False
        self._ping_pending = False
        self._ping_deadline = 0.0
        self._connected_at = 0.0
        self._host = ""
        self._port = 1883
        self._ssl: ssl.SSLContext | bool | None = None
        self._unix_path: str | None = None
        self._ws_url: str | None = None
        self._ws_headers: dict[str, str] | None = None
        self._reconnect = reconnect if reconnect is not None else ReconnectPolicy(enabled=False)
        self._ping_timeout = ping_timeout
        self._ack_timeout = ack_timeout
        self._intentional_disconnect = False
        self._transport_factory: Callable[..., Awaitable[AsyncTransport]] = TcpTransport.connect
        self._last_disconnect: DisconnectInfo | None = None

        self.on_message: OnMessage | None = None
        self.on_connect: OnConnect | None = None
        self.on_disconnect: OnDisconnect | None = None
        self.on_publish: OnPublish | None = None
        self.auth_handler: OnAuth | None = auth_handler

    # Diagnostic compatibility views. Effect state has one owner in EffectPump;
    # these historical read-only names remain for tests and instrumentation.
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

    # Historical private writer views remain for tests and instrumentation.
    # WritePump is the sole state owner; runtime code does not use these views.
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

    @property
    def _last_outbound(self) -> float:
        return self._write_pump.last_outbound

    @_last_outbound.setter
    def _last_outbound(self, value: float) -> None:
        self._write_pump.last_outbound = value

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
            protocol=ProtocolStats(
                pending_outbound_messages=outbound_stats.pending_messages,
                pending_outbound_bytes=outbound_stats.pending_bytes,
                pending_outbound_high_water_messages=(outbound_stats.pending_high_water_messages),
                pending_outbound_high_water_bytes=outbound_stats.pending_high_water_bytes,
                queued_outbound_messages=outbound_stats.queued_messages,
                flow_inflight=outbound_stats.flow_inflight,
                flow_limit=outbound_stats.flow_limit,
                packet_ids_in_use=outbound_stats.packet_ids_in_use,
                inbound_inflight=inbound_stats.inflight,
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
            delivery=DeliveryStats(
                iterator_queued=self._messages.qsize(),
                iterator_limit=self._messages.maxsize,
                callback_queued=self._callback_queue.qsize(),
                callback_limit=self._callback_queue.maxsize,
                pending_bytes=self._pending_delivery_bytes,
                pending_high_water_bytes=self._pending_delivery_high_water_bytes,
                accounted_limit=self._delivery_accounted_limit,
                small_budget_bytes=self._delivery_small_budget_bytes,
                small_message_limit=self._delivery_small_message_limit,
                waiters=self._delivery_waiters,
            ),
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
        return self._engine.state

    @property
    def is_connected(self) -> bool:
        return self._engine.state == ConnectionState.CONNECTED

    @property
    def negotiated(self) -> NegotiatedSettings:
        return self._engine.negotiated

    @property
    def effective_client_id(self) -> str:
        return self.negotiated.effective_client_id(self._engine.config.client_id)

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
    ) -> PublishReceipt:
        """Admit one publish and register its receipt without flushing.

        The caller must already execute synchronously on the client's event loop.
        Keeping finalization separate lets adapters commit a bounded batch and
        collect/drain effects once.
        """
        handle = self._engine.queue_publish(
            topic,
            payload,
            qos=qos,
            retain=retain,
            properties=properties,
        )
        if handle.qos == QoS.AT_MOST_ONCE:
            return PublishReceipt(mid=None, qos=handle.qos, _event=None)
        assert handle.mid is not None
        receipt = PublishReceipt(
            mid=handle.mid,
            qos=handle.qos,
            _event=asyncio.Event(),
        )
        self._register_publish_receipt(handle.mid, receipt)
        return receipt

    def _direct_qos0_ready(self) -> bool:
        """True when a native QoS 0 write can bypass the effect adapter safely."""

        return (
            self.on_publish is None
            and not self._pending_effects
            and not self._engine.has_pending_effects
        )

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
        if QoS(qos) is not QoS.AT_MOST_ONCE or not self._direct_qos0_ready():
            return None
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
                raise FlowControlError("Outbound backpressure limit reached")
            return None
        return PublishReceipt(mid=None, qos=QoS.AT_MOST_ONCE, _event=None)

    def _try_direct_qos0_many(
        self,
        requests: list[tuple[str, bytes, QoS | int, bool, Properties | None]],
        receipt: PublishBatchReceipt,
        *,
        nowait: bool,
    ) -> bool:
        if not requests or not self._direct_qos0_ready():
            return False
        for _topic, _payload, qos, _retain, _properties in requests:
            if QoS(qos) is not QoS.AT_MOST_ONCE:
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
                raise FlowControlError("Outbound backpressure limit reached")
            return False
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
        handle = self._engine.queue_publish(
            topic,
            payload,
            qos=qos,
            retain=retain,
            properties=properties,
        )
        assert handle.mid is not None
        receipt = PublishReceipt(
            mid=handle.mid,
            qos=handle.qos,
            _event=asyncio.Event(),
        )
        self._register_publish_receipt(handle.mid, receipt)
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
        self._engine.queue_publish(
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
        async with self._lifecycle_lock:
            await self._reset_message_stream()
            self._host = host
            self._port = port
            self._ssl = ssl
            was_alt = self._unix_path is not None or self._ws_url is not None
            self._unix_path = None
            self._ws_url = None
            self._ws_headers = None
            if was_alt:
                self._transport_factory = TcpTransport.connect
            self._intentional_disconnect = False
            timeout = timeout if timeout is not None else self._reconnect.connect_timeout
            self._reconnect.reset()
            return await self._connect_once_locked(host, port, ssl=ssl, timeout=timeout)

    async def connect_unix(
        self,
        path: str,
        *,
        timeout: float | None = None,
    ) -> ConnAckPacket:
        """Connect over a Unix domain socket (AF_UNIX)."""
        async with self._lifecycle_lock:
            await self._reset_message_stream()
            self._unix_path = path
            self._ws_url = None
            self._ws_headers = None
            self._host = path
            self._port = 0
            self._ssl = None
            self._intentional_disconnect = False

            async def _factory(
                host: str,
                port: int,
                *,
                ssl: object | None = None,
            ) -> AsyncTransport:
                return await UnixSocketTransport.connect(self._unix_path or host)

            self._transport_factory = _factory
            timeout = timeout if timeout is not None else self._reconnect.connect_timeout
            self._reconnect.reset()
            return await self._connect_once_locked(path, 0, ssl=None, timeout=timeout)

    async def connect_ws(
        self,
        url: str,
        *,
        ssl: ssl.SSLContext | bool | None = None,
        extra_headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> ConnAckPacket:
        """Connect over MQTT-over-WebSocket (``ws://`` / ``wss://``)."""
        async with self._lifecycle_lock:
            await self._reset_message_stream()
            self._ws_url = url
            self._ws_headers = extra_headers
            self._unix_path = None
            self._host = url
            self._port = 0
            self._ssl = ssl
            self._intentional_disconnect = False

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

            self._transport_factory = _factory
            timeout = timeout if timeout is not None else self._reconnect.connect_timeout
            self._reconnect.reset()
            return await self._connect_once_locked(url, 0, ssl=ssl, timeout=timeout)

    async def _connect_once_locked(
        self,
        host: str,
        port: int,
        *,
        ssl: ssl.SSLContext | bool | None = None,
        timeout: float = 30.0,
    ) -> ConnAckPacket:
        loop = asyncio.get_running_loop()
        owner_loop = self._owner_loop
        if owner_loop is None:
            self._owner_loop = loop
        elif owner_loop is not loop:
            raise RuntimeError("AsyncClient is bound to a different event loop")
        if self._engine.state in (ConnectionState.CONNECTED, ConnectionState.CONNECTING):
            raise ProtocolError("Already connected or connecting")
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
            self._closed.clear()
            self._disconnect_exc = None
            self._teardown_final = False
            self._last_disconnect = None
            self._decoder.clear()
            self._write_pump.reset()
            self._ping_pending = False
            connect_packet = self._engine.begin_connect()
            self._connack_fut = loop.create_future()
            self._write_pump.start(transport)
            await self._enqueue_outbound(connect_packet)
            self._reader_task = asyncio.create_task(self._read_loop(), name="mqttium-reader")
            try:
                connack = await asyncio.wait_for(self._connack_fut, timeout=timeout)
            except TimeoutError as exc:
                raise MQTTTimeoutError("CONNACK timed out") from exc
            if connack.reason_code != 0:
                raise ProtocolError(f"Connection refused: reason_code={connack.reason_code}")
            self._connected_at = time.monotonic()
            self._write_pump.last_outbound = time.monotonic()
            self._keepalive_task = asyncio.create_task(
                self._keepalive_loop(), name="mqttium-keepalive"
            )
            return connack
        except BaseException:
            self._intentional_disconnect = True
            if self._connack_fut is not None and not self._connack_fut.done():
                self._connack_fut.cancel()
            try:
                await self._force_close()
            except BaseException:
                pass
            if self._engine.state not in (
                ConnectionState.NEW,
                ConnectionState.DISCONNECTED,
            ):
                self._engine.notify_transport_closed()
                self._engine.take_effects()
            raise

    async def disconnect(self, reason_code: int = 0) -> None:
        self._intentional_disconnect = True
        reconnect_task = self._reconnect_task
        if reconnect_task is not None and reconnect_task is not asyncio.current_task():
            reconnect_task.cancel()
            try:
                await reconnect_task
            except asyncio.CancelledError:
                pass
            self._reconnect_task = None
        async with self._lifecycle_lock:
            if self._transport is None:
                return
            if self.is_connected:
                packet = self._engine.begin_disconnect(reason_code)
                await self._enqueue_outbound(packet)
                try:
                    # Skip the wait if the writer already died (connection lost).
                    writer_task = self._write_pump.task
                    if writer_task is not None and not writer_task.done():
                        await asyncio.wait_for(self._write_pump.join(), timeout=5.0)
                except TimeoutError:
                    pass
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
        self._check_nowait_publish_capacity(topic, data, qos, retain, properties)
        receipt = self._queue_publish_on_loop(
            topic,
            data,
            qos=qos,
            retain=retain,
            properties=properties,
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
        data = payload.encode("utf-8") if isinstance(payload, str) else payload
        while True:
            wait_for_space = False
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
                    if nowait:
                        self._check_nowait_publish_capacity(topic, data, qos, retain, properties)
                    # Keep the native async hot path inline. Routing these
                    # operations through the adapter boundary measured 2.36% slower.
                    handle = self._engine.queue_publish(
                        topic,
                        data,
                        qos=qos,
                        retain=retain,
                        properties=properties,
                    )
                    if handle.qos == QoS.AT_MOST_ONCE:
                        receipt = PublishReceipt(mid=None, qos=handle.qos, _event=None)
                    else:
                        assert handle.mid is not None
                        receipt = PublishReceipt(
                            mid=handle.mid,
                            qos=handle.qos,
                            _event=asyncio.Event(),
                        )
                        self._register_publish_receipt(handle.mid, receipt)
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
                    self._publish_space.clear()
                    self._publish_waiters += 1
                    wait_for_space = True
                else:
                    self._collect_effects_locked()
                    self._drain_effects_inline()
            if not wait_for_space:
                if self._pending_effects:
                    if nowait:
                        self._schedule_effect_flush()
                    else:
                        await self._drain_effects()
                return receipt
            try:
                await self._publish_space.wait()
            finally:
                self._publish_waiters -= 1

    async def _admit_publish_many(
        self,
        requests: list[tuple[str, bytes, QoS | int, bool, Properties | None]],
        receipt: PublishBatchReceipt,
        *,
        nowait: bool,
    ) -> None:
        while True:
            wait_for_space = False
            async with self._engine_lock:
                try:
                    if self._try_direct_qos0_many(requests, receipt, nowait=nowait):
                        return
                    if nowait:
                        self._check_nowait_publish_many_capacity(requests)
                    handles = self._engine.queue_publish_many(requests)
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
                    self._publish_space.clear()
                    self._publish_waiters += 1
                    wait_for_space = True
                else:
                    for handle in handles:
                        receipt._register(handle.mid)
                        if handle.mid is not None:
                            self._register_batch_receipt(handle.mid, receipt)
                    self._collect_effects_locked()
                    self._drain_effects_inline()
                    return
            if wait_for_space:
                try:
                    await self._publish_space.wait()
                finally:
                    self._publish_waiters -= 1

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
            self._engine.reconfigure(accept_auth=True)
            self._engine.queue_auth(reason_code=reason_code, properties=properties)
            self._collect_effects_locked()
        await self._drain_effects()

    def set_auth_handler(self, handler: OnAuth | None) -> None:
        """Register or clear the enhanced-authentication handler."""
        self.auth_handler = handler
        self._engine.reconfigure(accept_auth=handler is not None)

    async def subscribe(
        self,
        topics: str | Iterable[str | tuple[str, SubscribeOptions | int | QoS]],
        *,
        qos: int | QoS = 0,
        properties: Properties | None = None,
        timeout: float | None = None,
    ) -> SubscribeResult:
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
        await self._drain_effects()
        try:
            return await asyncio.wait_for(
                fut, timeout=timeout if timeout is not None else self._ack_timeout
            )
        except TimeoutError as exc:
            self._sub_futs.pop(mid, None)
            raise MQTTTimeoutError(f"SUBACK timed out for mid={mid}") from exc

    async def unsubscribe(
        self,
        topics: str | Iterable[str],
        *,
        timeout: float | None = None,
    ) -> UnsubscribeResult:
        loop = asyncio.get_running_loop()
        async with self._engine_lock:
            mid = self._engine.queue_unsubscribe(topics)
            fut: asyncio.Future[UnsubscribeResult] = loop.create_future()
            self._unsub_futs[mid] = fut
            self._collect_effects_locked()
        await self._drain_effects()
        try:
            return await asyncio.wait_for(
                fut, timeout=timeout if timeout is not None else self._ack_timeout
            )
        except TimeoutError as exc:
            self._unsub_futs.pop(mid, None)
            raise MQTTTimeoutError(f"UNSUBACK timed out for mid={mid}") from exc

    async def messages(self) -> AsyncIterator[Message]:
        # Fast-path get_nowait avoids allocating two tasks per delivered
        # message. The event is only a wake-up signal; the queue remains the
        # source of truth and is drained before the stream terminates.
        while True:
            try:
                item = self._messages.get_nowait()
                if isinstance(item, tuple):
                    message, delivery_token = item
                    self._release_delivery_reference_nowait(delivery_token)
                    yield message
                else:
                    yield item
                continue
            except asyncio.QueueEmpty:
                if self._closed.is_set():
                    return
            self._message_ready.clear()
            if not self._messages.empty() or self._closed.is_set():
                continue
            await self._message_ready.wait()

    async def ack(self, message: Message) -> None:
        """Acknowledge an inbound QoS>0 message when ``manual_ack=True``.

        Defers PUBACK (QoS 1) or PUBCOMP (QoS 2). PUBREC is always immediate.
        """
        if message.mid is None:
            return
        async with self._engine_lock:
            self._engine.ack(message.mid)
            self._collect_effects_locked()
        await self._drain_effects()

    async def _read_loop(self) -> None:
        assert self._transport is not None
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
                        with self._engine.store.batch():
                            handled, handled_bytes = self._decoder.process_packets_bounded(
                                self._engine.handle_raw,
                                limit=256,
                                max_bytes=self._max_ingress_batch_bytes,
                            )
                        if handled:
                            self._collect_effects_locked()
                    if not handled:
                        break
                    if self._pending_effects:
                        await self._drain_effects()
                    if handled >= 256 or handled_bytes >= self._max_ingress_batch_bytes:
                        await asyncio.sleep(0)
        except asyncio.CancelledError:
            raise
        except (PacketTooLargeError, MalformedPacketError, ProtocolError) as exc:
            # Fatal wire/protocol error: send a normative DISCONNECT (v5) before
            # tearing down, so a strict broker sees *why* we left.
            await self._send_fatal_disconnect(exc)
            self._disconnect_exc = exc
            if not self._will_reconnect():
                self._fail_pending(exc)
        except Exception as exc:
            self._disconnect_exc = exc
            if not self._will_reconnect():
                self._fail_pending(exc)
        finally:
            await self._invalidate_connection_epoch()
            async with self._engine_lock:
                self._engine.notify_transport_closed()
                self._collect_effects_locked()
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
            self._closed.set()
            self._message_ready.set()
            try:
                await self._invoke(self.on_disconnect, self._disconnect_exc)
            except Exception as exc:
                self._report_callback_error(self.on_disconnect, exc)
            if not will_reconnect:
                await self._shutdown_callback_worker(drain=True)
            if will_reconnect and (self._reconnect_task is None or self._reconnect_task.done()):
                self._reconnect_task = asyncio.create_task(
                    self._reconnect_loop(), name="mqttium-reconnect"
                )

    def _will_reconnect(self) -> bool:
        reason = None
        if self._last_disconnect is not None and self._last_disconnect.from_broker:
            reason = self._last_disconnect.reason_code
        return (
            not isinstance(self._disconnect_exc, MessageDeliveryError)
            and not self._intentional_disconnect
            and self._reconnect.enabled
            and self._reconnect.should_retry(reason, self._engine.config.protocol)
        )

    async def _writer_failed(self, exc: BaseException) -> None:
        """Apply AsyncClient's connection policy after a writer failure."""
        self._disconnect_exc = exc
        if not self._will_reconnect():
            self._fail_pending(exc)
        async with self._engine_lock:
            self._engine.notify_transport_closed()
            self._collect_effects_locked()
        await self._drain_effects()
        self._closed.set()
        # Close transport so the reader unblocks and runs shared cleanup.
        if self._transport is not None:
            await self._transport.close()

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
    ) -> None:
        level = QoS(qos)
        will_send = self._engine.state == ConnectionState.CONNECTED and (
            level == QoS.AT_MOST_ONCE or self._engine.flow.available > 0
        )
        if not will_send:
            return
        size = self._preview_publish_size(topic, payload, level, properties)
        if not self._can_enqueue_outbound_size(size):
            raise FlowControlError("Outbound backpressure limit reached")

    def _check_nowait_publish_many_capacity(
        self,
        requests: list[tuple[str, bytes, QoS | int, bool, Properties | None]],
    ) -> None:
        if self._engine.state != ConnectionState.CONNECTED:
            return
        messages = self._write_pump.queued_messages
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
                raise FlowControlError("Outbound backpressure limit reached")
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
                        await self._force_close()
                        return
                    await asyncio.sleep(min(0.5, self._ping_deadline - now))
                    continue
                due = self._write_pump.last_outbound + k
                if now >= due:
                    async with self._engine_lock:
                        self._engine.queue_ping()
                        self._collect_effects_locked()
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
                reason = None
                if self._last_disconnect is not None and self._last_disconnect.from_broker:
                    reason = self._last_disconnect.reason_code
                elif isinstance(self._disconnect_exc, ProtocolError):
                    msg = str(self._disconnect_exc)
                    if "reason_code=" in msg:
                        try:
                            reason = int(msg.rsplit("=", 1)[-1])
                        except ValueError:
                            reason = None
                if not self._reconnect.should_retry(reason, self._engine.config.protocol):
                    self._fail_pending(self._disconnect_exc or MQTTError("Reconnect exhausted"))
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
        if kind is EffectKind.CONNACK and self.on_connect is None:
            connack: ConnAckPacket = effect.data
            if self._connack_fut is not None and not self._connack_fut.done():
                self._connack_fut.set_result(connack)
            return True
        if kind is EffectKind.PUBLISH_COMPLETE and self.on_publish is None:
            self._settle_publish(effect.data, None)
            return True
        if kind is EffectKind.PUBLISH_FAILED and self.on_publish is None:
            failure: PublishFailure = effect.data
            self._settle_publish(failure.mid, failure.reason)
            return True
        if kind is EffectKind.SUBACK:
            sub_result = SubscribeResult.from_packet(effect.data)
            sub_fut = self._sub_futs.pop(sub_result.mid, None)
            if sub_fut is not None and not sub_fut.done():
                sub_fut.set_result(sub_result)
            return True
        if kind is EffectKind.UNSUBACK:
            unsub_result = UnsubscribeResult.from_packet(effect.data)
            unsub_fut = self._unsub_futs.pop(unsub_result.mid, None)
            if unsub_fut is not None and not unsub_fut.done():
                unsub_fut.set_result(unsub_result)
            return True
        if kind is EffectKind.PINGRESP:
            self._ping_pending = False
            return True
        return False

    def _apply_message_effect_batch_inline(
        self,
        effects: deque[EngineEffect],
        epoch: int,
    ) -> int:
        if epoch != self._connection_epoch or len(effects) < 2:
            return 0
        first: Message = effects[0].data
        if first.qos != QoS.AT_MOST_ONCE:
            return 0
        callback = self.on_message
        callback_delivery = callback is not None and self._message_delivery in (
            "auto",
            "callback",
            "both",
        )
        iterator_delivery = self._message_delivery in ("iterator", "both") or (
            self._message_delivery == "auto" and callback is None
        )
        if not callback_delivery and not iterator_delivery:
            return 0
        small_limit = self._delivery_small_message_limit
        callback_worker_ready = False
        applied = 0
        for effect in effects:
            if effect.kind is not EffectKind.MESSAGE:
                break
            msg: Message = effect.data
            if msg.qos != QoS.AT_MOST_ONCE:
                break
            if small_limit is not None:
                if (
                    small_limit <= 0
                    or msg.properties
                    or len(msg.payload) + 4 * len(msg.topic) > small_limit
                ):
                    break
            if iterator_delivery and self._messages.full():
                break
            if callback_delivery and self._callback_queue.full():
                break
            if iterator_delivery:
                self._messages.put_nowait(msg)
            if callback_delivery:
                assert callback is not None
                if not callback_worker_ready:
                    self._ensure_callback_worker()
                    callback_worker_ready = True
                self._callback_queue.put_nowait((callback, (msg,), None))
            applied += 1
        if applied and iterator_delivery:
            self._message_ready.set()
        return applied

    async def _flush_effects(self, *, nowait: bool = False) -> None:
        async with self._engine_lock:
            self._collect_effects_locked()
        if nowait:
            self._schedule_effect_flush()
            return
        await self._drain_effects()

    async def _apply_effect(
        self,
        effect: EngineEffect,
        *,
        nowait: bool,
        epoch: int | None = None,
    ) -> None:
        kind = effect.kind
        if kind is EffectKind.SEND:
            await self._enqueue_outbound(effect.data, nowait=nowait, epoch=epoch)
        elif kind is EffectKind.CONNACK:
            connack: ConnAckPacket = effect.data
            if self._connack_fut is not None and not self._connack_fut.done():
                self._connack_fut.set_result(connack)
            if self.on_connect is not None:
                await self._enqueue_callback(self.on_connect, connack)
        elif kind is EffectKind.AUTH:
            challenge: AuthPacket = effect.data
            if self.auth_handler is None:
                await self._enqueue_outbound(
                    encode_disconnect(0x8C, self._engine.config.protocol),
                    nowait=nowait,
                    epoch=epoch,
                )
                return
            response = await asyncio.wait_for(
                self._invoke(self.auth_handler, challenge), timeout=10.0
            )
            if isinstance(response, AuthPacket):
                async with self._engine_lock:
                    self._engine.queue_auth(
                        reason_code=response.reason_code,
                        properties=response.properties,
                    )
                    self._collect_effects_locked()
        elif kind is EffectKind.MESSAGE:
            msg: Message = effect.data
            callback_delivery = self.on_message is not None and self._message_delivery in (
                "auto",
                "callback",
                "both",
            )
            iterator_delivery = self._message_delivery in ("iterator", "both") or (
                self._message_delivery == "auto" and self.on_message is None
            )
            references = int(iterator_delivery) + int(callback_delivery)
            small_limit = self._delivery_small_message_limit
            small_delivery = references and (
                small_limit is None
                or (
                    small_limit > 0
                    # An MQTT 5 PUBLISH without properties decodes to an empty
                    # `Properties`, not to None: testing truthiness rather than
                    # identity is what keeps those messages on the fast path.
                    # An empty table contributes no logical bytes, so the bound
                    # below stays a valid upper bound of _delivery_logical_size.
                    and not msg.properties
                    and len(msg.payload) + 4 * len(msg.topic) <= small_limit
                )
            )
            if small_delivery:
                if iterator_delivery:
                    try:
                        self._messages.put_nowait(msg)
                    except asyncio.QueueFull:
                        await self._put_message_slow(msg)
                    else:
                        self._message_ready.set()
                if callback_delivery:
                    assert self.on_message is not None
                    self._ensure_callback_worker()
                    job: CallbackJob = (self.on_message, (msg,), None)
                    try:
                        self._callback_queue.put_nowait(job)
                    except asyncio.QueueFull:
                        await self._enqueue_callback_slow(job)
                if msg.mid is not None:
                    async with self._engine_lock:
                        self._engine.mark_inbound_delivered(msg.mid)
                return

            delivery_token: DeliveryToken = None
            iterator_enqueued = False
            callback_enqueued = False
            if references:
                logical_bytes = self._delivery_logical_size(msg)
                limit = self._delivery_accounted_limit
                if limit is not None and logical_bytes > limit:
                    raise MessageDeliveryError(
                        f"Message requires {logical_bytes} delivery bytes, exceeding limit {limit}"
                    )
                delivery_token = self._try_reserve_delivery(logical_bytes, references)
                if delivery_token is None:
                    delivery_token = await self._reserve_delivery_slow(logical_bytes, references)
            try:
                if iterator_delivery:
                    iterator_item: IteratorQueueItem = (
                        (msg, delivery_token) if delivery_token is not None else msg
                    )
                    try:
                        self._messages.put_nowait(iterator_item)
                    except asyncio.QueueFull:
                        await self._put_message_slow(iterator_item)
                    else:
                        self._message_ready.set()
                    iterator_enqueued = True
                if callback_delivery:
                    assert self.on_message is not None
                    self._ensure_callback_worker()
                    job = (self.on_message, (msg,), delivery_token)
                    try:
                        self._callback_queue.put_nowait(job)
                    except asyncio.QueueFull:
                        await self._enqueue_callback_slow(job)
                    callback_enqueued = True
            except BaseException:
                if delivery_token is not None:
                    unqueued = references - int(iterator_enqueued) - int(callback_enqueued)
                    for _ in range(unqueued):
                        self._release_delivery_reference_nowait(delivery_token)
                raise
            if msg.mid is not None:
                async with self._engine_lock:
                    self._engine.mark_inbound_delivered(msg.mid)
        elif kind is EffectKind.PUBLISH_COMPLETE:
            mid: int | None = effect.data
            self._settle_publish(mid, None)
            if self.on_publish is not None:
                await self._enqueue_callback(self.on_publish, mid, None)
        elif kind is EffectKind.PUBLISH_FAILED:
            failure: PublishFailure = effect.data
            self._settle_publish(failure.mid, failure.reason)
            if self.on_publish is not None:
                await self._enqueue_callback(self.on_publish, failure.mid, failure.reason)
        elif kind is EffectKind.SUBACK:
            sub_result = SubscribeResult.from_packet(effect.data)
            sub_fut = self._sub_futs.pop(sub_result.mid, None)
            if sub_fut is not None and not sub_fut.done():
                sub_fut.set_result(sub_result)
        elif kind is EffectKind.UNSUBACK:
            unsub_result = UnsubscribeResult.from_packet(effect.data)
            unsub_fut = self._unsub_futs.pop(unsub_result.mid, None)
            if unsub_fut is not None and not unsub_fut.done():
                unsub_fut.set_result(unsub_result)
        elif kind is EffectKind.PINGRESP:
            self._ping_pending = False
        elif kind is EffectKind.DISCONNECTED:
            info = effect.data
            if isinstance(info, DisconnectInfo):
                self._last_disconnect = info
                if info.from_broker and self._transport is not None:
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
            raise ProtocolError(str(effect.data))
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

    def _try_reserve_delivery(self, logical_bytes: int, references: int) -> DeliveryToken:
        limit = self._delivery_accounted_limit
        if limit is not None and self._pending_delivery_bytes + logical_bytes > limit:
            return None
        self._pending_delivery_bytes += logical_bytes
        if self._pending_delivery_bytes > self._pending_delivery_high_water_bytes:
            self._pending_delivery_high_water_bytes = self._pending_delivery_bytes
        if references == 1:
            return logical_bytes
        # `references` is derived from callback_delivery + iterator_delivery,
        # so the only remaining runtime case is exactly two consumers.
        return _SharedDeliveryReservation(logical_bytes)

    async def _reserve_delivery(self, message: Message, references: int) -> DeliveryToken:
        logical_bytes = self._delivery_logical_size(message)
        limit = self._delivery_accounted_limit
        if limit is not None and logical_bytes > limit:
            raise MessageDeliveryError(
                f"Message requires {logical_bytes} delivery bytes, exceeding limit {limit}"
            )
        token = self._try_reserve_delivery(logical_bytes, references)
        if token is not None:
            return token
        return await self._reserve_delivery_slow(logical_bytes, references)

    async def _reserve_delivery_slow(self, logical_bytes: int, references: int) -> DeliveryToken:
        while True:
            self._delivery_waiters += 1
            try:
                self._delivery_space.clear()
                token = self._try_reserve_delivery(logical_bytes, references)
                if token is not None:
                    return token
                await self._delivery_space.wait()
            finally:
                self._delivery_waiters -= 1
            token = self._try_reserve_delivery(logical_bytes, references)
            if token is not None:
                return token

    def _release_delivery_reference_nowait(self, token: AccountedDeliveryToken) -> None:
        if isinstance(token, int):
            self._pending_delivery_bytes -= token
        else:
            remaining = token.remaining
            if remaining <= 0:
                return
            remaining -= 1
            token.remaining = remaining
            if remaining:
                return
            logical_bytes = token.logical_bytes
            token.logical_bytes = 0
            self._pending_delivery_bytes -= logical_bytes
        if self._delivery_waiters:
            self._delivery_space.set()

    async def _release_delivery_reference(self, token: AccountedDeliveryToken) -> None:
        self._release_delivery_reference_nowait(token)

    def _delivery_logical_size(self, message: Message) -> int:
        property_bytes = 0
        if (
            self._engine.config.protocol == MQTTProtocolVersion.MQTTv5
            and message.properties is not None
            and message.properties.values
        ):
            property_bytes = len(encode_properties(message.properties, PUBLISH))
        topic_bytes = (
            len(message.topic) if message.topic.isascii() else len(message.topic.encode("utf-8"))
        )
        return len(message.payload) + topic_bytes + property_bytes

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
            receipt = self._pop_publish_receipt(mid)
            if receipt is not None:
                if reason is not None:
                    receipt._error = reason
                if receipt._event is not None:
                    receipt._event.set()
            batch = self._pop_batch_receipt(mid)
            if batch is not None:
                batch._complete(mid, reason)
        self._notify_publish_space()

    def _notify_publish_space(self) -> None:
        if self._publish_waiters:
            self._publish_space.set()

    def _publish_wait_failure(self) -> BaseException | None:
        """Why a producer must not park on outbound admission capacity.

        Admission capacity is only released by an ACK, so once the connection is
        gone for good nothing can ever wake a parked ``publish()``. Reconnect in
        progress is not terminal: the replayed session still settles the budget.
        """
        if not self._teardown_final or self._will_reconnect():
            return None
        return self._disconnect_exc or MQTTError("Connection closed")

    async def _reset_message_stream(self) -> None:
        if not self._closed.is_set():
            return
        while True:
            try:
                item = self._messages.get_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                if isinstance(item, tuple):
                    _message, delivery_token = item
                    self._release_delivery_reference_nowait(delivery_token)
        self._messages = _DeliveryQueue(maxsize=self._max_pending_messages)
        self._message_ready = asyncio.Event()
        self._closed.clear()

    async def _put_message(self, item: IteratorQueueItem) -> None:
        try:
            self._messages.put_nowait(item)
        except asyncio.QueueFull:
            await self._put_message_slow(item)
        else:
            self._message_ready.set()

    async def _put_message_slow(self, item: IteratorQueueItem) -> None:
        try:
            await asyncio.wait_for(
                self._messages.put(item),
                timeout=self._delivery_timeout,
            )
        except TimeoutError as exc:
            raise MessageDeliveryError(
                f"Iterator delivery queue remained full for {self._delivery_timeout:.3f}s"
            ) from exc
        self._message_ready.set()

    def _ensure_callback_worker(self) -> None:
        if self._callback_worker_task is None or self._callback_worker_task.done():
            self._callback_worker_task = asyncio.create_task(
                self._callback_worker(), name="mqttium-callback-worker"
            )

    def _spawn_callback(self, callback: Callable[..., Any], *args: Any) -> None:
        """Compatibility entry point for loop-thread callback producers."""
        self._ensure_callback_worker()
        try:
            self._callback_queue.put_nowait((callback, args, None))
        except asyncio.QueueFull as exc:
            raise MessageDeliveryError("Callback delivery queue is full") from exc

    async def _enqueue_callback(
        self,
        callback: Callable[..., Any],
        *args: Any,
        delivery_token: DeliveryToken = None,
    ) -> None:
        self._ensure_callback_worker()
        job = (callback, args, delivery_token)
        try:
            self._callback_queue.put_nowait(job)
        except asyncio.QueueFull:
            await self._enqueue_callback_slow(job)

    async def _enqueue_callback_slow(self, job: CallbackJob) -> None:
        try:
            await asyncio.wait_for(
                self._callback_queue.put(job),
                timeout=self._delivery_timeout,
            )
        except TimeoutError as exc:
            raise MessageDeliveryError(
                f"Callback delivery queue remained full for {self._delivery_timeout:.3f}s"
            ) from exc

    async def _callback_worker(self) -> None:
        try:
            while True:
                callback, args, delivery_token = await self._callback_queue.get()
                try:
                    await self._invoke(callback, *args)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._report_callback_error(callback, exc)
                finally:
                    if delivery_token is not None:
                        self._release_delivery_reference_nowait(delivery_token)
                    self._callback_queue.task_done()
        except asyncio.CancelledError:
            raise

    def _report_callback_error(
        self,
        callback: Callable[..., Any] | None,
        exc: BaseException,
    ) -> None:
        asyncio.get_running_loop().call_exception_handler(
            {
                "message": "mqttium user callback failed",
                "exception": exc,
                "callback": callback,
            }
        )

    async def _shutdown_callback_worker(self, *, drain: bool) -> None:
        task = self._callback_worker_task
        if task is None:
            return
        if drain and not task.done():
            try:
                await asyncio.wait_for(
                    self._callback_queue.join(),
                    timeout=self._callback_shutdown_timeout,
                )
            except TimeoutError:
                pass
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._callback_worker_task = None
        while True:
            try:
                _callback, _args, delivery_token = self._callback_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                if delivery_token is not None:
                    self._release_delivery_reference_nowait(delivery_token)
                self._callback_queue.task_done()

    async def _invalidate_connection_epoch(self) -> None:
        self._connection_epoch += 1
        await self._write_pump.advance_epoch(self._connection_epoch)

    def _settle_terminal_effect(self, effect: EngineEffect) -> None:
        """Settle one terminal publish effect during final teardown."""
        if effect.kind is EffectKind.PUBLISH_COMPLETE:
            mid: int | None = effect.data
            reason: BaseException | None = None
        else:
            failure: PublishFailure = effect.data
            mid = failure.mid
            reason = failure.reason
        self._settle_publish(mid, reason)
        if self.on_publish is not None:
            try:
                self._spawn_callback(self.on_publish, mid, reason)
            except MessageDeliveryError as exc:
                self._report_callback_error(self.on_publish, exc)

    def _register_publish_receipt(self, mid: int, receipt: PublishReceipt) -> None:
        current = self._receipts.get(mid)
        if current is None:
            self._receipts[mid] = receipt
        elif isinstance(current, deque):
            current.append(receipt)
        else:
            self._receipts[mid] = deque((current, receipt))

    def _pop_publish_receipt(self, mid: int) -> PublishReceipt | None:
        current = self._receipts.get(mid)
        if current is None:
            return None
        if not isinstance(current, deque):
            self._receipts.pop(mid, None)
            return current
        receipt = current.popleft()
        if len(current) == 1:
            self._receipts[mid] = current[0]
        elif not current:
            self._receipts.pop(mid, None)
        return receipt

    def _register_batch_receipt(self, mid: int, receipt: PublishBatchReceipt) -> None:
        current = self._batch_receipts.get(mid)
        if current is None:
            self._batch_receipts[mid] = receipt
        elif isinstance(current, deque):
            current.append(receipt)
        else:
            self._batch_receipts[mid] = deque((current, receipt))

    def _pop_batch_receipt(self, mid: int) -> PublishBatchReceipt | None:
        current = self._batch_receipts.get(mid)
        if current is None:
            return None
        if not isinstance(current, deque):
            self._batch_receipts.pop(mid, None)
            return current
        receipt = current.popleft()
        if len(current) == 1:
            self._batch_receipts[mid] = current[0]
        elif not current:
            self._batch_receipts.pop(mid, None)
        return receipt

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
                if receipt._event is not None:
                    receipt._event.set()
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
            await self._transport.write(encode_disconnect(reason, MQTTProtocolVersion.MQTTv5))
        except Exception:
            pass

    async def _invoke(self, callback: Callable[..., Any] | None, *args: Any) -> Any:
        if callback is None:
            return None
        result = callback(*args)
        if isinstance(result, Awaitable):
            return await result
        return result

    async def _force_close(self, *, preserve_reconnect: bool = False) -> None:
        await self._invalidate_connection_epoch()
        current = asyncio.current_task()
        tasks = [
            self._reader_task,
            self._keepalive_task,
            self._effect_flush_task,
        ]
        if not preserve_reconnect:
            tasks.append(self._reconnect_task)
        for task in tasks:
            if task is not None and task is not current:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        await self._write_pump.stop()
        self._discard_connection_effects(settle_publish=True)
        self._write_pump.discard()
        self._decoder.clear()
        await self._write_pump.wake_waiters()
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
        self._closed.set()
        self._message_ready.set()
