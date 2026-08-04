"""Async-native MQTT client (phase 1).

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
from dataclasses import dataclass
from itertools import islice
from contextlib import nullcontext
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable
from typing import Any, Literal, Never

from mqttium.api.models import (
    PublishBatchReceipt,
    PublishMessage,
    PublishReceipt,
    SubscribeResult,
    UnsubscribeResult,
)
from mqttium.codec.buffer import DEFAULT_MAX_PACKET_SIZE, IncrementalDecoder
from mqttium.codec.properties import PUBLISH, encode_properties
from mqttium.enums import ConnectionState, MQTTProtocolVersion, QoS
from mqttium.errors import (
    FlowControlError,
    MQTTError,
    MQTTTimeoutError,
    MalformedPacketError,
    PublishBatchError,
    MessageDeliveryError,
    PacketTooLargeError,
    ProtocolError,
)
from mqttium.packets import AuthPacket, ConnAckPacket, SubscribeOptions, encode_disconnect
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
from mqttium.transport.writes import WriteItem, item_size
from mqttium.types import Message, Properties

OnMessage = Callable[[Message], Any]
OnConnect = Callable[[ConnAckPacket], Any]
OnDisconnect = Callable[[BaseException | None], Any]
OnPublish = Callable[[int | None, BaseException | None], Any]
OnAuth = Callable[[AuthPacket], Any]
MessageDelivery = Literal["auto", "iterator", "callback", "both"]
PublishBackpressure = Literal["wait", "error"]
CallbackJob = tuple[Callable[..., Any], tuple[Any, ...], int | None]


@dataclass(slots=True)
class _DeliveryReservation:
    logical_bytes: int
    references: int


@dataclass(slots=True)
class _EffectWaiter:
    target: int
    future: asyncio.Future[None]


@dataclass(slots=True)
class _PendingEffect:
    effect: EngineEffect
    epoch: int

    @property
    def kind(self) -> EffectKind:
        return self.effect.kind

    @property
    def data(self) -> Any:
        return self.effect.data


class _StaleConnectionEffect(Exception):
    pass


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
        if (
            max_pending_outbound_messages is not None
            and max_pending_outbound_messages < 0
        ):
            raise ValueError("max_pending_outbound_messages must be non-negative or None")
        if max_pending_outbound_bytes is not None and max_pending_outbound_bytes < 0:
            raise ValueError("max_pending_outbound_bytes must be non-negative or None")
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
        self._delivery_reservations: dict[int, _DeliveryReservation] = {}
        self._delivery_space = asyncio.Condition()
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
        self._transport: AsyncTransport | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._writer_task: asyncio.Task[None] | None = None
        self._keepalive_task: asyncio.Task[None] | None = None
        self._reconnect_task: asyncio.Task[None] | None = None
        self._lifecycle_lock = asyncio.Lock()
        self._engine_lock = asyncio.Lock()
        self._effect_flush_lock = asyncio.Lock()
        self._pending_effects: deque[_PendingEffect] = deque()
        self._connection_epoch = 0
        self._effect_enqueued = 0
        self._effect_applied = 0
        self._effect_waiters: deque[_EffectWaiter] = deque()
        self._callback_queue: asyncio.Queue[CallbackJob] = asyncio.Queue(
            maxsize=max_pending_callbacks
        )
        self._callback_worker_task: asyncio.Task[None] | None = None
        self._effect_flush_task: asyncio.Task[None] | None = None
        self._effect_flush_requested = False
        self._delivery_timeout = delivery_timeout
        self._callback_shutdown_timeout = callback_shutdown_timeout
        self._outbound: asyncio.Queue[WriteItem] = asyncio.Queue()
        self._outbound_bytes = 0
        self._max_outbound_bytes = max_outbound_bytes
        self._max_outbound_messages = max_outbound_messages
        self._outbound_space = asyncio.Condition()
        self._outbound_waiters = 0
        self._publish_space = asyncio.Condition()
        self._connack_fut: asyncio.Future[ConnAckPacket] | None = None
        self._receipts: dict[int, PublishReceipt] = {}
        self._batch_receipts: dict[int, PublishBatchReceipt] = {}
        self._sub_futs: dict[int, asyncio.Future[SubscribeResult]] = {}
        self._unsub_futs: dict[int, asyncio.Future[UnsubscribeResult]] = {}
        self._messages: asyncio.Queue[Message] = asyncio.Queue(maxsize=self._max_pending_messages)
        self._message_ready = asyncio.Event()
        self._closed = asyncio.Event()
        self._disconnect_exc: BaseException | None = None
        self._last_outbound = 0.0
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
            self._last_disconnect = None
            self._decoder.clear()
            self._outbound = asyncio.Queue()
            self._outbound_bytes = 0
            self._ping_pending = False
            connect_packet = self._engine.begin_connect()
            loop = asyncio.get_running_loop()
            self._connack_fut = loop.create_future()
            self._writer_task = asyncio.create_task(self._write_loop(), name="mqttium-writer")
            await self._enqueue_outbound(connect_packet)
            self._reader_task = asyncio.create_task(self._read_loop(), name="mqttium-reader")
            try:
                connack = await asyncio.wait_for(self._connack_fut, timeout=timeout)
            except TimeoutError as exc:
                raise MQTTTimeoutError("CONNACK timed out") from exc
            if connack.reason_code != 0:
                raise ProtocolError(f"Connection refused: reason_code={connack.reason_code}")
            self._connected_at = time.monotonic()
            self._last_outbound = time.monotonic()
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
                    if self._writer_task is not None and not self._writer_task.done():
                        await asyncio.wait_for(self._outbound.join(), timeout=5.0)
                except TimeoutError:
                    pass
            await self._force_close()

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
        receipt = await self._admit_publish(
            topic,
            data,
            qos=qos,
            retain=retain,
            properties=properties,
            nowait=nowait,
        )
        if nowait:
            self._schedule_effect_flush()
        else:
            await self._drain_effects()
        return receipt

    async def _admit_publish(
        self,
        topic: str,
        payload: bytes,
        *,
        qos: int | QoS,
        retain: bool,
        properties: Properties | None,
        nowait: bool,
    ) -> PublishReceipt:
        while True:
            async with self._publish_space:
                async with self._engine_lock:
                    try:
                        handle = self._engine.queue_publish(
                            topic,
                            payload,
                            qos=qos,
                            retain=retain,
                            properties=properties,
                        )
                    except FlowControlError:
                        if (
                            nowait
                            or self._publish_backpressure == "error"
                            or not self._engine.can_ever_admit_publish(
                                topic, payload, qos, properties
                            )
                        ):
                            raise
                    else:
                        if handle.qos == QoS.AT_MOST_ONCE:
                            receipt = PublishReceipt(mid=None, qos=handle.qos, _event=None)
                        else:
                            assert handle.mid is not None
                            receipt = PublishReceipt(
                                mid=handle.mid,
                                qos=handle.qos,
                                _event=asyncio.Event(),
                            )
                            self._receipts[handle.mid] = receipt
                        self._collect_effects_locked()
                        return receipt
                await self._publish_space.wait()

    async def _admit_publish_many(
        self,
        requests: list[tuple[str, bytes, QoS | int, bool, Properties | None]],
        receipt: PublishBatchReceipt,
        *,
        nowait: bool,
    ) -> None:
        while True:
            async with self._publish_space:
                async with self._engine_lock:
                    try:
                        handles = self._engine.queue_publish_many(requests)
                    except FlowControlError:
                        if (
                            nowait
                            or self._publish_backpressure == "error"
                            or not self._engine.can_ever_admit_publish_many(requests)
                        ):
                            raise
                    else:
                        for handle in handles:
                            receipt._register(handle.mid)
                            if handle.mid is not None:
                                self._batch_receipts[handle.mid] = receipt
                        self._collect_effects_locked()
                        return
                await self._publish_space.wait()

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
            self._engine.config.accept_auth = True
            self._engine.queue_auth(reason_code=reason_code, properties=properties)
            self._collect_effects_locked()
        await self._drain_effects()

    def set_auth_handler(self, handler: OnAuth | None) -> None:
        """Register or clear the enhanced-authentication handler."""
        self.auth_handler = handler
        self._engine.config.accept_auth = handler is not None

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
                message = self._messages.get_nowait()
                self._messages.task_done()
                await self._release_delivery_reference(id(message))
                yield message
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
                        store_batch = getattr(self._engine.store, "batch", None)
                        with store_batch() if store_batch is not None else nullcontext():
                            handled = self._decoder.process_packets(
                                self._engine.handle_raw,
                                limit=256,
                            )
                        if handled:
                            self._collect_effects_locked()
                    if not handled:
                        break
                    await self._drain_effects()
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
                async with self._outbound_space:
                    self._outbound_space.notify_all()
                # Cancel writer + close transport so no task/fd leaks.
                if (
                    self._writer_task is not None
                    and self._writer_task is not asyncio.current_task()
                ):
                    self._writer_task.cancel()
                    try:
                        await self._writer_task
                    except (asyncio.CancelledError, Exception):
                        pass
                    self._writer_task = None
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

    async def _write_contiguous(self, transport: AsyncTransport, parts: list[bytes]) -> None:
        if not parts:
            return
        write_many = getattr(transport, "write_many", None)
        if write_many is not None:
            await write_many(parts)
        else:
            for part in parts:
                await transport.write(part)
        parts.clear()

    async def _write_loop(self) -> None:
        assert self._transport is not None
        try:
            while True:
                first = await self._outbound.get()
                batch: list[WriteItem] = [first]
                while len(batch) < 256:
                    try:
                        batch.append(self._outbound.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                try:
                    # Coalesce contiguous small frames into one writelines call.
                    # Never await drain() after the batch (deadlocks vs reader ACK).
                    contiguous: list[bytes] = []
                    transport = self._transport
                    if transport is None:
                        raise ConnectionError("Transport closed while writer was active")

                    for data in batch:
                        if isinstance(data, tuple):
                            await self._write_contiguous(transport, contiguous)
                            for part in data:
                                await transport.write(part)
                        else:
                            contiguous.append(data)
                    await self._write_contiguous(transport, contiguous)
                    self._last_outbound = time.monotonic()
                finally:
                    released = 0
                    for data in batch:
                        released += item_size(data)
                        self._outbound.task_done()
                    async with self._outbound_space:
                        self._outbound_bytes = max(0, self._outbound_bytes - released)
                        if self._outbound_waiters:
                            self._outbound_space.notify_all()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
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

    async def _enqueue_outbound(
        self,
        item: WriteItem,
        *,
        nowait: bool = False,
        epoch: int | None = None,
    ) -> None:
        if epoch is None:
            epoch = self._connection_epoch
        if epoch != self._connection_epoch:
            raise _StaleConnectionEffect
        size = item_size(item)
        # Fast path (no Condition): safe under asyncio's cooperative scheduling
        # as long as we do not await between the capacity check and the update.
        messages_full = self._outbound.qsize() >= self._max_outbound_messages
        bytes_blocked = self._outbound_bytes + size > self._max_outbound_bytes and not (
            self._outbound_bytes == 0 and self._outbound.empty()
        )
        if not messages_full and not bytes_blocked:
            if epoch != self._connection_epoch:
                raise _StaleConnectionEffect
            self._outbound.put_nowait(item)
            self._outbound_bytes += size
            return
        if nowait:
            raise FlowControlError("Outbound backpressure limit reached")

        async with self._outbound_space:
            while True:
                if epoch != self._connection_epoch:
                    raise _StaleConnectionEffect
                messages_full = self._outbound.qsize() >= self._max_outbound_messages
                # Allow a single oversized item into an empty queue (segmented
                # payloads can exceed max_outbound_bytes by the MQTT header).
                bytes_blocked = self._outbound_bytes + size > self._max_outbound_bytes and not (
                    self._outbound_bytes == 0 and self._outbound.empty()
                )
                if not messages_full and not bytes_blocked:
                    break
                # Escape hatch: if the writer is idle but space accounting is
                # wedged, do not block protocol forever.
                if self._outbound.empty() and self._outbound_bytes == 0:
                    break
                self._outbound_waiters += 1
                try:
                    await self._outbound_space.wait()
                finally:
                    self._outbound_waiters -= 1
            if epoch != self._connection_epoch:
                raise _StaleConnectionEffect
            self._outbound.put_nowait(item)
            self._outbound_bytes += size

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
                due = self._last_outbound + k
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

    def _collect_effects_locked(self) -> None:
        sends: list[EngineEffect] = []
        events: list[EngineEffect] = []
        for effect in self._engine.take_effects():
            (sends if effect.kind is EffectKind.SEND else events).append(effect)
        epoch = self._connection_epoch
        self._pending_effects.extend(_PendingEffect(effect, epoch) for effect in sends)
        self._pending_effects.extend(_PendingEffect(effect, epoch) for effect in events)
        self._effect_enqueued += len(sends) + len(events)

    def _schedule_effect_flush(self) -> None:
        # The flusher owns effect progress independently of producer task
        # cancellation. Multiple producers only signal the same worker.
        self._effect_flush_requested = True
        task = self._effect_flush_task
        if task is not None and not task.done():
            return
        task = asyncio.create_task(
            self._run_scheduled_effect_flush(),
            name="mqttium-effect-flush",
        )
        self._effect_flush_task = task
        task.add_done_callback(self._effect_flush_done)

    async def _run_scheduled_effect_flush(self) -> None:
        async with self._effect_flush_lock:
            while True:
                self._effect_flush_requested = False
                while self._pending_effects:
                    pending = self._pending_effects[0]
                    if pending.epoch != self._connection_epoch:
                        self._pending_effects.popleft()
                        self._effect_applied += 1
                        self._resolve_effect_waiters()
                        continue
                    try:
                        await self._apply_effect(
                            pending.effect,
                            nowait=False,
                            epoch=pending.epoch,
                        )
                    except asyncio.CancelledError:
                        raise
                    except _StaleConnectionEffect:
                        self._pending_effects.popleft()
                        self._effect_applied += 1
                        self._resolve_effect_waiters()
                    except BaseException as exc:
                        self._pending_effects.popleft()
                        self._effect_applied += 1
                        self._fail_effect_waiters(exc)
                    else:
                        self._pending_effects.popleft()
                        self._effect_applied += 1
                        self._resolve_effect_waiters()
                if not self._effect_flush_requested:
                    return

    def _effect_flush_done(self, task: asyncio.Task[None]) -> None:
        if self._effect_flush_task is task:
            self._effect_flush_task = None
        try:
            task.result()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            self._fail_effect_waiters(exc)
            asyncio.get_running_loop().call_exception_handler(
                {
                    "message": "mqttium scheduled effect flush failed",
                    "exception": exc,
                    "task": task,
                }
            )

    async def _flush_effects(self, *, nowait: bool = False) -> None:
        async with self._engine_lock:
            self._collect_effects_locked()
        if nowait:
            self._schedule_effect_flush()
            return
        await self._drain_effects()

    async def _drain_effects(self, *, nowait: bool = False) -> None:
        if nowait:
            self._schedule_effect_flush()
            return
        target = self._effect_enqueued
        if self._effect_applied >= target:
            return
        future = asyncio.get_running_loop().create_future()
        self._effect_waiters.append(_EffectWaiter(target=target, future=future))
        self._schedule_effect_flush()
        await asyncio.shield(future)

    def _resolve_effect_waiters(self) -> None:
        while self._effect_waiters and self._effect_waiters[0].target <= self._effect_applied:
            waiter = self._effect_waiters.popleft()
            if not waiter.future.done():
                waiter.future.set_result(None)

    def _fail_effect_waiters(self, exc: BaseException) -> None:
        while self._effect_waiters:
            waiter = self._effect_waiters.popleft()
            if not waiter.future.done():
                waiter.future.set_exception(exc)

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
            delivery_key: int | None = None
            iterator_enqueued = False
            callback_enqueued = False
            if references:
                delivery_key = await self._reserve_delivery(msg, references)
            try:
                if iterator_delivery:
                    await self._put_message(msg)
                    iterator_enqueued = True
                if callback_delivery:
                    assert self.on_message is not None
                    await self._enqueue_callback(
                        self.on_message,
                        msg,
                        delivery_key=delivery_key,
                    )
                    callback_enqueued = True
            except BaseException:
                if delivery_key is not None:
                    unqueued = references - int(iterator_enqueued) - int(callback_enqueued)
                    for _ in range(unqueued):
                        await self._release_delivery_reference(delivery_key)
                raise
            if msg.mid is not None:
                async with self._engine_lock:
                    self._engine.mark_inbound_delivered(msg.mid)
        elif kind is EffectKind.PUBLISH_COMPLETE:
            mid: int = effect.data
            receipt = self._receipts.pop(mid, None)
            if receipt is not None and receipt._event is not None:
                receipt._event.set()
            batch = self._batch_receipts.pop(mid, None)
            if batch is not None:
                batch._complete(mid)
            if self.on_publish is not None:
                await self._enqueue_callback(self.on_publish, mid, None)
            await self._notify_publish_space()
        elif kind is EffectKind.PUBLISH_FAILED:
            failure: PublishFailure = effect.data
            receipt = self._receipts.pop(failure.mid, None)
            if receipt is not None:
                receipt._error = failure.reason
                if receipt._event is not None:
                    receipt._event.set()
            batch = self._batch_receipts.pop(failure.mid, None)
            if batch is not None:
                batch._complete(failure.mid, failure.reason)
            if self.on_publish is not None:
                await self._enqueue_callback(self.on_publish, failure.mid, failure.reason)
            await self._notify_publish_space()
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
        elif kind is EffectKind.PROTOCOL_ERROR:
            raise ProtocolError(str(effect.data))
        else:
            never: Never = kind
            raise MQTTError(f"Unhandled effect {never!r}")

    @property
    def pending_delivery_bytes(self) -> int:
        return self._pending_delivery_bytes

    @property
    def pending_delivery_high_water_bytes(self) -> int:
        return self._pending_delivery_high_water_bytes

    async def _reserve_delivery(self, message: Message, references: int) -> int:
        logical_bytes = self._delivery_logical_size(message)
        limit = self._max_pending_delivery_bytes
        if limit is not None and logical_bytes > limit:
            raise MessageDeliveryError(
                f"Message requires {logical_bytes} delivery bytes, exceeding limit {limit}"
            )
        async with self._delivery_space:
            while limit is not None and self._pending_delivery_bytes + logical_bytes > limit:
                await self._delivery_space.wait()
            key = id(message)
            if key in self._delivery_reservations:
                raise MQTTError("Duplicate delivery reservation for one Message object")
            self._delivery_reservations[key] = _DeliveryReservation(
                logical_bytes=logical_bytes,
                references=references,
            )
            self._pending_delivery_bytes += logical_bytes
            self._pending_delivery_high_water_bytes = max(
                self._pending_delivery_high_water_bytes,
                self._pending_delivery_bytes,
            )
            return key

    async def _release_delivery_reference(self, key: int) -> None:
        async with self._delivery_space:
            reservation = self._delivery_reservations.get(key)
            if reservation is None:
                return
            reservation.references -= 1
            if reservation.references > 0:
                return
            self._delivery_reservations.pop(key, None)
            self._pending_delivery_bytes = max(
                0,
                self._pending_delivery_bytes - reservation.logical_bytes,
            )
            self._delivery_space.notify_all()

    def _delivery_logical_size(self, message: Message) -> int:
        property_bytes = 0
        if (
            self._engine.config.protocol == MQTTProtocolVersion.MQTTv5
            and message.properties is not None
            and message.properties.values
        ):
            property_bytes = len(encode_properties(message.properties, PUBLISH))
        return len(message.payload) + len(message.topic.encode("utf-8")) + property_bytes

    async def _notify_publish_space(self) -> None:
        async with self._publish_space:
            self._publish_space.notify_all()

    async def _reset_message_stream(self) -> None:
        if not self._closed.is_set():
            return
        while True:
            try:
                message = self._messages.get_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                self._messages.task_done()
                await self._release_delivery_reference(id(message))
        self._messages = asyncio.Queue(maxsize=self._max_pending_messages)
        self._message_ready = asyncio.Event()
        self._closed.clear()

    async def _put_message(self, message: Message) -> None:
        try:
            self._messages.put_nowait(message)
        except asyncio.QueueFull:
            try:
                await asyncio.wait_for(
                    self._messages.put(message),
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
        delivery_key: int | None = None,
    ) -> None:
        self._ensure_callback_worker()
        job = (callback, args, delivery_key)
        try:
            self._callback_queue.put_nowait(job)
        except asyncio.QueueFull:
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
                callback, args, delivery_key = await self._callback_queue.get()
                try:
                    await self._invoke(callback, *args)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._report_callback_error(callback, exc)
                finally:
                    if delivery_key is not None:
                        await self._release_delivery_reference(delivery_key)
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
                _callback, _args, delivery_key = self._callback_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                if delivery_key is not None:
                    await self._release_delivery_reference(delivery_key)
                self._callback_queue.task_done()

    async def _invalidate_connection_epoch(self) -> None:
        self._connection_epoch += 1
        async with self._outbound_space:
            self._outbound_space.notify_all()

    def _discard_outbound_queue(self) -> None:
        while True:
            try:
                self._outbound.get_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                self._outbound.task_done()
        self._outbound_bytes = 0

    def _discard_connection_effects(self) -> None:
        # Every pending effect was derived from the old protocol/transport
        # epoch. Session replay, failures and inbound redelivery are rebuilt
        # from the engine/store after the next CONNACK; retaining an old
        # MESSAGE or completion effect can duplicate delivery just as surely
        # as retaining old wire bytes can duplicate a publish. Committed
        # producers are released because durable QoS work continues via replay.
        discarded = len(self._pending_effects)
        self._pending_effects.clear()
        self._effect_applied += discarded
        self._resolve_effect_waiters()

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
        for receipt in self._receipts.values():
            receipt._error = exc
            if receipt._event is not None:
                receipt._event.set()
        self._receipts.clear()
        batches = set(self._batch_receipts.values())
        self._batch_receipts.clear()
        for batch in batches:
            batch._fail_remaining(exc)
        self._fail_non_replayable(exc)

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
            self._writer_task,
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
        self._discard_connection_effects()
        self._discard_outbound_queue()
        self._decoder.clear()
        async with self._outbound_space:
            self._outbound_space.notify_all()
        await self._notify_publish_space()
        if self._reader_task is not current:
            self._reader_task = None
        if self._writer_task is not current:
            self._writer_task = None
        if self._keepalive_task is not current:
            self._keepalive_task = None
        if self._effect_flush_task is not current:
            self._effect_flush_task = None
            self._effect_flush_requested = False
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
