"""Reusable mqttium scenarios for the cooperative concurrency scheduler."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

from mqttium.api.async_client import AsyncClient
from mqttium.protocol.reconnect import ReconnectPolicy

from tests.concurrency.broker import BrokerFactory, ControllableBroker
from tests.concurrency.instrument import RuntimeHooks, factory_wrapping
from tests.concurrency.scheduler import Chooser, CooperativeScheduler, RunResult, Schedule

ActionFn = Callable[[], Awaitable[None] | None]


@dataclass
class MqttHarness:
    scheduler: CooperativeScheduler
    client: AsyncClient
    factory: BrokerFactory
    hooks: RuntimeHooks

    @property
    def broker(self) -> ControllableBroker:
        return self.factory.current

    def close_transport(self) -> Awaitable[None]:
        return self.broker.close()

    def fail_write(self) -> None:
        self.broker.fail_writes()

    def inject_last_puback(self) -> None:
        publishes = self.broker.publishes
        if not publishes or publishes[-1].mid is None:
            return
        self.broker.inject_puback(publishes[-1].mid)

    def inject_inbound(self, topic: str = "in/1", payload: bytes = b"x") -> None:
        self.broker.inject_publish(topic, payload)

    def actions(self, extra: dict[str, ActionFn] | None = None) -> dict[str, ActionFn]:
        mapping: dict[str, ActionFn] = {
            "close_transport": self.close_transport,
            "fail_write": self.fail_write,
            "inject_puback": self.inject_last_puback,
            "inject_inbound": self.inject_inbound,
        }
        if extra:
            mapping.update(extra)
        return mapping


def _default_reconnect() -> ReconnectPolicy:
    return ReconnectPolicy(
        enabled=False,
        initial_delay=0.0,
        max_delay=0.0,
        stable_after=0.0,
        connect_timeout=1.0,
    )


async def connected_harness(
    scheduler: CooperativeScheduler,
    *,
    auto_ack: bool = True,
    reconnect: ReconnectPolicy | None = None,
    **client_kwargs: Any,
) -> MqttHarness:
    factory = BrokerFactory(auto_ack=auto_ack)
    client_kwargs.setdefault("client_id", "concurrency")
    client_kwargs.setdefault("keepalive", 0)
    client_kwargs.setdefault("reconnect", reconnect or _default_reconnect())
    client = AsyncClient(**client_kwargs)
    hooks = RuntimeHooks(scheduler)
    hooks.install(client)
    client._transport_factory = factory_wrapping(factory, hooks)
    await client.connect("fake", 1883, timeout=2.0)
    return MqttHarness(scheduler=scheduler, client=client, factory=factory, hooks=hooks)


async def run_connected_scenario(
    actors: Callable[[MqttHarness], Sequence[tuple[str, Awaitable[Any]]]],
    *,
    enabled: frozenset[str],
    schedule: Schedule | None = None,
    chooser: Chooser | None = None,
    timeout: float = 2.0,
    idle_timeout: float = 0.15,
    auto_ack: bool = True,
    extra_actions: Callable[[MqttHarness], dict[str, ActionFn]] | None = None,
    reconnect: ReconnectPolicy | None = None,
    **client_kwargs: Any,
) -> tuple[RunResult, MqttHarness]:
    scheduler = CooperativeScheduler(
        enabled=enabled,
        timeout=timeout,
        idle_timeout=idle_timeout,
    )
    harness = await connected_harness(
        scheduler,
        auto_ack=auto_ack,
        reconnect=reconnect,
        **client_kwargs,
    )
    actions = harness.actions(extra_actions(harness) if extra_actions else None)
    try:
        result = await scheduler.run(
            actors(harness),
            chooser=chooser,
            schedule=schedule,
            actions=actions,
            seed=schedule.seed if schedule is not None else None,
            policy=schedule.policy if schedule is not None else "explicit",
        )
        return result, harness
    finally:
        try:
            await asyncio.wait_for(harness.client.disconnect(), timeout=1.0)
        except (Exception, asyncio.CancelledError):
            try:
                await asyncio.wait_for(harness.client._force_close(), timeout=1.0)
            except (Exception, asyncio.CancelledError):
                pass
        harness.hooks.uninstall()


def publish_admit(harness: MqttHarness, *, topic: str = "t/a", qos: int = 1):
    """Return after admission; do not wait for the protocol ACK."""

    async def publish() -> Any:
        return await harness.client.publish(topic, b"A", qos=qos)

    return [("publish_a", publish())]


def publish_one(harness: MqttHarness, *, topic: str = "t/a", qos: int = 1):
    async def publish() -> Any:
        receipt = await harness.client.publish(topic, b"A", qos=qos)
        if qos:
            await receipt.wait()
        return receipt

    return [("publish_a", publish())]


def two_publishers(harness: MqttHarness, *, qos: int = 1):
    async def publish(name: str) -> Any:
        receipt = await harness.client.publish(f"t/{name}", name.encode(), qos=qos)
        if qos:
            await receipt.wait()
        return receipt

    return [("publish_a", publish("a")), ("publish_b", publish("b"))]


WRITE_BOUNDARIES = frozenset({"transport.write.before"})
ENQUEUE_AND_WRITE = frozenset({"writer.enqueue.before", "transport.write.before"})
BACKPRESSURE_BOUNDARIES = frozenset({"transport.write.before", "writer.enqueue.wait"})
DELIVERY_BOUNDARIES = frozenset({"delivery.put.before", "transport.write.before"})
CALLBACK_BOUNDARIES = frozenset({"delivery.callback.before", "transport.write.before"})
EPOCH_BOUNDARIES = frozenset({"transport.write.before", "epoch.invalidate.before"})
RECONNECT_BOUNDARIES = frozenset({"transport.write.before", "reconnect.loop.before"})
