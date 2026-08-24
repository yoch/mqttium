"""Demonstration scenarios that exercise real mqttium runtime interleavings."""

from __future__ import annotations

import asyncio

from mqttium.api.models import PublishReceipt
from mqttium.protocol.reconnect import ReconnectPolicy

from tests.concurrency.scheduler import FirstChooser, Schedule
from tests.concurrency.scenarios import (
    BACKPRESSURE_BOUNDARIES,
    CALLBACK_BOUNDARIES,
    DELIVERY_BOUNDARIES,
    RECONNECT_BOUNDARIES,
    WRITE_BOUNDARIES,
    MqttHarness,
    publish_admit,
    publish_one,
    run_connected_scenario,
    two_publishers,
)


def _immediate_reconnect() -> ReconnectPolicy:
    return ReconnectPolicy(
        enabled=True,
        initial_delay=0.0,
        max_delay=0.0,
        max_retries=2,
        stable_after=0.0,
        connect_timeout=1.0,
    )


async def test_qos1_publish_completes_when_writer_is_released() -> None:
    result, _harness = await run_connected_scenario(
        publish_one,
        enabled=WRITE_BOUNDARIES,
        chooser=FirstChooser(),
        auto_ack=True,
    )
    assert result.ok, result.error
    receipt = result.actor_results["publish_a"]
    assert isinstance(receipt, PublishReceipt)
    assert receipt.is_done()
    assert any(step.checkpoint == "transport.write.before" for step in result.schedule.steps)
    replay, _ = await run_connected_scenario(
        publish_one,
        enabled=WRITE_BOUNDARIES,
        schedule=result.schedule,
        auto_ack=True,
    )
    assert replay.ok, replay.error
    assert [step.format() for step in replay.schedule.steps] == [
        step.format() for step in result.schedule.steps
    ]


async def test_close_before_write_is_replayable() -> None:
    schedule = Schedule.parse(
        "\n".join(
            [
                "# policy=explicit",
                "action close_transport",
                "resume mqttium-writer @ transport.write.before #1",
            ]
        )
    )
    result, _harness = await run_connected_scenario(
        publish_admit,
        enabled=WRITE_BOUNDARIES,
        schedule=schedule,
        auto_ack=False,
    )
    assert not result.timed_out
    assert not result.deadlock
    replay, _ = await run_connected_scenario(
        publish_admit,
        enabled=WRITE_BOUNDARIES,
        schedule=schedule,
        auto_ack=False,
    )
    assert [step.format() for step in result.schedule.steps[:2]] == [
        "action close_transport",
        "resume mqttium-writer @ transport.write.before #1",
    ]
    assert [step.format() for step in replay.schedule.steps[:2]] == [
        step.format() for step in result.schedule.steps[:2]
    ]


async def test_cancel_waiter_under_writer_backpressure() -> None:
    cancelled: list[str] = []

    def extra(harness: MqttHarness):
        async def cancel_b() -> None:
            task = harness.scheduler._actor_tasks["publish_b"]
            task.cancel()
            cancelled.append("publish_b")

        return {"cancel_publish_b": cancel_b}

    schedule = Schedule.parse(
        "\n".join(
            [
                "action cancel_publish_b",
                "action close_transport",
                "resume mqttium-writer @ transport.write.before #1",
            ]
        )
    )

    def actors(harness: MqttHarness):
        return two_publishers(harness, qos=1)

    result, _harness = await run_connected_scenario(
        actors,
        enabled=BACKPRESSURE_BOUNDARIES,
        schedule=schedule,
        extra_actions=extra,
        auto_ack=False,
        max_outbound_messages=1,
    )
    assert not result.timed_out, result.error
    assert cancelled == ["publish_b"]
    publish_b = result.actor_results.get("publish_b")
    assert isinstance(publish_b, asyncio.CancelledError)


async def test_delivery_queue_park_then_shutdown() -> None:
    def actors(harness: MqttHarness):
        async def consume() -> list[object]:
            messages = []
            async for message in harness.client.messages():
                messages.append(message)
                if len(messages) >= 1:
                    break
            return messages

        async def feed() -> None:
            harness.broker.inject_publish("in/1", b"one")
            harness.broker.inject_publish("in/2", b"two")

        return [("consumer", consume()), ("feeder", feed())]

    result, _harness = await run_connected_scenario(
        actors,
        enabled=DELIVERY_BOUNDARIES,
        chooser=FirstChooser(),
        auto_ack=True,
        message_delivery="iterator",
        max_pending_messages=1,
        max_pending_delivery_bytes=16,
    )
    assert not result.timed_out, result.error
    assert not result.deadlock, result.error


async def test_callback_worker_handoff_then_disconnect() -> None:
    seen: list[bytes] = []

    def actors(harness: MqttHarness):
        harness.client.on_message = lambda message: seen.append(message.payload)

        async def feed() -> None:
            harness.broker.inject_publish("cb/1", b"hello")
            await asyncio.sleep(0)

        async def shutdown() -> None:
            await harness.scheduler.checkpoint("scenario.shutdown")
            await harness.client.disconnect()

        return [("feeder", feed()), ("shutdown", shutdown())]

    result, _harness = await run_connected_scenario(
        actors,
        enabled=CALLBACK_BOUNDARIES | frozenset({"scenario.shutdown"}),
        chooser=FirstChooser(),
        auto_ack=True,
        message_delivery="callback",
        max_pending_callbacks=1,
    )
    assert not result.timed_out, result.error
    assert not result.deadlock, result.error


async def test_reconnect_after_close_at_write_boundary() -> None:
    result, harness = await run_connected_scenario(
        publish_admit,
        enabled=RECONNECT_BOUNDARIES,
        schedule=Schedule.parse(
            "\n".join(
                [
                    "action close_transport",
                    "resume mqttium-writer @ transport.write.before #1",
                ]
            )
        ),
        auto_ack=True,
        reconnect=_immediate_reconnect(),
        clean_start=False,
    )
    assert not result.timed_out, result.error
    assert not result.deadlock, result.error
    assert len(harness.factory.transports) >= 1
