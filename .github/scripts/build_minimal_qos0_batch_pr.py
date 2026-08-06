from __future__ import annotations

import sys
from pathlib import Path

from apply_qos0_effect_batch_candidate import apply


TESTS = '''from __future__ import annotations

from collections import deque

import pytest

from mqttium.api import AsyncClient
from mqttium.enums import QoS
from mqttium.protocol.engine import EffectKind, EngineEffect
from mqttium.types import Message


def _effect(*, qos: QoS = QoS.AT_MOST_ONCE, mid: int | None = None) -> EngineEffect:
    return EngineEffect(
        kind=EffectKind.MESSAGE,
        data=Message(topic="hot/path", payload=b"payload", qos=qos, mid=mid),
    )


@pytest.mark.asyncio
async def test_small_qos0_callback_messages_apply_as_one_synchronous_batch() -> None:
    client = AsyncClient(message_delivery="callback")
    seen: list[Message] = []
    client.on_message = seen.append
    effects = deque([_effect(), _effect(), _effect()])

    applied = client._apply_message_effect_batch_inline(effects, client._connection_epoch)

    assert applied == 3
    await client._callback_queue.join()
    assert len(seen) == 3
    await client._shutdown_callback_worker(drain=False)


def test_single_message_keeps_the_established_effect_path() -> None:
    client = AsyncClient(message_delivery="iterator")

    applied = client._apply_message_effect_batch_inline(
        deque([_effect()]), client._connection_epoch
    )

    assert applied == 0
    assert client._messages.empty()


def test_stale_epoch_keeps_the_established_effect_path() -> None:
    client = AsyncClient(message_delivery="iterator")

    applied = client._apply_message_effect_batch_inline(
        deque([_effect(), _effect()]), client._connection_epoch - 1
    )

    assert applied == 0
    assert client._messages.empty()


def test_batch_stops_before_half_delivering_both_mode() -> None:
    client = AsyncClient(
        message_delivery="both",
        max_pending_callbacks=1,
        max_pending_messages=1,
    )
    client.on_message = lambda _message: None
    sentinel = (lambda: None, (), None)
    client._callback_queue.put_nowait(sentinel)

    applied = client._apply_message_effect_batch_inline(
        deque([_effect(), _effect()]), client._connection_epoch
    )

    assert applied == 0
    assert client._messages.empty()
    assert client._callback_queue.get_nowait() is sentinel
    client._callback_queue.task_done()


@pytest.mark.parametrize("qos", [QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
def test_batch_defers_acknowledged_messages(qos: QoS) -> None:
    client = AsyncClient(message_delivery="callback")
    client.on_message = lambda _message: None

    applied = client._apply_message_effect_batch_inline(
        deque([_effect(qos=qos, mid=7), _effect()]), client._connection_epoch
    )

    assert applied == 0
    assert client._callback_queue.empty()


def test_batch_stops_at_first_non_qos0_effect() -> None:
    client = AsyncClient(message_delivery="iterator")
    effects = deque(
        [
            _effect(),
            _effect(qos=QoS.AT_LEAST_ONCE, mid=7),
            _effect(),
        ]
    )

    applied = client._apply_message_effect_batch_inline(effects, client._connection_epoch)

    assert applied == 1
    assert client._messages.qsize() == 1
    assert client._message_ready.is_set()
'''


DOC = '''# Batched small QoS 0 delivery

## Problem

The reader already decodes inbound packets and schedules their effects in bounded
lots. With the default isolated callback contract, each eligible QoS 0 `MESSAGE`
effect was nevertheless transferred to the iterator/callback queues through a
separate async dispatch call inside that scheduled flush.

The cost is downstream of MQTT decoding and distinct from the direct MQTT 3.1.1
QoS 0/QoS 1 decoders. It only applies when messages are delivered through the
bounded isolated queues.

## Change

`EffectPump` may consume a consecutive prefix of at least two eligible QoS 0
`MESSAGE` effects in one synchronous pump pass. The optimization is deliberately
narrow:

- a single message keeps the established per-effect path;
- QoS 1 and QoS 2 are never batched here;
- large, property-bearing or exact-byte-accounted messages keep the existing
  awaited path;
- callback and iterator capacity are checked before either destination mutates;
- `message_delivery="both"` remains atomic;
- the batch stops at the first ineligible effect;
- callback execution remains isolated in the callback worker;
- the scheduled flush boundary and reader backpressure remain unchanged.

## Evidence

Seven rotated cycles were run on the final native hot-path stack containing the
exact `publish_nowait()` admission calculation and the MQTT 3.1.1 QoS 0/QoS 1
direct decoders.

| Delivery contract | Direct ingress | Broker-fed ingress |
| --- | ---: | ---: |
| Default isolated callback | **+11.45%** | **+9.34%** |
| Experimental synchronous inline callback | +0.52% | +1.93% |

Every isolated-callback cycle was positive: direct ratios ranged from 1.079 to
1.125 and broker-fed ratios from 1.060 to 1.151. Inline-callback results crossed
both sides of neutral, confirming that this patch specifically amortizes the
isolated queue transfer rather than decoding or callback execution itself.

Final interaction run:
<https://github.com/yoch/mqttium/actions/runs/31062402168>

## Risks

The principal risks are effect ordering, partial `both` delivery and bypassing
backpressure. Tests cover the consecutive-prefix rule, stale epochs, single-item
fallback, acknowledged-message fallback and atomic destination-capacity checks.
The full unit and fuzz suites are required on the final branch.
'''


def build(root: Path) -> None:
    apply(root)
    (root / "tests/unit/test_batched_message_effects.py").write_text(TESTS)
    (root / "docs/QOS0-MESSAGE-BATCH-DELIVERY.md").write_text(DOC)

    changelog = root / "CHANGELOG.md"
    text = changelog.read_text()
    heading = "### Changed\n\n"
    index = text.index(heading) + len(heading)
    entry = (
        "- Consecutive small QoS 0 `MESSAGE` effects can now be transferred to "
        "the bounded iterator/callback queues in one `EffectPump` pass. Single "
        "messages, acknowledged QoS, exact byte accounting and full destinations "
        "retain the established path; callback execution remains isolated.\n"
    )
    changelog.write_text(text[:index] + entry + text[index:])


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: build_minimal_qos0_batch_pr.py ROOT")
    build(Path(sys.argv[1]))
