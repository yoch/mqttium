"""Seed-reproducible soak schedules and prefix/ddmin reduction."""

from __future__ import annotations

import random
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum

from mqttium.enums import MQTTProtocolVersion, QoS


class OpKind(StrEnum):
    CONNECT = "connect"
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    PUBLISH = "publish"
    PUBLISH_NOWAIT = "publish_nowait"
    PUBLISH_MANY = "publish_many"
    CANCEL_PUBLISH = "cancel_publish"
    CONSUME = "consume"
    SLOW_CALLBACK = "slow_callback"
    FAST_CALLBACK = "fast_callback"
    DROP_NETWORK = "drop_network"
    WAIT_RECONNECT = "wait_reconnect"
    DRAIN = "drain"
    QUIESCE = "quiesce"
    GRACEFUL_SHUTDOWN = "graceful_shutdown"
    FORCE_SHUTDOWN = "force_shutdown"


@dataclass(frozen=True, slots=True)
class Op:
    """One scheduled client action. Execution does not consult extra RNG."""

    kind: OpKind
    qos: int = 0
    count: int = 1
    topic: str = "soak/topic"
    session_present: bool = False
    payload_size: int = 16

    def label(self) -> str:
        extra = ""
        if self.kind in {OpKind.PUBLISH, OpKind.PUBLISH_NOWAIT, OpKind.PUBLISH_MANY}:
            extra = f" qos={self.qos} n={self.count}"
        elif self.kind is OpKind.DROP_NETWORK:
            extra = f" session_present={self.session_present}"
        return f"{self.kind.value}{extra}"


def schedule_for_seed(
    seed: int,
    *,
    operations: int,
    protocol: MQTTProtocolVersion,
    include_shutdown: bool = True,
) -> list[Op]:
    """Build a mixed-lifecycle schedule that starts connected and ends drained."""
    del protocol
    rng = random.Random(seed)
    topic = f"soak/{seed}"
    ops: list[Op] = [
        Op(OpKind.CONNECT, topic=topic),
        Op(OpKind.SUBSCRIBE, topic=topic, qos=2),
        Op(OpKind.FAST_CALLBACK, topic=topic),
    ]
    budget = max(operations, 8)
    while len(ops) < budget:
        ops.append(_random_op(rng, topic))
        if len(ops) % 24 == 0:
            ops.append(Op(OpKind.DRAIN, topic=topic))
            ops.append(Op(OpKind.QUIESCE, topic=topic))
    ops.append(Op(OpKind.DRAIN, topic=topic))
    ops.append(Op(OpKind.QUIESCE, topic=topic))
    if include_shutdown:
        ops.append(
            Op(
                OpKind.GRACEFUL_SHUTDOWN if rng.random() < 0.7 else OpKind.FORCE_SHUTDOWN,
                topic=topic,
            )
        )
        ops.append(Op(OpKind.QUIESCE, topic=topic))
    return ops[: budget + 8]


def _random_op(rng: random.Random, topic: str) -> Op:
    kind = rng.choices(
        [
            OpKind.PUBLISH,
            OpKind.PUBLISH_NOWAIT,
            OpKind.PUBLISH_MANY,
            OpKind.CANCEL_PUBLISH,
            OpKind.CONSUME,
            OpKind.SUBSCRIBE,
            OpKind.UNSUBSCRIBE,
            OpKind.DROP_NETWORK,
            OpKind.SLOW_CALLBACK,
            OpKind.FAST_CALLBACK,
            OpKind.DRAIN,
        ],
        weights=[28, 16, 10, 8, 12, 4, 4, 8, 3, 3, 4],
        k=1,
    )[0]
    qos = int(rng.choice([QoS.AT_MOST_ONCE, QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE]))
    if kind is OpKind.DROP_NETWORK:
        return Op(kind, topic=topic, session_present=rng.random() < 0.5)
    if kind is OpKind.PUBLISH_MANY:
        return Op(kind, qos=qos, count=rng.choice([2, 4, 8]), topic=topic)
    if kind in {OpKind.SUBSCRIBE, OpKind.UNSUBSCRIBE}:
        return Op(kind, topic=topic, qos=2)
    return Op(kind, qos=qos, count=1, topic=topic, payload_size=rng.choice([0, 16, 64]))


ReplayFn = Callable[[Sequence[Op]], bool]


def reduce_schedule(failing: Sequence[Op], replay: ReplayFn) -> list[Op]:
    """Return a shorter failing schedule via prefix search then greedy deletion.

    ``replay`` must return True when the schedule succeeds and False when it
    fails. The original ``failing`` sequence is assumed to fail.
    """
    if not failing:
        return []
    lo, hi = 1, len(failing)
    while lo < hi:
        mid = (lo + hi) // 2
        if replay(failing[:mid]):
            lo = mid + 1
        else:
            hi = mid
    prefix = list(failing[:lo])
    changed = True
    while changed:
        changed = False
        index = 0
        while index < len(prefix):
            candidate = prefix[:index] + prefix[index + 1 :]
            if candidate and not replay(candidate):
                prefix = candidate
                changed = True
            else:
                index += 1
    return prefix
