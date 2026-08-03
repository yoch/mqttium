"""Reconnect policy (IMPLEMENTATION-GUIDE.md §5)."""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from mqttium.enums import MQTTProtocolVersion


_V311_TERMINAL = frozenset({1, 2, 4, 5})
_V5_TERMINAL = frozenset(
    {
        0x84,
        0x85,
        0x86,
        0x87,
        0x8C,
        0x9D,
        0x9C,
    }
)


@dataclass(slots=True)
class ReconnectPolicy:
    enabled: bool = True
    initial_delay: float = 1.0
    multiplier: float = 2.0
    max_delay: float = 60.0
    max_retries: int | None = None
    stable_after: float = 30.0
    connect_timeout: float = 30.0
    follow_server_reference: bool = False
    _attempt: int = field(default=0, init=False, repr=False)
    _current_delay: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.initial_delay < 0:
            raise ValueError("initial_delay must be non-negative")
        if self.multiplier < 1:
            raise ValueError("multiplier must be at least 1")
        if self.max_delay < self.initial_delay:
            raise ValueError("max_delay must be greater than or equal to initial_delay")
        if self.max_retries is not None and self.max_retries < 0:
            raise ValueError("max_retries must be non-negative or None")
        if self.stable_after < 0:
            raise ValueError("stable_after must be non-negative")
        if self.connect_timeout <= 0:
            raise ValueError("connect_timeout must be greater than 0")
        self._attempt = 0
        self._current_delay = self.initial_delay

    def reset(self) -> None:
        self._attempt = 0
        self._current_delay = self.initial_delay

    def next_delay(self) -> float:
        """Return delay for the next retry (with full jitter) and advance state."""
        base = min(self._current_delay, self.max_delay)
        delay = random.uniform(0.5, 1.0) * base
        self._attempt += 1
        self._current_delay = min(self._current_delay * self.multiplier, self.max_delay)
        return delay

    def should_retry(self, reason_code: int | None, protocol: MQTTProtocolVersion) -> bool:
        if not self.enabled:
            return False
        if self.max_retries is not None and self._attempt >= self.max_retries:
            return False
        if reason_code is None:
            return True
        if protocol == MQTTProtocolVersion.MQTTv5:
            if reason_code in _V5_TERMINAL:
                if reason_code == 0x9C and self.follow_server_reference:
                    return True
                return False
            return True
        return reason_code not in _V311_TERMINAL

    @property
    def attempt(self) -> int:
        return self._attempt


def is_terminal_connack(reason_code: int, protocol: MQTTProtocolVersion) -> bool:
    policy = ReconnectPolicy(enabled=True)
    return not policy.should_retry(reason_code, protocol)
