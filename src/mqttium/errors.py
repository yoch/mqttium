"""Exception hierarchy for mqttium."""

from __future__ import annotations

from collections.abc import Mapping


class MQTTError(Exception):
    """Base error for mqttium."""


class MalformedPacketError(MQTTError):
    """Wire data cannot be parsed as a valid MQTT packet."""


class ProtocolError(MQTTError):
    """Valid framing but illegal MQTT protocol usage."""


class PacketTooLargeError(ProtocolError):
    """Packet exceeds local or negotiated maximum size."""


class MandatoryResponseTooLargeError(PacketTooLargeError):
    """A peer limit makes a mandatory local MQTT response impossible to send."""


class FlowControlError(MQTTError):
    """Outbound inflight window exhausted (raise mode)."""


class MessageDeliveryError(FlowControlError):
    """Application delivery could not keep up within the configured deadline."""


class NotConnectedError(MQTTError):
    """Operation requires an active connection."""


class MQTTTimeoutError(MQTTError):
    """Operation exceeded its deadline."""


class SessionDiscardedError(MQTTError):
    """Pending publish was discarded because a clean session replaced the old one."""


class PublishBatchError(MQTTError):
    """One or more publications in a batch failed."""

    def __init__(
        self,
        failures: Mapping[int, BaseException] | None = None,
        *,
        failure_count: int | None = None,
        failure_counts: dict[str, int] | None = None,
        cause: BaseException | None = None,
        receipt: object | None = None,
    ) -> None:
        self.failures = dict(failures or {})
        self.failure_count = len(self.failures) if failure_count is None else failure_count
        self.failure_counts = dict(failure_counts or {})
        self.cause = cause
        self.receipt = receipt
        if cause is not None:
            message = f"Batch submission failed: {cause}"
        else:
            message = f"{self.failure_count} publication(s) failed in batch"
        super().__init__(message)
