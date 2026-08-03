"""Exception hierarchy for mqttium."""

from __future__ import annotations


class MQTTError(Exception):
    """Base error for mqttium."""


class MalformedPacketError(MQTTError):
    """Wire data cannot be parsed as a valid MQTT packet."""


class ProtocolError(MQTTError):
    """Valid framing but illegal MQTT protocol usage."""


class PacketTooLargeError(ProtocolError):
    """Packet exceeds local or negotiated maximum size."""


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
        failures: dict[int, BaseException] | None = None,
        *,
        cause: BaseException | None = None,
        receipt: object | None = None,
    ) -> None:
        self.failures = dict(failures or {})
        self.cause = cause
        self.receipt = receipt
        if cause is not None:
            message = f"Batch submission failed: {cause}"
        else:
            message = f"{len(self.failures)} publication(s) failed in batch"
        super().__init__(message)
