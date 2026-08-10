"""Table-compaction contract for the in-memory inflight store."""

from __future__ import annotations

from sys import getsizeof

from mqttium.enums import OutboundQoSState
from mqttium.persistence.memory import MemoryInflightStore
from mqttium.types import OutboundMessage


def _message(mid: int) -> OutboundMessage:
    return OutboundMessage(
        mid=mid,
        topic="store/compaction",
        payload=b"x",
        qos=1,
        retain=False,
        dup=False,
        state=OutboundQoSState.WAIT_PUBACK,
        properties=None,
        encoded_publish=None,
        logical_size=1,
    )


def test_shallow_window_does_not_reallocate_on_every_ack() -> None:
    """A window of 8 drains to empty constantly; the table must survive it."""
    store = MemoryInflightStore()
    for cycle in range(5):
        for mid in range(1, 9):
            store.put_out(_message(mid))
        table = store._out
        for mid in range(1, 9):
            assert store.delete_out(mid)
        assert store._out is table, f"table replaced on cycle {cycle}"


def test_large_table_is_still_released() -> None:
    """The memory-campaign intent: a peak-sized table is dropped when empty."""
    store = MemoryInflightStore()
    for mid in range(1, 1025):
        store.put_out(_message(mid))
    grown = store._out
    assert getsizeof(grown) > 2048
    for mid in range(1, 1025):
        store.delete_out(mid)
    assert store._out is not grown
    assert getsizeof(store._out) < 256


def test_inbound_table_follows_the_same_rule() -> None:
    from mqttium.enums import InboundQoSState
    from mqttium.types import InboundMessage

    def inbound(mid: int) -> InboundMessage:
        return InboundMessage(
            mid=mid,
            topic="store/in",
            payload=b"x",
            qos=2,
            retain=False,
            state=InboundQoSState.WAIT_PUBREL,
            properties=None,
            delivered=False,
            logical_size=1,
        )

    store = MemoryInflightStore()
    for mid in range(1, 9):
        store.put_in(inbound(mid))
    table = store._in
    for mid in range(1, 9):
        assert store.pop_in(mid) is not None
    assert store._in is table

    for mid in range(1, 1025):
        store.put_in(inbound(mid))
    grown = store._in
    for mid in range(1, 1025):
        store.pop_in(mid)
    assert store._in is not grown
