"""Concurrency regressions for aggregate publish receipt progress waits."""

from __future__ import annotations

import asyncio

from mqttium.api.models import PublishBatchReceipt


class _SettleOnClearEvent(asyncio.Event):
    """Force settlement in the clear/recheck window of the progress waiter."""

    def __init__(self, receipt: PublishBatchReceipt, mid: int) -> None:
        super().__init__()
        self._receipt = receipt
        self._mid = mid

    def clear(self) -> None:
        # _complete() sets this same event, then clear it again to model a
        # settlement that lands after the outer condition check but before the
        # waiter parks. The post-clear pending recheck must observe progress.
        self._receipt._complete(self._mid)
        super().clear()


async def test_pending_wait_rechecks_after_clear_to_avoid_lost_wakeup() -> None:
    receipt = PublishBatchReceipt()
    receipt._register(7)
    receipt._progress = _SettleOnClearEvent(receipt, 7)

    await asyncio.wait_for(receipt._wait_pending_at_most(0), timeout=0.1)

    assert receipt.pending_count == 0
    assert receipt.completed == 1
