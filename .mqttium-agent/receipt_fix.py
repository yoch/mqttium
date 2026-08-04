from pathlib import Path


def replace(path: str, before: str, after: str) -> None:
    target = Path(path)
    content = target.read_text()
    if before not in content:
        raise RuntimeError(f"Expected block was not found in {path}: {before[:120]!r}")
    target.write_text(content.replace(before, after, 1))


path = Path("src/mqttium/api/async_client.py")
content = path.read_text()
content = content.replace(
    "self._receipts: dict[int, PublishReceipt] = {}\n        self._batch_receipts: dict[int, PublishBatchReceipt] = {}",
    "self._receipts: dict[int, deque[PublishReceipt]] = {}\n        self._batch_receipts: dict[int, deque[PublishBatchReceipt]] = {}",
)
content = content.replace(
    "self._receipts[handle.mid] = receipt",
    "self._register_publish_receipt(handle.mid, receipt)",
)
content = content.replace(
    "self._batch_receipts[handle.mid] = receipt",
    "self._register_batch_receipt(handle.mid, receipt)",
)
content = content.replace(
    """        elif kind is EffectKind.PUBLISH_COMPLETE:\n            mid: int = effect.data\n            receipt = self._receipts.pop(mid, None)\n            if receipt is not None and receipt._event is not None:\n                receipt._event.set()\n            batch = self._batch_receipts.pop(mid, None)\n            if batch is not None:\n                batch._complete(mid)\n""",
    """        elif kind is EffectKind.PUBLISH_COMPLETE:\n            mid: int = effect.data\n            receipt = self._pop_publish_receipt(mid)\n            if receipt is not None and receipt._event is not None:\n                receipt._event.set()\n            batch = self._pop_batch_receipt(mid)\n            if batch is not None:\n                batch._complete(mid)\n""",
)
content = content.replace(
    """        elif kind is EffectKind.PUBLISH_FAILED:\n            failure: PublishFailure = effect.data\n            receipt = self._receipts.pop(failure.mid, None)\n            if receipt is not None:\n                receipt._error = failure.reason\n                if receipt._event is not None:\n                    receipt._event.set()\n            batch = self._batch_receipts.pop(failure.mid, None)\n            if batch is not None:\n                batch._complete(failure.mid, failure.reason)\n""",
    """        elif kind is EffectKind.PUBLISH_FAILED:\n            failure: PublishFailure = effect.data\n            receipt = self._pop_publish_receipt(failure.mid)\n            if receipt is not None:\n                receipt._error = failure.reason\n                if receipt._event is not None:\n                    receipt._event.set()\n            batch = self._pop_batch_receipt(failure.mid)\n            if batch is not None:\n                batch._complete(failure.mid, failure.reason)\n""",
)
marker = "    def _fail_non_replayable(self, exc: BaseException) -> None:\n"
helpers = """    def _register_publish_receipt(self, mid: int, receipt: PublishReceipt) -> None:\n        self._receipts.setdefault(mid, deque()).append(receipt)\n\n    def _pop_publish_receipt(self, mid: int) -> PublishReceipt | None:\n        receipts = self._receipts.get(mid)\n        if not receipts:\n            return None\n        receipt = receipts.popleft()\n        if not receipts:\n            self._receipts.pop(mid, None)\n        return receipt\n\n    def _register_batch_receipt(self, mid: int, receipt: PublishBatchReceipt) -> None:\n        self._batch_receipts.setdefault(mid, deque()).append(receipt)\n\n    def _pop_batch_receipt(self, mid: int) -> PublishBatchReceipt | None:\n        receipts = self._batch_receipts.get(mid)\n        if not receipts:\n            return None\n        receipt = receipts.popleft()\n        if not receipts:\n            self._batch_receipts.pop(mid, None)\n        return receipt\n\n"""
if marker not in content:
    raise RuntimeError("Receipt helper insertion point was not found")
content = content.replace(marker, helpers + marker, 1)
content = content.replace(
    """        for receipt in self._receipts.values():\n            receipt._error = exc\n            if receipt._event is not None:\n                receipt._event.set()\n        self._receipts.clear()\n        batches = set(self._batch_receipts.values())\n        self._batch_receipts.clear()\n        for batch in batches:\n            batch._fail_remaining(exc)\n""",
    """        for receipts in self._receipts.values():\n            for receipt in receipts:\n                receipt._error = exc\n                if receipt._event is not None:\n                    receipt._event.set()\n        self._receipts.clear()\n        batches = {batch for receipts in self._batch_receipts.values() for batch in receipts}\n        self._batch_receipts.clear()\n        for batch in batches:\n            batch._fail_remaining(exc)\n""",
)
path.write_text(content)

path = Path("src/mqttium/compat/paho.py")
content = path.read_text().replace(
    "self._async._receipts[handle.mid] = receipt",
    "self._async._register_publish_receipt(handle.mid, receipt)",
)
path.write_text(content)

path = Path("tests/unit/test_nonreplayable_failures.py")
content = path.read_text()
content = content.replace(
    "client._receipts[13] = receipt",
    "client._register_publish_receipt(13, receipt)",
)
content = content.replace(
    "assert client._receipts[13] is receipt",
    "assert list(client._receipts[13]) == [receipt]",
)
content = content.replace(
    """    client._receipts[3] = PublishReceipt(\n        mid=3,\n        qos=QoS.AT_LEAST_ONCE,\n        _event=event,\n    )\n""",
    """    client._register_publish_receipt(\n        3,\n        PublishReceipt(\n            mid=3,\n            qos=QoS.AT_LEAST_ONCE,\n            _event=event,\n        ),\n    )\n""",
)
path.write_text(content)

path = Path("tests/unit/test_async_client.py")
content = path.read_text()
test = """\n\nasync def test_sequential_qos1_receipts_survive_immediate_mid_reuse() -> None:\n    \"\"\"A completed MID can be reused before its completion effect is applied.\"\"\"\n    client, fake = _client_with_fake()\n    await client.connect(\"fake\", 1883, timeout=2.0)\n\n    receipts = []\n    for index in range(250):\n        receipts.append(await client.publish(\"reuse/qos1\", str(index), qos=1))\n        if len(receipts) >= 100:\n            await asyncio.wait_for(receipts[-100].wait(), timeout=2.0)\n\n    await asyncio.wait_for(asyncio.gather(*(receipt.wait() for receipt in receipts)), timeout=5.0)\n    assert all(receipt.is_done() for receipt in receipts)\n    assert not client._receipts\n    await client.disconnect()\n"""
if "test_sequential_qos1_receipts_survive_immediate_mid_reuse" in content:
    raise RuntimeError("MID reuse regression test already exists")
path.write_text(content + test)
