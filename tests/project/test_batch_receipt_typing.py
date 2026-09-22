"""Type-check the documented batch recovery path as a public API consumer."""

import os
from pathlib import Path
import subprocess
import sys


def test_batch_failure_receipt_supports_typed_recovery(tmp_path):
    consumer = tmp_path / "consumer.py"
    consumer.write_text("""from typing import assert_type
from mqttium import PublishBatchError
from mqttium.api import AsyncClient, PublishBatchReceipt, PublishMessage

async def publish_work(client: AsyncClient) -> None:
    try:
        result = await client.publish_many([PublishMessage("t", b"body", qos=1)])
        assert_type(result, PublishBatchReceipt)
    except PublishBatchError as error:
        assert_type(error.receipt, PublishBatchReceipt | None)
        if error.receipt is not None:
            assert_type(error.receipt.submitted, int)
            await error.receipt.wait()
""")
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy",
            "--no-incremental",
            "--follow-imports=silent",
            str(consumer),
        ],
        cwd=root,
        env={**os.environ, "MYPYPATH": str(root / "src")},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
