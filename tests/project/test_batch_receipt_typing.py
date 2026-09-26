"""Type-check the documented batch recovery path as a public API consumer."""

import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]

_TYPED_RECOVERY = """from typing import assert_type
from mqttium import PublishBatchError
from mqttium.api import AsyncClient, PublishBatchReceipt, PublishMessage

async def publish_work(client: AsyncClient) -> None:
    try:
        result = await client.publish_many([PublishMessage("t", b"body", qos=1)])
        assert_type(result, PublishBatchReceipt)
    except PublishBatchError as error:
        assert_type(error.receipt, PublishBatchReceipt)
        assert_type(error.receipt.submitted, int)
        await error.receipt.wait()
"""

_UNNARROWED = """from mqttium import PublishBatchError

def submitted(error: PublishBatchError) -> int:
    return error.receipt.submitted
"""


def _mypy(tmp_path, *consumers):
    # Hermetic run: no project config (whose ignore_missing_imports would turn
    # an unresolved mqttium into Any), no installed package, and no cache, so
    # only this checkout's src can satisfy the imports.
    config = tmp_path / "mypy.ini"
    config.write_text("[mypy]\nshow_error_codes = True\n")
    command = [
        sys.executable,
        "-m",
        "mypy",
        "--config-file",
        str(config),
        "--no-site-packages",
        "--no-incremental",
        "--cache-dir",
        str(tmp_path / "cache"),
        *map(str, consumers),
    ]
    try:
        return subprocess.run(
            command,
            cwd=tmp_path,
            env={**os.environ, "MYPYPATH": str(ROOT / "src")},
            capture_output=True,
            text=True,
            check=False,
            timeout=25,
        )
    except subprocess.TimeoutExpired as exc:
        raise AssertionError(f"mypy timed out:\n{exc.stdout or ''}{exc.stderr or ''}") from exc


def test_batch_failure_receipt_supports_typed_recovery_without_narrowing(tmp_path):
    recovery = tmp_path / "recovery.py"
    recovery.write_text(_TYPED_RECOVERY)
    unnarrowed = tmp_path / "unnarrowed.py"
    unnarrowed.write_text(_UNNARROWED)

    result = _mypy(tmp_path, recovery, unnarrowed)

    report = result.stdout + result.stderr
    errors = [line for line in result.stdout.splitlines() if ": error:" in line]
    assert result.returncode == 0, report
    assert errors == [], report


def test_receipt_annotation_is_deferred_and_the_runtime_value_is_unchanged():
    import inspect

    from mqttium import PublishBatchError
    from mqttium.api import PublishBatchReceipt

    parameter = inspect.signature(PublishBatchError).parameters["receipt"]
    # Postponed annotations keep the TYPE_CHECKING-only import out of runtime.
    assert parameter.annotation == "PublishBatchReceipt"
    assert parameter.default is inspect.Parameter.empty
    receipt = PublishBatchReceipt()
    assert PublishBatchError(receipt).receipt is receipt
