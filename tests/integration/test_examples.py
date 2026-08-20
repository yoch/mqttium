"""Run the user-facing examples against the mandatory local broker."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
BROKER_HOST = "127.0.0.1"
BROKER_PORT = 11883


def _run_example(script: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")
    return subprocess.run(
        [
            sys.executable,
            str(ROOT / "examples" / script),
            "--host",
            BROKER_HOST,
            "--port",
            str(BROKER_PORT),
            "--timeout",
            "5",
            *arguments,
        ],
        cwd=ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )


@pytest.mark.parametrize("protocol", ["311", "5"])
def test_native_pubsub_example(protocol: str) -> None:
    result = _run_example("pubsub.py", "--protocol", protocol)
    assert "hello from mqttium" in result.stdout


@pytest.mark.parametrize("protocol", ["311", "5"])
def test_durable_session_example(tmp_path: Path, protocol: str) -> None:
    database = tmp_path / f"session-{protocol}.sqlite"
    result = _run_example(
        "durable_session.py",
        "--protocol",
        protocol,
        "--database",
        str(database),
    )
    assert "pending=0" in result.stdout
    assert database.exists()


def test_paho_compatibility_example() -> None:
    result = _run_example("paho_compat.py")
    assert "hello through the Paho facade" in result.stdout


@pytest.mark.parametrize("protocol", ["311", "5"])
def test_runtime_stats_example(protocol: str) -> None:
    result = _run_example("runtime_stats.py", "--protocol", protocol)
    assert '"effective_client_id"' in result.stdout
    assert '"negotiated"' in result.stdout
    assert '"stats"' in result.stdout
