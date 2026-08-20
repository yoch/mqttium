"""Shared policy for tests that require the local Mosquitto instance."""

from __future__ import annotations

import os
import socket

import pytest


BROKER_HOST = "127.0.0.1"
BROKER_PORT = 11883


def _broker_available() -> bool:
    try:
        with socket.create_connection((BROKER_HOST, BROKER_PORT), timeout=0.5):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session", autouse=True)
def require_mosquitto() -> None:
    if _broker_available():
        return
    message = f"Mosquitto is not listening on {BROKER_HOST}:{BROKER_PORT}"
    if os.environ.get("MQTTIUM_REQUIRE_BROKER") == "1":
        pytest.fail(message)
    pytest.skip(message)
