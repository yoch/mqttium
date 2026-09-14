"""Maintained benchmarks follow publications through receipts."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

from mqttium.api import AsyncClient
from tests.support import ScriptedBrokerTransport, transport_factory


@pytest.mark.parametrize(
    ("module_name", "option"),
    [
        ("paired_network", "--completion"),
        ("paired_network", "--completions"),
        ("paired_open_loop", "--completion"),
        ("paired_open_loop", "--completions"),
        ("rate_regime_probe", "--completion"),
    ],
)
def test_removed_callback_completion_is_rejected(module_name, option, monkeypatch, capsys):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "benchmarks"))
    module = importlib.import_module(module_name)
    monkeypatch.setattr(sys, "argv", [module_name, "--worker", option, "callback"])
    with pytest.raises(SystemExit) as exit_info:
        module.parse_args()
    assert exit_info.value.code == 2
    assert option in capsys.readouterr().err


@pytest.mark.parametrize("qos", [0, 1])
async def test_writer_capacity_phase_completes_exact_receipt_count(qos, monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "benchmarks"))
    module = importlib.import_module("paired_writer_capacity")
    broker = ScriptedBrokerTransport()
    client = AsyncClient(max_outbound_messages=16)
    client._transport_factory = transport_factory(broker)
    await client.connect("fake")
    try:
        result = await module._run_phase(
            client,
            topic="benchmark/completion",
            payload=b"owned",
            qos=qos,
            count=129,
            outstanding=8,
            timeout=1,
        )
        assert result.count == 129
        assert len(broker.publishes) == 129
        assert all(packet.qos == qos and packet.payload == b"owned" for packet in broker.publishes)
        assert not client._receipts
        assert client._delivery.callback_invocations == 0
        assert client._write_pump.queued_bytes == 0
    finally:
        await client.disconnect()
