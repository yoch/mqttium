"""Publication diagnostics count acknowledged work and detect broken evidence."""

from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path

import pytest

from tests.support import ScriptedBrokerTransport


@pytest.fixture
def diagnostics(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "benchmarks"))
    return importlib.import_module("lean_native_diagnostics")


def test_diagnostic_import_does_not_require_unix_resource(monkeypatch):
    benchmarks = Path(__file__).resolve().parents[2] / "benchmarks"
    monkeypatch.syspath_prepend(str(benchmarks))
    monkeypatch.setitem(sys.modules, "resource", None)
    # Re-execute the diagnostic and any network harness import, even if another
    # test already loaded them on a platform that provides resource.
    monkeypatch.delitem(sys.modules, "lean_native_compare", raising=False)
    spec = importlib.util.spec_from_file_location(
        "portable_lean_native_diagnostics", benchmarks / "lean_native_diagnostics.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert callable(module._phase)
    assert "lean_native_compare" not in sys.modules


@pytest.mark.parametrize("scenario", ["publish_qos1_individual", "publish_qos1_batch"])
@pytest.mark.parametrize("count", [1, 257])
async def test_qos1_diagnostics_validate_every_publication_and_receipt(
    diagnostics, scenario, count
):
    result = await diagnostics._phase(scenario, count)
    assert (
        result["count"]
        == result["submitted"]
        == result["completed"]
        == result["wire_count"]
        == count
    )
    assert result["max_outbound_inflight"] == 20
    assert result["qos"] == 1
    assert result["callbacks"] == result["errors"] == 0
    assert result["elapsed_s"] > 0


@pytest.mark.parametrize("scenario", ["publish_qos1_individual", "publish_qos1_batch"])
async def test_diagnostic_rejects_incomplete_wire_evidence(diagnostics, scenario, monkeypatch):
    original = ScriptedBrokerTransport.handle_packet

    def omit_one_record(broker, raw):
        original(broker, raw)
        if broker.publishes:
            broker.publishes.pop()

    monkeypatch.setattr(ScriptedBrokerTransport, "handle_packet", omit_one_record)
    with pytest.raises(AssertionError, match="publication count mismatch"):
        await diagnostics._phase(scenario, 21)
