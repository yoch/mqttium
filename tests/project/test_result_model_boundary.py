"""Stable subscription result members do not require Internal packet types."""

from pathlib import Path

import pytest

from mqttium.api import SubscribeResult, UnsubscribeResult


@pytest.mark.parametrize("model", [SubscribeResult, UnsubscribeResult])
def test_result_model_supported_members_and_reference(model):
    result = model(mid=7, reason_codes=(0, 128))
    assert (result.mid, result.reason_codes) == (7, (0, 128))
    members = {"mid", "reason_codes", "properties"}
    if model is SubscribeResult:
        members.add("granted_qos")
    assert {name for name in vars(model) if not name.startswith("_")} == members
    reference = (Path(__file__).resolve().parents[2] / "docs/reference/models.md").read_text()
    entry = reference.split(f"::: mqttium.api.{model.__name__}\n", 1)[1].split("\n\n", 1)[0]
    documented = "granted_qos, " if model is SubscribeResult else ""
    assert f"members: [mid, reason_codes, {documented}properties]" in entry
