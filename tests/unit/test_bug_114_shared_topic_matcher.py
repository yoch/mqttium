"""Reproduction for GitHub issue #114.

Shared-subscription filters must match broker-delivered topics after the
``$share/{ShareName}/`` prefix is stripped.
"""

from __future__ import annotations

import pytest

from mqttium.dispatch.matcher import TopicMatcher


@pytest.mark.xfail(
    strict=True,
    reason="https://github.com/yoch/mqttium/issues/114 — shared filters do not match deliveries",
)
def test_shared_subscription_filter_matches_delivered_topic() -> None:
    matcher = TopicMatcher()
    matcher["$share/group/sensors/#"] = "shared"
    matcher["sensors/#"] = "normal"

    assert list(matcher.iter_match("sensors/temp")) == ["shared", "normal"]
