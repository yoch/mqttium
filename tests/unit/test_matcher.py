"""Topic matcher tests."""

from __future__ import annotations

from mqttium.dispatch.matcher import TopicMatcher


def test_exact_and_wildcards() -> None:
    m = TopicMatcher()
    m["sensors/kitchen/temp"] = "exact"
    m["sensors/+/temp"] = "plus"
    m["sensors/#"] = "hash"
    matches = set(m.iter_match("sensors/kitchen/temp"))
    assert matches == {"exact", "plus", "hash"}


def test_system_topic_wildcard_guard() -> None:
    m = TopicMatcher()
    m["#"] = "all"
    m["$SYS/#"] = "sys"
    assert list(m.iter_match("$SYS/broker/version")) == ["sys"]
    assert list(m.iter_match("foo/bar")) == ["all"]
