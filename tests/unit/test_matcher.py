"""Topic matcher tests."""

from __future__ import annotations

import pytest

from mqttium.dispatch.matcher import TopicMatcher


def test_exact_and_wildcards() -> None:
    matcher = TopicMatcher()
    matcher["sensors/kitchen/temp"] = "exact"
    matcher["sensors/+/temp"] = "plus"
    matcher["sensors/#"] = "hash"

    assert list(matcher.iter_match("sensors/kitchen/temp")) == ["exact", "plus", "hash"]


def test_hash_matches_zero_or_more_remaining_levels() -> None:
    matcher = TopicMatcher()
    matcher["sensors/#"] = "hash"

    assert list(matcher.iter_match("sensors")) == ["hash"]
    assert list(matcher.iter_match("sensors/kitchen/temp")) == ["hash"]


def test_system_topic_wildcard_guard() -> None:
    matcher = TopicMatcher()
    matcher["#"] = "all"
    matcher["$SYS/#"] = "sys"

    assert list(matcher.iter_match("$SYS/broker/version")) == ["sys"]
    assert list(matcher.iter_match("foo/bar")) == ["all"]


def test_shared_filter_matches_broker_delivered_topic() -> None:
    matcher = TopicMatcher()
    matcher["$share/group/sensors/#"] = "shared"
    matcher["sensors/#"] = "normal"

    assert list(matcher.iter_match("sensors/temp")) == ["shared", "normal"]
    assert list(matcher.iter_match("$share/group/sensors/temp")) == []
    assert matcher["$share/group/sensors/#"] == "shared"


def test_shared_system_filter_keeps_system_topic_wildcard_guard() -> None:
    matcher = TopicMatcher()
    matcher["$share/group/#"] = "shared-all"
    matcher["$share/group/$SYS/#"] = "shared-sys"

    assert list(matcher.iter_match("$SYS/broker/version")) == ["shared-sys"]
    assert list(matcher.iter_match("regular/topic")) == ["shared-all"]


def test_values_may_be_none_and_deletion_is_explicit() -> None:
    matcher = TopicMatcher()
    matcher["nullable/topic"] = None

    assert matcher["nullable/topic"] is None
    assert list(matcher.iter_match("nullable/topic")) == [None]

    del matcher["nullable/topic"]
    with pytest.raises(KeyError):
        _ = matcher["nullable/topic"]
