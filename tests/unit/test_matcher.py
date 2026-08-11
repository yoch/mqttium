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


def test_values_may_be_none_and_deletion_is_explicit() -> None:
    matcher = TopicMatcher()
    matcher["nullable/topic"] = None

    assert matcher["nullable/topic"] is None
    assert list(matcher.iter_match("nullable/topic")) == [None]

    del matcher["nullable/topic"]
    with pytest.raises(KeyError):
        _ = matcher["nullable/topic"]


def test_exact_only_matcher_is_constant_time_lookup() -> None:
    matcher = TopicMatcher()
    for i in range(200):
        matcher[f"sensors/{i}/temp"] = i

    assert list(matcher.iter_match("sensors/7/temp")) == [7]
    assert list(matcher.iter_match("missing")) == []


def test_mixed_exact_and_wildcard_preserve_insertion_order() -> None:
    matcher = TopicMatcher()
    matcher["sensors/#"] = "hash"
    matcher["sensors/kitchen/temp"] = "exact"
    matcher["sensors/+/temp"] = "plus"

    assert list(matcher.iter_match("sensors/kitchen/temp")) == ["hash", "exact", "plus"]


def test_replacing_exact_with_wildcard_updates_fast_path() -> None:
    matcher = TopicMatcher()
    matcher["sensors/kitchen/temp"] = "exact"
    assert list(matcher.iter_match("sensors/kitchen/temp")) == ["exact"]
    matcher["sensors/kitchen/temp"] = "still-exact"
    matcher["sensors/+/temp"] = "plus"
    assert list(matcher.iter_match("sensors/kitchen/temp")) == ["still-exact", "plus"]
