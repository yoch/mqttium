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


def test_exact_and_wildcard_matches_preserve_global_insertion_order() -> None:
    matcher = TopicMatcher()
    matcher["sensors/#"] = "hash-first"
    matcher["sensors/kitchen/temp"] = "exact-second"
    matcher["sensors/+/temp"] = "plus-third"

    assert list(matcher.iter_match("sensors/kitchen/temp")) == [
        "hash-first",
        "exact-second",
        "plus-third",
    ]


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


def test_shared_filter_matches_the_topic_the_broker_delivers() -> None:
    matcher = TopicMatcher()
    matcher["$share/group/sensors/#"] = "shared"
    matcher["sensors/#"] = "normal"

    # A broker delivers a shared subscription's messages under their real
    # Topic Name; "$share/group/" is never part of it.
    assert list(matcher.iter_match("sensors/temp")) == ["shared", "normal"]
    assert list(matcher.iter_match("$share/group/sensors/temp")) == []
    assert matcher["$share/group/sensors/#"] == "shared"


def test_shared_exact_filter_matches_its_topic_in_registration_order() -> None:
    matcher = TopicMatcher()
    matcher["sensors/temp"] = "normal"
    matcher["$share/first/sensors/temp"] = "shared"

    assert list(matcher.iter_match("sensors/temp")) == ["normal", "shared"]
    assert list(matcher.iter_match("sensors/other")) == []
    del matcher["$share/first/sensors/temp"]
    assert list(matcher.iter_match("sensors/temp")) == ["normal"]


def test_shared_wildcard_filter_keeps_the_system_topic_guard() -> None:
    matcher = TopicMatcher()
    matcher["$share/group/#"] = "shared"

    assert list(matcher.iter_match("a/b")) == ["shared"]
    assert list(matcher.iter_match("$SYS/broker/load")) == []


def test_replacing_value_preserves_insertion_order() -> None:
    matcher = TopicMatcher()
    matcher["sensors/#"] = "wildcard"
    matcher["sensors/temp"] = "old"
    matcher["sensors/temp"] = "new"

    assert list(matcher.iter_match("sensors/temp")) == ["wildcard", "new"]


def test_delete_then_reinsert_moves_filter_to_end() -> None:
    matcher = TopicMatcher()
    matcher["sensors/temp"] = "exact"
    matcher["sensors/#"] = "wildcard"
    del matcher["sensors/temp"]
    matcher["sensors/temp"] = "exact-again"

    assert list(matcher.iter_match("sensors/temp")) == ["wildcard", "exact-again"]


def test_exact_index_skips_exact_filters_during_wildcard_scan(monkeypatch) -> None:
    matcher = TopicMatcher()
    for index in range(1000):
        matcher[f"sensors/{index}/temp"] = index
    matcher["sensors/+/temp"] = "wildcard"

    calls = 0
    original = TopicMatcher._matches

    def counted(
        filter_levels: tuple[str, ...],
        topic_levels: tuple[str, ...],
        is_system_topic: bool,
    ) -> bool:
        nonlocal calls
        calls += 1
        return original(filter_levels, topic_levels, is_system_topic)

    monkeypatch.setattr(TopicMatcher, "_matches", staticmethod(counted))

    assert list(matcher.iter_match("sensors/7/temp")) == [7, "wildcard"]
    assert calls == 1


def test_values_may_be_none_and_deletion_is_explicit() -> None:
    matcher = TopicMatcher()
    matcher["nullable/topic"] = None

    assert matcher["nullable/topic"] is None
    assert list(matcher.iter_match("nullable/topic")) == [None]

    del matcher["nullable/topic"]
    with pytest.raises(KeyError):
        _ = matcher["nullable/topic"]


def test_empty_matcher_is_falsy() -> None:
    matcher = TopicMatcher()
    assert not matcher
    matcher["sensors/+"] = "cb"
    assert matcher
    del matcher["sensors/+"]
    assert not matcher
