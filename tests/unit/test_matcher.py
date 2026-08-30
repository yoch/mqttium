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


def test_shared_subscription_filter_is_matched_literally_like_paho() -> None:
    matcher = TopicMatcher()
    matcher["$share/group/sensors/#"] = "shared"
    matcher["sensors/#"] = "normal"

    # Paho registers the callback filter literally; broker-side removal of the
    # shared-subscription prefix is not emulated by the callback matcher.
    assert list(matcher.iter_match("sensors/temp")) == ["normal"]
    assert list(matcher.iter_match("$share/group/sensors/temp")) == ["shared"]
    assert matcher["$share/group/sensors/#"] == "shared"


def test_shared_exact_filter_does_not_alias_underlying_topic() -> None:
    matcher = TopicMatcher()
    matcher["$share/first/sensors/temp"] = "shared"
    matcher["sensors/temp"] = "normal"

    assert list(matcher.iter_match("sensors/temp")) == ["normal"]
    assert list(matcher.iter_match("$share/first/sensors/temp")) == ["shared"]


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
