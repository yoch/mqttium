"""Topic matcher tests."""

from __future__ import annotations

import random

import pytest

from mqttium.dispatch.matcher import TopicMatcher, _Node
from tests.matcher_oracle import LinearTopicMatcher


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


def _tree_filters(node: _Node | None) -> list[str]:
    found: list[str] = []
    stack = [] if node is None else [node]
    while stack:
        current = stack.pop()
        found.extend("/".join(entry[1]) for entry in current.entries or ())
        stack.extend(current.children.values())
        stack.extend(child for child in (current.plus, current.hash) if child is not None)
    return found


def _visit_counter(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """Count wildcard-tree nodes the matcher walks (children lookups)."""
    visits = [0]

    class CountingChildren(dict):
        def get(self, key, default=None):  # type: ignore[override]
            visits[0] += 1
            return super().get(key, default)

    original_init = _Node.__init__

    def counting_init(self: _Node) -> None:
        original_init(self)
        self.children = CountingChildren()

    monkeypatch.setattr(_Node, "__init__", counting_init)
    return visits


def test_exact_filters_stay_out_of_the_wildcard_tree() -> None:
    matcher = TopicMatcher()
    for index in range(1000):
        matcher[f"sensors/{index}/temp"] = index
    matcher["sensors/+/temp"] = "wildcard"

    assert _tree_filters(matcher._root) == ["sensors/+/temp"]
    assert list(matcher.iter_match("sensors/7/temp")) == [7, "wildcard"]


def test_wildcard_matching_cost_does_not_grow_with_filter_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    visits = _visit_counter(monkeypatch)
    few = TopicMatcher()
    few["sensors/+/temp"] = "match"
    many = TopicMatcher()
    for index in range(1000):
        many[f"devices/{index}/+"] = index
    many["sensors/+/temp"] = "match"

    visits[0] = 0
    assert list(few.iter_match("sensors/kitchen/temp")) == ["match"]
    few_visits, visits[0] = visits[0], 0
    assert list(many.iter_match("sensors/kitchen/temp")) == ["match"]
    # Unrelated wildcard filters live on other branches and are never walked.
    assert visits[0] == few_visits


def test_shared_aliases_of_one_filter_share_a_node_and_keep_order() -> None:
    matcher = TopicMatcher()
    matcher["$share/a/x/+"] = "a"
    matcher["x/+"] = "plain"
    matcher["$share/b/x/+"] = "b"

    assert list(matcher.iter_match("x/1")) == ["a", "plain", "b"]
    del matcher["x/+"]
    assert list(matcher.iter_match("x/1")) == ["a", "b"]
    del matcher["$share/a/x/+"]
    del matcher["$share/b/x/+"]
    assert matcher._root is None
    assert list(matcher.iter_match("x/1")) == []


def test_deleting_a_filter_prunes_only_its_own_branch() -> None:
    matcher = TopicMatcher()
    matcher["a/+/c"] = "abc"
    matcher["a/+"] = "a+"
    del matcher["a/+/c"]

    assert _tree_filters(matcher._root) == ["a/+"]
    assert list(matcher.iter_match("a/b")) == ["a+"]
    assert list(matcher.iter_match("a/b/c")) == []
    with pytest.raises(KeyError):
        del matcher["a/+/c"]
    with pytest.raises(KeyError):
        # An intermediate tree level is no registered filter.
        del matcher["a"]


def test_wildcard_spelled_topic_levels_match_once() -> None:
    # Topic Names never contain wildcards on the wire; the matcher still
    # answers exactly like the linear scan if handed one directly.
    matcher = TopicMatcher()
    matcher["a/+"] = "plus"
    matcher["a/#"] = "hash"

    assert list(matcher.iter_match("a/+")) == ["plus", "hash"]
    assert list(matcher.iter_match("a/#")) == ["plus", "hash"]


def test_matches_agree_with_the_linear_reference() -> None:
    rng = random.Random(20261005)
    levels = ["a", "b", "", "$SYS", "+", "#"]
    for _round in range(300):
        matcher = TopicMatcher()
        oracle = LinearTopicMatcher()
        registered: list[str] = []
        for _op in range(rng.randint(1, 25)):
            depth = rng.randint(1, 4)
            parts = [rng.choice(levels) for _ in range(depth)]
            if "#" in parts:
                parts = parts[: parts.index("#") + 1]
            topic_filter = "/".join(parts)
            if rng.random() < 0.2:
                topic_filter = f"$share/g{rng.randint(1, 2)}/{topic_filter}"
            if registered and rng.random() < 0.25:
                victim = rng.choice(registered)
                registered.remove(victim)
                del matcher[victim]
                del oracle[victim]
                continue
            value = rng.randint(0, 3)
            matcher[topic_filter] = value
            oracle[topic_filter] = value
            if topic_filter not in registered:
                registered.append(topic_filter)
        for _probe in range(20):
            depth = rng.randint(1, 5)
            topic = "/".join(rng.choice(["a", "b", "", "$SYS", "c"]) for _ in range(depth))
            assert list(matcher.iter_match(topic)) == list(oracle.iter_match(topic)), (
                topic,
                registered,
            )
        assert bool(matcher) == bool(oracle)


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
