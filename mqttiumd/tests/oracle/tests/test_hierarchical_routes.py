"""Descriptive wildcard/options readers; these synthetic controls qualify no run."""
from copy import deepcopy
import json
from types import SimpleNamespace
import unittest
from unittest import mock

from mqttiumd_oracle import recipe, results
from mqttiumd_oracle.documents import Contracts

from .support import ROOT


SCOPE = "the fixture-based qualifier does not cover hierarchical or No Local routing"
OPTIONS = (0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44)


def encoded(kind=9, topic=b"plants/+/temperature", options=4, grant=2):
    be = lambda n, size=8: n.to_bytes(size, "big")
    operation = b"".join(be(x) for x in (1, 1, 0, 24))
    return (b"MQDR\x00\x01" + bytes([kind]) + operation + be(2) + be(1) + be(2)
            + bytes([1, grant]) + (bytes([options]) if kind == 9 else b"")
            + be(len(topic), 2) + topic)


class HierarchicalFormatReaders(unittest.TestCase):
    def test_kind9_preserves_options_without_becoming_a_different_operation_type(self):
        found = recipe.decode(encoded())
        self.assertEqual(found.kind, "UpdateSubscriptions")
        self.assertEqual((found.changes[0].topic, found.changes[0].granted,
                          found.changes[0].options), (b"plants/+/temperature", 1, 4))

    def test_default_kind7_filter_grammar_changes_without_changing_its_layout(self):
        for topic in [b"#", b"+", b"/", b"plants/+/#", "é/+/温度".encode()]:
            with self.subTest(topic=topic):
                found = recipe.decode(encoded(7, topic))
                self.assertEqual(found.changes[0].topic, topic)
                self.assertEqual(found.changes[0].options, 0)
        with self.assertRaises(recipe.RecipeError):
            recipe.decode(encoded(3, b"plants/+", grant=1))

    def test_options_domain_and_noncanonical_or_unsubscribe_aliases_are_refused(self):
        for options in range(256):
            with self.subTest(options=options):
                if options in OPTIONS[1:]:
                    self.assertEqual(recipe.decode(encoded(options=options)).changes[0].options, options)
                else:
                    with self.assertRaises(recipe.RecipeError):
                        recipe.decode(encoded(options=options))
        with self.assertRaises(recipe.RecipeError):
            recipe.decode(encoded(options=4, grant=0))

    def test_bad_hierarchical_filters_are_not_silently_read_as_exact_topics(self):
        for topic in [b"", b"a#", b"a/+x", b"a/#/b", b"a/++", b"$share/g/a", b"a\x00b", b"\xff"]:
            with self.subTest(topic=topic), self.assertRaises(recipe.RecipeError):
                recipe.decode(encoded(topic=topic))

    def test_route_options_are_optional_for_legacy_evidence_and_bounded_when_present(self):
        contracts = Contracts.open(ROOT)
        path = ROOT / 'contracts/examples/v1/valid/trace-record-recovery-page-of-routes-only.json'
        legacy = json.loads(path.read_text())
        self.assertEqual(contracts.check_value(legacy), [])
        for options in OPTIONS:
            value = deepcopy(legacy)
            value['payload']['facts'][0]['options'] = options
            self.assertEqual(contracts.check_value(value), [], options)
        for options in [1, 3, 48, 255, True, -1, "4"]:
            value = deepcopy(legacy)
            value['payload']['facts'][0]['options'] = options
            self.assertTrue(contracts.check_value(value), options)


class HierarchicalQualificationScope(unittest.TestCase):
    def unmet(self, *, fixture=None, snapshot=None, facts=()):
        run = SimpleNamespace(fixture=fixture or {'routes': []},
                              snapshots={'first': (None, snapshot)} if snapshot is not None else {},
                              trace=SimpleNamespace(records=[SimpleNamespace(kind='RecoveryPageRead', payload={'facts': list(facts)})] if facts else []))
        with mock.patch.object(results.judge.Run, 'load', return_value=run):
            return results._scope_unmet(None, None, None)

    def snapshot(self, *, topic=b'plants/exact', options=0):
        return {'outbound': [], 'operations': [], 'route_table': {'routes': [
            {'topic_hex': topic.hex(), 'targets': [{'session': '2', 'qos': 1, 'options': options}]}]}}

    def test_wildcard_fixture_cannot_acquire_historical_exact_routing_qualification(self):
        self.assertIn(SCOPE, self.unmet(fixture={'routes': [{'topic': 'plants/+', 'targets': []}]}))

    def test_snapshot_options_and_wildcards_cannot_expand_qualification(self):
        self.assertIn(SCOPE, self.unmet(snapshot=self.snapshot(options=4)))
        self.assertIn(SCOPE, self.unmet(snapshot=self.snapshot(topic=b'plants/#')))

    def test_recovered_options_and_kind9_are_outside_qualification_even_without_commit_facts(self):
        self.assertIn(SCOPE, self.unmet(facts=[{'kind': 'Route', 'topic_hex': b'plants/exact'.hex(), 'options': 4}]))
        value = {'outbound': [], 'operations': [{'recipe_hex': encoded().hex(), 'decision': {}}]}
        self.assertIn(results.FIXTURE_PUBSUB_SCOPE, self.unmet(snapshot=value))

    def test_legacy_exact_route_default_options_preserve_existing_scope(self):
        self.assertEqual(self.unmet(snapshot=self.snapshot()), [])
