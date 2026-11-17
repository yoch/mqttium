"""Mutation evidence requires one executed named test and its exact reason."""
from pathlib import Path
import tempfile
import unittest

from message_expiry_mutants import (
    CONTROLS,
    digest,
    precise_failure,
    precise_positive,
    test_binary as executed_binary,
)


class MessageExpiryMutationDetectionTests(unittest.TestCase):
    test = 'expiry::tests::the_business_invariant'
    reason = 'publication_lifetime_was_renewed'

    def failure(self, message=None, test=None):
        test = self.test if test is None else test
        message = self.reason if message is None else message
        return (f'running 1 test\n\n'
                f"thread '{test}' panicked at src/expiry.rs:12:3:\n"
                f'{message}\n'
                f'note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace\n'
                f'test {test} ... FAILED\n\n'
                'failures:\n\n'
                f'    {test}\n\n'
                'test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 8 filtered out;\n')

    def detected(self, output, status=101):
        return precise_failure(output, status, self.reason, self.test)

    def test_exact_business_assertions_are_accepted(self):
        for message in (self.reason,
                        f'assertion failed: {self.reason}',
                        f'assertion `left == right` failed: {self.reason}\n  left: 2\n right: 1'):
            with self.subTest(message=message):
                self.assertTrue(self.detected(self.failure(message)))
        self.assertTrue(self.detected(self.failure().replace(
            f"thread '{self.test}' panicked", f"thread '{self.test}' (1234) panicked")))

    def test_wrong_partial_or_printed_reason_is_refused(self):
        for message in ('different_lifetime_failure', self.reason + '_other',
                        'debug output: ' + self.reason):
            with self.subTest(message=message):
                self.assertFalse(self.detected(self.failure(message)))
        # An expected marker printed before the panic is not its cause.
        self.assertFalse(self.detected(self.reason + '\n' + self.failure('different_failure')))
        # Nor is a marker printed after the failure summary.
        self.assertFalse(self.detected(self.failure('different_failure') + self.reason + '\n'))

    def test_unrelated_thread_or_test_cannot_supply_the_reason(self):
        self.assertFalse(self.detected(self.failure(test='other::test')))
        other_panic = "thread 'other::test' panicked at src/other.rs:1:1:\n" + self.reason + '\n'
        self.assertFalse(self.detected(self.failure('different_failure') + other_panic))

    def test_compiler_and_unwrap_failures_are_not_detection(self):
        for error in ('could not compile mqttiumd', 'error[E0308]: mismatched types',
                      'called `Result::unwrap()` on an `Err` value',
                      'called `Option::unwrap()` on a `None` value'):
            with self.subTest(error=error):
                self.assertFalse(self.detected(self.failure() + error))

    def test_missing_execution_wrong_status_or_multiple_tests_are_refused(self):
        for output in (self.failure().replace('running 1 test', 'running 0 tests'),
                       self.failure().replace('running 1 test', 'running 2 tests'),
                       self.failure().replace(f'test {self.test} ... FAILED', ''),
                       self.failure().replace('0 passed; 1 failed;', '1 passed; 1 failed;')):
            with self.subTest(output=output):
                self.assertFalse(self.detected(output))
        for status in (0, 1, -9, 124):
            with self.subTest(status=status):
                self.assertFalse(self.detected(self.failure(), status))

    def test_positive_requires_the_one_named_success(self):
        output = (f'running 1 test\ntest {self.test} ... ok\n'
                  'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured;')
        self.assertTrue(precise_positive(output, 0, self.test))
        for invalid in (output.replace(self.test, 'other::test'),
                        output.replace('running 1 test', 'running 0 tests'),
                        output.replace('1 passed;', '0 passed;'),
                        output.replace(f'test {self.test} ... ok', '')):
            with self.subTest(invalid=invalid):
                self.assertFalse(precise_positive(invalid, 0, self.test))
        self.assertFalse(precise_positive(output, 101, self.test))

    def test_executed_binary_is_hashed_and_ambiguous_execution_is_refused(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binary = root / 'actual-test-binary'
            binary.write_bytes(b'compiled test binary bytes')
            output = '     Running unittests src/main.rs (actual-test-binary)\n'
            found = executed_binary(output, root)
            self.assertEqual(found['sha256'], digest(binary))
            self.assertEqual(found['size'], binary.stat().st_size)
            for invalid in ('running 0 tests\n', output + output):
                with self.subTest(invalid=invalid), self.assertRaises(RuntimeError):
                    executed_binary(invalid, root)

    def test_catalog_has_six_independent_production_controls(self):
        self.assertEqual(len(CONTROLS), 6)
        self.assertEqual(len({control.name for control in CONTROLS}), 6)
        self.assertEqual(len({control.test for control in CONTROLS}), 6)
        for control in CONTROLS:
            with self.subTest(control=control.name):
                self.assertIn('/src/', control.source)
                self.assertNotIn('/tests/', control.source)
                self.assertNotEqual(control.before, control.after)
                self.assertNotIn('assert!', control.after)
                self.assertNotIn('assert_eq!', control.after)
                self.assertTrue(control.expected_reason)


if __name__ == '__main__':
    unittest.main()
