"""A mutation is detected only by its executed semantic assertion."""
import unittest

from retained_mutants import precise_failure


class RetainedMutationDetectionTests(unittest.TestCase):
    reason = 'replay_reread_current_retained'
    failed = 'test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured;'

    def test_exact_assert_messages_are_accepted(self):
        for message in (self.reason, f'assertion `left == right` failed: {self.reason}'):
            with self.subTest(message=message):
                self.assertTrue(precise_failure(message + '\n' + self.failed, 101, self.reason))

    def test_wrong_or_partial_reason_is_not_detection(self):
        for message in ('different_retained_failure', self.reason + '_other'):
            with self.subTest(message=message):
                self.assertFalse(precise_failure(message + '\n' + self.failed, 101, self.reason))

    def test_compilation_or_unwrap_failure_is_not_detection(self):
        for error in ('could not compile mqttiumd', 'error[E0308]: mismatched types',
                      'called `Result::unwrap()` on an `Err` value',
                      'called `Option::unwrap()` on a `None` value'):
            with self.subTest(error=error):
                self.assertFalse(precise_failure(self.reason + '\n' + self.failed + '\n' + error,
                                                101, self.reason))

    def test_no_test_or_wrong_status_is_not_detection(self):
        self.assertFalse(precise_failure(self.reason + '\ntest result: ok. 0 passed;', 0, self.reason))
        self.assertFalse(precise_failure(self.reason + '\n' + self.failed, -9, self.reason))


if __name__ == '__main__':
    unittest.main()
