"""The billing throttle's own check suite."""

from __future__ import annotations

import unittest

from billing.throttle import is_allowed, remaining, sustained_rate


class ThrottleAdmissionTests(unittest.TestCase):
    def test_admits_requests_up_to_the_limit(self) -> None:
        self.assertTrue(is_allowed("3/1s", 0))
        self.assertTrue(is_allowed("3/1s", 2))

    def test_refuses_the_request_that_would_exceed_the_limit(self) -> None:
        # With three already admitted in the window, the fourth must be
        # refused: a budget of three admits requests one through three.
        self.assertFalse(is_allowed("3/1s", 3))

    def test_remaining_never_goes_negative(self) -> None:
        self.assertEqual(remaining("120/1m", 20), 100)
        self.assertEqual(remaining("120/1m", 500), 0)


class SustainedRateTests(unittest.TestCase):
    def test_converts_windows_to_requests_per_second(self) -> None:
        self.assertEqual(sustained_rate("120/1m"), 2.0)
        self.assertEqual(sustained_rate("3600/1h"), 1.0)


if __name__ == "__main__":
    unittest.main()
