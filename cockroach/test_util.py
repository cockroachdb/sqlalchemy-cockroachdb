from datetime import timedelta
import time
import unittest

from cockroach import util


class RetryTest(unittest.TestCase):
    def test_retry(self):
        opts = util.RetryOptions("test", timedelta(microseconds=10), timedelta(seconds=1), 2, 10)
        retries = [0]

        def fn():
            retries[0] += 1
            if retries[0] >= 3:
                return util.RetryStatus.BREAK
            return util.RetryStatus.CONTINUE
        util.retry_with_backoff(opts, fn)
        self.assertEqual(retries[0], 3)

    def texst_retry_exceeds_max_backoff(self):
        opts = util.RetryOptions("test", timedelta(microseconds=10), timedelta(microseconds=10),
                                 1000, 3)
        start = time.time()
        with self.assertRaises(util.RetryMaxAttemptsError):
            util.retry_with_backoff(opts, lambda: util.RetryStatus.CONTINUE)
        end = time.time()
        self.assertLess(end - start, 1.0,
                        "max backoff not respected: 1000 attempts took %ss" % (end - start))

    def test_retry_exceeds_max_attempts(self):
        opts = util.RetryOptions("test", timedelta(microseconds=10), timedelta(seconds=1), 2, 3)
        retries = [0]

        def fn():
            retries[0] += 1
            return util.RetryStatus.CONTINUE
        with self.assertRaises(util.RetryMaxAttemptsError):
            util.retry_with_backoff(opts, fn)
        self.assertEqual(retries[0], 3)

    def test_retry_function_raises_error(self):
        opts = util.RetryOptions("test", timedelta(microseconds=10), timedelta(seconds=1), 2)
        with self.assertRaises(ZeroDivisionError):
            util.retry_with_backoff(opts, lambda: 1/0)

    def test_retry_reset(self):
        opts = util.RetryOptions("test", timedelta(microseconds=10), timedelta(seconds=1), 2, 1)
        # Backoff loop has 1 allowed retry; we always return RESET, so just
        # make sure we get to 2 retries and then break.
        count = [0]

        def fn():
            count[0] += 1
            if count[0] == 2:
                return util.RetryStatus.BREAK
            return util.RetryStatus.RESET
        util.retry_with_backoff(opts, fn)
        self.assertEqual(count[0], 2)
