import unittest
import time

from urllib3 import HTTPResponse
from urllib3.exceptions import MaxRetryError

from influxdb_client.client.write.retry import WritesRetry


class NonRandomMinWritesRetry(WritesRetry):
    def _random(self):
        return 0


class NonRandomMaxWritesRetry(WritesRetry):
    def _random(self):
        return 1


class TestWritesRetry(unittest.TestCase):
    def test_copy(self):
        retry = WritesRetry(exponential_base=3, max_retry_delay=145, total=10)
        self.assertEqual(retry.max_retry_delay, 145)
        self.assertEqual(retry.exponential_base, 3)
        self.assertEqual(retry.total, 10)

        retry = retry.increment()
        self.assertEqual(retry.max_retry_delay, 145)
        self.assertEqual(retry.exponential_base, 3)
        self.assertEqual(retry.total, 9)

        retry = retry.increment()
        self.assertEqual(retry.max_retry_delay, 145)
        self.assertEqual(retry.exponential_base, 3)
        self.assertEqual(retry.total, 8)

    def test_backoff_max_time(self):
        retry = NonRandomMinWritesRetry(max_retry_time=2)
        self.assertEqual(retry.is_exhausted(), False)
        self.assertEqual(retry.get_backoff_time(), 0)

        retry = retry.increment()
        self.assertEqual(retry.is_exhausted(), False)
        self.assertEqual(retry.get_backoff_time(), 5)

        retry = retry.increment()
        self.assertEqual(retry.is_exhausted(), False)
        self.assertEqual(retry.get_backoff_time(), 10)

        time.sleep(3)

        with self.assertRaises(MaxRetryError) as cm:
            retry.increment()
        exception = cm.exception

        self.assertEqual("max_retry_time exceeded", exception.reason.args[0])

    def test_backoff_start_range(self):
        retry = NonRandomMinWritesRetry(total=5, retry_interval=1, exponential_base=2,
                                        max_retry_delay=550)
        self.assertEqual(retry.total, 5)
        self.assertEqual(retry.is_exhausted(), False)
        self.assertEqual(retry.get_backoff_time(), 0)

        retry = retry.increment()
        self.assertEqual(retry.total, 4)
        self.assertEqual(retry.is_exhausted(), False)
        self.assertEqual(retry.get_backoff_time(), 1)

        retry = retry.increment()
        self.assertEqual(retry.total, 3)
        self.assertEqual(retry.is_exhausted(), False)
        self.assertEqual(retry.get_backoff_time(), 2)

        retry = retry.increment()
        self.assertEqual(retry.total, 2)
        self.assertEqual(retry.is_exhausted(), False)
        self.assertEqual(retry.get_backoff_time(), 4)

        retry = retry.increment()
        self.assertEqual(retry.total, 1)
        self.assertEqual(retry.is_exhausted(), False)
        self.assertEqual(retry.get_backoff_time(), 8)

        retry = retry.increment()
        self.assertEqual(retry.total, 0)
        self.assertEqual(retry.is_exhausted(), False)
        self.assertEqual(retry.get_backoff_time(), 16)

        with self.assertRaises(MaxRetryError) as cm:
            retry.increment()
        exception = cm.exception

        self.assertEqual("too many error responses", exception.reason.args[0])

    def test_backoff_stop_range(self):
        retry = NonRandomMaxWritesRetry(total=5, retry_interval=5, exponential_base=2,
                                        max_retry_delay=550)

        self.assertEqual(retry.total, 5)
        self.assertEqual(retry.is_exhausted(), False)
        self.assertEqual(retry.get_backoff_time(), 0)

        retry = retry.increment()
        self.assertEqual(retry.total, 4)
        self.assertEqual(retry.is_exhausted(), False)
        self.assertEqual(retry.get_backoff_time(), 10)

        retry = retry.increment()
        self.assertEqual(retry.total, 3)
        self.assertEqual(retry.is_exhausted(), False)
        self.assertEqual(retry.get_backoff_time(), 20)

        retry = retry.increment()
        self.assertEqual(retry.total, 2)
        self.assertEqual(retry.is_exhausted(), False)
        self.assertEqual(retry.get_backoff_time(), 40)

        retry = retry.increment()
        self.assertEqual(retry.total, 1)
        self.assertEqual(retry.is_exhausted(), False)
        self.assertEqual(retry.get_backoff_time(), 80)

        retry = retry.increment()
        self.assertEqual(retry.total, 0)
        self.assertEqual(retry.is_exhausted(), False)
        self.assertEqual(retry.get_backoff_time(), 160)

        with self.assertRaises(MaxRetryError) as cm:
            retry.increment()
        exception = cm.exception

        self.assertEqual("too many error responses", exception.reason.args[0])

    def test_backoff_max(self):
        retry = WritesRetry(total=5, retry_interval=1, max_retry_delay=15) \
            .increment() \
            .increment() \
            .increment() \
            .increment() \
            .increment()

        self.assertLessEqual(retry.get_backoff_time(), 15)

    def test_backoff_increment(self):
        retry = WritesRetry(total=5, retry_interval=4).increment()

        self.assertEqual(retry.total, 4)
        self.assertEqual(retry.is_exhausted(), False)

        backoff_time = retry.get_backoff_time()
        self.assertGreater(backoff_time, 4)
        self.assertLessEqual(backoff_time, 8)

    def test_backoff_exponential_base(self):
        retry = NonRandomMinWritesRetry(total=5, retry_interval=2, exponential_base=2)

        retry = retry.increment()
        self.assertEqual(retry.get_backoff_time(), 2)

        retry = retry.increment()
        self.assertEqual(retry.get_backoff_time(), 4)

        retry = retry.increment()
        self.assertEqual(retry.get_backoff_time(), 8)

        retry = retry.increment()
        self.assertEqual(retry.get_backoff_time(), 16)

    def test_get_retry_after(self):
        response = HTTPResponse()
        response.headers.add('Retry-After', '5')

        retry = WritesRetry()
        self.assertEqual(retry.get_retry_after(response), 5)

    def test_get_retry_after_jitter(self):
        response = HTTPResponse()
        response.headers.add('Retry-After', '5')

        retry = WritesRetry(jitter_interval=2)
        retry_after = retry.get_retry_after(response)
        self.assertGreater(retry_after, 5)
        self.assertLessEqual(retry_after, 7)

    def test_is_retry(self):
        retry = WritesRetry(allowed_methods=["POST"])

        self.assertTrue(retry.is_retry("POST", 429, True))

    def test_is_retry_428(self):
        retry = WritesRetry(allowed_methods=["POST"])

        self.assertFalse(retry.is_retry("POST", 428, True))

    def test_is_retry_430(self):
        retry = WritesRetry(allowed_methods=["POST"])

        self.assertTrue(retry.is_retry("POST", 430, True))

    def test_is_retry_retry_after_header_is_not_required(self):
        retry = WritesRetry(allowed_methods=["POST"])

        self.assertTrue(retry.is_retry("POST", 429, False))

    def test_is_retry_respect_method(self):
        retry = WritesRetry(allowed_methods=["POST"])

        self.assertFalse(retry.is_retry("GET", 429, False))

    def test_logging(self):
        response = HTTPResponse(
            body='{"code":"too many requests","message":"org 04014de4ed590000 has exceeded limited_write plan limit"}')
        response.headers.add('Retry-After', '63')

        with self.assertLogs('influxdb_client.client.write.retry', level='WARNING') as cm:
            WritesRetry(total=5, retry_interval=1, max_retry_delay=15) \
                .increment(response=response) \
                .increment(error=Exception("too many requests")) \
                .increment(url='http://localhost:9999')

        self.assertEqual("WARNING:influxdb_client.client.write.retry:The retriable error occurred during request. "
                         "Reason: 'org 04014de4ed590000 has exceeded limited_write plan limit'. Retry in 63s.",
                         cm.output[0])
        self.assertEqual("WARNING:influxdb_client.client.write.retry:The retriable error occurred during request. "
                         "Reason: 'too many requests'.",
                         cm.output[1])
        self.assertEqual("WARNING:influxdb_client.client.write.retry:The retriable error occurred during request. "
                         "Reason: 'Failed request to: http://localhost:9999'.",
                         cm.output[2])
