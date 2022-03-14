import unittest

from influxdb_client.client.write_api import WriteOptions


class TestWriteOptions(unittest.TestCase):
    def test_default(self):
        retry = WriteOptions().to_retry_strategy()

        self.assertEqual(retry.total, 5)
        self.assertEqual(retry.retry_interval, 5)
        self.assertEqual(retry.max_retry_time, 180)
        self.assertEqual(retry.max_retry_delay, 125)
        self.assertEqual(retry.exponential_base, 2)
        self.assertEqual(retry.allowed_methods, ["POST"])

    def test_custom(self):
        retry = WriteOptions(max_retries=5, max_retry_delay=7500,
                             retry_interval=500, jitter_interval=2000,
                             exponential_base=2)\
            .to_retry_strategy()

        self.assertEqual(retry.total, 5)
        self.assertEqual(retry.retry_interval, 0.5)
        self.assertEqual(retry.max_retry_delay, 7.5)
        self.assertEqual(retry.exponential_base, 2)
        self.assertEqual(retry.allowed_methods, ["POST"])
