import unittest

from influxdb_client.client.write_api import WriteOptions


class TestWriteOptions(unittest.TestCase):
    def test_default(self):
        retry = WriteOptions().to_retry_strategy()

        self.assertEqual(retry.total, 3)
        self.assertEqual(retry.backoff_factor, 5)
        self.assertEqual(retry.max_retry_delay, 180)
        self.assertEqual(retry.exponential_base, 5)
        self.assertEqual(retry.method_whitelist, ["POST"])

    def test_custom(self):
        retry = WriteOptions(max_retries=5, max_retry_delay=7500,
                             retry_interval=500, jitter_interval=2000,
                             exponential_base=2)\
            .to_retry_strategy()

        self.assertEqual(retry.total, 5)
        self.assertEqual(retry.backoff_factor, 0.5)
        self.assertEqual(retry.max_retry_delay, 7.5)
        self.assertEqual(retry.exponential_base, 2)
        self.assertEqual(retry.method_whitelist, ["POST"])
