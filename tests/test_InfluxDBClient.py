import unittest

from influxdb_client import InfluxDBClient


class InfluxDBClientTest(unittest.TestCase):

    def test_TrailingSlashInUrl(self):
        client = InfluxDBClient(url="http://localhost:9999", token="my-token", org="my-org")
        self.assertEqual('http://localhost:9999', client.api_client.configuration.host)

        client = InfluxDBClient(url="http://localhost:9999/", token="my-token", org="my-org")
        self.assertEqual('http://localhost:9999', client.api_client.configuration.host)
