from __future__ import absolute_import

import unittest

import influxdb2
from influxdb2.client.influxdb_client import InfluxDBClient


class BaseTest(unittest.TestCase):

    def setUp(self) -> None:
        conf = influxdb2.configuration.Configuration()
        conf.host = "http://localhost:9999/api/v2"
        conf.debug = False
        auth_token = "my-token-123"
        self.org = "my-org"
        self.bucket = "test_bucket"

        self.client = InfluxDBClient(conf.host, auth_token, debug=conf.debug, org="my-org")
        self.api_client = self.client.api_client

        self.write_api = influxdb2.api.write_api.WriteApi(self.api_client)
        self.write_client = self.client.write_client()

        self.query_client = self.client.query_client()

    def tearDown(self) -> None:
        self.client.__del__()


