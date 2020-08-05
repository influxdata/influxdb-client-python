import datetime
import unittest

import httpretty
import influxdb_client

from influxdb_client import InfluxDBClient
from tests.base_test import BaseTest


class TestHealth(BaseTest):

    def setUp(self) -> None:
        super(TestHealth, self).setUp()
        self.client.api_client.configuration.debug = True

    def test_health(self):
        health = self.client.health()
        self.assertEqual(health.message, 'ready for queries and writes')
        self.assertEqual(health.status, "pass")
        self.assertEqual(health.name, "influxdb")

    def test_health_not_running_instance(self):
        client_not_running = InfluxDBClient("http://localhost:8099", token="my-token", debug=True)
        check = client_not_running.health()
        self.assertTrue("Connection refused" in check.message)
        self.assertEqual(check.status, "fail")
        self.assertEqual(check.name, "influxdb")

    def test_ready(self):
        ready = self.client.ready()
        self.assertEqual(ready.status, "ready")
        self.assertIsNotNone(ready.started)
        self.assertTrue(datetime.datetime.now(tz=ready.started.tzinfo) > ready.started)
        self.assertIsNotNone(ready.up)


class TestHealthMock(unittest.TestCase):

    def setUp(self) -> None:
        httpretty.enable()
        httpretty.reset()

        conf = influxdb_client.configuration.Configuration()
        conf.host = "http://localhost"
        conf.debug = False

        self.influxdb_client = InfluxDBClient(url=conf.host, token="my-token")

    def tearDown(self) -> None:
        self.influxdb_client.__del__()
        httpretty.disable()

    def test_without_retry(self):
        httpretty.register_uri(httpretty.GET, uri="http://localhost/health", status=429,
                               adding_headers={'Retry-After': '5', 'Content-Type': 'application/json'},
                               body="{\"message\":\"Health is not working\"}")

        check = self.influxdb_client.health()
        self.assertTrue("Health is not working" in check.message, msg=check.message)
        self.assertEqual(check.status, "fail")
        self.assertEqual(check.name, "influxdb")

        self.assertEqual(1, len(httpretty.httpretty.latest_requests))
