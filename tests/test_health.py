import datetime

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
