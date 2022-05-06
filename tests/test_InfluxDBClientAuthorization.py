import unittest

import httpretty

from influxdb_client import InfluxDBClient


class InfluxDBClientAuthorization(unittest.TestCase):

    def setUp(self) -> None:
        httpretty.enable()
        httpretty.reset()

    def tearDown(self) -> None:
        if self.influxdb_client:
            self.influxdb_client.close()
        httpretty.disable()

    def test_session_request(self):
        httpretty.reset()
        self.influxdb_client = InfluxDBClient(url="http://localhost", token="my-token",
                                              username="my-username",
                                              password="my-password")

        # create user session
        httpretty.register_uri(httpretty.POST, uri="http://localhost/api/v2/signin",
                               adding_headers={'Set-Cookie': 'session=xyz'})
        # authorized request
        httpretty.register_uri(httpretty.GET, uri="http://localhost/ping")
        # expires current session
        httpretty.register_uri(httpretty.POST, uri="http://localhost/api/v2/signout")

        ping = self.influxdb_client.ping()
        self.assertTrue(ping)

        self.assertEqual(2, len(httpretty.httpretty.latest_requests))
        # basic auth header
        self.assertEqual('Basic bXktdXNlcm5hbWU6bXktcGFzc3dvcmQ=', httpretty.httpretty.latest_requests[0].headers['Authorization'])
        # cookie header
        self.assertEqual('session=xyz', httpretty.httpretty.latest_requests[1].headers['Cookie'])
        self.assertIsNotNone(self.influxdb_client.api_client.cookie)

        # signout
        self.influxdb_client.close()

        self.assertEqual(3, len(httpretty.httpretty.latest_requests))
