import unittest
from unittest.mock import Mock

import httpretty

from influxdb_client import InfluxDBClient
from influxdb_client._sync.api_client import ApiClient


class InfluxDBClientAuthorization(unittest.TestCase):

    def setUp(self) -> None:
        httpretty.enable()
        httpretty.reset()
        self.influxdb_client = None

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
        self.assertEqual(
            'Basic bXktdXNlcm5hbWU6bXktcGFzc3dvcmQ=',
            httpretty.httpretty.latest_requests[0].headers['Authorization'],
        )
        # cookie header
        self.assertEqual('session=xyz', httpretty.httpretty.latest_requests[1].headers['Cookie'])
        self.assertIsNotNone(self.influxdb_client.api_client.cookie)

        # signout
        self.influxdb_client.close()

        self.assertEqual(3, len(httpretty.httpretty.latest_requests))

    def test_api_client_finalizer_ignores_partially_torn_down_state(self):
        api_client = ApiClient()
        del api_client.configuration
        del api_client.rest_client

        api_client.__del__()

    def test_api_client_finalizer_cleans_pools_when_signout_fails(self):
        api_client = ApiClient()
        api_client._signout = Mock(side_effect=RuntimeError('session service unavailable'))
        pool = api_client._pool = Mock()
        manager = api_client.rest_client.pool_manager = Mock()

        api_client.__del__()

        api_client._signout.assert_called_once_with()
        pool.close.assert_called_once_with()
        pool.join.assert_called_once_with()
        self.assertIsNone(api_client._pool)
        manager.clear.assert_called_once_with()

    def test_api_client_finalizer_clears_http_pool_when_thread_pool_cleanup_fails(self):
        for operation in ('close', 'join'):
            with self.subTest(operation=operation):
                api_client = ApiClient()
                api_client._signout = Mock()
                pool = api_client._pool = Mock()
                getattr(pool, operation).side_effect = RuntimeError('pool already shut down')
                manager = api_client.rest_client.pool_manager = Mock()

                api_client.__del__()

                getattr(pool, operation).assert_called_once_with()
                self.assertIsNone(api_client._pool)
                manager.clear.assert_called_once_with()

    def test_api_client_finalizer_ignores_http_pool_cleanup_failure(self):
        api_client = ApiClient()
        api_client._signout = Mock()
        manager = api_client.rest_client.pool_manager = Mock()
        manager.clear.side_effect = RuntimeError('HTTP pool already shut down')

        api_client.__del__()

        manager.clear.assert_called_once_with()
