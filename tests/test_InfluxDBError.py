import unittest

from urllib3 import HTTPResponse

from influxdb_client.client.exceptions import InfluxDBError


class TestInfluxDBError(unittest.TestCase):
    def test_response(self):
        response = HTTPResponse()
        self.assertEqual(response, InfluxDBError(response=response).response)

    def test_message(self):

        response = HTTPResponse()
        response.headers.add('X-Platform-Error-Code', 'too many requests 1')
        self.assertEqual("too many requests 1", str(InfluxDBError(response=response)))

        response = HTTPResponse()
        response.headers.add('X-Influx-Error', 'too many requests 2')
        self.assertEqual("too many requests 2", str(InfluxDBError(response=response)))

        response = HTTPResponse()
        response.headers.add('X-InfluxDb-Error', 'too many requests 3')
        self.assertEqual("too many requests 3", str(InfluxDBError(response=response)))

        response = HTTPResponse(body='{"code":"too many requests","message":"org 04014de4ed590000 has exceeded limited_write plan limit"}')
        response.headers.add('X-InfluxDb-Error', 'error 3')
        self.assertEqual("org 04014de4ed590000 has exceeded limited_write plan limit", str(InfluxDBError(response=response)))

        response = HTTPResponse(body='org 04014de4ed590000 has exceeded limited_write plan limit')
        response.headers.add('X-InfluxDb-Error', 'error 3')
        self.assertEqual("org 04014de4ed590000 has exceeded limited_write plan limit", str(InfluxDBError(response=response)))

        response = HTTPResponse(reason='too many requests 4')
        self.assertEqual("too many requests 4", str(InfluxDBError(response=response)))

    def test_message_get_retry_after(self):
        response = HTTPResponse(reason="too many requests")
        response.headers.add('Retry-After', '63')

        influx_db_error = InfluxDBError(response=response)
        self.assertEqual("too many requests", str(influx_db_error))
        self.assertEqual("63", influx_db_error.retry_after)

        influx_db_error = InfluxDBError(response=HTTPResponse(reason="too many requests"))
        self.assertEqual("too many requests", str(influx_db_error))
        self.assertEqual(None, influx_db_error.retry_after)

    def test_no_response(self):
        influx_db_error = InfluxDBError(response=None)
        self.assertEqual("no response", str(influx_db_error))
        self.assertIsNone(influx_db_error.response)
        self.assertIsNone(influx_db_error.retry_after)
