import logging
import unittest
import unittest.mock

import urllib3

from influxdb_client import InfluxLoggingHandler, InfluxDBClient, WriteApi, WritePrecision, Point


class LoggingBaseTestCase(unittest.TestCase):
    fake_line_record = "tag,field=value 123456"
    URL_TOKEN_ORG = {
        'url': 'http://example.com',
        'token': 'my-token',
        'org': 'my-org',
    }
    BUCKET = 'my-bucket'

    def setUp(self) -> None:
        self.mock_InfluxDBClient = unittest.mock.patch("influxdb_client.client.logging_handler.InfluxDBClient").start()
        self.mock_client = unittest.mock.MagicMock(spec=InfluxDBClient)
        self.mock_write_api = unittest.mock.MagicMock(spec=WriteApi)
        self.mock_client.write_api.return_value = self.mock_write_api
        self.mock_InfluxDBClient.return_value = self.mock_client

    def gen_handler_and_logger(self):
        self.handler = InfluxLoggingHandler(**self.URL_TOKEN_ORG, bucket=self.BUCKET)
        self.handler.setLevel(logging.DEBUG)

        self.logger = logging.getLogger("test-logger")
        self.logger.setLevel(logging.DEBUG)

    def tearDown(self) -> None:
        unittest.mock.patch.stopall()


class TestHandlerCreation(LoggingBaseTestCase):
    def test_can_create_handler(self):
        self.handler = InfluxLoggingHandler(**self.URL_TOKEN_ORG, bucket=self.BUCKET)
        self.mock_InfluxDBClient.assert_called_once_with(**self.URL_TOKEN_ORG)
        self.assertEqual(self.BUCKET, self.handler.bucket)
        self.mock_client.write_api.assert_called_once_with()

    def test_can_create_handler_with_optional_args_for_client(self):
        self.handler = InfluxLoggingHandler(**self.URL_TOKEN_ORG, bucket=self.BUCKET,
                                            client_args={'arg2': 2.90, 'optArg': 'whot'})
        self.mock_InfluxDBClient.assert_called_once_with(**self.URL_TOKEN_ORG, arg2=2.90, optArg="whot")
        self.mock_client.write_api.assert_called_once_with()

    def test_can_create_handler_with_args_for_write_api(self):
        self.handler = InfluxLoggingHandler(**self.URL_TOKEN_ORG, bucket=self.BUCKET,
                                            client_args={'arg2': 2.90, 'optArg': 'whot'},
                                            write_api_args={'foo': 'bar'})
        self.mock_InfluxDBClient.assert_called_once_with(**self.URL_TOKEN_ORG, arg2=2.90, optArg="whot")
        self.mock_client.write_api.assert_called_once_with(foo='bar')


class CreatedHandlerTestCase(LoggingBaseTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.gen_handler_and_logger()


class LoggingSetupAndTearDown(CreatedHandlerTestCase):
    def test_is_handler(self):
        self.assertIsInstance(self.handler, logging.Handler)

    def test_set_up_client(self):
        self.mock_InfluxDBClient.assert_called_once()

    def test_closes_connections_on_close(self):
        self.handler.close()
        self.mock_write_api.close.assert_called_once()
        self.mock_client.close.assert_called_once()

    def test_handler_can_be_attached_to_logger(self):
        self.logger.addHandler(self.handler)
        self.assertTrue(self.logger.hasHandlers())
        self.assertTrue(self.handler in self.logger.handlers)


class LoggingWithAttachedHandler(CreatedHandlerTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.logger.addHandler(self.handler)


class LoggingHandlerTest(LoggingWithAttachedHandler):

    def test_can_log_str(self):
        self.logger.debug(self.fake_line_record)
        self.mock_write_api.write.assert_called_once_with(bucket="my-bucket", record=self.fake_line_record)

    def test_can_log_points(self):
        point = Point("measurement").field("field_name", "field_value").time(333, WritePrecision.NS)
        self.logger.debug(point)
        self.mock_write_api.write.assert_called_once_with(bucket="my-bucket", record=str(point))

    def test_catches_urllib_exceptions(self):
        self.mock_write_api.write.side_effect = urllib3.exceptions.HTTPError()
        try:
            with unittest.mock.patch("logging.sys.stderr") as _:
                # Handler writes logging errors to stderr. Don't display it in the test output.
                self.logger.debug(self.fake_line_record)
        finally:
            self.mock_write_api.write.side_effect = None

    def test_raises_on_exit(self):
        try:
            self.mock_write_api.write.side_effect = KeyboardInterrupt()
            self.assertRaises(KeyboardInterrupt, self.logger.debug, self.fake_line_record)
            self.mock_write_api.write.side_effect = SystemExit()
            self.assertRaises(SystemExit, self.logger.debug, self.fake_line_record)
        finally:
            self.mock_write_api.write.side_effect = None

    def test_can_set_bucket(self):
        self.handler.bucket = "new-bucket"
        self.logger.debug(self.fake_line_record)
        self.mock_write_api.write.assert_called_once_with(bucket="new-bucket", record=self.fake_line_record)

    def test_can_pass_bucket_on_log(self):
        self.logger.debug(self.fake_line_record, extra={'bucket': "other-bucket"})
        self.mock_write_api.write.assert_called_once_with(bucket="other-bucket", record=self.fake_line_record)

    def test_can_pass_optional_params_on_log(self):
        self.logger.debug(self.fake_line_record, extra={'org': "other-org", 'write_precision': WritePrecision.S,
                                                        "arg3": 3, "arg2": "two"})
        self.mock_write_api.write.assert_called_once_with(bucket="my-bucket", org='other-org',
                                                          record=self.fake_line_record,
                                                          write_precision=WritePrecision.S,
                                                          arg3=3, arg2="two")

    def test_formatter(self):
        class MyFormatter(logging.Formatter):
            def format(self, record: logging.LogRecord) -> str:
                time_ns = int(record.created * 1e9) * 0 + 123
                return f"{record.name},level={record.levelname} message=\"{record.msg}\" {time_ns}"

        self.handler.setFormatter(MyFormatter())
        msg = "a debug message"
        self.logger.debug(msg)
        expected_record = f"test-logger,level=DEBUG message=\"{msg}\" 123"
        self.mock_write_api.write.assert_called_once_with(bucket="my-bucket", record=expected_record)


if __name__ == "__main__":
    unittest.main()
