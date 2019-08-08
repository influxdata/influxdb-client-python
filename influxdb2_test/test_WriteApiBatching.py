# coding: utf-8

from __future__ import absolute_import

import time
import unittest

import httpretty
import rx

import influxdb2
from influxdb2 import WritePrecision, WriteService
from influxdb2.client.write.point import Point
from influxdb2.client.write_api import WriteOptions, WriteApiClient
from influxdb2_test.base_test import BaseTest


class BatchingWriteTest(BaseTest):

    def setUp(self) -> None:
        # https://github.com/gabrielfalcao/HTTPretty/issues/368
        import warnings
        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed.*")
        warnings.filterwarnings("ignore", category=PendingDeprecationWarning, message="isAlive*")

        httpretty.enable()
        httpretty.reset()

        conf = influxdb2.configuration.Configuration()
        conf.host = "http://localhost"
        conf.debug = False

        self._api_client = influxdb2.ApiClient(configuration=conf, header_name="Authorization",
                                               header_value="Token my-token")

        self._write_client = WriteApiClient(service=WriteService(api_client=self._api_client),
                                            write_options=WriteOptions(batch_size=2, flush_interval=5_000))

    def tearDown(self) -> None:
        pass
        self._write_client.__del__()
        httpretty.disable()

    def test_batch_size(self):
        httpretty.register_uri(httpretty.POST, uri="http://localhost/write", status=204)
        httpretty.register_uri(httpretty.POST, uri="http://localhost/write", status=204)

        _result = self._write_client.write("my-bucket", "my-org",
                                           ["h2o_feet,location=coyote_creek level\\ water_level=1.0 1",
                                            "h2o_feet,location=coyote_creek level\\ water_level=2.0 2",
                                            "h2o_feet,location=coyote_creek level\\ water_level=3.0 3",
                                            "h2o_feet,location=coyote_creek level\\ water_level=4.0 4"])

        time.sleep(1)

        _requests = httpretty.httpretty.latest_requests

        self.assertEqual(2, len(_requests))
        _request1 = "h2o_feet,location=coyote_creek level\\ water_level=1.0 1\n" \
                    "h2o_feet,location=coyote_creek level\\ water_level=2.0 2"
        _request2 = "h2o_feet,location=coyote_creek level\\ water_level=3.0 3\n" \
                    "h2o_feet,location=coyote_creek level\\ water_level=4.0 4"

        self.assertEqual(_request1, _requests[0].parsed_body)
        self.assertEqual(_request2, _requests[1].parsed_body)
        pass

    def test_subscribe_wait(self):
        httpretty.register_uri(httpretty.POST, uri="http://localhost/write", status=204)

        self._write_client.write("my-bucket", "my-org", "h2o_feet,location=coyote_creek level\\ water_level=1.0 1")
        self._write_client.write("my-bucket", "my-org", "h2o_feet,location=coyote_creek level\\ water_level=2.0 2")

        time.sleep(1)

        _requests = httpretty.httpretty.latest_requests

        self.assertEqual(1, len(_requests))

        _request = "h2o_feet,location=coyote_creek level\\ water_level=1.0 1\n" \
                   "h2o_feet,location=coyote_creek level\\ water_level=2.0 2"

        self.assertEqual(_request, _requests[0].parsed_body)

    def test_batch_size_group_by(self):
        httpretty.register_uri(httpretty.POST, uri="http://localhost/write", status=204)

        _result = self._write_client.write("my-bucket", "my-org",
                                           "h2o_feet,location=coyote_creek level\\ water_level=1.0 1")

        _result = self._write_client.write("my-bucket", "my-org",
                                           "h2o_feet,location=coyote_creek level\\ water_level=2.0 2",
                                           write_precision=WritePrecision.S)

        _result = self._write_client.write("my-bucket", "my-org-a",
                                           "h2o_feet,location=coyote_creek level\\ water_level=3.0 3")

        _result = self._write_client.write("my-bucket", "my-org-a",
                                           "h2o_feet,location=coyote_creek level\\ water_level=4.0 4")

        _result = self._write_client.write("my-bucket2", "my-org-a",
                                           "h2o_feet,location=coyote_creek level\\ water_level=5.0 5")

        _result = self._write_client.write("my-bucket", "my-org-a",
                                           "h2o_feet,location=coyote_creek level\\ water_level=6.0 6")

        time.sleep(1)

        _requests = httpretty.httpretty.latest_requests

        self.assertEqual(5, len(_requests))

        self.assertEqual("h2o_feet,location=coyote_creek level\\ water_level=1.0 1", _requests[0].parsed_body)
        self.assertEqual("ns", _requests[0].querystring["precision"][0])
        self.assertEqual("my-bucket", _requests[0].querystring["bucket"][0])

        self.assertEqual("h2o_feet,location=coyote_creek level\\ water_level=2.0 2", _requests[1].parsed_body)
        self.assertEqual("s", _requests[1].querystring["precision"][0])
        self.assertEqual("my-bucket", _requests[1].querystring["bucket"][0])

        self.assertEqual("h2o_feet,location=coyote_creek level\\ water_level=3.0 3\n"
                         "h2o_feet,location=coyote_creek level\\ water_level=4.0 4", _requests[2].parsed_body)
        self.assertEqual("ns", _requests[2].querystring["precision"][0])
        self.assertEqual("my-bucket", _requests[2].querystring["bucket"][0])

        self.assertEqual("h2o_feet,location=coyote_creek level\\ water_level=5.0 5", _requests[3].parsed_body)
        self.assertEqual("ns", _requests[3].querystring["precision"][0])
        self.assertEqual("my-bucket2", _requests[3].querystring["bucket"][0])

        self.assertEqual("h2o_feet,location=coyote_creek level\\ water_level=6.0 6", _requests[4].parsed_body)
        self.assertEqual("ns", _requests[4].querystring["precision"][0])
        self.assertEqual("my-bucket", _requests[4].querystring["bucket"][0])

        pass

    def test_flush_interval(self):
        httpretty.register_uri(httpretty.POST, uri="http://localhost/write", status=204)

        self._write_client.write("my-bucket", "my-org",
                                 ["h2o_feet,location=coyote_creek level\\ water_level=1.0 1",
                                  "h2o_feet,location=coyote_creek level\\ water_level=2.0 2"])

        time.sleep(1)
        self.assertEqual(1, len(httpretty.httpretty.latest_requests))

        self._write_client.write("my-bucket", "my-org", "h2o_feet,location=coyote_creek level\\ water_level=3.0 3")

        time.sleep(2)

        self.assertEqual(1, len(httpretty.httpretty.latest_requests))

        time.sleep(3)

        self.assertEqual(2, len(httpretty.httpretty.latest_requests))

        self.assertEqual("h2o_feet,location=coyote_creek level\\ water_level=3.0 3",
                         httpretty.httpretty.latest_requests[1].parsed_body)

    def test_recover_from_error(self):
        httpretty.register_uri(httpretty.POST, uri="http://localhost/write", status=204)
        httpretty.register_uri(httpretty.POST, uri="http://localhost/write", status=400)

        self._write_client.write("my-bucket", "my-org",
                                 ["h2o_feet,location=coyote_creek",
                                  "h2o_feet,location=coyote_creek"])

        self._write_client.write("my-bucket", "my-org",
                                 ["h2o_feet,location=coyote_creek level\\ water_level=1.0 1",
                                  "h2o_feet,location=coyote_creek level\\ water_level=2.0 2"])

        time.sleep(1)

        _requests = httpretty.httpretty.latest_requests

        self.assertEqual(2, len(_requests))

        pass

    def test_record_types(self):
        httpretty.register_uri(httpretty.POST, uri="http://localhost/write", status=204)

        # Record item
        _record = "h2o_feet,location=coyote_creek level\\ water_level=1.0 1"
        self._write_client.write("my-bucket", "my-org", _record)

        # Point item
        _point = Point("h2o_feet").tag("location", "coyote_creek").field("level water_level", 2.0).time(2)
        self._write_client.write("my-bucket", "my-org", _point)

        # Record list
        self._write_client.write("my-bucket", "my-org",
                                 ["h2o_feet,location=coyote_creek level\\ water_level=3.0 3",
                                  "h2o_feet,location=coyote_creek level\\ water_level=4.0 4"])

        # Point list
        _point1 = Point("h2o_feet").tag("location", "coyote_creek").field("level water_level", 5.0).time(5)
        _point2 = Point("h2o_feet").tag("location", "coyote_creek").field("level water_level", 6.0).time(6)
        self._write_client.write("my-bucket", "my-org", [_point1, _point2])

        # Observable
        _recordObs = "h2o_feet,location=coyote_creek level\\ water_level=7.0 7"
        _pointObs = Point("h2o_feet").tag("location", "coyote_creek").field("level water_level", 8.0).time(8)

        self._write_client.write("my-bucket", "my-org", rx.of(_recordObs, _pointObs))

        time.sleep(1)

        _requests = httpretty.httpretty.latest_requests

        self.assertEqual(4, len(_requests))

        self.assertEqual("h2o_feet,location=coyote_creek level\\ water_level=1.0 1\n"
                         "h2o_feet,location=coyote_creek level\\ water_level=2.0 2", _requests[0].parsed_body)
        self.assertEqual("h2o_feet,location=coyote_creek level\\ water_level=3.0 3\n"
                         "h2o_feet,location=coyote_creek level\\ water_level=4.0 4", _requests[1].parsed_body)
        self.assertEqual("h2o_feet,location=coyote_creek level\\ water_level=5.0 5\n"
                         "h2o_feet,location=coyote_creek level\\ water_level=6.0 6", _requests[2].parsed_body)
        self.assertEqual("h2o_feet,location=coyote_creek level\\ water_level=7.0 7\n"
                         "h2o_feet,location=coyote_creek level\\ water_level=8.0 8", _requests[3].parsed_body)

        pass

    def test_write_result(self):
        httpretty.register_uri(httpretty.POST, uri="http://localhost/write", status=204)

        _record = "h2o_feet,location=coyote_creek level\\ water_level=1.0 1"
        _result = self._write_client.write("my-bucket", "my-org", _record)

        self.assertEqual(None, _result)

    def test_del(self):
        httpretty.register_uri(httpretty.POST, uri="http://localhost/write", status=204)

        _record = "h2o_feet,location=coyote_creek level\\ water_level=1.0 1"
        _result = self._write_client.write("my-bucket", "my-org", _record)

        self._write_client.__del__()

        _requests = httpretty.httpretty.latest_requests

        self.assertEqual(1, len(_requests))
        self.assertEqual("h2o_feet,location=coyote_creek level\\ water_level=1.0 1", _requests[0].parsed_body)


if __name__ == '__main__':
    unittest.main()
