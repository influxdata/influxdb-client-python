# coding: utf-8

from __future__ import absolute_import

import datetime
import json
import logging
import os
import re
import sys
import unittest
from collections import namedtuple
from datetime import timedelta
from multiprocessing.pool import ApplyResult
from types import SimpleNamespace

import httpretty
import pytest

import influxdb_client
from influxdb_client import Point, WritePrecision, InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS, PointSettings
from influxdb_client.rest import ApiException
from tests.base_test import BaseTest


class SynchronousWriteTest(BaseTest):

    def setUp(self) -> None:
        super().setUp()

        os.environ['data_center'] = "LA"

        self.id_tag = "132-987-655"
        self.customer_tag = "California Miner"
        self.data_center_key = "data_center"

        self.write_client = self.client.write_api(write_options=SYNCHRONOUS,
                                                  point_settings=PointSettings(**{"id": self.id_tag,
                                                                                  "customer": self.customer_tag,
                                                                                  self.data_center_key:
                                                                                      '${env.data_center}'}))

    def tearDown(self) -> None:
        self.write_client.close()
        super().tearDown()

    def test_write_line_protocol(self):
        bucket = self.create_test_bucket()

        record = "h2o_feet,location=coyote_creek level\\ water_level=1 1"
        self.write_client.write(bucket.name, self.org, record)

        result = self.query_api.query(f"from(bucket:\"{bucket.name}\") |> range(start: 1970-01-01T00:00:00.000000001Z)",
                                      self.org)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].records[0].get_measurement(), "h2o_feet")
        self.assertEqual(result[0].records[0].get_value(), 1.0)
        self.assertEqual(result[0].records[0].get_field(), "level water_level")
        self.assertEqual(result[0].records[0].get_time(),
                         datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc))

        self.delete_test_bucket(bucket)

    def test_write_precision(self):
        bucket = self.create_test_bucket()

        self.write_client.write(org="my-org", bucket=bucket.name, record="air,location=Python humidity=99",
                                write_precision=WritePrecision.MS)

        result = self.query_api.query(
            "from(bucket:\"" + bucket.name + "\") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()", self.org)

        self.assertEqual(len(result), 1)

        self.delete_test_bucket(bucket)

    def test_write_records_list(self):
        bucket = self.create_test_bucket()

        _record1 = "h2o_feet,location=coyote_creek level\\ water_level=1 1"
        _record2 = "h2o_feet,location=coyote_creek level\\ water_level=2 2"

        record_list = [_record1, _record2]

        self.write_client.write(bucket.name, self.org, record_list)

        query = 'from(bucket:"' + bucket.name + '") |> range(start: 1970-01-01T00:00:00.000000001Z)'

        flux_result = self.client.query_api().query(query)

        self.assertEqual(1, len(flux_result))

        records = flux_result[0].records

        self.assertEqual(2, len(records))

        self.assertEqual("h2o_feet", records[0].get_measurement())
        self.assertEqual(1, records[0].get_value())
        self.assertEqual("level water_level", records[0].get_field())

        self.assertEqual("h2o_feet", records[1].get_measurement())
        self.assertEqual(2, records[1].get_value())
        self.assertEqual("level water_level", records[1].get_field())
        self.delete_test_bucket(bucket)

    def test_write_points_unicode(self):
        bucket = self.create_test_bucket()

        measurement = "h2o_feet_ěščřĚŠČŘ"
        field_name = "field_ěščř"
        utf8_val = "Přerov 🍺"
        tag = "tag_ěščř"
        tag_value = "tag_value_ěščř"

        p = Point(measurement)
        p.field(field_name, utf8_val)
        p.tag(tag, tag_value)
        record_list = [p]

        self.write_client.write(bucket.name, self.org, record_list)

        query = 'from(bucket:"' + bucket.name + '") |> range(start: 1970-01-01T00:00:00.000000001Z)'
        flux_result = self.client.query_api().query(query)
        self.assertEqual(1, len(flux_result))
        rec = flux_result[0].records[0]

        self.assertEqual(self.id_tag, rec["id"])
        self.assertEqual(self.customer_tag, rec["customer"])
        self.assertEqual("LA", rec[self.data_center_key])

        self.assertEqual(measurement, rec.get_measurement())
        self.assertEqual(utf8_val, rec.get_value())
        self.assertEqual(field_name, rec.get_field())

    def test_write_using_default_tags(self):
        bucket = self.create_test_bucket()

        measurement = "h2o_feet"
        field_name = "water_level"
        val = "1.0"
        val2 = "2.0"
        tag = "location"
        tag_value = "creek level"

        p = Point(measurement)
        p.field(field_name, val)
        p.tag(tag, tag_value)
        p.time(1)

        p2 = Point(measurement)
        p2.field(field_name, val2)
        p2.tag(tag, tag_value)
        p2.time(2)

        record_list = [p, p2]

        self.write_client.write(bucket.name, self.org, record_list)

        query = 'from(bucket:"' + bucket.name + '") |> range(start: 1970-01-01T00:00:00.000000001Z)'
        flux_result = self.client.query_api().query(query)
        self.assertEqual(1, len(flux_result))
        rec = flux_result[0].records[0]
        rec2 = flux_result[0].records[1]

        self.assertEqual(self.id_tag, rec["id"])
        self.assertEqual(self.customer_tag, rec["customer"])
        self.assertEqual("LA", rec[self.data_center_key])

        self.assertEqual(self.id_tag, rec2["id"])
        self.assertEqual(self.customer_tag, rec2["customer"])
        self.assertEqual("LA", rec2[self.data_center_key])

        self.delete_test_bucket(bucket)

    def test_write_result(self):
        _bucket = self.create_test_bucket()

        _record = "h2o_feet,location=coyote_creek level\\ water_level=1 1"
        result = self.write_client.write(_bucket.name, self.org, _record)

        # The success response is 204 - No Content
        self.assertEqual(None, result)

    def test_write_error(self):
        _bucket = self.create_test_bucket()

        _record = "h2o_feet,location=coyote_creek level\\"

        with self.assertRaises(ApiException) as cm:
            self.write_client.write(_bucket.name, self.org, _record)
        exception = cm.exception

        self.assertEqual(400, exception.status)
        self.assertEqual("Bad Request", exception.reason)
        # assert headers
        self.assertIsNotNone(exception.headers)
        self.assertIsNotNone(exception.headers.get("Content-Length"))
        self.assertIsNotNone(exception.headers.get("Date"))
        self.assertIsNotNone(exception.headers.get("X-Platform-Error-Code"))
        self.assertIn("application/json", exception.headers.get("Content-Type"))
        self.assertTrue(re.compile("^v.*").match(exception.headers.get("X-Influxdb-Version")))
        self.assertEqual("OSS", exception.headers.get("X-Influxdb-Build"))
        # assert body
        b = json.loads(exception.body, object_hook=lambda d: SimpleNamespace(**d))
        self.assertTrue(re.compile("^unable to parse.*invalid field format").match(b.message))

    def test_write_dictionary(self):
        _bucket = self.create_test_bucket()
        _point = {"measurement": "h2o_feet", "tags": {"location": "coyote_creek"},
                  "time": "2009-11-10T23:00:00Z", "fields": {"water_level": 1.0}}

        self.write_client.write(_bucket.name, self.org, _point)

        result = self.query_api.query(
            "from(bucket:\"" + _bucket.name + "\") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()", self.org)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].records[0].get_measurement(), "h2o_feet")
        self.assertEqual(result[0].records[0].get_value(), 1.0)
        self.assertEqual(result[0].records[0].values.get("location"), "coyote_creek")
        self.assertEqual(result[0].records[0].get_field(), "water_level")
        self.assertEqual(result[0].records[0].get_time(),
                         datetime.datetime(2009, 11, 10, 23, 0, tzinfo=datetime.timezone.utc))

        self.delete_test_bucket(_bucket)

    def test_write_bytes(self):
        _bucket = self.create_test_bucket()
        _bytes = "h2o_feet,location=coyote_creek level\\ water_level=1 1".encode("utf-8")

        self.write_client.write(_bucket.name, self.org, _bytes)

        result = self.query_api.query(
            f"from(bucket:\"{_bucket.name}\") |> range(start: 1970-01-01T00:00:00.000000001Z)", self.org)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].records[0].get_measurement(), "h2o_feet")
        self.assertEqual(result[0].records[0].get_value(), 1.0)
        self.assertEqual(result[0].records[0].values.get("location"), "coyote_creek")
        self.assertEqual(result[0].records[0].get_field(), "level water_level")
        self.assertEqual(result[0].records[0].get_time(),
                         datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc))

        self.delete_test_bucket(_bucket)

    def test_write_tuple(self):
        bucket = self.create_test_bucket()

        _record1 = "h2o_feet,location=coyote_creek level\\ water_level=1 1"
        _record2 = "h2o_feet,location=coyote_creek level\\ water_level=2 2"
        _bytes = "h2o_feet,location=coyote_creek level\\ water_level=3 3".encode("utf-8")

        p = (Point("h2o_feet").tag("location", "coyote_creek").field("level water_level", 4.0).time(4))

        tuple = (_record1, _record2, _bytes, (p, ))

        self.write_client = self.client.write_api(write_options=SYNCHRONOUS)
        self.write_client.write(bucket.name, self.org, tuple)

        query = f'from(bucket:"{bucket.name}") |> range(start: 1970-01-01T00:00:00.000000001Z)'

        flux_result = self.client.query_api().query(query)

        self.assertEqual(1, len(flux_result))

        records = flux_result[0].records

        self.assertEqual(4, len(records))

        self.assertEqual("h2o_feet", records[0].get_measurement())
        self.assertEqual(1, records[0].get_value())
        self.assertEqual("level water_level", records[0].get_field())

        self.assertEqual("h2o_feet", records[1].get_measurement())
        self.assertEqual(2, records[1].get_value())
        self.assertEqual("level water_level", records[1].get_field())

        self.assertEqual("h2o_feet", records[2].get_measurement())
        self.assertEqual(3, records[2].get_value())
        self.assertEqual("level water_level", records[2].get_field())

        self.assertEqual("h2o_feet", records[3].get_measurement())
        self.assertEqual(4, records[3].get_value())
        self.assertEqual("level water_level", records[3].get_field())
        self.delete_test_bucket(bucket)

    def test_write_data_frame(self):
        from influxdb_client.extras import pd

        bucket = self.create_test_bucket()

        now = pd.Timestamp('1970-01-01 00:00+00:00')
        data_frame = pd.DataFrame(data=[["coyote_creek", 1], ["coyote_creek", 2]],
                                  index=[now + timedelta(hours=1), now + timedelta(hours=2)],
                                  columns=["location", "water water_level"])

        self.write_client.write(bucket.name, record=data_frame, data_frame_measurement_name='h2o_feet',
                                data_frame_tag_columns=['location'])

        result = self.query_api.query(
            f"from(bucket:\"{bucket.name}\") |> range(start: 1970-01-01T00:00:00.000000001Z)", self.org)

        self.assertEqual(1, len(result))
        self.assertEqual(2, len(result[0].records))

        self.assertEqual(result[0].records[0].get_measurement(), "h2o_feet")
        self.assertEqual(result[0].records[0].get_value(), 1.0)
        self.assertEqual(result[0].records[0].values.get("location"), "coyote_creek")
        self.assertEqual(result[0].records[0].get_field(), "water water_level")
        self.assertEqual(result[0].records[0].get_time(),
                         datetime.datetime(1970, 1, 1, 1, 0, tzinfo=datetime.timezone.utc))

        self.assertEqual(result[0].records[1].get_measurement(), "h2o_feet")
        self.assertEqual(result[0].records[1].get_value(), 2.0)
        self.assertEqual(result[0].records[1].values.get("location"), "coyote_creek")
        self.assertEqual(result[0].records[1].get_field(), "water water_level")
        self.assertEqual(result[0].records[1].get_time(),
                         datetime.datetime(1970, 1, 1, 2, 0, tzinfo=datetime.timezone.utc))

        self.delete_test_bucket(bucket)

    def test_write_data_frame_without_measurement_name(self):
        from influxdb_client.extras import pd

        bucket = self.create_test_bucket()

        now = pd.Timestamp('1970-01-01 00:00+00:00')
        data_frame = pd.DataFrame(data=[["coyote_creek", 1.0], ["coyote_creek", 2.0]],
                                  index=[now + timedelta(hours=1), now + timedelta(hours=2)],
                                  columns=["location", "water_level"])

        with self.assertRaises(TypeError) as cm:
            self.write_client.write(bucket.name, record=data_frame)
        exception = cm.exception

        self.assertEqual('"data_frame_measurement_name" is a Required Argument', exception.__str__())

    def test_use_default_org(self):
        bucket = self.create_test_bucket()

        record = "h2o_feet,location=coyote_creek level\\ water_level=1 1"
        self.write_client.write(bucket.name, record=record)

        result = self.query_api.query(
            f"from(bucket:\"{bucket.name}\") |> range(start: 1970-01-01T00:00:00.000000001Z)", self.org)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].records[0].get_measurement(), "h2o_feet")
        self.assertEqual(result[0].records[0].get_value(), 1.0)
        self.assertEqual(result[0].records[0].get_field(), "level water_level")
        self.assertEqual(result[0].records[0].get_time(),
                         datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc))

    def test_write_empty_data(self):
        bucket = self.create_test_bucket()

        result = self.query_api.query(
            "from(bucket:\"" + bucket.name + "\") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()", self.org)

        self.assertEqual(len(result), 0)

    def test_write_point_different_precision(self):
        bucket = self.create_test_bucket()

        point1 = Point('test_precision') \
            .field('power', 10) \
            .tag('powerFlow', 'low') \
            .time(datetime.datetime(2020, 4, 20, 6, 30, tzinfo=datetime.timezone.utc), WritePrecision.S)

        point2 = Point('test_precision') \
            .field('power', 20) \
            .tag('powerFlow', 'high') \
            .time(datetime.datetime(2020, 4, 20, 5, 30, tzinfo=datetime.timezone.utc), WritePrecision.MS)

        writer = self.client.write_api(write_options=SYNCHRONOUS)
        writer.write(bucket.name, self.org, [point1, point2])

        result = self.query_api.query(
            f"from(bucket:\"{bucket.name}\") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last() "
            "|> sort(columns: [\"_time\"], desc: false)", self.org)

        self.assertEqual(len(result), 2)
        self.assertEqual(len(result[0].records), 1)
        self.assertEqual(len(result[1].records), 1)
        self.assertEqual(result[0].records[0].get_time(),
                         datetime.datetime(2020, 4, 20, 5, 30, tzinfo=datetime.timezone.utc))
        self.assertEqual(result[1].records[0].get_time(),
                         datetime.datetime(2020, 4, 20, 6, 30, tzinfo=datetime.timezone.utc))

    def test_write_point_with_default_tags(self):
        bucket = self.create_test_bucket()

        point = Point("h2o_feet")\
            .field("water_level", 1)\
            .tag("location", "creek level")

        self.write_client.write(bucket.name, self.org, point)

        flux_result = self.client.query_api().query(f'from(bucket:"{bucket.name}") |> range(start: 1970-01-01T00:00:00.000000001Z)')
        self.assertEqual(1, len(flux_result))

        record = flux_result[0].records[0]

        self.assertEqual(self.id_tag, record["id"])
        self.assertEqual(self.customer_tag, record["customer"])
        self.assertEqual("LA", record[self.data_center_key])

    def test_write_list_of_list_point_with_default_tags(self):
        bucket = self.create_test_bucket()

        point = Point("h2o_feet")\
            .field("water_level", 1)\
            .tag("location", "creek level")

        self.write_client.write(bucket.name, self.org, [[point]])

        flux_result = self.client.query_api().query(f'from(bucket:"{bucket.name}") |> range(start: 1970-01-01T00:00:00.000000001Z)')
        self.assertEqual(1, len(flux_result))

        record = flux_result[0].records[0]

        self.assertEqual(self.id_tag, record["id"])
        self.assertEqual(self.customer_tag, record["customer"])
        self.assertEqual("LA", record[self.data_center_key])

    def test_check_write_permission_by_empty_data(self):
        client = InfluxDBClient(url="http://localhost:8086", token="my-token-wrong", org="my-org")
        write_api = client.write_api(write_options=SYNCHRONOUS)

        with self.assertRaises(ApiException) as cm:
            write_api.write("my-bucket", self.org, b'')
        exception = cm.exception

        self.assertEqual(401, exception.status)
        self.assertEqual("Unauthorized", exception.reason)

        client.close()

    def test_write_query_data_nanoseconds(self):

        from influxdb_client.client.util.date_utils_pandas import PandasDateTimeHelper
        import influxdb_client.client.util.date_utils as date_utils

        date_utils.date_helper = PandasDateTimeHelper()

        bucket = self.create_test_bucket()

        point = Point("h2o_feet") \
            .field("water_level", 155) \
            .tag("location", "creek level")\
            .time('1996-02-25T21:20:00.001001231Z')

        self.write_client.write(bucket.name, self.org, [point])

        flux_result = self.client.query_api().query(
            f'from(bucket:"{bucket.name}") |> range(start: 1970-01-01T00:00:00.000000001Z)')
        self.assertEqual(1, len(flux_result))

        record = flux_result[0].records[0]

        self.assertEqual(self.id_tag, record["id"])
        self.assertEqual(record["_value"], 155)
        self.assertEqual(record["location"], "creek level")
        self.assertEqual(record["_time"].year, 1996)
        self.assertEqual(record["_time"].month, 2)
        self.assertEqual(record["_time"].day, 25)
        self.assertEqual(record["_time"].hour, 21)
        self.assertEqual(record["_time"].minute, 20)
        self.assertEqual(record["_time"].second, 00)
        self.assertEqual(record["_time"].microsecond, 1001)
        self.assertEqual(record["_time"].nanosecond, 231)

        date_utils.date_helper = None


class WriteApiTestMock(BaseTest):

    def setUp(self) -> None:
        httpretty.enable()
        httpretty.reset()

        conf = influxdb_client.configuration.Configuration()
        conf.host = "http://localhost"
        conf.debug = False

        self.influxdb_client = InfluxDBClient(url=conf.host, token="my-token")

    def tearDown(self) -> None:
        self.influxdb_client.close()
        httpretty.disable()

    def test_writes_synchronous_without_retry(self):
        httpretty.register_uri(httpretty.POST, uri="http://localhost/api/v2/write", status=503)

        self.write_client = self.influxdb_client.write_api(write_options=SYNCHRONOUS)
        with self.assertRaises(ApiException) as cm:
            self.write_client.write("my-bucket", "my-org", "h2o_feet,location=coyote_creek water_level=1 1")
        exception = cm.exception

        self.assertEqual("Service Unavailable", exception.reason)
        self.assertEqual(1, len(httpretty.httpretty.latest_requests))

    def test_writes_asynchronous_without_retry(self):
        httpretty.register_uri(httpretty.POST, uri="http://localhost/api/v2/write", status=503)

        self.write_client = self.influxdb_client.write_api(write_options=ASYNCHRONOUS)
        with self.assertRaises(ApiException) as cm:
            self.write_client.write("my-bucket", "my-org", "h2o_feet,location=coyote_creek water_level=1 1").get()
        exception = cm.exception

        self.assertEqual("Service Unavailable", exception.reason)
        self.assertEqual(1, len(httpretty.httpretty.latest_requests))

    def test_writes_default_tags_dict_without_tag(self):
        httpretty.register_uri(httpretty.POST, uri="http://localhost/api/v2/write", status=204)

        point_settings = PointSettings(**{"id": "132-987-655", "customer": "California Miner"})
        self.write_client = self.influxdb_client.write_api(write_options=SYNCHRONOUS,
                                                           point_settings=point_settings)

        self.write_client.write("my-bucket", "my-org", {"measurement": "h2o", "fields": {"level": 1.0}, "time": 1})

        requests = httpretty.httpretty.latest_requests
        self.assertEqual(1, len(requests))
        self.assertEqual("h2o,customer=California\\ Miner,id=132-987-655 level=1 1", requests[0].parsed_body)

    def test_redirect(self):
        from urllib3 import Retry
        Retry.DEFAULT_REMOVE_HEADERS_ON_REDIRECT = frozenset()
        Retry.DEFAULT.remove_headers_on_redirect = Retry.DEFAULT_REMOVE_HEADERS_ON_REDIRECT
        self.influxdb_client.close()

        self.influxdb_client = InfluxDBClient(url="http://localhost", token="my-token", org="my-org")

        httpretty.register_uri(httpretty.POST, uri="http://localhost2/api/v2/write", status=204)
        httpretty.register_uri(httpretty.POST, uri="http://localhost/api/v2/write", status=301,
                               adding_headers={'Location': 'http://localhost2/api/v2/write'})

        self.write_client = self.influxdb_client.write_api(write_options=SYNCHRONOUS)

        self.write_client.write("my-bucket", "my-org", {"measurement": "h2o", "fields": {"level": 1.0}, "time": 1})

        requests = httpretty.httpretty.latest_requests
        self.assertEqual(2, len(requests))
        self.assertEqual('Token my-token', requests[0].headers['Authorization'])
        self.assertEqual('Token my-token', requests[1].headers['Authorization'])

        from urllib3 import Retry
        Retry.DEFAULT.remove_headers_on_redirect = Retry.DEFAULT_REMOVE_HEADERS_ON_REDIRECT

    def test_named_tuple(self):
        httpretty.register_uri(httpretty.POST, uri="http://localhost/api/v2/write", status=204)

        self.write_client = self.influxdb_client.write_api(write_options=SYNCHRONOUS)

        Factory = namedtuple('Factory', ['measurement', 'position', 'customers'])
        factory = Factory(measurement='factory', position="central europe", customers=123456)

        self.write_client.write("my-bucket", "my-org", factory,
                                record_measurement_key="measurement",
                                record_tag_keys=["position"],
                                record_field_keys=["customers"])

        requests = httpretty.httpretty.latest_requests
        self.assertEqual(1, len(requests))
        self.assertEqual("factory,position=central\\ europe customers=123456i", requests[0].parsed_body)

    @pytest.mark.skipif(sys.version_info < (3, 8), reason="requires python3.8 or higher")
    def test_data_class(self):
        httpretty.register_uri(httpretty.POST, uri="http://localhost/api/v2/write", status=204)

        self.write_client = self.influxdb_client.write_api(write_options=SYNCHRONOUS)

        from dataclasses import dataclass

        @dataclass
        class Car:
            engine: str
            type: str
            speed: float

        car = Car('12V-BT', 'sport-cars', 125.25)
        self.write_client.write("my-bucket", "my-org",
                                record=car,
                                record_measurement_name="performance",
                                record_tag_keys=["engine", "type"],
                                record_field_keys=["speed"])

        requests = httpretty.httpretty.latest_requests
        self.assertEqual(1, len(requests))
        self.assertEqual("performance,engine=12V-BT,type=sport-cars speed=125.25", requests[0].parsed_body)


class AsynchronousWriteTest(BaseTest):

    def setUp(self) -> None:
        super().setUp()

        os.environ['data_center'] = "LA"

        self.id_tag = "132-987-655"
        self.customer_tag = "California Miner"
        self.data_center_key = "data_center"

        self.write_client = self.client.write_api(write_options=ASYNCHRONOUS,
                                                  point_settings=PointSettings(**{"id": self.id_tag,
                                                                                  "customer": self.customer_tag,
                                                                                  self.data_center_key:
                                                                                      '${env.data_center}'}))

    def tearDown(self) -> None:
        self.write_client.close()
        super().tearDown()

    def test_write_result(self):
        _bucket = self.create_test_bucket()

        _record = "h2o_feet,location=coyote_creek level\\ water_level=1 1"
        result = self.write_client.write(_bucket.name, self.org, _record)

        self.assertEqual(ApplyResult, type(result))
        self.assertEqual(None, result.get())
        self.delete_test_bucket(_bucket)

    def test_write_error(self):
        _bucket = self.create_test_bucket()

        _record = "h2o_feet,location=coyote_creek level\\ water_level="
        result = self.write_client.write(_bucket.name, self.org, _record)

        with self.assertRaises(ApiException) as cm:
            result.get()
        self.assertEqual(400, cm.exception.status)
        self.assertEqual("Bad Request", cm.exception.reason)
        # assert headers
        self.assertIsNotNone(cm.exception.headers)
        self.assertIsNotNone(cm.exception.headers.get("Content-Length"))
        self.assertIsNotNone(cm.exception.headers.get("Date"))
        self.assertIsNotNone(cm.exception.headers.get("X-Platform-Error-Code"))
        self.assertIn("application/json", cm.exception.headers.get("Content-Type"))
        self.assertTrue(re.compile("^v.*").match(cm.exception.headers.get("X-Influxdb-Version")))
        self.assertEqual("OSS", cm.exception.headers.get("X-Influxdb-Build"))
        # assert body
        b = json.loads(cm.exception.body, object_hook=lambda d: SimpleNamespace(**d))
        self.assertTrue(re.compile("^unable to parse.*missing field value").match(b.message))

    def test_write_dictionaries(self):
        bucket = self.create_test_bucket()

        _point1 = {"measurement": "h2o_feet", "tags": {"location": "coyote_creek"},
                   "time": "2009-11-10T22:00:00Z", "fields": {"water_level": 1.0}}
        _point2 = {"measurement": "h2o_feet", "tags": {"location": "coyote_creek"},
                   "time": "2009-11-10T23:00:00Z", "fields": {"water_level": 2.0}}

        _point_list = [_point1, _point2]

        async_result = self.write_client.write(bucket.name, self.org, _point_list)
        async_result.get()

        query = 'from(bucket:"' + bucket.name + '") |> range(start: 1970-01-01T00:00:00.000000001Z)'

        flux_result = self.client.query_api().query(query)

        self.assertEqual(1, len(flux_result))

        records = flux_result[0].records

        self.assertEqual(2, len(records))

        self.assertEqual("h2o_feet", records[0].get_measurement())
        self.assertEqual(1, records[0].get_value())
        self.assertEqual("water_level", records[0].get_field())
        self.assertEqual("coyote_creek", records[0].values.get('location'))
        self.assertEqual(records[0].get_time(),
                         datetime.datetime(2009, 11, 10, 22, 0, tzinfo=datetime.timezone.utc))

        self.assertEqual("h2o_feet", records[1].get_measurement())
        self.assertEqual(2, records[1].get_value())
        self.assertEqual("water_level", records[1].get_field())
        self.assertEqual("coyote_creek", records[1].values.get('location'))
        self.assertEqual(records[1].get_time(),
                         datetime.datetime(2009, 11, 10, 23, 0, tzinfo=datetime.timezone.utc))

        self.delete_test_bucket(bucket)

    def test_use_default_tags_with_dictionaries(self):
        bucket = self.create_test_bucket()

        _point1 = {"measurement": "h2o_feet", "tags": {"location": "coyote_creek"},
                   "time": "2009-11-10T22:00:00Z", "fields": {"water_level": 1.0}}
        _point2 = {"measurement": "h2o_feet", "tags": {"location": "coyote_creek"},
                   "time": "2009-11-10T23:00:00Z", "fields": {"water_level": 2.0}}

        _point_list = [_point1, _point2]

        async_result = self.write_client.write(bucket.name, self.org, _point_list)
        async_result.get()

        query = 'from(bucket:"' + bucket.name + '") |> range(start: 1970-01-01T00:00:00.000000001Z)'

        flux_result = self.client.query_api().query(query)

        self.assertEqual(1, len(flux_result))

        records = flux_result[0].records

        self.assertEqual(2, len(records))

        rec = records[0]
        rec2 = records[1]

        self.assertEqual(self.id_tag, rec["id"])
        self.assertEqual(self.customer_tag, rec["customer"])
        self.assertEqual("LA", rec[self.data_center_key])

        self.assertEqual(self.id_tag, rec2["id"])
        self.assertEqual(self.customer_tag, rec2["customer"])
        self.assertEqual("LA", rec2[self.data_center_key])

        self.delete_test_bucket(bucket)

    def test_use_default_tags_with_data_frame(self):
        from influxdb_client.extras import pd

        bucket = self.create_test_bucket()

        now = pd.Timestamp('1970-01-01 00:00+00:00')
        data_frame = pd.DataFrame(data=[["coyote_creek", 1.0], ["coyote_creek", 2.0]],
                                  index=[now + timedelta(hours=1), now + timedelta(hours=2)],
                                  columns=["location", "water_level"])

        async_result = self.write_client.write(bucket.name, record=data_frame, data_frame_measurement_name='h2o_feet',
                                               data_frame_tag_columns=['location'])
        async_result.get()

        query = 'from(bucket:"' + bucket.name + '") |> range(start: 1970-01-01T00:00:00.000000001Z)'

        flux_result = self.client.query_api().query(query)

        self.assertEqual(1, len(flux_result))

        records = flux_result[0].records

        self.assertEqual(2, len(records))

        rec = records[0]
        rec2 = records[1]

        self.assertEqual(self.id_tag, rec["id"])
        self.assertEqual(self.customer_tag, rec["customer"])
        self.assertEqual("LA", rec[self.data_center_key])

        self.assertEqual(self.id_tag, rec2["id"])
        self.assertEqual(self.customer_tag, rec2["customer"])
        self.assertEqual("LA", rec2[self.data_center_key])

        self.delete_test_bucket(bucket)

    def test_write_bytes(self):
        bucket = self.create_test_bucket()

        _bytes1 = "h2o_feet,location=coyote_creek level\\ water_level=1 1".encode("utf-8")
        _bytes2 = "h2o_feet,location=coyote_creek level\\ water_level=2 2".encode("utf-8")

        _bytes_list = [_bytes1, _bytes2]

        async_result = self.write_client.write(bucket.name, self.org, _bytes_list, write_precision=WritePrecision.S)
        async_result.get()

        query = 'from(bucket:"' + bucket.name + '") |> range(start: 1970-01-01T00:00:00.000000001Z)'

        flux_result = self.client.query_api().query(query)

        self.assertEqual(1, len(flux_result))

        records = flux_result[0].records

        self.assertEqual(2, len(records))

        self.assertEqual("h2o_feet", records[0].get_measurement())
        self.assertEqual(1, records[0].get_value())
        self.assertEqual("level water_level", records[0].get_field())
        self.assertEqual("coyote_creek", records[0].values.get('location'))
        self.assertEqual(records[0].get_time(),
                         datetime.datetime(1970, 1, 1, 0, 0, 1, tzinfo=datetime.timezone.utc))

        self.assertEqual("h2o_feet", records[1].get_measurement())
        self.assertEqual(2, records[1].get_value())
        self.assertEqual("level water_level", records[1].get_field())
        self.assertEqual("coyote_creek", records[1].values.get('location'))
        self.assertEqual(records[1].get_time(),
                         datetime.datetime(1970, 1, 1, 0, 0, 2, tzinfo=datetime.timezone.utc))

        self.delete_test_bucket(bucket)

    def test_write_point_different_precision(self):
        bucket = self.create_test_bucket()

        _point1 = Point("h2o_feet").tag("location", "coyote_creek").field("level water_level", 5.0).time(5,
                                                                                                         WritePrecision.S)
        _point2 = Point("h2o_feet").tag("location", "coyote_creek").field("level water_level", 6.0).time(6,
                                                                                                         WritePrecision.US)

        _point_list = [_point1, _point2]

        async_results = self.write_client.write(bucket.name, self.org, _point_list)
        self.assertEqual(2, len(async_results))
        for async_result in async_results:
            async_result.get()

        query = f'from(bucket:"{bucket.name}") |> range(start: 1970-01-01T00:00:00.000000001Z) '\
                '|> sort(columns: [\"_time\"], desc: false)'

        flux_result = self.client.query_api().query(query)

        self.assertEqual(1, len(flux_result))

        records = flux_result[0].records

        self.assertEqual(2, len(records))
        self.assertEqual(records[0].get_time(),
                         datetime.datetime(1970, 1, 1, 0, 0, 0, 6, tzinfo=datetime.timezone.utc))
        self.assertEqual(records[1].get_time(),
                         datetime.datetime(1970, 1, 1, 0, 0, 5, tzinfo=datetime.timezone.utc))



class PointSettingTest(BaseTest):

    def setUp(self) -> None:
        super().setUp()
        self.id_tag = "132-987-655"
        self.customer_tag = "California Miner"

    def tearDown(self) -> None:
        self.write_client.close()
        super().tearDown()

    def test_point_settings(self):
        self.write_client = self.client.write_api(write_options=SYNCHRONOUS,
                                                  point_settings=PointSettings(**{"id": self.id_tag,
                                                                                  "customer": self.customer_tag}))

        default_tags = self.write_client._point_settings.defaultTags

        self.assertEqual(self.id_tag, default_tags.get("id"))
        self.assertEqual(self.customer_tag, default_tags.get("customer"))

    def test_point_settings_with_add(self):
        os.environ['data_center'] = "LA"

        point_settings = PointSettings()
        point_settings.add_default_tag("id", self.id_tag)
        point_settings.add_default_tag("customer", self.customer_tag)
        point_settings.add_default_tag("data_center", "${env.data_center}")

        self.write_client = self.client.write_api(write_options=SYNCHRONOUS, point_settings=point_settings)

        default_tags = self.write_client._point_settings.defaultTags

        self.assertEqual(self.id_tag, default_tags.get("id"))
        self.assertEqual(self.customer_tag, default_tags.get("customer"))
        self.assertEqual("LA", default_tags.get("data_center"))


class DefaultTagsConfiguration(BaseTest):

    def setUp(self) -> None:
        super().setUp()

        os.environ['data_center'] = "LA"

        self.id_tag = "132-987-655"
        self.customer_tag = "California Miner"
        self.data_center_key = "data_center"

        os.environ['INFLUXDB_V2_TOKEN'] = "my-token"
        os.environ['INFLUXDB_V2_TIMEOUT'] = "6000"
        os.environ['INFLUXDB_V2_ORG'] = "my-org"

        os.environ['INFLUXDB_V2_TAG_ID'] = self.id_tag
        os.environ['INFLUXDB_V2_TAG_CUSTOMER'] = self.customer_tag
        os.environ['INFLUXDB_V2_TAG_DATA_CENTER'] = "${env.data_center}"

    def tearDown(self) -> None:
        self.write_client.close()
        super().tearDown()

    def test_connection_option_from_conf_file(self):
        self.client.close()
        self.client = InfluxDBClient.from_config_file(self._path_to_config(), self.debug)

        self.assertEqual("http://localhost:8086", self.client.url)
        self._check_connection_settings()

    def test_connection_option_from_env(self):
        self.client.close()
        self.client = InfluxDBClient.from_env_properties(self.debug)

        self.assertEqual("http://localhost:8086", self.client.url)
        self._check_connection_settings()

    def _check_connection_settings(self):
        self.write_client = self.client.write_api(write_options=SYNCHRONOUS)

        self.assertEqual("my-org", self.client.org)
        self.assertEqual("my-token", self.client.token)
        self.assertEqual(6000, self.client.api_client.configuration.timeout)

    def test_default_tags_from_conf_file(self):
        self.client.close()
        self.client = InfluxDBClient.from_config_file(self._path_to_config(), self.debug)

        self._write_point()

    def test_default_tags_from_env(self):
        self.client.close()
        self.client = InfluxDBClient.from_env_properties(self.debug)

        self._write_point()

    def _write_point(self):
        self.write_client = self.client.write_api(write_options=SYNCHRONOUS)

        bucket = self.create_test_bucket()

        measurement = "h2o_feet"
        field_name = "water_level"
        val = "1.0"
        tag = "location"
        tag_value = "creek level"

        p = Point(measurement)
        p.field(field_name, val)
        p.tag(tag, tag_value)

        record_list = [p]

        self.write_client.write(bucket.name, self.org, record_list)

        query = 'from(bucket:"' + bucket.name + '") |> range(start: 1970-01-01T00:00:00.000000001Z)'
        flux_result = self.client.query_api().query(query)
        self.assertEqual(1, len(flux_result))
        rec = flux_result[0].records[0]

        self.assertEqual(self.id_tag, rec["id"])
        self.assertEqual(self.customer_tag, rec["customer"])
        self.assertEqual("LA", rec[self.data_center_key])

        self.delete_test_bucket(bucket)

    @staticmethod
    def _path_to_config():
        return os.path.dirname(os.path.realpath(__file__)) + "/config.ini"


if __name__ == '__main__':
    unittest.main()
