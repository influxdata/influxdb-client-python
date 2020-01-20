# coding: utf-8

from __future__ import absolute_import

import datetime
import os
import unittest
import time
from multiprocessing.pool import ApplyResult

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
        self.write_client.__del__()
        super().tearDown()

    def test_write_line_protocol(self):
        bucket = self.create_test_bucket()

        record = "h2o_feet,location=coyote_creek level\\ water_level=1.0 1"
        self.write_client.write(bucket.name, self.org, record)

        result = self.query_api.query(
            "from(bucket:\"" + bucket.name + "\") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()", self.org)

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

        _record1 = "h2o_feet,location=coyote_creek level\\ water_level=1.0 1"
        _record2 = "h2o_feet,location=coyote_creek level\\ water_level=2.0 2"

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

        _record = "h2o_feet,location=coyote_creek level\\ water_level=1.0 1"
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
        _bytes = "h2o_feet,location=coyote_creek level\\ water_level=1.0 1".encode("utf-8")

        self.write_client.write(_bucket.name, self.org, _bytes)

        result = self.query_api.query(
            "from(bucket:\"" + _bucket.name + "\") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()", self.org)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].records[0].get_measurement(), "h2o_feet")
        self.assertEqual(result[0].records[0].get_value(), 1.0)
        self.assertEqual(result[0].records[0].values.get("location"), "coyote_creek")
        self.assertEqual(result[0].records[0].get_field(), "level water_level")
        self.assertEqual(result[0].records[0].get_time(),
                         datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc))

        self.delete_test_bucket(_bucket)

    def test_use_default_org(self):
        bucket = self.create_test_bucket()

        record = "h2o_feet,location=coyote_creek level\\ water_level=1.0 1"
        self.write_client.write(bucket.name, record=record)

        result = self.query_api.query(
            "from(bucket:\"" + bucket.name + "\") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()", self.org)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].records[0].get_measurement(), "h2o_feet")
        self.assertEqual(result[0].records[0].get_value(), 1.0)
        self.assertEqual(result[0].records[0].get_field(), "level water_level")
        self.assertEqual(result[0].records[0].get_time(),
                         datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc))

    def test_write_empty_data(self):
        bucket = self.create_test_bucket()

        with self.assertRaises(ApiException) as cm:
            self.write_client.write(bucket.name)
        exception = cm.exception

        self.assertEqual(400, exception.status)
        self.assertEqual("Bad Request", exception.reason)

        result = self.query_api.query(
            "from(bucket:\"" + bucket.name + "\") |> range(start: 1970-01-01T00:00:00.000000001Z) |> last()", self.org)

        self.assertEqual(len(result), 0)


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
        self.write_client.__del__()
        super().tearDown()

    def test_write_result(self):
        _bucket = self.create_test_bucket()

        _record = "h2o_feet,location=coyote_creek level\\ water_level=1.0 1"
        result = self.write_client.write(_bucket.name, self.org, _record)

        self.assertEqual(ApplyResult, type(result))
        self.assertEqual(None, result.get())
        self.delete_test_bucket(_bucket)

    def test_write_dictionaries(self):
        bucket = self.create_test_bucket()

        _point1 = {"measurement": "h2o_feet", "tags": {"location": "coyote_creek"},
                   "time": "2009-11-10T22:00:00Z", "fields": {"water_level": 1.0}}
        _point2 = {"measurement": "h2o_feet", "tags": {"location": "coyote_creek"},
                   "time": "2009-11-10T23:00:00Z", "fields": {"water_level": 2.0}}

        _point_list = [_point1, _point2]

        self.write_client.write(bucket.name, self.org, _point_list)
        time.sleep(1)

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

        self.write_client.write(bucket.name, self.org, _point_list)
        time.sleep(1)

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

        _bytes1 = "h2o_feet,location=coyote_creek level\\ water_level=1.0 1".encode("utf-8")
        _bytes2 = "h2o_feet,location=coyote_creek level\\ water_level=2.0 2".encode("utf-8")

        _bytes_list = [_bytes1, _bytes2]

        self.write_client.write(bucket.name, self.org, _bytes_list, write_precision=WritePrecision.S)
        time.sleep(1)

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


class PointSettingTest(BaseTest):

    def setUp(self) -> None:
        super().setUp()
        self.id_tag = "132-987-655"
        self.customer_tag = "California Miner"

    def tearDown(self) -> None:
        self.write_client.__del__()
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
        self.write_client.__del__()
        super().tearDown()

    def test_connection_option_from_conf_file(self):
        self.client.close()
        self.client = InfluxDBClient.from_config_file(self._path_to_config(), self.debug)

        self.assertEqual("http://localhost:9999", self.client.url)
        self._check_connection_settings()

    def test_connection_option_from_env(self):
        self.client.close()
        self.client = InfluxDBClient.from_env_properties(self.debug)

        self.assertEqual("http://localhost:9999", self.client.url)
        self._check_connection_settings()

    def _check_connection_settings(self):
        self.write_client = self.client.write_api(write_options=SYNCHRONOUS)

        self.assertEqual("my-org", self.client.org)
        self.assertEqual("my-token", self.client.token)
        self.assertEqual(6000, self.client.timeout)

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
