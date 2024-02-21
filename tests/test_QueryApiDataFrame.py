import random

import httpretty
import pytest
import reactivex as rx
import pandas
import warnings

from pandas import DataFrame
from pandas._libs.tslibs.timestamps import Timestamp
from reactivex import operators as ops

from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
from influxdb_client.client.warnings import MissingPivotFunction
from influxdb_client.rest import ApiException
from tests.base_test import BaseTest, current_milli_time


class QueryDataFrameApi(BaseTest):

    def setUp(self) -> None:
        super(QueryDataFrameApi, self).setUp()

        httpretty.enable()
        httpretty.reset()

    def tearDown(self) -> None:
        self.client.close()
        httpretty.disable()

    def test_one_table(self):
        query_response = \
            '#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string\n' \
            '#group,false,false,true,true,false,false,true,true,true\n' \
            '#default,_result,,,,,,,,\n' \
            ',result,table,_start,_stop,_time,_value,_field,_measurement,host\n' \
            ',,0,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:05Z,11125907456,used,mem,mac.local\n' \
            ',,0,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:06Z,11127103488,used,mem,mac.local\n' \
            ',,0,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:07Z,11127291904,used,mem,mac.local\n' \
            ',,0,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:08Z,11126190080,used,mem,mac.local\n' \
            ',,0,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:09Z,11127832576,used,mem,mac.local\n' \
            '\n\n'

        httpretty.register_uri(httpretty.POST, uri="http://localhost/api/v2/query", status=200, body=query_response)

        self.client = InfluxDBClient("http://localhost", "my-token", org="my-org", enable_gzip=False)

        _dataFrame = self.client.query_api().query_data_frame(
            'from(bucket: "my-bucket") '
            '|> range(start: -5s, stop: now()) '
            '|> filter(fn: (r) => r._measurement == "mem") '
            '|> filter(fn: (r) => r._field == "used")',
            "my-org")

        self.assertEqual(DataFrame, type(_dataFrame))
        self.assertListEqual(
            ["result", "table", "_start", "_stop", "_time", "_value", "_field", "_measurement", "host"],
            list(_dataFrame.columns))
        self.assertListEqual([0, 1, 2, 3, 4], list(_dataFrame.index))
        self.assertEqual(5, len(_dataFrame))
        self.assertEqual("_result", _dataFrame['result'][0])
        self.assertEqual("_result", _dataFrame['result'][1])
        self.assertEqual("_result", _dataFrame['result'][2])
        self.assertEqual("_result", _dataFrame['result'][3])
        self.assertEqual("_result", _dataFrame['result'][4])
        self.assertEqual(0, _dataFrame['table'][0], None)
        self.assertEqual(0, _dataFrame['table'][1], None)
        self.assertEqual(0, _dataFrame['table'][2], None)
        self.assertEqual(0, _dataFrame['table'][3], None)
        self.assertEqual(0, _dataFrame['table'][4], None)
        self.assertEqual(Timestamp('2019-11-12 08:09:04.795385+0000'), _dataFrame['_start'][0])
        self.assertEqual(Timestamp('2019-11-12 08:09:04.795385+0000'), _dataFrame['_start'][1])
        self.assertEqual(Timestamp('2019-11-12 08:09:04.795385+0000'), _dataFrame['_start'][2])
        self.assertEqual(Timestamp('2019-11-12 08:09:04.795385+0000'), _dataFrame['_start'][3])
        self.assertEqual(Timestamp('2019-11-12 08:09:04.795385+0000'), _dataFrame['_start'][4])
        self.assertEqual(Timestamp('2019-11-12 08:09:09.795385+0000'), _dataFrame['_stop'][0])
        self.assertEqual(Timestamp('2019-11-12 08:09:09.795385+0000'), _dataFrame['_stop'][1])
        self.assertEqual(Timestamp('2019-11-12 08:09:09.795385+0000'), _dataFrame['_stop'][2])
        self.assertEqual(Timestamp('2019-11-12 08:09:09.795385+0000'), _dataFrame['_stop'][3])
        self.assertEqual(Timestamp('2019-11-12 08:09:09.795385+0000'), _dataFrame['_stop'][4])
        self.assertEqual(Timestamp('2019-11-12 08:09:05+0000'), _dataFrame['_time'][0])
        self.assertEqual(Timestamp('2019-11-12 08:09:06+0000'), _dataFrame['_time'][1])
        self.assertEqual(Timestamp('2019-11-12 08:09:07+0000'), _dataFrame['_time'][2])
        self.assertEqual(Timestamp('2019-11-12 08:09:08+0000'), _dataFrame['_time'][3])
        self.assertEqual(Timestamp('2019-11-12 08:09:09+0000'), _dataFrame['_time'][4])
        self.assertEqual(11125907456, _dataFrame['_value'][0])
        self.assertEqual(11127103488, _dataFrame['_value'][1])
        self.assertEqual(11127291904, _dataFrame['_value'][2])
        self.assertEqual(11126190080, _dataFrame['_value'][3])
        self.assertEqual(11127832576, _dataFrame['_value'][4])
        self.assertEqual('used', _dataFrame['_field'][0])
        self.assertEqual('used', _dataFrame['_field'][1])
        self.assertEqual('used', _dataFrame['_field'][2])
        self.assertEqual('used', _dataFrame['_field'][3])
        self.assertEqual('used', _dataFrame['_field'][4])
        self.assertEqual('mem', _dataFrame['_measurement'][0])
        self.assertEqual('mem', _dataFrame['_measurement'][1])
        self.assertEqual('mem', _dataFrame['_measurement'][2])
        self.assertEqual('mem', _dataFrame['_measurement'][3])
        self.assertEqual('mem', _dataFrame['_measurement'][4])
        self.assertEqual('mac.local', _dataFrame['host'][0])
        self.assertEqual('mac.local', _dataFrame['host'][1])
        self.assertEqual('mac.local', _dataFrame['host'][2])
        self.assertEqual('mac.local', _dataFrame['host'][3])
        self.assertEqual('mac.local', _dataFrame['host'][4])

    def test_more_table(self):
        query_response = \
            '#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string\n' \
            '#group,false,false,true,true,false,false,true,true,true\n' \
            '#default,_result,,,,,,,,\n' \
            ',result,table,_start,_stop,_time,_value,_field,_measurement,host\n' \
            ',,0,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:05Z,11125907456,used,mem,mac.local\n' \
            ',,0,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:06Z,11127103488,used,mem,mac.local\n' \
            ',,0,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:07Z,11127291904,used,mem,mac.local\n' \
            ',,0,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:08Z,11126190080,used,mem,mac.local\n' \
            ',,0,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:09Z,11127832576,used,mem,mac.local\n' \
            '\n\n' \
            '#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string\n' \
            '#group,false,false,true,true,false,false,true,true,true\n' \
            '#default,_result,,,,,,,,\n' \
            ',result,table,_start,_stop,_time,_value,_field,_measurement,host\n' \
            ',,1,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:05Z,6053961728,available,mem,mac.local\n' \
            ',,1,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:06Z,6052765696,available,mem,mac.local\n' \
            ',,1,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:07Z,6052577280,available,mem,mac.local\n' \
            ',,1,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:08Z,6053679104,available,mem,mac.local\n' \
            ',,1,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:09Z,6052036608,available,mem,mac.local\n' \
            '\n\n' \
            '#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string\n' \
            '#group,false,false,true,true,false,false,true,true,true\n' \
            '#default,_result,,,,,,,,\n' \
            ',result,table,_start,_stop,_time,_value,_field,_measurement,host\n' \
            ',,2,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:05Z,18632704,free,mem,mac.local\n' \
            ',,2,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:06Z,17420288,free,mem,mac.local\n' \
            ',,2,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:07Z,17256448,free,mem,mac.local\n' \
            ',,2,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:08Z,18362368,free,mem,mac.local\n' \
            ',,2,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:09Z,16723968,free,mem,mac.local\n\n'

        httpretty.register_uri(httpretty.POST, uri="http://localhost/api/v2/query", status=200, body=query_response)

        self.client = InfluxDBClient("http://localhost", "my-token", org="my-org", enable_gzip=False)

        _dataFrames = self.client.query_api().query_data_frame(
            'from(bucket: "my-bucket") '
            '|> range(start: -5s, stop: now()) '
            '|> filter(fn: (r) => r._measurement == "mem") '
            '|> filter(fn: (r) => r._field == "available" or r._field == "free" or r._field == "used")',
            "my-org")

        self.assertEqual(list, type(_dataFrames))
        self.assertEqual(len(_dataFrames), 3)

        self.assertListEqual(
            ["result", "table", "_start", "_stop", "_time", "_value", "_field", "_measurement", "host"],
            list(_dataFrames[0].columns))
        self.assertListEqual([0, 1, 2, 3, 4], list(_dataFrames[0].index))
        self.assertEqual(5, len(_dataFrames[0]))

        self.assertListEqual(
            ["result", "table", "_start", "_stop", "_time", "_value", "_field", "_measurement", "host"],
            list(_dataFrames[1].columns))
        self.assertListEqual([0, 1, 2, 3, 4], list(_dataFrames[1].index))
        self.assertEqual(5, len(_dataFrames[1]))

        self.assertListEqual(
            ["result", "table", "_start", "_stop", "_time", "_value", "_field", "_measurement", "host"],
            list(_dataFrames[2].columns))
        self.assertListEqual([0, 1, 2, 3, 4], list(_dataFrames[2].index))
        self.assertEqual(5, len(_dataFrames[2]))

    def test_empty_data_set(self):
        query_response = '\n'

        httpretty.register_uri(httpretty.POST, uri="http://localhost/api/v2/query", status=200, body=query_response)

        self.client = InfluxDBClient("http://localhost", "my-token", org="my-org", enable_gzip=False)

        _dataFrame = self.client.query_api().query_data_frame(
            'from(bucket: "my-bucket") '
            '|> range(start: -5s, stop: now()) '
            '|> filter(fn: (r) => r._measurement == "mem") '
            '|> filter(fn: (r) => r._field == "not_exit")',
            "my-org")

        self.assertEqual(DataFrame, type(_dataFrame))
        self.assertListEqual([], list(_dataFrame.columns))
        self.assertListEqual([], list(_dataFrame.index))

    def test_more_table_custom_index(self):
        query_response = \
            '#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string\n' \
            '#group,false,false,true,true,false,false,true,true,true\n' \
            '#default,_result,,,,,,,,\n' \
            ',result,table,_start,_stop,_time,_value,_field,_measurement,host\n' \
            ',,0,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:05Z,11125907456,used,mem,mac.local\n' \
            ',,0,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:06Z,11127103488,used,mem,mac.local\n' \
            ',,0,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:07Z,11127291904,used,mem,mac.local\n' \
            ',,0,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:08Z,11126190080,used,mem,mac.local\n' \
            ',,0,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:09Z,11127832576,used,mem,mac.local\n' \
            '\n\n' \
            '#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string\n' \
            '#group,false,false,true,true,false,false,true,true,true\n' \
            '#default,_result,,,,,,,,\n' \
            ',result,table,_start,_stop,_time,_value,_field,_measurement,host\n' \
            ',,1,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:05Z,6053961728,available,mem,mac.local\n' \
            ',,1,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:06Z,6052765696,available,mem,mac.local\n' \
            ',,1,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:07Z,6052577280,available,mem,mac.local\n' \
            ',,1,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:08Z,6053679104,available,mem,mac.local\n' \
            ',,1,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:09Z,6052036608,available,mem,mac.local\n' \
            '\n\n' \
            '#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string\n' \
            '#group,false,false,true,true,false,false,true,true,true\n' \
            '#default,_result,,,,,,,,\n' \
            ',result,table,_start,_stop,_time,_value,_field,_measurement,host\n' \
            ',,2,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:05Z,18632704,free,mem,mac.local\n' \
            ',,2,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:06Z,17420288,free,mem,mac.local\n' \
            ',,2,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:07Z,17256448,free,mem,mac.local\n' \
            ',,2,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:08Z,18362368,free,mem,mac.local\n' \
            ',,2,2019-11-12T08:09:04.795385031Z,2019-11-12T08:09:09.795385031Z,2019-11-12T08:09:09Z,16723968,free,mem,mac.local\n\n'

        httpretty.register_uri(httpretty.POST, uri="http://localhost/api/v2/query", status=200, body=query_response)

        self.client = InfluxDBClient("http://localhost", "my-token", org="my-org", enable_gzip=False)

        _dataFrames = self.client.query_api().query_data_frame(
            'from(bucket: "my-bucket") '
            '|> range(start: -5s, stop: now()) '
            '|> filter(fn: (r) => r._measurement == "mem") '
            '|> filter(fn: (r) => r._field == "available" or r._field == "free" or r._field == "used")',
            "my-org", data_frame_index=["_time"])

        self.assertEqual(list, type(_dataFrames))
        self.assertEqual(len(_dataFrames), 3)

        print(_dataFrames[0].to_string())
        self.assertListEqual(
            ["result", "table", "_start", "_stop", "_value", "_field", "_measurement", "host"],
            list(_dataFrames[0].columns))
        self.assertListEqual([Timestamp('2019-11-12 08:09:05+0000'), Timestamp('2019-11-12 08:09:06+0000'),
                              Timestamp('2019-11-12 08:09:07+0000'), Timestamp('2019-11-12 08:09:08+0000'),
                              Timestamp('2019-11-12 08:09:09+0000')], list(_dataFrames[0].index))
        self.assertEqual(5, len(_dataFrames[0]))

        self.assertListEqual(
            ["result", "table", "_start", "_stop", "_value", "_field", "_measurement", "host"],
            list(_dataFrames[1].columns))
        self.assertListEqual([Timestamp('2019-11-12 08:09:05+0000'), Timestamp('2019-11-12 08:09:06+0000'),
                              Timestamp('2019-11-12 08:09:07+0000'), Timestamp('2019-11-12 08:09:08+0000'),
                              Timestamp('2019-11-12 08:09:09+0000')], list(_dataFrames[1].index))
        self.assertEqual(5, len(_dataFrames[1]))

        self.assertListEqual(
            ["result", "table", "_start", "_stop", "_value", "_field", "_measurement", "host"],
            list(_dataFrames[2].columns))
        self.assertListEqual([Timestamp('2019-11-12 08:09:05+0000'), Timestamp('2019-11-12 08:09:06+0000'),
                              Timestamp('2019-11-12 08:09:07+0000'), Timestamp('2019-11-12 08:09:08+0000'),
                              Timestamp('2019-11-12 08:09:09+0000')], list(_dataFrames[2].index))
        self.assertEqual(5, len(_dataFrames[2]))

    def test_query_with_warning(self):
        httpretty.register_uri(httpretty.POST, uri="http://localhost/api/v2/query", status=200, body='\n')

        self.client = InfluxDBClient("http://localhost", "my-token", org="my-org", enable_gzip=False)

        with pytest.warns(MissingPivotFunction) as warnings:
            self.client.query_api().query_data_frame(
                'from(bucket: "my-bucket")'
                '|> range(start: -5s, stop: now()) '
                '|> filter(fn: (r) => r._measurement == "mem") '
                "my-org")
        self.assertEqual(1, len([w for w in warnings if w.category == MissingPivotFunction]))

    def test_query_without_warning(self):
        httpretty.register_uri(httpretty.POST, uri="http://localhost/api/v2/query", status=200, body='\n')

        self.client = InfluxDBClient("http://localhost", "my-token", org="my-org", enable_gzip=False)

        with warnings.catch_warnings(record=True) as warns:
            self.client.query_api().query_data_frame(
                'import "influxdata/influxdb/schema"'
                ''
                'from(bucket: "my-bucket")'
                '|> range(start: -5s, stop: now()) '
                '|> filter(fn: (r) => r._measurement == "mem") '
                '|> schema.fieldsAsCols() '
                "my-org")
        self.assertEqual(0, len([w for w in warns if w.category == MissingPivotFunction]))

        with warnings.catch_warnings(record=True) as warns:
            self.client.query_api().query_data_frame(
                'from(bucket: "my-bucket")'
                '|> range(start: -5s, stop: now()) '
                '|> filter(fn: (r) => r._measurement == "mem") '
                '|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")'
                "my-org")
        self.assertEqual(0, len([w for w in warns if w.category == MissingPivotFunction]))

    def test_pivoted_data(self):
        query_response = \
            '#group,false,false,true,true,false,true,false,false,false,false\n' \
            '#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,double,long,string,boolean\n' \
            '#default,_result,,,,,,,,,\n' \
            ',result,table,_start,_stop,_time,_measurement,test_double,test_long,test_string,test_boolean\n' \
            ',,0,2023-12-15T13:19:45Z,2023-12-15T13:20:00Z,2023-12-15T13:19:55Z,test,4,,,\n' \
            ',,0,2023-12-15T13:19:45Z,2023-12-15T13:20:00Z,2023-12-15T13:19:56Z,test,,1,,\n' \
            ',,0,2023-12-15T13:19:45Z,2023-12-15T13:20:00Z,2023-12-15T13:19:57Z,test,,,hi,\n' \
            ',,0,2023-12-15T13:19:45Z,2023-12-15T13:20:00Z,2023-12-15T13:19:58Z,test,,,,true\n' \
            '\n\n'

        httpretty.register_uri(httpretty.POST, uri="http://localhost/api/v2/query", status=200, body=query_response)

        self.client = InfluxDBClient("http://localhost", "my-token", org="my-org", enable_gzip=False)

        _dataFrame = self.client.query_api().query_data_frame(
            'from(bucket: "my-bucket") '
            '|> range(start: 2023-12-15T13:19:45Z, stop: 2023-12-15T13:20:00Z)'
            '|> filter(fn: (r) => r["_measurement"] == "test")'
            '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'
            "my-org", use_extension_dtypes=True)

        self.assertEqual(DataFrame, type(_dataFrame))
        self.assertListEqual(
            ["result", "table", "_start", "_stop", "_time", "_measurement",
             "test_double", "test_long", "test_string", "test_boolean"],
            list(_dataFrame.columns))
        self.assertListEqual([0, 1, 2, 3], list(_dataFrame.index))
        # self.assertEqual('Int64', _dataFrame.dtypes['test_long'].name)
        # self.assertEqual('Float64', _dataFrame.dtypes['test_double'].name)
        self.assertEqual('string', _dataFrame.dtypes['test_string'].name)
        self.assertEqual('boolean', _dataFrame.dtypes['test_boolean'].name)
        self.assertEqual(4, len(_dataFrame))
        self.assertEqual("_result", _dataFrame['result'][0])
        self.assertEqual("_result", _dataFrame['result'][1])
        self.assertEqual("_result", _dataFrame['result'][2])
        self.assertEqual("_result", _dataFrame['result'][3])
        self.assertEqual(0, _dataFrame['table'][0], None)
        self.assertEqual(0, _dataFrame['table'][1], None)
        self.assertEqual(0, _dataFrame['table'][2], None)
        self.assertEqual(0, _dataFrame['table'][3], None)
        self.assertEqual(Timestamp('2023-12-15 13:19:45+0000'), _dataFrame['_start'][0])
        self.assertEqual(Timestamp('2023-12-15 13:19:45+0000'), _dataFrame['_start'][1])
        self.assertEqual(Timestamp('2023-12-15 13:19:45+0000'), _dataFrame['_start'][2])
        self.assertEqual(Timestamp('2023-12-15 13:19:45+0000'), _dataFrame['_start'][3])
        self.assertEqual(Timestamp('2023-12-15 13:20:00+0000'), _dataFrame['_stop'][0])
        self.assertEqual(Timestamp('2023-12-15 13:20:00+0000'), _dataFrame['_stop'][1])
        self.assertEqual(Timestamp('2023-12-15 13:20:00+0000'), _dataFrame['_stop'][2])
        self.assertEqual(Timestamp('2023-12-15 13:20:00+0000'), _dataFrame['_stop'][3])
        self.assertEqual(Timestamp('2023-12-15 13:19:55+0000'), _dataFrame['_time'][0])
        self.assertEqual(Timestamp('2023-12-15 13:19:56+0000'), _dataFrame['_time'][1])
        self.assertEqual(Timestamp('2023-12-15 13:19:57+0000'), _dataFrame['_time'][2])
        self.assertEqual(Timestamp('2023-12-15 13:19:58+0000'), _dataFrame['_time'][3])
        self.assertEqual(4, _dataFrame['test_double'][0])
        self.assertTrue(pandas.isna(_dataFrame['test_double'][1]))
        self.assertTrue(pandas.isna(_dataFrame['test_double'][2]))
        self.assertTrue(pandas.isna(_dataFrame['test_double'][3]))
        self.assertTrue(pandas.isna(_dataFrame['test_long'][0]))
        self.assertEqual(1, _dataFrame['test_long'][1])
        self.assertTrue(pandas.isna(_dataFrame['test_long'][2]))
        self.assertTrue(pandas.isna(_dataFrame['test_long'][3]))
        self.assertTrue(pandas.isna(_dataFrame['test_string'][0]))
        self.assertTrue(pandas.isna(_dataFrame['test_string'][1]))
        self.assertEqual('hi', _dataFrame['test_string'][2])
        self.assertTrue(pandas.isna(_dataFrame['test_string'][3]))
        self.assertTrue(pandas.isna(_dataFrame['test_boolean'][0]))
        self.assertTrue(pandas.isna(_dataFrame['test_boolean'][1]))
        self.assertTrue(pandas.isna(_dataFrame['test_boolean'][2]))
        self.assertEqual(True, _dataFrame['test_boolean'][3])


class QueryDataFrameIntegrationApi(BaseTest):

    def test_large_amount_of_data(self):
        _measurement_name = "data_frame_" + str(current_milli_time())

        def _create_point(index) -> Point:
            return Point(_measurement_name) \
                .tag("deviceType", str(random.choice(['A', 'B']))) \
                .tag("name", random.choice(['A', 'B'])) \
                .field("uuid", random.randint(0, 10_000)) \
                .field("co2", random.randint(0, 10_000)) \
                .field("humid", random.randint(0, 10_000)) \
                .field("lux", random.randint(0, 10_000)) \
                .field("water", random.randint(0, 10_000)) \
                .field("shine", random.randint(0, 10_000)) \
                .field("temp", random.randint(0, 10_000)) \
                .field("voc", random.randint(0, 10_000)) \
                .time(time=(1583828781 + index), write_precision=WritePrecision.S)

        data = rx.range(0, 2_000).pipe(ops.map(lambda index: _create_point(index)))

        write_api = self.client.write_api(write_options=WriteOptions(batch_size=500))
        write_api.write(org="my-org", bucket="my-bucket", record=data, write_precision=WritePrecision.S)
        write_api.close()

        query = 'from(bucket: "my-bucket")' \
                '|> range(start: 2020-02-19T23:30:00Z, stop: now())' \
                f'|> filter(fn: (r) => r._measurement == "{_measurement_name}")'

        result = self.client.query_api().query_data_frame(org="my-org", query=query)

        self.assertGreater(len(result), 1)

    def test_query_without_credentials(self):
        _client = InfluxDBClient(url="http://localhost:8086", token="my-token-wrong-credentials", org="my-org")

        with self.assertRaises(ApiException) as ae:
            query = 'from(bucket: "my-bucket")' \
                    '|> range(start: 2020-02-19T23:30:00Z, stop: now())' \
                    f'|> filter(fn: (r) => r._measurement == "my-measurement")'
            _client.query_api().query_data_frame(query=query)

        exception = ae.exception
        self.assertEqual(401, exception.status)
        self.assertEqual("Unauthorized", exception.reason)

        _client.close()
