import itertools
import time
import types

from influxdb_client import WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from tests.base_test import BaseTest


class QueryStreamApi(BaseTest):

    def setUp(self) -> None:
        super().setUp()
        self.write_client = self.client.write_api(write_options=SYNCHRONOUS)
        self.bucket = self.create_test_bucket()

    def tearDown(self) -> None:
        self.write_client.close()
        super().tearDown()

    def test_block(self):
        self._prepareData()

        _result = self.query_api.query(
            f'from(bucket:"{self.bucket.name}") |> range(start: 1970-01-01T00:00:00.000000001Z)', self.org)

        self.assertEqual(len(_result), 1)
        self.assertEqual(len(_result[0].records), 100)

    def test_stream(self):
        self._prepareData()

        _result = self.query_api.query_stream(
            f'from(bucket:"{self.bucket.name}") |> range(start: 1970-01-01T00:00:00.000000001Z)', self.org)

        self.assertTrue(isinstance(_result, types.GeneratorType))
        _result_list = list(_result)

        self.assertEqual(len(_result_list), 100)

    def test_stream_break(self):
        self._prepareData()

        _result = self.query_api.query_stream(
            f'from(bucket:"{self.bucket.name}") |> range(start: 1970-01-01T00:00:00.000000001Z)', self.org)

        _result_list = list(itertools.islice(_result, 10))
        _result.close()

        self.assertEqual(len(_result_list), 10)

    def _prepareData(self):
        _list = [f'h2o_feet,location=coyote_creek water_level={x} {x}' for x in range(1, 101)]
        self.write_client.write(self.bucket.name, self.org, _list, write_precision=WritePrecision.S)
        time.sleep(1)
