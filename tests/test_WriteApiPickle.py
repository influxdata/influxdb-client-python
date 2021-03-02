import pickle
import sys

import pytest

from influxdb_client import InfluxDBClient, WriteOptions
from influxdb_client.client.write_api import WriteType
from tests.base_test import current_milli_time, BaseTest


class InfluxDBWriterToPickle:

    def __init__(self):
        self.client = InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org", debug=False)
        self.write_api = self.client.write_api(
            write_options=WriteOptions(write_type=WriteType.batching, batch_size=50_000, flush_interval=10_000))

    def write(self, record):
        self.write_api.write(bucket="my-bucket", record=record)

    def terminate(self) -> None:
        self.write_api.close()
        self.client.close()


class WriteApiPickle(BaseTest):

    def setUp(self) -> None:
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()

    @pytest.mark.skipif(sys.version_info < (3, 7), reason="requires python3.7 or higher")
    def test_write_line_protocol(self):
        writer = InfluxDBWriterToPickle()

        pickle_out = open("writer.pickle", "wb")
        pickle.dump(writer, pickle_out)
        pickle_out.close()

        writer = pickle.load(open("writer.pickle", "rb"))

        measurement = "h2o_feet_" + str(current_milli_time())
        writer.write(record=f"{measurement},location=coyote_creek water_level=1.0")
        writer.terminate()

        tables = self.query_api.query(
            f'from(bucket: "my-bucket") |> range(start: 0) |> filter(fn: (r) => r._measurement == "{measurement}")')

        self.assertEqual(len(tables), 1)
        self.assertEqual(len(tables[0].records), 1)
        self.assertEqual(tables[0].records[0].get_measurement(), measurement)
        self.assertEqual(tables[0].records[0].get_value(), 1.0)
        self.assertEqual(tables[0].records[0].get_field(), "water_level")
