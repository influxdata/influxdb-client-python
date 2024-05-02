import os
import unittest
from datetime import datetime, timezone

from influxdb_client import WritePrecision, InfluxDBClient
from influxdb_client.client.util.date_utils import get_date_helper
from influxdb_client.client.util.multiprocessing_helper import MultiprocessingWriter
from influxdb_client.client.write_api import SYNCHRONOUS


# noinspection PyMethodMayBeStatic
class MultiprocessingWriterTest(unittest.TestCase):

    def setUp(self) -> None:
        self.url = os.getenv('INFLUXDB_V2_URL', "http://localhost:8086")
        self.token = os.getenv('INFLUXDB_V2_TOKEN', "my-token")
        self.org = os.getenv('INFLUXDB_V2_ORG', "my-org")
        self.writer = None

    def tearDown(self) -> None:
        if self.writer:
            self.writer.__del__()

    def test_write_without_start(self):
        self.writer = MultiprocessingWriter(url=self.url, token=self.token, org=self.org,
                                            write_options=SYNCHRONOUS)

        with self.assertRaises(AssertionError) as ve:
            self.writer.write(bucket="my-bucket", record=f"mem,tag=a value=5")

        self.assertEqual('Cannot write data: the writer is not started.', f'{ve.exception}')

    def test_write_after_terminate(self):
        self.writer = MultiprocessingWriter(url=self.url, token=self.token, org=self.org,
                                            write_options=SYNCHRONOUS)
        self.writer.start()
        self.writer.__del__()

        with self.assertRaises(AssertionError) as ve:
            self.writer.write(bucket="my-bucket", record=f"mem,tag=a value=5")

        self.assertEqual('Cannot write data: the writer is closed.', f'{ve.exception}')

    def test_terminate_twice(self):
        with MultiprocessingWriter(url=self.url, token=self.token, org=self.org, write_options=SYNCHRONOUS) as writer:
            writer.__del__()
            writer.terminate()
            writer.terminate()
            writer.__del__()

    def test_use_context_manager(self):
        with MultiprocessingWriter(url=self.url, token=self.token, org=self.org, write_options=SYNCHRONOUS) as writer:
            self.assertIsNotNone(writer)

    def test_pass_parameters(self):
        unique = get_date_helper().to_nanoseconds(datetime.now(tz=timezone.utc) - datetime.fromtimestamp(0, tz=timezone.utc))

        # write data
        with MultiprocessingWriter(url=self.url, token=self.token, org=self.org, write_options=SYNCHRONOUS) as writer:
            writer.write(bucket="my-bucket", record=f"mem_{unique},tag=a value=5i 10", write_precision=WritePrecision.S)

        # query data
        with InfluxDBClient(url=self.url, token=self.token, org=self.org) as client:
            query_api = client.query_api()
            tables = query_api.query(
                f'from(bucket: "my-bucket") |> range(start: 0) |> filter(fn: (r) => r._measurement == "mem_{unique}")',
                self.org)
            record = tables[0].records[0]
            self.assertIsNotNone(record)
            self.assertEqual("a", record["tag"])
            self.assertEqual(5, record["_value"])
            self.assertEqual(get_date_helper().to_utc(datetime.fromtimestamp(10, tz=timezone.utc)), record["_time"])
