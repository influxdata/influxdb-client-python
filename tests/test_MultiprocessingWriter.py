import os
import unittest
from datetime import datetime

from influxdb_client import WritePrecision, InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.util.date_utils import get_date_helper
from influxdb_client.client.util.multiprocessing_helper import MultiprocessingWriter
from influxdb_client.client.write_api import SYNCHRONOUS


# noinspection PyUnusedLocal
def _error_callback(conf: (str, str, str), data: str, error: InfluxDBError):
    with open("test_MultiprocessingWriter.txt", "w+") as file:
        file.write(error.message)


# noinspection PyMethodMayBeStatic
class MultiprocessingWriterTest(unittest.TestCase):

    def setUp(self) -> None:
        self.url = os.getenv('INFLUXDB_V2_URL', "http://localhost:8086")
        self.token = os.getenv('INFLUXDB_V2_TOKEN', "my-token")
        self.org = os.getenv('INFLUXDB_V2_ORG', "my-org")
        self.writer = None
        if os.path.exists("test_MultiprocessingWriter.txt"):
            os.remove("test_MultiprocessingWriter.txt")

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
        unique = get_date_helper().to_nanoseconds(datetime.utcnow() - datetime.utcfromtimestamp(0))

        # write data
        with MultiprocessingWriter(url=self.url, token=self.token, org=self.org, write_options=SYNCHRONOUS) as writer:
            print(f"write: {os.getpid()}")
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
            self.assertEqual(get_date_helper().to_utc(datetime.utcfromtimestamp(10)), record["_time"])

    def test_wrong_configuration(self):

        with MultiprocessingWriter(url=self.url, token="ddd", org=self.org, error_callback=_error_callback) as writer:
            writer.write(bucket="my-bucket", record=f"mem,tag=a value=5i 10", write_precision=WritePrecision.S)

        self.assertTrue(os.path.exists("test_MultiprocessingWriter.txt"))
