import asyncio
import unittest

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client.client.write_api import SYNCHRONOUS
from tests.base_test import generate_name


def async_test(coro):
    def wrapper(*args, **kwargs):
        return asyncio.get_event_loop().run_until_complete(coro(*args, **kwargs))

    return wrapper


# noinspection PyMethodMayBeStatic
class InfluxDBClientAsyncTest(unittest.TestCase):

    @async_test
    async def setUp(self) -> None:
        self.client = InfluxDBClientAsync(url="http://localhost:8086", token="my-token", org="my-org")

    @async_test
    async def tearDown(self) -> None:
        if self.client:
            await self.client.close()

    def test_use_async_context_manager(self):
        self.assertIsNotNone(self.client)

    @async_test
    async def test_ping(self):
        ping = await self.client.ping()
        self.assertTrue(ping)

    @async_test
    async def test_version(self):
        version = await self.client.version()
        self.assertTrue(len(version) > 0)

    def test_create_query_api(self):
        query_api = self.client.query_api()
        self.assertIsNotNone(query_api)

    @async_test
    async def test_query_tables(self):
        measurement = generate_name("measurement")
        self._prepare_data(measurement)
        query = f'''
                    from(bucket:"my-bucket") 
                        |> range(start: -10m)
                        |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                '''
        query_api = self.client.query_api()
        tables = await query_api.query(query)
        self.assertEqual(2, len(tables))
        self.assertEqual(1, len(tables[0].records))
        self.assertEqual("New York", tables[0].records[0]['location'])
        self.assertEqual(24.3, tables[0].records[0]['_value'])
        self.assertEqual(1, len(tables[1].records))
        self.assertEqual("Prague", tables[1].records[0]['location'])
        self.assertEqual(25.3, tables[1].records[0]['_value'])

    def _prepare_data(self, measurement: str):
        with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org", debug=True) as client_sync:
            write_api = client_sync.write_api(write_options=SYNCHRONOUS)

            """
            Prepare data
            """
            _point1 = Point(measurement).tag("location", "Prague").field("temperature", 25.3)
            _point2 = Point(measurement).tag("location", "New York").field("temperature", 24.3)
            write_api.write(bucket="my-bucket", record=[_point1, _point2])
