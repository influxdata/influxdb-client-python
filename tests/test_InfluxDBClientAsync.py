import asyncio
import unittest
import os
from datetime import datetime

from influxdb_client import Point, WritePrecision
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
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

    def test_create_write_api(self):
        write_api = self.client.write_api()
        self.assertIsNotNone(write_api)

    def test_create_delete_api(self):
        delete_api = self.client.delete_api()
        self.assertIsNotNone(delete_api)

    @async_test
    async def test_query_tables(self):
        measurement = generate_name("measurement")
        await self._prepare_data(measurement)
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

    @async_test
    async def test_query_raw(self):
        measurement = generate_name("measurement")
        await self._prepare_data(measurement)
        query = f'''
                    from(bucket:"my-bucket") 
                        |> range(start: -10m)
                        |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                        |> drop(columns: ["_start", "_stop", "_time", "_measurement"])
                '''
        query_api = self.client.query_api()

        raw = await query_api.query_raw(query)
        self.assertEqual(7, len(raw.splitlines()))
        self.assertEqual(',,0,24.3,temperature,New York', raw.splitlines()[4])
        self.assertEqual(',,1,25.3,temperature,Prague', raw.splitlines()[5])

    @async_test
    async def test_query_stream_records(self):
        measurement = generate_name("measurement")
        await self._prepare_data(measurement)
        query = f'''
                    from(bucket:"my-bucket") 
                        |> range(start: -10m)
                        |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                '''
        query_api = self.client.query_api()

        records = []
        async for record in await query_api.query_stream(query):
            records.append(record)

        self.assertEqual(2, len(records))
        self.assertEqual("New York", records[0]['location'])
        self.assertEqual(24.3, records[0]['_value'])
        self.assertEqual("Prague", records[1]['location'])
        self.assertEqual(25.3, records[1]['_value'])

    @async_test
    async def test_query_data_frame(self):
        measurement = generate_name("measurement")
        await self._prepare_data(measurement)
        query = f'''
                    from(bucket:"my-bucket") 
                        |> range(start: -10m)
                        |> filter(fn: (r) => r["_measurement"] == "{measurement}") 
                        |> group()
                '''
        query_api = self.client.query_api()

        dataframe = await query_api.query_data_frame(query)
        self.assertIsNotNone(dataframe)
        self.assertEqual(2, len(dataframe))
        self.assertEqual(24.3, dataframe['_value'][0])
        self.assertEqual(25.3, dataframe['_value'][1])
        self.assertEqual('temperature', dataframe['_field'][0])
        self.assertEqual('temperature', dataframe['_field'][1])
        self.assertEqual(measurement, dataframe['_measurement'][0])
        self.assertEqual(measurement, dataframe['_measurement'][1])
        self.assertEqual('New York', dataframe['location'][0])
        self.assertEqual('Prague', dataframe['location'][1])

    @async_test
    async def test_write_response_type(self):
        measurement = generate_name("measurement")
        point = Point(measurement).tag("location", "Prague").field("temperature", 25.3)
        response = await self.client.write_api().write(bucket="my-bucket", record=point)

        self.assertEqual(True, response)

    @async_test
    async def test_write_empty_data(self):
        measurement = generate_name("measurement")
        point = Point(measurement).tag("location", "Prague")
        response = await self.client.write_api().write(bucket="my-bucket", record=point)

        self.assertEqual(True, response)

    @async_test
    async def test_write_points_different_precision(self):
        measurement = generate_name("measurement")
        _point1 = Point(measurement).tag("location", "Prague").field("temperature", 25.3) \
            .time(datetime.utcfromtimestamp(0), write_precision=WritePrecision.S)
        _point2 = Point(measurement).tag("location", "New York").field("temperature", 24.3) \
            .time(datetime.utcfromtimestamp(1), write_precision=WritePrecision.MS)
        _point3 = Point(measurement).tag("location", "Berlin").field("temperature", 24.3) \
            .time(datetime.utcfromtimestamp(2), write_precision=WritePrecision.NS)
        await self.client.write_api().write(bucket="my-bucket", record=[_point1, _point2, _point3],
                                            write_precision=WritePrecision.NS)
        query = f'''
                    from(bucket:"my-bucket") 
                        |> range(start: 0)
                        |> filter(fn: (r) => r["_measurement"] == "{measurement}") 
                        |> keep(columns: ["_time"])
                '''
        query_api = self.client.query_api()

        raw = await query_api.query_raw(query)
        self.assertEqual(8, len(raw.splitlines()))
        self.assertEqual(',,0,1970-01-01T00:00:02Z', raw.splitlines()[4])
        self.assertEqual(',,0,1970-01-01T00:00:01Z', raw.splitlines()[5])
        self.assertEqual(',,0,1970-01-01T00:00:00Z', raw.splitlines()[6])

    @async_test
    async def test_delete_api(self):
        measurement = generate_name("measurement")
        await self._prepare_data(measurement)

        successfully = await self.client.delete_api().delete(start=datetime.utcfromtimestamp(0), stop=datetime.now(),
                                                             predicate="location = \"Prague\"", bucket="my-bucket")
        self.assertEqual(True, successfully)
        query = f'''
                        from(bucket:"my-bucket") 
                            |> range(start: -10m)
                            |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                    '''
        query_api = self.client.query_api()
        tables = await query_api.query(query)
        self.assertEqual(1, len(tables))
        self.assertEqual(1, len(tables[0].records))
        self.assertEqual("New York", tables[0].records[0]['location'])
        self.assertEqual(24.3, tables[0].records[0]['_value'])

    @async_test
    async def test_init_from_ini_file(self):
        client_from_config = InfluxDBClientAsync.from_config_file(f'{os.path.dirname(__file__)}/config.ini')
        self.assertEqual("http://localhost:8086", client_from_config.url)
        self.assertEqual("my-org", client_from_config.org)
        self.assertEqual("my-token", client_from_config.token)
        self.assertEqual(6000, client_from_config.api_client.configuration.timeout)
        self.assertEqual(3, len(client_from_config.default_tags))
        self.assertEqual("132-987-655", client_from_config.default_tags["id"])
        self.assertEqual("California Miner", client_from_config.default_tags["customer"])
        self.assertEqual("${env.data_center}", client_from_config.default_tags["data_center"])

        await client_from_config.close()

    @async_test
    async def test_init_from_env(self):
        os.environ["INFLUXDB_V2_URL"] = "http://localhost:8086"
        os.environ["INFLUXDB_V2_ORG"] = "my-org"
        os.environ["INFLUXDB_V2_TOKEN"] = "my-token"
        os.environ["INFLUXDB_V2_TIMEOUT"] = "5500"
        client_from_envs = InfluxDBClientAsync.from_env_properties()
        self.assertEqual("http://localhost:8086", client_from_envs.url)
        self.assertEqual("my-org", client_from_envs.org)
        self.assertEqual("my-token", client_from_envs.token)
        self.assertEqual(5500, client_from_envs.api_client.configuration.timeout)

        await client_from_envs.close()


    async def _prepare_data(self, measurement: str):
        _point1 = Point(measurement).tag("location", "Prague").field("temperature", 25.3)
        _point2 = Point(measurement).tag("location", "New York").field("temperature", 24.3)
        await self.client.write_api().write(bucket="my-bucket", record=[_point1, _point2])
