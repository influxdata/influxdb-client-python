import asyncio
import unittest

from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync


def async_test(coro):
    def wrapper(*args, **kwargs):
        return asyncio.get_event_loop().run_until_complete(coro(*args, **kwargs))

    return wrapper


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
