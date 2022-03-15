"""
How to use `aiohttp-retry` with async client.

This example depends on `aiohttp_retry <https://github.com/inyutin/aiohttp_retry>`_.
Install ``aiohttp_retry`` by: pip install aiohttp-retry.

"""
import asyncio

from aiohttp_retry import ExponentialRetry, RetryClient

from influxdb_client import Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync


async def main():
    """
    Configure Retries - for more info see https://github.com/inyutin/aiohttp_retry
    """
    retry_options = ExponentialRetry(attempts=3)
    async with InfluxDBClientAsync(url="http://localhost:8086", token="my-token", org="my-org",
                                   client_session_type=RetryClient,
                                   client_session_kwargs={"retry_options": retry_options}) as client:
        """
        Write data:
        """
        print(f"\n------- Written data: -------\n")
        write_api = client.write_api()
        _point1 = Point("async_m").tag("location", "Prague").field("temperature", 25.3)
        _point2 = Point("async_m").tag("location", "New York").field("temperature", 24.3)
        successfully = await write_api.write(bucket="my-bucket", record=[_point1, _point2])
        print(f" > successfully: {successfully}")

        """
        Query: Stream of FluxRecords
        """
        print(f"\n------- Query: Stream of FluxRecords -------\n")
        query_api = client.query_api()
        records = await query_api.query_stream('from(bucket:"my-bucket") '
                                               '|> range(start: -10m) '
                                               '|> filter(fn: (r) => r["_measurement"] == "async_m")')
        async for record in records:
            print(record)


if __name__ == "__main__":
    asyncio.run(main())
