"""
How to use Asyncio with InfluxDB client.
"""
import asyncio

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client.client.write_api import SYNCHRONOUS


async def main():
    async with InfluxDBClientAsync(url="http://localhost:8086", token="my-token", org="my-org") as client:
        """
        Check the version of the InfluxDB
        """
        version = await client.version()
        print(f"\n------- Version -------\n")
        print(f"InfluxDB: {version}")

        """
        Prepare data
        """
        # tmp solution before implements WriteApiAsync
        with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org", debug=False) as client_sync:
            write_api = client_sync.write_api(write_options=SYNCHRONOUS)

            """
            Prepare data
            """
            _point1 = Point("async_m").tag("location", "Prague").field("temperature", 25.3)
            _point2 = Point("async_m").tag("location", "New York").field("temperature", 24.3)
            write_api.write(bucket="my-bucket", record=[_point1, _point2])

        """
        Query: using Table structure
        """
        print(f"\n------- Query: using Table structure -------\n")
        query_api = client.query_api()
        tables = await query_api.query('from(bucket:"my-bucket") '
                                       '|> range(start: -10m) '
                                       '|> filter(fn: (r) => r["_measurement"] == "async_m")')

        for table in tables:
            for record in table.records:
                print(f'Temperature in {record["location"]} is {record["_value"]}')

        """
        Query: using raw str output
        """
        print(f"\n------- Query: using raw str output -------\n")
        query_api = client.query_api()
        raw = await query_api.query_raw('from(bucket:"my-bucket") '
                                        '|> range(start: -10m) '
                                        '|> filter(fn: (r) => r["_measurement"] == "async_m")')
        print(raw)

if __name__ == "__main__":
    asyncio.run(main())
