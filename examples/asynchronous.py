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
        print(f"InfluxDB: {version}")

        """
        Prepare data
        """
        # tmp solution before implements WriteApiAsync
        with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org", debug=True) as client_sync:
            write_api = client_sync.write_api(write_options=SYNCHRONOUS)

            """
            Prepare data
            """
            _point1 = Point("my_measurement").tag("location", "Prague").field("temperature", 25.3)
            _point2 = Point("my_measurement").tag("location", "New York").field("temperature", 24.3)
            write_api.write(bucket="my-bucket", record=[_point1, _point2])

        """
        Query: using Table structure
        """
        query_api = client.query_api()
        tables = await query_api.query('from(bucket:"my-bucket") |> range(start: -10m)')

        for table in tables:
            print(table)
            for record in table.records:
                print(record.values)

        print()
        print()


if __name__ == "__main__":
    asyncio.run(main())
