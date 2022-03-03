"""
How to use Asyncio with InfluxDB client.
"""
import asyncio

from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync


async def main():
    async with InfluxDBClientAsync(url="http://localhost:8086", token="my-token", org="my-org") as client:
        """
        Check the version of the InfluxDB
        """
        version = await client.version()
        print(f"InfluxDB: {version}")


if __name__ == "__main__":
    asyncio.run(main())
