import asyncio
import time
from datetime import datetime, timezone

from influxdb_client.client.exceptions import InfluxDBError

from influxdb_client.rest import ApiException

from influxdb_client.client.write_api import SYNCHRONOUS

from influxdb_client import WritePrecision, InfluxDBClient, Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync


class BatchCB(object):

    @staticmethod
    def success(self, conf: (str, str, str), data: str):
        print(f"Write success: {conf}, data: {data}")

    @staticmethod
    def error(self, conf: (str, str, str), data: str, exception: InfluxDBError):
        print(f"Write failed: {conf}, data: {data}, error: {exception.message}")
        print(f"   Date:                  {exception.headers.get("Date")}")
        print(f"   X-Influxdb-Build:      {exception.headers.get("X-Influxdb-Build")}")
        print(f"   X-Influxdb-Version:    {exception.headers.get("X-Influxdb-Version")}")
        print(f"   X-Platform-Error-Code: {exception.headers.get("X-Platform-Error-Code")}")
        print(f"   Retry-After:           {exception.headers.get("Retry-After")}")

    @staticmethod
    def retry(self, conf: (str, str, str), data: str, exception: InfluxDBError):
        print(f"Write failed but retryable: {conf}, data: {data}, error: {exception}")


def report_ping(ping: bool):
    if not ping:
        raise ValueError("InfluxDB: Failed to ping server")
    else:
        print("InfluxDB: ready")


def use_sync():
    print("Using sync")
    with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:
        report_ping(client.ping())
        try:
            client.write_api(write_options=SYNCHRONOUS).write(bucket="my-bucket", record="cpu,location=G4 usage=")
        except ApiException as ae:
            print("\nCaught ae: ", ae.message)
            print("   Date:                  ", ae.headers.get("Date"))
            print("   X-Influxdb-Build:      ", ae.headers.get("X-Influxdb-Build"))
            print("   X-Influxdb-Version:    ", ae.headers.get("X-Influxdb-Version"))
            print("   X-Platform-Error-Code: ", ae.headers.get("X-Platform-Error-Code"))
            print("   Retry-After:           ", ae.headers.get("Retry-After"))  # Should be None

        print("Sync write done")


def use_batch():
    print("Using batch")
    with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:
        cb = BatchCB()
        with client.write_api(success_callback=cb.success,
                              error_callback=cb.error,
                              retry_callback=cb.retry) as write_api:
            write_api.write(bucket="my-bucket", record="cpu,location=G9 usage=")
            print("Batch write sent")


async def use_async():
    print("Using async")
    async with InfluxDBClientAsync(url="http://localhost:8086", token="my-token", org="my-org") as client:
        report_ping(await client.ping())
        try:
            await client.write_api().write(bucket="my-bucket", record="cpu,location=G7 usage=")
        except InfluxDBError as ie:
            print("\nCaught ie: ", ie)
        print("Async write done")


if __name__ == "__main__":
    use_sync()
    print("\n   Continuing...")
    use_batch()
    print("\n   Continuing...")
    asyncio.run(use_async())
