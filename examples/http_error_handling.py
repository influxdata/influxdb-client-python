import asyncio
from typing import MutableMapping

from influxdb_client import InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException


# To encapsulate functions used in batch writing
class BatchCB(object):

    def success(self, conf: (str, str, str), data: str):
        print(f"Write success: {conf}, data: {data}")

    def error(self, conf: (str, str, str), data: str, exception: InfluxDBError):
        print(f"\nBatch -> Write failed: {conf}, data: {data}, error: {exception.message}")
        report_headers(exception.headers)

    def retry(self, conf: (str, str, str), data: str, exception: InfluxDBError):
        print(f"Write failed but retryable: {conf}, data: {data}, error: {exception}")


# simple reporter that server is available
def report_ping(ping: bool):
    if not ping:
        raise ValueError("InfluxDB: Failed to ping server")
    else:
        print("InfluxDB: ready")


# report some useful expected header fields
def report_headers(headers: MutableMapping[str, str]):
    print("   Date:                  ", headers.get("Date"))
    print("   X-Influxdb-Build:      ", headers.get("X-Influxdb-Build"))
    print("   X-Influxdb-Version:    ", headers.get("X-Influxdb-Version"))
    print("   X-Platform-Error-Code: ", headers.get("X-Platform-Error-Code"))
    print("   Retry-After:           ", headers.get("Retry-After"))  # Should be None


# try a write using a synchronous call
def use_sync():
    print("Using sync")
    with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:
        report_ping(client.ping())
        try:
            client.write_api(write_options=SYNCHRONOUS).write(bucket="my-bucket", record="cpu,location=G4 usage=")
        except ApiException as ae:
            print("\nSync -> Caught ApiException: ", ae.message)
            report_headers(ae.headers)

        print("Sync write done")


# try a write using batch API
def use_batch():
    print("Using batch")
    with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:
        cb = BatchCB()
        with client.write_api(success_callback=cb.success,
                              error_callback=cb.error,
                              retry_callback=cb.retry) as write_api:
            write_api.write(bucket="my-bucket", record="cpu,location=G9 usage=")
            print("Batch write sent")
        print("Batch write done")


# try a write using async.io
async def use_async():
    print("Using async")
    async with InfluxDBClientAsync(url="http://localhost:8086", token="my-token", org="my-org") as client:
        report_ping(await client.ping())
        try:
            await client.write_api().write(bucket="my-bucket", record="cpu,location=G7 usage=")
        except InfluxDBError as ie:
            print("\nAsync -> Caught InfluxDBError: ", ie.message)
            report_headers(ie.headers)
        print("Async write done")


if __name__ == "__main__":
    use_sync()
    print("\n   Continuing...\n")
    use_batch()
    print("\n   Continuing...\n")
    asyncio.run(use_async())
