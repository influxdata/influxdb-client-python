"""
Illustrates getting header values from Errors that may occur on write.

To test against cloud set the following environment variables:
   INFLUX_URL
   INFLUX_TOKEN
   INFLUX_DATABASE
   INFLUX_ORG

...otherwise will run against a standard OSS endpoint.
"""
import asyncio
import os
from typing import MutableMapping

from influxdb_client import InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException


def get_envar(key, default):
    try:
        return os.environ[key]
    except:
        return default


class Config(object):

    def __init__(self):
        self.url = get_envar("INFLUX_URL", "http://localhost:8086")
        self.token = get_envar("INFLUX_TOKEN", "my-token")
        self.bucket = get_envar("INFLUX_DATABASE", "my-bucket")
        self.org = get_envar("INFLUX_ORG", "my-org")

    def __str__(self):
        return (f"config:\n"
                f"   url: {self.url}\n"
                f"   token: ****redacted*****\n"
                f"   bucket: {self.bucket}\n"
                f"   org: {self.org}\n"
                )


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
    print("   X-Influxdb-Version:    ", headers.get("X-Influxdb-Version")) # OSS version, Cloud should be None
    print("   X-Platform-Error-Code: ", headers.get("X-Platform-Error-Code")) # OSS invalid, Cloud should be None
    print("   Retry-After:           ", headers.get("Retry-After"))  # Should be None
    print("   Trace-Id:              ", headers.get("Trace-Id")) # OSS should be None, Cloud should return value


# try a write using a synchronous call
def use_sync(conf: Config):
    print("Using sync")
    with InfluxDBClient(url=conf.url, token=conf.token, org=conf.org) as client:
        report_ping(client.ping())
        try:
            client.write_api(write_options=SYNCHRONOUS).write(bucket=conf.bucket, record="cpu,location=G4 usage=")
        except ApiException as ae:
            print("\nSync -> Caught ApiException: ", ae.message)
            report_headers(ae.headers)

        print("Sync write done")


# try a write using batch API
def use_batch(conf: Config):
    print("Using batch")
    with InfluxDBClient(url=conf.url, token=conf.token, org=conf.org) as client:
        cb = BatchCB()
        with client.write_api(success_callback=cb.success,
                              error_callback=cb.error,
                              retry_callback=cb.retry) as write_api:
            write_api.write(bucket=conf.bucket, record="cpu,location=G9 usage=")
            print("Batch write sent")
        print("Batch write done")


# try a write using async.io
async def use_async(conf: Config):
    print("Using async")
    async with InfluxDBClientAsync(url=conf.url, token=conf.token, org=conf.org) as client:
        report_ping(await client.ping())
        try:
            await client.write_api().write(bucket=conf.bucket, record="cpu,location=G7 usage=")
        except InfluxDBError as ie:
            print("\nAsync -> Caught InfluxDBError: ", ie.message)
            report_headers(ie.headers)
        print("Async write done")


if __name__ == "__main__":
    conf = Config()
    print(conf)
    use_sync(conf)
    print("\n   Continuing...\n")
    use_batch(conf)
    print("\n   Continuing...\n")
    asyncio.run(use_async(conf))
