"""
How to use WriteApi's callbacks to notify about state of background batches.
"""

from influxdb_client import InfluxDBClient, Point

"""
Configuration
"""
url = 'http://localhost:8086'
token = 'my-token'
org = 'my-org'
bucket = 'my-bucket'

"""
Data
"""
points = [Point("my-temperature").tag("location", "Prague").field("temperature", 25.3),
          Point("my-temperature").tag("location", "New York").field("temperature", 18.4)]


class BatchingCallback(object):

    def success(self, conf: (str, str, str), data: str):
        """Successfully writen batch."""
        print(f"Written batch: {conf}, data: {data}")

    def error(self, conf: (str, str, str), data: str, exception: Exception):
        """Unsuccessfully writen batch."""
        print(f"Cannot write batch: {conf}, data: {data} due: {exception}")

    def retry(self, conf: (str, str, str), data: str, exception: Exception):
        """Retryable error."""
        print(f"Retryable error occurs for batch: {conf}, data: {data} retry: {exception}")


callback = BatchingCallback()
with InfluxDBClient(url=url, token=token, org=org) as client:
    """
    Use batching API
    """
    with client.write_api(success_callback=callback.success,
                          error_callback=callback.error,
                          retry_callback=callback.retry) as write_api:
        write_api.write(bucket=bucket, record=points)
        print()
        print("Wait to finishing ingesting...")
        print()
