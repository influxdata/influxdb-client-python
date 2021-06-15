"""
How to check that connection credentials are suitable for queries and writes from/into specified bucket.
"""

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException

"""
Define credentials
"""
url = "http://localhost:8086"
token = "my-token"
org = "my-org"
bucket = "my-bucket"


def check_connection():
    """Check that the InfluxDB is running."""
    print("> Checking connection ...", end=" ")
    client.api_client.call_api('/ping', 'GET')
    print("ok")


def check_query():
    """Check that the credentials has permission to query from the Bucket"""
    print("> Checking credentials for query ...", end=" ")
    try:
        client.query_api().query(f"from(bucket:\"{bucket}\") |> range(start: -1m) |> limit(n:1)", org)
    except ApiException as e:
        # missing credentials
        if e.status == 404:
            raise Exception(f"The specified token doesn't have sufficient credentials to read from '{bucket}' "
                            f"or specified bucket doesn't exists.") from e
        raise
    print("ok")


def check_write():
    """Check that the credentials has permission to write into the Bucket"""
    print("> Checking credentials for write ...", end=" ")
    try:
        client.write_api(write_options=SYNCHRONOUS).write(bucket, org, b"")
    except ApiException as e:
        # missing credentials
        if e.status == 404:
            raise Exception(f"The specified token doesn't have sufficient credentials to write to '{bucket}' "
                            f"or specified bucket doesn't exists.") from e
        # 400 (BadRequest) caused by empty LineProtocol
        if e.status != 400:
            raise
    print("ok")


with InfluxDBClient(url=url, token=token, org=org) as client:
    check_connection()
    check_query()
    check_write()
    pass
