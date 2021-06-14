"""
How to How to check that connection information are suitable for queries and writes from/into specified bucket.
"""

from influxdb_client import InfluxDBClient

"""
Define credentials
"""
url = "http://localhost:8086"
token = "my-token"
org = "my-org"
bucket = "my-bucket"


def check_connection():
    """Check that the InfluxDB is running."""
    pass


def check_query():
    """Check that the credentials has permission to query from the Bucket"""
    pass


def check_write():
    """Check that the credentials has permission to write into the Bucket"""
    pass


with InfluxDBClient(url=url, token=token, org=org) as client:
    check_connection()
    check_query()
    check_write()
    pass
