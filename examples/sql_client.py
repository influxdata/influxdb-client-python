#!/usr/bin/env python3
"""Demonstrate how to use the SQL client with InfluxDB."""
from influxdb_client import InfluxDBClient


with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:
    # Each connection is specific to a bucket
    sql_client = client.sql_client("my-bucket")

    # To help understand the shape and makeup of a table users can use these
    # two helper functions.
    tables = sql_client.tables()
    print(tables)
    schemas = sql_client.schemas()
    print(schemas)

    # The returned result is a stream of data. For large result-sets users can
    # iterate through those one-by-one to avoid using large chunks of memory.
    with sql_client.query("select * from cpu") as reader:
        for batch in reader:
            print(batch)

    # For smaller results you might want to read the results at once. You
    # can do so by using the `read_all()` method.
    with sql_client.query("select * from cpu limit 10") as result:
        data = result.read_all()
        print(data)

    # To get you data into a Pandas DataFrame use the following helper function
    df = data.to_pandas()

    # Close the connection to this bucket.
    sql_client.close()
