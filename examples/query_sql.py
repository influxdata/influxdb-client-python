#!/usr/bin/env python3
"""Demonstrate how to query data from InfluxDB."""
from influxdb_client import InfluxDBClient

with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org", debug=False) as client:
    query_sql_api = client.query_sql_api()
    result = query_sql_api.query("my-bucket", "select * from cpu limit 10")
    print(result.read_all())
