"""
How to load and execute query that is stored in file.
"""
import calendar
import random
from datetime import datetime, timedelta, timezone

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:

    write_api = client.write_api(write_options=SYNCHRONOUS)
    """
    Prepare data
    """

    _points = []
    now = datetime.now(timezone.utc).replace(hour=13, minute=20, second=15, microsecond=0)
    for i in range(50):
        _point = Point("weather")\
            .tag("location", "New York")\
            .field("temperature", random.randint(-10, 30))\
            .time(now - timedelta(days=i))
        _points.append(_point)

    write_api.write(bucket="my-bucket", record=_points)

    query_api = client.query_api()

    """
    Query: using Flux from file
    """
    with open('query.flux', 'r') as file:
        query = file.read()

    tables = query_api.query(query)

    for table in tables:
        for record in table.records:
            day_name = calendar.day_name[record["weekDay"]]
            print(f'Temperature in {record["location"]} is {record["temperature"]}°C at {day_name}')


