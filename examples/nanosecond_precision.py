from influxdb_client import Point, InfluxDBClient
from influxdb_client.client.util.date_utils_pandas import PandasDateTimeHelper
from influxdb_client.client.write_api import SYNCHRONOUS

"""
Set PandasDate helper which supports nanoseconds.
"""
import influxdb_client.client.util.date_utils as date_utils

date_utils.date_helper = PandasDateTimeHelper()

"""
Prepare client.
"""
client = InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org")

write_api = client.write_api(write_options=SYNCHRONOUS)
query_api = client.query_api()

"""
Prepare data
"""

point = Point("h2o_feet") \
    .field("water_level", 10) \
    .tag("location", "pacific") \
    .time('1996-02-25T21:20:00.001001231Z')

print(f'Time serialized with nanosecond precision: {point.to_line_protocol()}')
print()

write_api.write(bucket="my-bucket", record=point)

"""
Query: using Stream
"""
query = '''
from(bucket:"my-bucket")
        |> range(start: 0, stop: now())
        |> filter(fn: (r) => r._measurement == "h2o_feet")
'''
records = query_api.query_stream(query)

for record in records:
    print(f'Temperature in {record["location"]} is {record["_value"]} at time: {record["_time"]}')

"""
Close client
"""
client.close()
