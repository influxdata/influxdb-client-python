"""
Connect to InfluxDB 2.0 - write data and query them
"""

from datetime import datetime

from influxdb_client import Point, InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

"""
Configure credentials
"""
influx_cloud_url = 'https://us-west-2-1.aws.cloud2.influxdata.com'
influx_cloud_token = '...'
bucket = '...'
org = '...'

client = InfluxDBClient(url=influx_cloud_url, token=influx_cloud_token)
try:
    kind = 'temperature'
    host = 'host1'
    device = 'opt-123'

    """
    Write data by Point structure
    """
    point = Point(kind).tag('host', host).tag('device', device).field('value', 25.3).time(time=datetime.utcnow())

    print(f'Writing to InfluxDB cloud: {point.to_line_protocol()} ...')

    write_api = client.write_api(write_options=SYNCHRONOUS)
    write_api.write(bucket=bucket, org=org, record=point)

    print()
    print('success')
    print()
    print()

    """
    Query written data
    """
    query = f'from(bucket: "{bucket}") |> range(start: -1d) |> filter(fn: (r) => r._measurement == "{kind}")'
    print(f'Querying from InfluxDB cloud: "{query}" ...')
    print()

    query_api = client.query_api()
    tables = query_api.query(query=query, org=org)

    for table in tables:
        for row in table.records:
            print(f'{row.values["_time"]}: host={row.values["host"]},device={row.values["device"]} '
                  f'{row.values["_value"]} °C')

    print()
    print('success')

except Exception as e:
    print(e)
finally:
    client.close()
