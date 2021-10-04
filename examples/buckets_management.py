from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

with InfluxDBClient(url='http://localhost:8086', token='my-token', org='my-org') as client:
    write_api = client.write_api(write_options=SYNCHRONOUS)

    write_api.write(bucket='my-bucket', record='h2o_feet,location=coyote_creek water_level=1.0 1')
