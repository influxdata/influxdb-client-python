import codecs
import io
import builtins
from datetime import datetime

import pandas

from influxdb2 import WritePrecision
from influxdb2.client.influxdb_client import InfluxDBClient
from influxdb2.client.write.point import Point

bucket = "test_bucket"

client = InfluxDBClient(url="http://localhost:9999/api/v2", token="my-token-123", org="my-org")

write_api = client.write_api()
query_api = client.query_api()

p = Point("my_measurement").tag("location", "Prague").field("temperature", 25.3).time(datetime.now(), WritePrecision.MS)

# write using point structure
write_api.write(org="my-org", bucket=bucket, record=p)

line_protocol = p.to_line_protocol()
print(line_protocol)

# write using line protocol string
write_api.write(org="my-org", bucket=bucket, record=line_protocol)

# using Table structure
tables = query_api.query('from(bucket:"my-bucket") |> range(start: -1m)')
for table in tables:
    print(table)
    for record in table.records:
        # process record
        print(record.values)

# using csv library
csv_result = query_api.query_csv('from(bucket:"my-bucket") |> range(start: -10m)')
val_count = 0
for record in csv_result:
    for cell in record:
        val_count += 1
print("val count: ", val_count)

response = query_api.query_raw('from(bucket:"my-bucket") |> range(start: -10m)')
print (codecs.decode(response.data))
