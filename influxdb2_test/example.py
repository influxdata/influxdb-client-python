from influxdb2.client.influxdb_client import InfluxDBClient
from influxdb2.client.write.point import Point

if __name__ == '__main__':
    pass

bucket = "test_bucket"

client = InfluxDBClient(url="http://localhost:9999/api/v2", token="my-token-123", org="my-org")

write_client = client.write_client()
query_client = client.query_client()

p = Point("my_measurement").tag("location", "Prague").field("temperature", 25.3)

write_client.write(bucket=bucket, org="my-org", record=p)

## using Table structure
tables = query_client.query('from(bucket:"my-bucket") |> range(start: -10m)')

for table in tables:
    print(table)
    for row in table.records:
        print (row.values)


## using csv library
csv_result = query_client.query_csv('from(bucket:"my-bucket") |> range(start: -10m)')
val_count = 0
for row in csv_result:
    for cell in row:
        val_count += 1


