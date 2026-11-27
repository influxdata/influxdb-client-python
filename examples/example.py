import codecs
from datetime import datetime, timezone

from influxdb_client import WritePrecision, InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org", debug=False) as client:
    query_api = client.query_api()

    p = Point("my_measurement").tag("location", "Prague").field("temperature", 25.3) \
        .time(datetime.now(tz=timezone.utc), WritePrecision.MS)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # write using point structure
    write_api.write(bucket="my-bucket", record=p)

    plp = Point("my_measurement").tag("location", "Athens").field("temperature", 35.3) \
        .time(datetime.now(tz=timezone.utc))
    line_protocol = plp.to_line_protocol()
    print(line_protocol)

    # write using line protocol string
    write_api.write(bucket="my-bucket", record=line_protocol)

    # using Table structure
    tables = query_api.query('from(bucket:"my-bucket") |> range(start: -10m)')
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
    print(codecs.decode(response.data))
