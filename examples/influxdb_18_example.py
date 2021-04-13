from influxdb_client import InfluxDBClient, Point

username = 'username'
password = 'password'

database = 'telegraf'
retention_policy = 'autogen'

bucket = f'{database}/{retention_policy}'

with InfluxDBClient(url='http://localhost:8086', token=f'{username}:{password}', org='-') as client:

    with client.write_api() as write_api:
        print('*** Write Points ***')

        point = Point("mem").tag("host", "host1").field("used_percent", 25.43234543)
        print(point.to_line_protocol())

        write_api.write(bucket=bucket, record=point)

    print('*** Query Points ***')

    query_api = client.query_api()
    query = f'from(bucket: \"{bucket}\") |> range(start: -1h)'
    tables = query_api.query(query)
    for record in tables[0].records:
        print(f'#{record.get_time()} #{record.get_measurement()}: #{record.get_field()} #{record.get_value()}')

