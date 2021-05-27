import datetime as datetime

from influxdb_client import InfluxDBClient, Point, Dialect
from influxdb_client.client.write_api import SYNCHRONOUS

with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org",debug=True) as client:

    write_api = client.write_api(write_options=SYNCHRONOUS)

    """
    Prepare data
    """

    _point1 = Point("my_measurement").tag("location", "Prague").field("temperature", 25.3)
    _point2 = Point("my_measurement").tag("location", "New York").field("temperature", 24.3)

    write_api.write(bucket="my-bucket", record=[_point1, _point2])

    query_api = client.query_api()

    """
    Query: using Table structure
    """
    tables = query_api.query('from(bucket:"my-bucket") |> range(start: -10m)')

    for table in tables:
        print(table)
        for record in table.records:
            print(record.values)

    print()
    print()

    """
    Query: using Bind parameters
    """

    p = {"_start": datetime.timedelta(hours=-1),
         "_location": "Prague",
         "_desc": True,
         "_floatParam": 25.1,
         "_every": datetime.timedelta(minutes=5)
         }

    tables = query_api.query('''
        from(bucket:"my-bucket") |> range(start: _start)
            |> filter(fn: (r) => r["_measurement"] == "my_measurement")
            |> filter(fn: (r) => r["_field"] == "temperature")
            |> filter(fn: (r) => r["location"] == _location and r["_value"] > _floatParam)
            |> aggregateWindow(every: _every, fn: mean, createEmpty: true)        
            |> sort(columns: ["_time"], desc: _desc) 
    ''', params=p)

    for table in tables:
        print(table)
        for record in table.records:
            print(str(record["_time"]) + " - " + record["location"] + ": " + str(record["_value"]))

    print()
    print()

    """
    Query: using Stream
    """
    records = query_api.query_stream('''
    from(bucket:"my-bucket") 
        |> range(start: -10m)  
        |> filter(fn: (r) => r["_measurement"] == "my_measurement")
    ''')

    for record in records:
        print(f'Temperature in {record["location"]} is {record["_value"]}')

    """
    Interrupt a stream after retrieve a required data
    """
    large_stream = query_api.query_stream('''
    from(bucket:"my-bucket") 
        |> range(start: -100d) 
        |> filter(fn: (r) => r["_measurement"] == "my_measurement")
    ''')

    for record in large_stream:
        if record["location"] == "New York":
            print(f'New York temperature: {record["_value"]}')
            break

    large_stream.close()

    print()
    print()

    """
    Query: using csv library
    """
    csv_result = query_api.query_csv('from(bucket:"my-bucket") |> range(start: -10m)',
                                     dialect=Dialect(header=False, delimiter=",", comment_prefix="#", annotations=[],
                                                     date_time_format="RFC3339"))
    for csv_line in csv_result:
        if not len(csv_line) == 0:
            print(f'Temperature in {csv_line[9]} is {csv_line[6]}')

    print()
    print()

    """
    Query: using Pandas DataFrame
    """
    data_frame = query_api.query_data_frame('''
    from(bucket:"my-bucket") 
        |> range(start: -10m) 
        |> filter(fn: (r) => r["_measurement"] == "my_measurement")
        |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value") 
        |> keep(columns: ["_time","location", "temperature"])
    ''')
    print(data_frame.to_string())
