from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:

    """
    Prepare data
    """
    _point1 = Point("my_measurement").tag("location", "Prague").field("temperature", 25.3)
    _point2 = Point("my_measurement").tag("location", "New York").field("temperature", 24.3)

    client.write_api(write_options=SYNCHRONOUS).write(bucket="my-bucket", record=[_point1, _point2])

    """
    Query: using Table structure
    """
    tables = client.query_api().query('from(bucket:"my-bucket") |> range(start: -10m)')

    """
    Serialize to JSON
    """
    output = tables.to_json(indent=5)
    print(output)
