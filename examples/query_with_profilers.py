from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.flux_table import FluxRecord
from influxdb_client.client.query_api import QueryOptions
from influxdb_client.client.write_api import SYNCHRONOUS

with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org", debug=True) as client:

    """
    Define callback to process profiler results.
    """
    class ProfilersCallback(object):
        def __init__(self):
            self.records = []

        def __call__(self, flux_record):
            self.records.append(flux_record.values)


    callback = ProfilersCallback()

    write_api = client.write_api(write_options=SYNCHRONOUS)

    """
    Prepare data
    """
    _point1 = Point("my_measurement").tag("location", "Prague").field("temperature", 25.3)
    _point2 = Point("my_measurement").tag("location", "New York").field("temperature", 24.3)
    write_api.write(bucket="my-bucket", record=[_point1, _point2])

    """
    Pass callback to QueryOptions
    """
    query_api = client.query_api(
        query_options=QueryOptions(profilers=["query", "operator"], profiler_callback=callback))

    """
    Perform query
    """
    tables = query_api.query('from(bucket:"my-bucket") |> range(start: -10m)')

    for profiler in callback.records:
        print(f'Custom processing of profiler result: {profiler}')