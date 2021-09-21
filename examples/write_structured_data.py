from collections import namedtuple
from datetime import datetime

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS


class Sensor(namedtuple('Sensor', ['name', 'location', 'version', 'pressure', 'temperature', 'timestamp'])):
    """
    Sensor named structure
    """
    pass


"""
Initialize client
"""
with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:
    write_api = client.write_api(write_options=SYNCHRONOUS)

    """
    Sensor "current" value
    """
    sensor = Sensor(name="sensor_pt859",
                    location="warehouse_125",
                    version="2021.06.05.5874",
                    pressure=125,
                    temperature=10,
                    timestamp=datetime.utcnow())
    print(sensor)

    """
    Synchronous write
    """
    write_api.write(bucket="my-bucket",
                    record=sensor,
                    dictionary_measurement_key="name",
                    dictionary_time_key="timestamp",
                    dictionary_tag_keys=["location", "version"],
                    dictionary_field_keys=["pressure", "temperature"])

    from influxdb_client import Point
    point = Point("h2o_feet")\
        .tag("location", "coyote_creek")\
        .field("water_level", 4.0)\
        .time(4)
    write_api.write("my-bucket", "my-org", point)
