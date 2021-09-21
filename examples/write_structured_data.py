from collections import namedtuple
from dataclasses import dataclass
from datetime import datetime

from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS


class Sensor(namedtuple('Sensor', ['name', 'location', 'version', 'pressure', 'temperature', 'timestamp'])):
    """
    Named structure - Sensor
    """
    pass


@dataclass
class Car:
    """
    DataClass structure - Car
    """
    engine: str
    type: str
    speed: float


"""
Initialize client
"""
with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org", debug=True) as client:
    write_api = client.write_api(write_options=SYNCHRONOUS)

    """
    Sensor "current" state
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

    """
    Car "current" speed
    """
    car = Car('12V-BT', 'sport-cars', 125.25)
    print(car)

    """
    Synchronous write
    """
    write_api.write(bucket="my-bucket",
                    record=car,
                    dictionary_measurement_key="engine",
                    dictionary_tag_keys=["engine", "type"],
                    dictionary_field_keys=["speed"])
