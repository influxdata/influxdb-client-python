"""
Efficiency write data from IOT sensor - write changed temperature every minute
"""
import atexit
import platform
from datetime import timedelta

import psutil as psutil
import rx
from rx import operators as ops

from influxdb_client import WriteApi, WriteOptions
from influxdb_client.client.influxdb_client import InfluxDBClient


def on_exit(db_client: InfluxDBClient, write_api: WriteApi):
    """Close clients after terminate a script.

    :param db_client: InfluxDB client
    :param write_api: WriteApi
    :return: nothing
    """
    write_api.__del__()
    db_client.__del__()


def sensor_temperature():
    """Read a CPU temperature. The [psutil] doesn't support MacOS so we use [sysctl].

    :return: actual CPU temperature
    """
    os_name = platform.system()
    if os_name == 'Darwin':
        from subprocess import check_output
        output = check_output(["sysctl", "machdep.xcpm.cpu_thermal_level"])
        import re
        return re.findall(r'\d+', str(output))[0]
    else:
        return psutil.sensors_temperatures()["coretemp"][0]


def line_protocol(temperature):
    """Create a InfluxDB line protocol with structure:

        iot_sensor,hostname=mine_sensor_12,type=temperature value=68

    :param temperature: the sensor temperature
    :return: Line protocol to write into InfluxDB
    """

    import socket
    return 'iot_sensor,hostname={},type=temperature value={}'.format(socket.gethostname(), temperature)


"""
Read temperature every minute; distinct_until_changed - produce only if temperature change
"""
data = rx \
    .interval(period=timedelta(seconds=60)) \
    .pipe(ops.map(lambda t: sensor_temperature()),
          ops.distinct_until_changed(),
          ops.map(lambda temperature: line_protocol(temperature)))

_db_client = InfluxDBClient(url="http://localhost:9999", token="my-token", org="my-org", debug=True)

"""
Create client that writes data into InfluxDB
"""
_write_api = _db_client.write_api(write_options=WriteOptions(batch_size=1))
_write_api.write(bucket="my-bucket", record=data)

"""
Call after terminate a script
"""
atexit.register(on_exit, _db_client, _write_api)

input()
