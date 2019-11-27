"""
Import public NYC taxi and for-hire vehicle (Uber, Lyft, etc.) trip data into InfluxDB 2.0

https://github.com/toddwschneider/nyc-taxi-data
"""
import concurrent.futures
import multiprocessing
from collections import OrderedDict
from csv import DictReader
from datetime import datetime
from multiprocessing import Value

import requests
import rx
from rx import operators as ops

from influxdb_client import Point, InfluxDBClient, WriteOptions


def parse_row(row: OrderedDict):
    """Parse row of CSV file into Point with structure:

        taxi-trip-data,DOLocationID=152,PULocationID=79,dispatching_base_num=B02510 dropoff_datetime="2019-01-01 01:27:24" 1546304267000000000

    CSV format:
        dispatching_base_num,pickup_datetime,dropoff_datetime,PULocationID,DOLocationID,SR_Flag
        B00001,2019-01-01 00:30:00,2019-01-01 02:51:55,,,
        B00001,2019-01-01 00:45:00,2019-01-01 00:54:49,,,
        B00001,2019-01-01 00:15:00,2019-01-01 00:54:52,,,
        B00008,2019-01-01 00:19:00,2019-01-01 00:39:00,,,
        B00008,2019-01-01 00:27:00,2019-01-01 00:37:00,,,
        B00008,2019-01-01 00:48:00,2019-01-01 01:02:00,,,
        B00008,2019-01-01 00:50:00,2019-01-01 00:59:00,,,
        B00008,2019-01-01 00:51:00,2019-01-01 00:56:00,,,
        B00009,2019-01-01 00:44:00,2019-01-01 00:58:00,,,
        B00009,2019-01-01 00:19:00,2019-01-01 00:36:00,,,
        B00009,2019-01-01 00:36:00,2019-01-01 00:49:00,,,
        B00009,2019-01-01 00:26:00,2019-01-01 00:32:00,,,
        ...

    :param row: the row of CSV file
    :return: Parsed csv row to [Point]
    """

    return Point("taxi-trip-data") \
        .tag("dispatching_base_num", row['dispatching_base_num']) \
        .tag("PULocationID", row['PULocationID']) \
        .tag("DOLocationID", row['DOLocationID']) \
        .tag("SR_Flag", row['SR_Flag']) \
        .field("dropoff_datetime", row['dropoff_datetime']) \
        .time(row['pickup_datetime']) \
        .to_line_protocol()


def parse_rows(rows):
    """
    Parse bunch of CSV rows into LineProtocol

    :param rows: CSV rows
    :return: List of line protocols
    """
    _parsed_rows = list(map(parse_row, rows))

    counter_.value += len(_parsed_rows)
    if counter_.value % 10_000 == 0:
        _count = counter_.value
        print('{0:8} - {1:.2f} %'.format(_count, 100 * float(_count) / float(23_130_811)))
        pass

    return _parsed_rows


def init_counter(counter):
    """
    Initialize shared counter for display progress
    """
    global counter_
    counter_ = counter


"""
Init clients
"""
client = InfluxDBClient(url="http://localhost:9999", token="my-token", org="my-org", debug=False)

"""
Create client that writes data in batches with 50_000 items.
"""
write_api = client.write_api(write_options=WriteOptions(batch_size=50_000, flush_interval=10_000))

url = "https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2019-01.csv"
response = requests.get(url, stream=True)

counter_ = Value('i', 0)
startTime = datetime.now()

"""
Create process pool for parallel encoding into LineProtocol
"""
cpu_count = multiprocessing.cpu_count()
with concurrent.futures.ProcessPoolExecutor(cpu_count, initializer=init_counter, initargs=(counter_,)) as executor:
    """
    Converts incoming HTTP stream into sequence of LineProtocol
    """
    data = rx \
        .from_iterable(DictReader(response.iter_lines(decode_unicode=True))) \
        .pipe(ops.buffer_with_count(5_000),
              # Parse 5_000 rows into LineProtocol on subprocess
              ops.flat_map(lambda s: executor.submit(parse_rows, s)))

    """
    Write data into InfluxDB
    """
    write_api.write(org="my-org", bucket="my-bucket", record=data)

"""
Dispose write_api
"""
write_api.__del__()

print()
print(f'Import finished in: {datetime.now() - startTime} {datetime.now()}')
print()

"""
Querying 10 pickups from dispatching 'B00008'
"""
query = 'from(bucket:"my-bucket")' \
        '|> range(start: 2019-01-01T00:00:00Z, stop: now()) ' \
        '|> filter(fn: (r) => r._measurement == "taxi-trip-data")' \
        '|> filter(fn: (r) => r.dispatching_base_num == "B00008")' \
        '|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")' \
        '|> rename(columns: {_time: "pickup_datetime"})' \
        '|> drop(columns: ["_start", "_stop"])|> limit(n:10, offset: 0)'

result = client.query_api().query(org="my-org", query=query)

"""
Processing results
"""
print()
print("=== Querying 10 pickups from dispatching 'B00008' ===")
print()
for table in result:
    for record in table.records:
        print(
            f'Dispatching: {record["dispatching_base_num"]} pickup: {record["pickup_datetime"]} dropoff: {record["dropoff_datetime"]}')

"""
Close client
"""
client.__del__()
