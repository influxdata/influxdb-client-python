"""
Import public NYC taxi and for-hire vehicle (Uber, Lyft, etc.) trip data into InfluxDB 2.0

https://github.com/toddwschneider/nyc-taxi-data
"""
import concurrent.futures
import io
import multiprocessing
from collections import OrderedDict
from csv import DictReader
from datetime import datetime
from multiprocessing import Value
from urllib.request import urlopen

import rx
from rx import operators as ops

from influxdb_client import Point, InfluxDBClient, WriteOptions
from influxdb_client.client.write_api import WriteType


class ProgressTextIOWrapper(io.TextIOWrapper):
    """
    TextIOWrapper that store progress of read.
    """
    def __init__(self, *args, **kwargs):
        io.TextIOWrapper.__init__(self, *args, **kwargs)
        self.progress = None
        pass

    def readline(self, *args, **kwarg) -> str:
        readline = super().readline(*args, **kwarg)
        self.progress.value += len(readline)
        return readline


class InfluxDBWriter(multiprocessing.Process):
    """
    Writer that writes data in batches with 50_000 items.
    """
    def __init__(self, queue):
        multiprocessing.Process.__init__(self)
        self.queue = queue
        self.client = InfluxDBClient(url="http://localhost:9999", token="my-token", org="my-org", debug=False)
        self.write_api = self.client.write_api(
            write_options=WriteOptions(write_type=WriteType.batching, batch_size=50_000, flush_interval=10_000))

    def run(self):
        while True:
            next_task = self.queue.get()
            if next_task is None:
                # Poison pill means terminate
                self.terminate()
                self.queue.task_done()
                break
            self.write_api.write(bucket="my-bucket", record=next_task)
            self.queue.task_done()

    def terminate(self) -> None:
        proc_name = self.name
        print()
        print('Writer: flushing data...')
        self.write_api.__del__()
        self.client.__del__()
        print('Writer: closed'.format(proc_name))


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


def parse_rows(rows, total_size):
    """
    Parse bunch of CSV rows into LineProtocol

    :param total_size: Total size of file
    :param rows: CSV rows
    :return: List of line protocols
    """
    _parsed_rows = list(map(parse_row, rows))

    counter_.value += len(_parsed_rows)
    if counter_.value % 10_000 == 0:
        print('{0:8}{1}'.format(counter_.value, ' - {0:.2f} %'
                                .format(100 * float(progress_.value) / float(int(total_size))) if total_size else ""))
        pass

    queue_.put(_parsed_rows)
    return None


def init_counter(counter, progress, queue):
    """
    Initialize shared counter for display progress
    """
    global counter_
    counter_ = counter
    global progress_
    progress_ = progress
    global queue_
    queue_ = queue


"""
Create multiprocess shared environment
"""
queue_ = multiprocessing.Manager().Queue()
counter_ = Value('i', 0)
progress_ = Value('i', 0)
startTime = datetime.now()

url = "https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_2019-01.csv"
# url = "file:///Users/bednar/Developer/influxdata/influxdb-client-python/examples/fhv_tripdata_2019-01.csv"

"""
Open URL and for stream data 
"""
response = urlopen(url)
if response.headers:
    content_length = response.headers['Content-length']
io_wrapper = ProgressTextIOWrapper(response)
io_wrapper.progress = progress_

"""
Start writer as a new process
"""
writer = InfluxDBWriter(queue_)
writer.start()

"""
Create process pool for parallel encoding into LineProtocol
"""
cpu_count = multiprocessing.cpu_count()
with concurrent.futures.ProcessPoolExecutor(cpu_count, initializer=init_counter,
                                            initargs=(counter_, progress_, queue_)) as executor:
    """
    Converts incoming HTTP stream into sequence of LineProtocol
    """
    data = rx \
        .from_iterable(DictReader(io_wrapper)) \
        .pipe(ops.buffer_with_count(10_000),
              # Parse 10_000 rows into LineProtocol on subprocess
              ops.flat_map(lambda rows: executor.submit(parse_rows, rows, content_length)))

    """
    Write data into InfluxDB
    """
    data.subscribe(on_next=lambda x: None, on_error=lambda ex: print(f'Unexpected error: {ex}'))

"""
Terminate Writer
"""
queue_.put(None)
queue_.join()

print()
print(f'Import finished in: {datetime.now() - startTime}')
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

client = InfluxDBClient(url="http://localhost:9999", token="my-token", org="my-org", debug=False)
result = client.query_api().query(query=query)

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
