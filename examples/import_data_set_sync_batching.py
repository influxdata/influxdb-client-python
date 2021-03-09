"""
How to use RxPY to prepare batches for synchronous write into InfluxDB
"""

from csv import DictReader

import rx
from rx import operators as ops

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write.retry import WritesRetry
from influxdb_client.client.write_api import SYNCHRONOUS


def csv_to_generator(csv_file_path):
    """
    Parse your CSV file into generator
    """
    for row in DictReader(open(csv_file_path, 'r')):
        point = Point('financial-analysis') \
            .tag('type', 'vix-daily') \
            .field('open', float(row['VIX Open'])) \
            .field('high', float(row['VIX High'])) \
            .field('low', float(row['VIX Low'])) \
            .field('close', float(row['VIX Close'])) \
            .time(row['Date'])
        yield point


"""
Define Retry strategy - 3 attempts => 2, 4, 8
"""
retries = WritesRetry(total=3, backoff_factor=1, exponential_base=2)
client = InfluxDBClient(url='http://localhost:8086', token='my-token', org='my-org', retries=retries)

"""
Use synchronous version of WriteApi to strongly depends on result of write
"""
write_api = client.write_api(write_options=SYNCHRONOUS)

"""
Prepare batches from generator
"""
batches = rx \
    .from_iterable(csv_to_generator('vix-daily.csv')) \
    .pipe(ops.buffer_with_count(500))


def write_batch(batch):
    """
    Synchronous write
    """
    print(f'Writing... {len(batch)}')
    write_api.write(bucket='my-bucket', record=batch)


"""
Write batches
"""
batches.subscribe(on_next=lambda batch: write_batch(batch),
                  on_error=lambda ex: print(f'Unexpected error: {ex}'),
                  on_completed=lambda: print('Import finished!'))

"""
Dispose client
"""
write_api.close()
client.close()
