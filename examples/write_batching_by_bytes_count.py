"""
How to use RxPY to prepare batches by maximum bytes count.
"""

from csv import DictReader
from functools import reduce
from typing import Collection

import rx
from rx import operators as ops, Observable

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


def _buffer_bytes_size(buffer: Collection['bytes']):
    """
    Calculate size of buffer
    """
    return reduce(lambda total, actual: total + actual, map(lambda x: len(x), buffer)) + (
        len(buffer))


def buffer_by_bytes_count(bytes_count: int = 5120):
    """
    Buffer items until the bytes count is reached.
    """

    def _buffer_by_bytes_count(source: Observable) -> Observable:
        def subscribe(observer, scheduler=None):
            observer.buffer = []

            def on_next(current):
                observer.buffer.append(current)
                # Emit new batch if the buffer size is greater then boundary
                if (_buffer_bytes_size(observer.buffer) + len(current)) >= bytes_count:
                    # emit batch
                    observer.on_next(observer.buffer)
                    observer.buffer = []

            def on_error(exception):
                observer.buffer = []
                observer.on_error(exception)

            def on_completed():
                if len(observer.buffer) >= 0:
                    # flush rest of buffer
                    observer.on_next(observer.buffer)
                    observer.buffer = []
                observer.on_completed()

            return source.subscribe(
                on_next,
                on_error,
                on_completed,
                scheduler)

        return Observable(subscribe)

    return _buffer_by_bytes_count


"""
Define Retry strategy - 3 attempts => 2, 4, 8
"""
retries = WritesRetry(total=3, retry_interval=1, exponential_base=2)
with InfluxDBClient(url='http://localhost:8086', token='my-token', org='my-org', retries=retries) as client:
    """
    Use synchronous version of WriteApi.
    """
    write_api = client.write_api(write_options=SYNCHRONOUS)

    """
    Prepare batches from generator:
     1. Map Point into LineProtocol
     2. Map LineProtocol into bytes
     3. Create batches by bytes count - 5120 - 5KiB
    """
    batches = rx \
        .from_iterable(csv_to_generator('vix-daily.csv')) \
        .pipe(ops.map(lambda point: point.to_line_protocol())) \
        .pipe(ops.map(lambda line_protocol: line_protocol.encode("utf-8"))) \
        .pipe(buffer_by_bytes_count(bytes_count=5120))


    def write_batch(batch):
        """
        Synchronous write
        """
        print(f'Writing batch...')
        write_api.write(bucket='my-bucket', record=batch)
        print(f' > {_buffer_bytes_size(batch)} bytes')


    """
    Write batches
    """
    batches.subscribe(on_next=lambda batch: write_batch(batch),
                      on_error=lambda ex: print(f'Unexpected error: {ex}'),
                      on_completed=lambda: print('Import finished!'))
