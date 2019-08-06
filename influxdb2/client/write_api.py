# coding: utf-8
from enum import Enum

from influxdb2 import WritePrecision
from influxdb2.client.abstract_client import AbstractClient
from influxdb2.client.write.point import Point


class WriteType(Enum):
    batching = 1
    asynchronous = 2
    synchronous = 3


class WriteOptions(object):

    def __init__(self, write_type=WriteType.batching, batch_size=None, flush_interval=None, jitter_interval=None,
                 retry_interval=None, buffer_limit=None, write_scheduler=None) -> None:
        self.write_type = write_type
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.jitter_interval = jitter_interval
        self.retry_interval = retry_interval
        self.buffer_limit = buffer_limit
        self.write_scheduler = write_scheduler


SYNCHRONOUS = WriteOptions(write_type=WriteType.synchronous)
ASYNCHRONOUS = WriteOptions(write_type=WriteType.asynchronous)


class WriteApiClient(AbstractClient):

    def __init__(self, service, write_options=WriteOptions()) -> None:
        self._write_service = service
        self._write_options = write_options

    def write(self, bucket, org, record, write_precision=None):

        if write_precision is None:
            write_precision = WritePrecision.NS

        final_string = ''

        if isinstance(record, str):
            final_string = record

        if isinstance(record, Point):
            final_string = record.to_line_protocol()

        if isinstance(record, list):
            lines = []
            for item in record:
                if isinstance(item, str):
                    lines.append(item)
                if isinstance(item, Point):
                    lines.append(item.to_line_protocol())
            final_string = '\n'.join(lines)

        _async_req = True if self._write_options.write_type == WriteType.asynchronous else False

        return self._write_service.post_write(org=org, bucket=bucket, body=final_string, precision=write_precision,
                                              async_req=_async_req)

    def flush(self):
        # TODO
        pass

    def __del__(self):
        pass
