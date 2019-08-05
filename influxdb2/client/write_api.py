# coding: utf-8


from rx.scheduler import NewThreadScheduler
from rx.subject import Subject

from influxdb2 import WritePrecision
from influxdb2.client.abstract_client import AbstractClient
from influxdb2.client.write.point import Point


class WriteOptions(object):

    def __init__(self, batch_size=5000, flush_interval=1000, jitter_interval=0, retry_interval=1000,
                 buffer_limit=10000,
                 write_scheduler=NewThreadScheduler) -> None:
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.jitter_interval = jitter_interval
        self.retry_interval = retry_interval
        self.buffer_limit = buffer_limit
        self.write_scheduler = write_scheduler


class WriteApiClient(AbstractClient):

    def __init__(self, service, write_options=None) -> None:
        self._write_service = service
        self.write_options = write_options

        _subject = Subject

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

        return self._write_service.post_write(org=org, bucket=bucket, body=final_string, precision=write_precision)

    def flush(self):
        # TODO
        pass
