# coding: utf-8


from rx.scheduler import NewThreadScheduler
from rx.subject import Subject

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

        # todo back pressure strategy


class WriteApiClient(AbstractClient):

    def __init__(self, service, write_options=None) -> None:
        self._write_api = service
        self.write_options = write_options

        _subject = Subject

    def write_record(self, org, bucket, record):
        # self.service.post
        #
        print(self._write_api)

    def write(self, bucket, record, org=None):

        if isinstance(record, Point):
            self._write_api.post_write(org, bucket, record.to_line_protocol())
        if isinstance(record, str):
            self._write_api.post_write(org, bucket, record)
        pass

    def flush(self):
        # TODO
        pass
