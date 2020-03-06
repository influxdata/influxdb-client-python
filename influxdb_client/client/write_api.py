# coding: utf-8
import logging
from datetime import timedelta
from enum import Enum
from random import random
from time import sleep
from typing import Union, List

import os

import rx
from rx import operators as ops, Observable
from rx.scheduler import ThreadPoolScheduler
from rx.subject import Subject

from influxdb_client import WritePrecision, WriteService
from influxdb_client.client.abstract_client import AbstractClient
from influxdb_client.client.write.point import Point, DEFAULT_WRITE_PRECISION
from influxdb_client.rest import ApiException

logger = logging.getLogger(__name__)


class WriteType(Enum):
    batching = 1
    asynchronous = 2
    synchronous = 3


class WriteOptions(object):

    def __init__(self, write_type: WriteType = WriteType.batching,
                 batch_size=1_000, flush_interval=1_000,
                 jitter_interval=0,
                 retry_interval=1_000,
                 write_scheduler=ThreadPoolScheduler(max_workers=1)) -> None:
        """
        Creates write api configuration.

        :param write_type: methods of write (batching, asynchronous, synchronous)
        :param batch_size: the number of data point to collect in batch
        :param flush_interval: flush data at least in this interval
        :param jitter_interval: this is primarily to avoid large write spikes for users running a large number of
               client instances ie, a jitter of 5s and flush duration 10s means flushes will happen every 10-15s.
        :param retry_interval: the time to wait before retry unsuccessful write
        :param write_scheduler:
        """
        self.write_type = write_type
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.jitter_interval = jitter_interval
        self.retry_interval = retry_interval
        self.write_scheduler = write_scheduler

    def __getstate__(self):
        state = self.__dict__.copy()
        # Remove write scheduler
        del state['write_scheduler']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        # Init default write Scheduler
        self.write_scheduler = ThreadPoolScheduler(max_workers=1)


SYNCHRONOUS = WriteOptions(write_type=WriteType.synchronous)
ASYNCHRONOUS = WriteOptions(write_type=WriteType.asynchronous)


class PointSettings(object):

    def __init__(self, **default_tags) -> None:

        """
        Creates point settings for write api.

        :param default_tags: Default tags which will be added to each point written by api.
        """

        self.defaultTags = dict()

        for key, val in default_tags.items():
            self.add_default_tag(key, val)

    @staticmethod
    def _get_value(value):

        if value.startswith("${env."):
            return os.environ.get(value[6:-1])

        return value

    def add_default_tag(self, key, value) -> None:
        self.defaultTags[key] = self._get_value(value)


class _BatchItem(object):
    def __init__(self, key, data, size=1) -> None:
        self.key = key
        self.data = data
        self.size = size
        pass

    def __str__(self) -> str:
        return '_BatchItem[key:\'{}\', \'{}\']' \
            .format(str(self.key), str(self.size))


class _BatchItemKey(object):
    def __init__(self, bucket, org, precision=DEFAULT_WRITE_PRECISION) -> None:
        self.bucket = bucket
        self.org = org
        self.precision = precision
        pass

    def __hash__(self) -> int:
        return hash((self.bucket, self.org, self.precision))

    def __eq__(self, o: object) -> bool:
        return isinstance(o, self.__class__) \
               and self.bucket == o.bucket and self.org == o.org and self.precision == o.precision

    def __str__(self) -> str:
        return '_BatchItemKey[bucket:\'{}\', org:\'{}\', precision:\'{}\']' \
            .format(str(self.bucket), str(self.org), str(self.precision))


class _BatchResponse(object):
    def __init__(self, data: _BatchItem, exception=None):
        self.data = data
        self.exception = exception
        pass

    def __str__(self) -> str:
        return '_BatchResponse[status:\'{}\', \'{}\']' \
            .format("failed" if self.exception else "success", str(self.data))


def _body_reduce(batch_items):
    return b'\n'.join(map(lambda batch_item: batch_item.data, batch_items))


class WriteApi(AbstractClient):

    def __init__(self, influxdb_client, write_options: WriteOptions = WriteOptions(),
                 point_settings: PointSettings = PointSettings()) -> None:
        self._influxdb_client = influxdb_client
        self._write_service = WriteService(influxdb_client.api_client)
        self._write_options = write_options
        self._point_settings = point_settings

        if influxdb_client.default_tags:
            for key, value in influxdb_client.default_tags.items():
                self._point_settings.add_default_tag(key, value)

        if self._write_options.write_type is WriteType.batching:
            # Define Subject that listen incoming data and produces writes into InfluxDB
            self._subject = Subject()

            self._disposable = self._subject.pipe(
                # Split incoming data to windows by batch_size or flush_interval
                ops.window_with_time_or_count(count=write_options.batch_size,
                                              timespan=timedelta(milliseconds=write_options.flush_interval)),
                # Map  window into groups defined by 'organization', 'bucket' and 'precision'
                ops.flat_map(lambda window: window.pipe(
                    # Group window by 'organization', 'bucket' and 'precision'
                    ops.group_by(lambda batch_item: batch_item.key),
                    # Create batch (concatenation line protocols by \n)
                    ops.map(lambda group: group.pipe(
                        ops.to_iterable(),
                        ops.map(lambda xs: _BatchItem(key=group.key, data=_body_reduce(xs), size=len(xs))))),
                    ops.merge_all())),
                # Write data into InfluxDB (possibility to retry if its fail)
                ops.map(mapper=lambda batch: self._retryable(data=batch, delay=self._jitter_delay())),  #
                ops.merge_all())\
                .subscribe(self._on_next, self._on_error, self._on_complete)

        else:
            self._subject = None
            self._disposable = None

    def write(self, bucket: str, org: str = None,
              record: Union[
                  str, List['str'], Point, List['Point'], dict, List['dict'], bytes, List['bytes'], Observable] = None,
              write_precision: WritePrecision = DEFAULT_WRITE_PRECISION) -> None:
        """
        Writes time-series data into influxdb.

        :param str org: specifies the destination organization for writes; take either the ID or Name interchangeably; if both orgID and org are specified, org takes precedence. (required)
        :param str bucket: specifies the destination bucket for writes (required)
        :param WritePrecision write_precision: specifies the precision for the unix timestamps within the body line-protocol
        :param record: Points, line protocol, RxPY Observable to write

        """

        if org is None:
            org = self._influxdb_client.org

        if self._point_settings.defaultTags and record:
            for key, val in self._point_settings.defaultTags.items():
                if isinstance(record, dict):
                    record.get("tags")[key] = val
                else:
                    for r in record:
                        if isinstance(r, dict):
                            r.get("tags")[key] = val
                        elif isinstance(r, Point):
                            r.tag(key, val)

        if self._write_options.write_type is WriteType.batching:
            return self._write_batching(bucket, org, record, write_precision)

        final_string = self._serialize(record, write_precision)

        _async_req = True if self._write_options.write_type == WriteType.asynchronous else False

        return self._post_write(_async_req, bucket, org, final_string, write_precision)

    def flush(self):
        # TODO
        pass

    def __del__(self):
        if self._subject:
            self._subject.on_completed()
            self._subject.dispose()
            self._subject = None

            # Wait for finish writing
            while not self._disposable.is_disposed:
                sleep(0.1)

        if self._disposable:
            self._disposable = None
        pass

    def _serialize(self, record, write_precision) -> bytes:
        _result = b''
        if isinstance(record, bytes):
            _result = record

        elif isinstance(record, str):
            _result = record.encode("utf-8")

        elif isinstance(record, Point):
            _result = self._serialize(record.to_line_protocol(), write_precision=write_precision)

        elif isinstance(record, dict):
            _result = self._serialize(Point.from_dict(record, write_precision=write_precision),
                                      write_precision=write_precision)
        elif isinstance(record, list):
            _result = b'\n'.join([self._serialize(item, write_precision=write_precision) for item in record])

        return _result

    def _write_batching(self, bucket, org, data, precision=DEFAULT_WRITE_PRECISION):
        _key = _BatchItemKey(bucket, org, precision)
        if isinstance(data, bytes):
            self._subject.on_next(_BatchItem(key=_key, data=data))

        elif isinstance(data, str):
            self._write_batching(bucket, org, data.encode("utf-8"), precision)

        elif isinstance(data, Point):
            self._write_batching(bucket, org, data.to_line_protocol(), precision)

        elif isinstance(data, dict):
            self._write_batching(bucket, org, Point.from_dict(data, write_precision=precision), precision)

        elif isinstance(data, list):
            for item in data:
                self._write_batching(bucket, org, item, precision)

        elif isinstance(data, Observable):
            data.subscribe(lambda it: self._write_batching(bucket, org, it, precision))
            pass

        return None

    def _http(self, batch_item: _BatchItem):

        logger.debug("Write time series data into InfluxDB: %s", batch_item)

        self._post_write(False, batch_item.key.bucket, batch_item.key.org, batch_item.data,
                         batch_item.key.precision)

        logger.debug("Write request finished %s", batch_item)

        return _BatchResponse(data=batch_item)

    def _post_write(self, _async_req, bucket, org, body, precision):
        return self._write_service.post_write(org=org, bucket=bucket, body=body, precision=precision,
                                              async_req=_async_req, content_encoding="identity",
                                              content_type="text/plain; charset=utf-8")

    def _retryable(self, data: str, delay: timedelta):

        return rx.of(data).pipe(
            ops.subscribe_on(self._write_options.write_scheduler),
            # use delay if its specified
            ops.delay(duetime=delay, scheduler=self._write_options.write_scheduler),
            # invoke http call
            ops.map(lambda x: self._http(x)),
            # if there is an error than retry
            ops.catch(handler=lambda exception, source: self._retry_handler(exception, source, data)),
        )

    def _retry_handler(self, exception, source, data):

        if isinstance(exception, ApiException):

            if exception.status == 429 or exception.status == 503:
                _delay = self._jitter_delay() + timedelta(milliseconds=self._write_options.retry_interval)
                return self._retryable(data, delay=_delay)

        return rx.just(_BatchResponse(exception=exception, data=data))

    def _jitter_delay(self):
        return timedelta(milliseconds=random() * self._write_options.jitter_interval)

    @staticmethod
    def _on_next(response: _BatchResponse):
        if response.exception:
            logger.error("The batch item wasn't processed successfully because: %s", response.exception)
        else:
            logger.debug("The batch item: %s was processed successfully.", response)

    @staticmethod
    def _on_error(ex):
        logger.error("unexpected error during batching: %s", ex)

    def _on_complete(self):
        self._disposable.dispose()
        logger.info("the batching processor was disposed")

    def __getstate__(self):
        state = self.__dict__.copy()
        # Remove rx
        del state['_subject']
        del state['_disposable']
        del state['_write_service']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        # Init Rx
        self.__init__(self._influxdb_client, self._write_options, self._point_settings)
