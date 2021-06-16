"""Collect and write time series data to InfluxDB Cloud and InfluxDB OSS."""

# coding: utf-8
import logging
import os
from collections import defaultdict
from datetime import timedelta
from enum import Enum
from random import random
from time import sleep
from typing import Union, Any, Iterable

import rx
from rx import operators as ops, Observable
from rx.scheduler import ThreadPoolScheduler
from rx.subject import Subject

from influxdb_client import WritePrecision, WriteService
from influxdb_client.client.util.helpers import get_org_query_param
from influxdb_client.client.write.dataframe_serializer import data_frame_to_list_of_points
from influxdb_client.client.write.point import Point, DEFAULT_WRITE_PRECISION
from influxdb_client.client.write.retry import WritesRetry

logger = logging.getLogger(__name__)


class WriteType(Enum):
    """Configuration which type of writes will client use."""

    batching = 1
    asynchronous = 2
    synchronous = 3


class WriteOptions(object):
    """Write configuration."""

    def __init__(self, write_type: WriteType = WriteType.batching,
                 batch_size=1_000, flush_interval=1_000,
                 jitter_interval=0,
                 retry_interval=5_000,
                 max_retries=5,
                 max_retry_delay=125_000,
                 max_retry_time=180_000,
                 exponential_base=2,
                 write_scheduler=ThreadPoolScheduler(max_workers=1)) -> None:
        """
        Create write api configuration.

        :param write_type: methods of write (batching, asynchronous, synchronous)
        :param batch_size: the number of data point to collect in batch
        :param flush_interval: flush data at least in this interval
        :param jitter_interval: this is primarily to avoid large write spikes for users running a large number of
               client instances ie, a jitter of 5s and flush duration 10s means flushes will happen every 10-15s.
        :param retry_interval: the time to wait before retry unsuccessful write
        :param max_retries: the number of max retries when write fails, 0 means retry is disabled
        :param max_retry_delay: the maximum delay between each retry attempt in milliseconds
        :param max_retry_time: total timeout for all retry attempts in milliseconds, if 0 retry is disabled
        :param exponential_base: base for the exponential retry delay
        :param write_scheduler:
        """
        self.write_type = write_type
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.jitter_interval = jitter_interval
        self.retry_interval = retry_interval
        self.max_retries = max_retries
        self.max_retry_delay = max_retry_delay
        self.max_retry_time = max_retry_time
        self.exponential_base = exponential_base
        self.write_scheduler = write_scheduler

    def to_retry_strategy(self):
        """Create a Retry strategy from write options."""
        return WritesRetry(
            total=self.max_retries,
            retry_interval=self.retry_interval / 1_000,
            jitter_interval=self.jitter_interval / 1_000,
            max_retry_delay=self.max_retry_delay / 1_000,
            max_retry_time=self.max_retry_time / 1_000,
            exponential_base=self.exponential_base,
            method_whitelist=["POST"])

    def __getstate__(self):
        """Return a dict of attributes that you want to pickle."""
        state = self.__dict__.copy()
        # Remove write scheduler
        del state['write_scheduler']
        return state

    def __setstate__(self, state):
        """Set your object with the provided dict."""
        self.__dict__.update(state)
        # Init default write Scheduler
        self.write_scheduler = ThreadPoolScheduler(max_workers=1)


SYNCHRONOUS = WriteOptions(write_type=WriteType.synchronous)
ASYNCHRONOUS = WriteOptions(write_type=WriteType.asynchronous)


class PointSettings(object):
    """Settings to store default tags."""

    def __init__(self, **default_tags) -> None:
        """
        Create point settings for write api.

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
        """Add new default tag with key and value."""
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


class WriteApi:
    """Implementation for '/api/v2/write' endpoint."""

    def __init__(self, influxdb_client, write_options: WriteOptions = WriteOptions(),
                 point_settings: PointSettings = PointSettings()) -> None:
        """Initialize defaults."""
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
                ops.filter(lambda batch: batch.size > 0),
                ops.map(mapper=lambda batch: self._to_response(data=batch, delay=self._jitter_delay())),
                ops.merge_all()) \
                .subscribe(self._on_next, self._on_error, self._on_complete)

        else:
            self._subject = None
            self._disposable = None

    def write(self, bucket: str, org: str = None,
              record: Union[
                  str, Iterable['str'], Point, Iterable['Point'], dict, Iterable['dict'], bytes, Iterable['bytes'],
                  Observable] = None,
              write_precision: WritePrecision = DEFAULT_WRITE_PRECISION, **kwargs) -> Any:
        """
        Write time-series data into InfluxDB.

        :param str, Organization org: specifies the destination organization for writes;
                                      take the ID, Name or Organization;
                                      if it's not specified then is used default from client.org.
        :param str bucket: specifies the destination bucket for writes (required)
        :param WritePrecision write_precision: specifies the precision for the unix timestamps within
                                               the body line-protocol. The precision specified on a Point has precedes
                                               and is use for write.
        :param record: Points, line protocol, Pandas DataFrame, RxPY Observable to write
        :key data_frame_measurement_name: name of measurement for writing Pandas DataFrame
        :key data_frame_tag_columns: list of DataFrame columns which are tags, rest columns will be fields
        """
        org = get_org_query_param(org=org, client=self._influxdb_client)

        if self._point_settings.defaultTags and record is not None:
            for key, val in self._point_settings.defaultTags.items():
                self._append_default_tag(key, val, record)

        if self._write_options.write_type is WriteType.batching:
            return self._write_batching(bucket, org, record,
                                        write_precision, **kwargs)

        payloads = defaultdict(list)
        self._serialize(record, write_precision, payloads, **kwargs)

        _async_req = True if self._write_options.write_type == WriteType.asynchronous else False

        def write_payload(payload):
            final_string = b'\n'.join(payload[1])
            return self._post_write(_async_req, bucket, org, final_string, payload[0])

        results = list(map(write_payload, payloads.items()))
        if not _async_req:
            return None
        elif len(results) == 1:
            return results[0]
        return results

    def flush(self):
        """Flush data."""
        # TODO
        pass

    def close(self):
        """Flush data and dispose a batching buffer."""
        self.__del__()

    def __enter__(self):
        """
        Enter the runtime context related to this object.

        It will bind this method’s return value to the target(s)
        specified in the `as` clause of the statement.

        return: self instance
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the runtime context related to this object and close the WriteApi."""
        self.close()

    def __del__(self):
        """Close WriteApi."""
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

    def _serialize(self, record, write_precision, payload, **kwargs):
        if isinstance(record, bytes):
            payload[write_precision].append(record)

        elif isinstance(record, str):
            self._serialize(record.encode("utf-8"), write_precision, payload, **kwargs)

        elif isinstance(record, Point):
            self._serialize(record.to_line_protocol(), record.write_precision, payload, **kwargs)

        elif isinstance(record, dict):
            self._serialize(Point.from_dict(record, write_precision=write_precision),
                            write_precision, payload, **kwargs)
        elif 'DataFrame' in type(record).__name__:
            _data = data_frame_to_list_of_points(record, self._point_settings, write_precision, **kwargs)
            self._serialize(_data, write_precision, payload, **kwargs)

        elif isinstance(record, Iterable):
            for item in record:
                self._serialize(item, write_precision, payload, **kwargs)

    def _write_batching(self, bucket, org, data,
                        precision=DEFAULT_WRITE_PRECISION,
                        **kwargs):
        if isinstance(data, bytes):
            _key = _BatchItemKey(bucket, org, precision)
            self._subject.on_next(_BatchItem(key=_key, data=data))

        elif isinstance(data, str):
            self._write_batching(bucket, org, data.encode("utf-8"),
                                 precision, **kwargs)

        elif isinstance(data, Point):
            self._write_batching(bucket, org, data.to_line_protocol(), data.write_precision, **kwargs)

        elif isinstance(data, dict):
            self._write_batching(bucket, org, Point.from_dict(data, write_precision=precision),
                                 precision, **kwargs)

        elif 'DataFrame' in type(data).__name__:
            self._write_batching(bucket, org,
                                 data_frame_to_list_of_points(data, self._point_settings, precision, **kwargs),
                                 precision, **kwargs)

        elif isinstance(data, Iterable):
            for item in data:
                self._write_batching(bucket, org, item, precision, **kwargs)

        elif isinstance(data, Observable):
            data.subscribe(lambda it: self._write_batching(bucket, org, it, precision, **kwargs))
            pass

        return None

    def _append_default_tag(self, key, val, record):
        if isinstance(record, bytes) or isinstance(record, str):
            pass
        elif isinstance(record, Point):
            record.tag(key, val)
        elif isinstance(record, dict):
            record.setdefault("tags", {})
            record.get("tags")[key] = val
        elif isinstance(record, Iterable):
            for item in record:
                self._append_default_tag(key, val, item)

    def _http(self, batch_item: _BatchItem):

        logger.debug("Write time series data into InfluxDB: %s", batch_item)

        retry = self._write_options.to_retry_strategy()

        self._post_write(False, batch_item.key.bucket, batch_item.key.org, batch_item.data,
                         batch_item.key.precision, urlopen_kw={'retries': retry})

        logger.debug("Write request finished %s", batch_item)

        return _BatchResponse(data=batch_item)

    def _post_write(self, _async_req, bucket, org, body, precision, **kwargs):

        return self._write_service.post_write(org=org, bucket=bucket, body=body, precision=precision,
                                              async_req=_async_req, content_encoding="identity",
                                              content_type="text/plain; charset=utf-8",
                                              **kwargs)

    def _to_response(self, data: _BatchItem, delay: timedelta):

        return rx.of(data).pipe(
            ops.subscribe_on(self._write_options.write_scheduler),
            # use delay if its specified
            ops.delay(duetime=delay, scheduler=self._write_options.write_scheduler),
            # invoke http call
            ops.map(lambda x: self._http(x)),
            # catch exception to fail batch response
            ops.catch(handler=lambda exception, source: rx.just(_BatchResponse(exception=exception, data=data))),
        )

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
        """Return a dict of attributes that you want to pickle."""
        state = self.__dict__.copy()
        # Remove rx
        del state['_subject']
        del state['_disposable']
        del state['_write_service']
        return state

    def __setstate__(self, state):
        """Set your object with the provided dict."""
        self.__dict__.update(state)
        # Init Rx
        self.__init__(self._influxdb_client, self._write_options, self._point_settings)
