"""Collect and write time series data to InfluxDB Cloud or InfluxDB OSS."""

# coding: utf-8
import logging
import os
import warnings
from collections import defaultdict
from datetime import timedelta
from enum import Enum
from random import random
from time import sleep
from typing import Union, Any, Iterable, NamedTuple

import reactivex as rx
from reactivex import operators as ops, Observable
from reactivex.scheduler import ThreadPoolScheduler
from reactivex.subject import Subject

from influxdb_client import WritePrecision
from influxdb_client.client._base import _BaseWriteApi, _HAS_DATACLASS
from influxdb_client.client.util.helpers import get_org_query_param
from influxdb_client.client.write.dataframe_serializer import DataframeSerializer
from influxdb_client.client.write.point import Point, DEFAULT_WRITE_PRECISION
from influxdb_client.client.write.retry import WritesRetry
from influxdb_client.rest import _UTF_8_encoding

logger = logging.getLogger('influxdb_client.client.write_api')


if _HAS_DATACLASS:
    import dataclasses
    from dataclasses import dataclass


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
                 max_close_wait=300_000,
                 write_scheduler=ThreadPoolScheduler(max_workers=1)) -> None:
        """
        Create write api configuration.

        :param write_type: methods of write (batching, asynchronous, synchronous)
        :param batch_size: the number of data point to collect in batch
        :param flush_interval: flush data at least in this interval (milliseconds)
        :param jitter_interval: this is primarily to avoid large write spikes for users running a large number of
               client instances ie, a jitter of 5s and flush duration 10s means flushes will happen every 10-15s
               (milliseconds)
        :param retry_interval: the time to wait before retry unsuccessful write (milliseconds)
        :param max_retries: the number of max retries when write fails, 0 means retry is disabled
        :param max_retry_delay: the maximum delay between each retry attempt in milliseconds
        :param max_retry_time: total timeout for all retry attempts in milliseconds, if 0 retry is disabled
        :param exponential_base: base for the exponential retry delay
        :parama max_close_wait: the maximum time to wait for writes to be flushed if close() is called
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
        self.max_close_wait = max_close_wait

    def to_retry_strategy(self, **kwargs):
        """
        Create a Retry strategy from write options.

        :key retry_callback: The callable ``callback`` to run after retryable error occurred.
                             The callable must accept one argument:
                                - `Exception`: an retryable error
        """
        return WritesRetry(
            total=self.max_retries,
            retry_interval=self.retry_interval / 1_000,
            jitter_interval=self.jitter_interval / 1_000,
            max_retry_delay=self.max_retry_delay / 1_000,
            max_retry_time=self.max_retry_time / 1_000,
            exponential_base=self.exponential_base,
            retry_callback=kwargs.get("retry_callback", None),
            allowed_methods=["POST"])

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


class _BatchItem(object):
    def __init__(self, key: _BatchItemKey, data, size=1) -> None:
        self.key = key
        self.data = data
        self.size = size
        pass

    def to_key_tuple(self) -> (str, str, str):
        return self.key.bucket, self.key.org, self.key.precision

    def __str__(self) -> str:
        return '_BatchItem[key:\'{}\', size: \'{}\']' \
            .format(str(self.key), str(self.size))


class _BatchResponse(object):
    def __init__(self, data: _BatchItem, exception: Exception = None):
        self.data = data
        self.exception = exception
        pass

    def __str__(self) -> str:
        return '_BatchResponse[status:\'{}\', \'{}\']' \
            .format("failed" if self.exception else "success", str(self.data))


def _body_reduce(batch_items):
    return b'\n'.join(map(lambda batch_item: batch_item.data, batch_items))


class WriteApi(_BaseWriteApi):
    """
    Implementation for '/api/v2/write' endpoint.

    Example:
        .. code-block:: python

            from influxdb_client import InfluxDBClient
            from influxdb_client.client.write_api import SYNCHRONOUS


            # Initialize SYNCHRONOUS instance of WriteApi
            with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:
                write_api = client.write_api(write_options=SYNCHRONOUS)
    """

    def __init__(self,
                 influxdb_client,
                 write_options: WriteOptions = WriteOptions(),
                 point_settings: PointSettings = PointSettings(),
                 **kwargs) -> None:
        """
        Initialize defaults.

        :param influxdb_client: with default settings (organization)
        :param write_options: write api configuration
        :param point_settings: settings to store default tags.
        :key success_callback: The callable ``callback`` to run after successfully writen a batch.

                               The callable must accept two arguments:
                                    - `Tuple`: ``(bucket, organization, precision)``
                                    - `str`: written data

                               **[batching mode]**
        :key error_callback: The callable ``callback`` to run after unsuccessfully writen a batch.

                             The callable must accept three arguments:
                                - `Tuple`: ``(bucket, organization, precision)``
                                - `str`: written data
                                - `Exception`: an occurred error

                             **[batching mode]**
        :key retry_callback: The callable ``callback`` to run after retryable error occurred.

                             The callable must accept three arguments:
                                - `Tuple`: ``(bucket, organization, precision)``
                                - `str`: written data
                                - `Exception`: an retryable error

                             **[batching mode]**
        """
        super().__init__(influxdb_client=influxdb_client, point_settings=point_settings)
        self._write_options = write_options
        self._success_callback = kwargs.get('success_callback', None)
        self._error_callback = kwargs.get('error_callback', None)
        self._retry_callback = kwargs.get('retry_callback', None)
        self._window_scheduler = None

        if self._write_options.write_type is WriteType.batching:
            # Define Subject that listen incoming data and produces writes into InfluxDB
            self._subject = Subject()

            self._window_scheduler = ThreadPoolScheduler(1)
            self._disposable = self._subject.pipe(
                # Split incoming data to windows by batch_size or flush_interval
                ops.window_with_time_or_count(count=write_options.batch_size,
                                              timespan=timedelta(milliseconds=write_options.flush_interval),
                                              scheduler=self._window_scheduler),
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

        if self._write_options.write_type is WriteType.asynchronous:
            message = """The 'WriteType.asynchronous' is deprecated and will be removed in future major version.

You can use native asynchronous version of the client:
- https://influxdb-client.readthedocs.io/en/stable/usage.html#how-to-use-asyncio
        """
            warnings.warn(message, DeprecationWarning)

    def write(self, bucket: str, org: str = None,
              record: Union[
                  str, Iterable['str'], Point, Iterable['Point'], dict, Iterable['dict'], bytes, Iterable['bytes'],
                  Observable, NamedTuple, Iterable['NamedTuple'], 'dataclass', Iterable['dataclass']
              ] = None,
              write_precision: WritePrecision = DEFAULT_WRITE_PRECISION, **kwargs) -> Any:
        """
        Write time-series data into InfluxDB.

        :param str bucket: specifies the destination bucket for writes (required)
        :param str, Organization org: specifies the destination organization for writes;
                                      take the ID, Name or Organization.
                                      If not specified the default value from ``InfluxDBClient.org`` is used.
        :param WritePrecision write_precision: specifies the precision for the unix timestamps within
                                               the body line-protocol. The precision specified on a Point has precedes
                                               and is use for write.
        :param record: Point, Line Protocol, Dictionary, NamedTuple, Data Classes, Pandas DataFrame or
                       RxPY Observable to write
        :key data_frame_measurement_name: name of measurement for writing Pandas DataFrame - ``DataFrame``
        :key data_frame_tag_columns: list of DataFrame columns which are tags,
                                     rest columns will be fields - ``DataFrame``
        :key data_frame_timestamp_column: name of DataFrame column which contains a timestamp. The column can be defined as a :class:`~str` value
                                          formatted as `2018-10-26`, `2018-10-26 12:00`, `2018-10-26 12:00:00-05:00`
                                          or other formats and types supported by `pandas.to_datetime <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.to_datetime.html#pandas.to_datetime>`_ - ``DataFrame``
        :key data_frame_timestamp_timezone: name of the timezone which is used for timestamp column - ``DataFrame``
        :key record_measurement_key: key of record with specified measurement -
                                     ``dictionary``, ``NamedTuple``, ``dataclass``
        :key record_measurement_name: static measurement name - ``dictionary``, ``NamedTuple``, ``dataclass``
        :key record_time_key: key of record with specified timestamp - ``dictionary``, ``NamedTuple``, ``dataclass``
        :key record_tag_keys: list of record keys to use as a tag - ``dictionary``, ``NamedTuple``, ``dataclass``
        :key record_field_keys: list of record keys to use as a field  - ``dictionary``, ``NamedTuple``, ``dataclass``

        Example:
            .. code-block:: python

                # Record as Line Protocol
                write_api.write("my-bucket", "my-org", "h2o_feet,location=us-west level=125i 1")

                # Record as Dictionary
                dictionary = {
                    "measurement": "h2o_feet",
                    "tags": {"location": "us-west"},
                    "fields": {"level": 125},
                    "time": 1
                }
                write_api.write("my-bucket", "my-org", dictionary)

                # Record as Point
                from influxdb_client import Point
                point = Point("h2o_feet").tag("location", "us-west").field("level", 125).time(1)
                write_api.write("my-bucket", "my-org", point)

        DataFrame:
            If the ``data_frame_timestamp_column`` is not specified the index of `Pandas DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_
            is used as a ``timestamp`` for written data. The index can be `PeriodIndex <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.PeriodIndex.html#pandas.PeriodIndex>`_
            or its must be transformable to ``datetime`` by
            `pandas.to_datetime <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.to_datetime.html#pandas.to_datetime>`_.

            If you would like to transform a column to ``PeriodIndex``, you can use something like:

            .. code-block:: python

                import pandas as pd

                # DataFrame
                data_frame = ...
                # Set column as Index
                data_frame.set_index('column_name', inplace=True)
                # Transform index to PeriodIndex
                data_frame.index = pd.to_datetime(data_frame.index, unit='s')

        """  # noqa: E501
        org = get_org_query_param(org=org, client=self._influxdb_client)

        self._append_default_tags(record)

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

            """
            We impose a maximum wait time to ensure that we do not cause a deadlock if the
            background thread has exited abnormally

            Each iteration waits 100ms, but sleep expects the unit to be seconds so convert
            the maximum wait time to seconds.

            We keep a counter of how long we've waited
            """
            max_wait_time = self._write_options.max_close_wait / 1000
            waited = 0
            sleep_period = 0.1

            # Wait for writing to finish
            while not self._disposable.is_disposed:
                sleep(sleep_period)
                waited += sleep_period

                # Have we reached the upper limit?
                if waited >= max_wait_time:
                    logger.warning(
                        "Reached max_close_wait (%s seconds) waiting for batches to finish writing. Force closing",
                        max_wait_time
                    )
                    break

        if self._window_scheduler:
            self._window_scheduler.executor.shutdown(wait=False)
            self._window_scheduler = None

        if self._disposable:
            self._disposable = None
        pass

    def _write_batching(self, bucket, org, data,
                        precision=DEFAULT_WRITE_PRECISION,
                        **kwargs):
        if isinstance(data, bytes):
            _key = _BatchItemKey(bucket, org, precision)
            self._subject.on_next(_BatchItem(key=_key, data=data))

        elif isinstance(data, str):
            self._write_batching(bucket, org, data.encode(_UTF_8_encoding),
                                 precision, **kwargs)

        elif isinstance(data, Point):
            self._write_batching(bucket, org, data.to_line_protocol(), data.write_precision, **kwargs)

        elif isinstance(data, dict):
            self._write_batching(bucket, org, Point.from_dict(data, write_precision=precision, **kwargs),
                                 precision, **kwargs)

        elif 'DataFrame' in type(data).__name__:
            serializer = DataframeSerializer(data, self._point_settings, precision, self._write_options.batch_size,
                                             **kwargs)
            for chunk_idx in range(serializer.number_of_chunks):
                self._write_batching(bucket, org,
                                     serializer.serialize(chunk_idx),
                                     precision, **kwargs)
        elif hasattr(data, "_asdict"):
            # noinspection PyProtectedMember
            self._write_batching(bucket, org, data._asdict(), precision, **kwargs)

        elif _HAS_DATACLASS and dataclasses.is_dataclass(data):
            self._write_batching(bucket, org, dataclasses.asdict(data), precision, **kwargs)

        elif isinstance(data, Iterable):
            for item in data:
                self._write_batching(bucket, org, item, precision, **kwargs)

        elif isinstance(data, Observable):
            data.subscribe(lambda it: self._write_batching(bucket, org, it, precision, **kwargs))
            pass

        return None

    def _http(self, batch_item: _BatchItem):

        logger.debug("Write time series data into InfluxDB: %s", batch_item)

        if self._retry_callback:
            def _retry_callback_delegate(exception):
                return self._retry_callback(batch_item.to_key_tuple(), batch_item.data, exception)
        else:
            _retry_callback_delegate = None

        retry = self._write_options.to_retry_strategy(retry_callback=_retry_callback_delegate)

        self._post_write(False, batch_item.key.bucket, batch_item.key.org, batch_item.data,
                         batch_item.key.precision, urlopen_kw={'retries': retry})

        logger.debug("Write request finished %s", batch_item)

        return _BatchResponse(data=batch_item)

    def _post_write(self, _async_req, bucket, org, body, precision, **kwargs):

        return self._write_service.post_write(org=org, bucket=bucket, body=body, precision=precision,
                                              async_req=_async_req,
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

    def _on_next(self, response: _BatchResponse):
        if response.exception:
            logger.error("The batch item wasn't processed successfully because: %s", response.exception)
            if self._error_callback:
                try:
                    self._error_callback(response.data.to_key_tuple(), response.data.data, response.exception)
                except Exception as e:
                    """
                    Unfortunately, because callbacks are user-provided generic code, exceptions can be entirely
                    arbitrary

                    We trap it, log that it occurred and then proceed - there's not much more that we can
                    really do.
                    """
                    logger.error("The configured error callback threw an exception: %s", e)

        else:
            logger.debug("The batch item: %s was processed successfully.", response)
            if self._success_callback:
                try:
                    self._success_callback(response.data.to_key_tuple(), response.data.data)
                except Exception as e:
                    logger.error("The configured success callback threw an exception: %s", e)

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
        del state['_window_scheduler']
        del state['_write_service']
        return state

    def __setstate__(self, state):
        """Set your object with the provided dict."""
        self.__dict__.update(state)
        # Init Rx
        self.__init__(self._influxdb_client,
                      self._write_options,
                      self._point_settings,
                      success_callback=self._success_callback,
                      error_callback=self._error_callback,
                      retry_callback=self._retry_callback)
