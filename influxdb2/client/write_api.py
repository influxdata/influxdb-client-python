# coding: utf-8
import logging
import time
from enum import Enum

import rx
from rx import operators as ops
from rx.core import GroupedObservable
from rx.scheduler import NewThreadScheduler
from rx.subject import Subject

from influxdb2 import WritePrecision
from influxdb2.client.abstract_client import AbstractClient
from influxdb2.client.write.point import Point

logger = logging.getLogger(__name__)


class WriteType(Enum):
    batching = 1
    asynchronous = 2
    synchronous = 3


class WriteOptions(object):

    def __init__(self, write_type=WriteType.batching, batch_size=1_000, flush_interval=None, jitter_interval=None,
                 retry_interval=None, buffer_limit=None, write_scheduler=None) -> None:
        self.write_type = write_type
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.jitter_interval = jitter_interval
        self.retry_interval = retry_interval
        self.buffer_limit = buffer_limit
        self.write_scheduler = write_scheduler


DEFAULT_WRITE_PRECISION = WritePrecision.NS
SYNCHRONOUS = WriteOptions(write_type=WriteType.synchronous)
ASYNCHRONOUS = WriteOptions(write_type=WriteType.asynchronous)


class _BatchItem(object):
    def __init__(self, key, data) -> None:
        self.key = key
        self.data = data
        pass

    def __str__(self) -> str:
        return '_BatchItem[key:\'{}\', \'{}\']'\
            .format(str(self.key), str(self.data))


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
        return '_BatchItemKey[bucket:\'{}\', org:\'{}\', precision:\'{}\']'\
            .format(str(self.bucket), str(self.org), str(self.precision))


class _BatchResponse(object):
    def __init__(self, data: _BatchItem, exception=None):
        self.data = data
        self.exception = exception
        pass

    def __str__(self) -> str:
        return '_BatchResponse[status:\'{}\', \'{}\']'\
            .format("failed" if self.exception else "success", str(self.data))


def _body_reduce(batch_items):
    return '\n'.join(map(lambda batch_item: batch_item.data, batch_items))


def _create_batch(group: GroupedObservable):
    return lambda xs: _BatchItem(key=group.key, data=_body_reduce(xs))


def _group_by(batch_item: _BatchItem):
    return batch_item.key


def _group_to_batch(group: GroupedObservable):
    return group.pipe(ops.to_iterable(),
                      ops.map(list),
                      ops.map(_create_batch(group)))


def _retry_handler(exception, source, data):
    return rx.just(_BatchResponse(exception=exception, data=data))


def _window_to_group(value):
    return value.pipe(
        ops.to_iterable(),
        ops.map(lambda x: rx.from_iterable(x).pipe(
            ops.group_by(_group_by), ops.map(_group_to_batch), ops.merge_all())), ops.merge_all())


class WriteApiClient(AbstractClient):

    def __init__(self, service, write_options=WriteOptions()) -> None:
        self._write_service = service
        self._write_options = write_options
        if self._write_options.write_type is WriteType.batching:
            self._subject = Subject()

            observable = self._subject.pipe(ops.observe_on(NewThreadScheduler()))
            self._disposable = observable\
                .pipe(ops.window_with_count(write_options.batch_size),
                      ops.flat_map(lambda v: _window_to_group(v)),
                      ops.map(mapper=lambda x: self._retryable(x)),
                      ops.merge_all()) \
                .subscribe(self._on_next, self._on_error)
        else:
            self._subject = None
            self._disposable = None

    def write(self, bucket, org, record, write_precision=None):

        if write_precision is None:
            write_precision = DEFAULT_WRITE_PRECISION

        if self._write_options.write_type is WriteType.batching:
            return self._write_batching(bucket, org, record, write_precision)

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

        return self._post_write(_async_req, bucket, org, final_string, write_precision)

    def flush(self):
        # TODO
        pass

    def _post_write(self, _async_req, bucket, org, body, precision):
        return self._write_service.post_write(org=org, bucket=bucket, body=body, precision=precision,
                                              async_req=_async_req)

    def __del__(self):
        if self._subject:
            self._subject.on_completed()
            self._subject.dispose()
            self._subject = None
            # TODO remove sleep
            time.sleep(2)
        if self._disposable:
            self._disposable.dispose()
            self._disposable = None
        pass

    def _write_batching(self, bucket, org, data, precision=DEFAULT_WRITE_PRECISION):
        _key = _BatchItemKey(bucket, org, precision)
        if isinstance(data, str):
            self._subject.on_next(_BatchItem(key=_key, data=data))

        elif isinstance(data, Point):
            self._subject.on_next(_BatchItem(key=_key, data=data.to_line_protocol()))
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, str):
                    self._subject.on_next(
                        _BatchItem(key=_key, data=item))
                if isinstance(item, Point):
                    self._subject.on_next(_BatchItem(key=_key, data=item.to_line_protocol()))
        return None

    def _http(self, batch_item: _BatchItem):

        logger.debug("http post to: %s", batch_item)

        self._post_write(False, batch_item.key.bucket, batch_item.key.org, batch_item.data,
                         batch_item.key.precision)

        return _BatchResponse(data=batch_item)

    def _retryable(self, data: str):

        return rx.of(data).pipe(
            ops.map(lambda x: self._http(x)),
            ops.catch(handler=lambda exception, source: _retry_handler(exception, source, data)),
        )

    @staticmethod
    def _on_next(response: _BatchResponse):
        if response.exception:
            logger.error("The batch item wasn't processed successfully because: %s", response.exception)
        else:
            logger.debug("The batch item: %s was processed successfully.", response)

    @staticmethod
    def _on_error(ex):
        logger.error("unexpected error during batching: %s", ex)

