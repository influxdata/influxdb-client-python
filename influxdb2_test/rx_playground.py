import datetime
import time
from threading import current_thread

import rx
from rx import operators as ops
from rx.core import GroupedObservable
from rx.scheduler import ThreadPoolScheduler
from rx.subject import Subject


class _WriterKey(object):

    def __init__(self, key):
        self.key = key

    def __hash__(self) -> int:
        return hash((self.key, self.key))

    def __eq__(self, o: object) -> bool:
        return isinstance(o, self.__class__) and self.key == o.key and self.key == o.key

    def __str__(self) -> str:
        return 'key[\'{}\']'.format(self.key)


class _Notification(object):

    def __init__(self, data, exception=None):
        self.data = data
        self.exception = exception
        pass

    def __str__(self) -> str:
        return '_Notification[status:\'{}\', \'{}\']'\
            .format("failed" if self.exception else "success", self.data)


class _RxWriter(object):

    success_count = 0
    failed_count = 0

    def __init__(self) -> None:
        self._subject = Subject()
        obs = self._subject.pipe(ops.observe_on(ThreadPoolScheduler(max_workers=1)))
        self._disposable = obs \
            .pipe(ops.window_with_time_or_count(count=5, timespan=datetime.timedelta(days=3, milliseconds=4)),
                  ops.flat_map(lambda x: self._window_to_group(x)),
                  ops.map(mapper=lambda x: self._retryable(x)),
                  ops.merge_all()) \
            .subscribe(self._result)
        pass

    def __del__(self):
        if self._subject:
            self._subject.on_completed()
            self._subject.dispose()
            self._subject = None

        time.sleep(2)

        if self._disposable:
            self._disposable.dispose()
            self._disposable = None
        pass

    def _window_to_group(self, value):
        return value.pipe(
            ops.to_iterable(),
            ops.map(lambda x: rx.from_iterable(x).pipe(
                ops.group_by(_group_by), ops.map(_group_to_batch), ops.merge_all())),
            ops.merge_all())

    def _retryable(self, data: str):

        return rx.of(data).pipe(
            ops.map(lambda x: self._http(x)),
            ops.catch(handler=lambda exception, source: _retry_handler(exception, source, data)),
        )

    def _http(self, data: str):
        if "gamma" in data:
            print('bad request[{}]: {}'.format(current_thread().name, data))
            raise Exception('unexpected token: {}'.format(data))
            pass

        print("http[" + current_thread().name + "]: " + data)
        return _Notification(data=data)

    def write(self, data: str):
        print("write[" + current_thread().name + "]")
        self._subject.on_next(data)
        pass

    def _result(self, data: _Notification):
        print("result[" + current_thread().name + "]: " + str(data))
        if data.exception:
            self.failed_count += 1
        else:
            self.success_count += 1
        pass

    def _error(self, error):
        print(error)


def _retry_handler(exception, source, data):
    print('retry_handler: {}, source: {}'.format(exception, source))
    notification = _Notification(exception=exception, data=data)

    return rx.just(notification)


def _create_batch(group: GroupedObservable):
    return lambda xs: '{}: {}'.format(str(group.key), ', '.join(xs))


def _group_by(v):
    # print("_group_by[" + current_thread().name + "]")
    return _WriterKey(v[0])


def _group_to_batch(group: GroupedObservable):
    return group.pipe(ops.to_iterable(),
                      ops.map(list),
                      ops.map(_create_batch(group)))


rxWriter = _RxWriter()

print("\n== init[" + current_thread().name + "] ==\n")

rxWriter.write("alpha")
rxWriter.write("beta")
rxWriter.write("brick")
rxWriter.write("racket")
rxWriter.write("bat")

rxWriter.write("apple")
rxWriter.write("gamma")

time.sleep(2)

rxWriter.write("apricot")
rxWriter.write("root")
rxWriter.write("delta")

rxWriter.write("double")
rxWriter.write("backpack")
rxWriter.write("giant")
rxWriter.write("balloon")

print("\n== finish writing ==\n")
time.sleep(2)

print("\n== __del__ ==\n")
rxWriter.__del__()

print("\n== finished ==\n")
time.sleep(2)

print('success: {}, failed: {}'.format(rxWriter.success_count, rxWriter.failed_count))
