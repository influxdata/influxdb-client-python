"""Implementation for Retry strategy during HTTP requests."""

import logging
from datetime import datetime, timedelta
from itertools import takewhile
from random import random
from typing import Callable

from urllib3 import Retry
from urllib3.exceptions import MaxRetryError, ResponseError

from influxdb_client.client.exceptions import InfluxDBError

logger = logging.getLogger('influxdb_client.client.write.retry')


class WritesRetry(Retry):
    """
    Writes retry configuration.

    The next delay is computed as random value between range
        `retry_interval * exponential_base^(attempts-1)` and `retry_interval * exponential_base^(attempts)

    Example:
        for retry_interval=5, exponential_base=2, max_retry_delay=125, total=5
        retry delays are random distributed values within the ranges of
        [5-10, 10-20, 20-40, 40-80, 80-125]
    """

    def __init__(self, jitter_interval=0, max_retry_delay=125, exponential_base=2, max_retry_time=180, total=5,
                 retry_interval=5, retry_callback: Callable[[Exception], int] = None, **kw):
        """
        Initialize defaults.

        :param int jitter_interval: random milliseconds when retrying writes
        :param num max_retry_delay: maximum delay when retrying write in seconds
        :param int max_retry_time: maximum total retry timeout in seconds,
                                   attempt after this timout throws MaxRetryError
        :param int total: maximum number of retries
        :param num retry_interval: initial first retry delay range in seconds
        :param int exponential_base: base for the exponential retry delay,
        :param Callable[[Exception], int] retry_callback: the callable ``callback`` to run after retryable
                                                          error occurred.
                                                          The callable must accept one argument:
                                                                - `Exception`: an retryable error
        """
        super().__init__(**kw)
        self.jitter_interval = jitter_interval
        self.total = total
        self.retry_interval = retry_interval
        self.max_retry_delay = max_retry_delay
        self.max_retry_time = max_retry_time
        self.exponential_base = exponential_base
        self.retry_timeout = datetime.now() + timedelta(seconds=max_retry_time)
        self.retry_callback = retry_callback

    def new(self, **kw):
        """Initialize defaults."""
        if 'jitter_interval' not in kw:
            kw['jitter_interval'] = self.jitter_interval
        if 'retry_interval' not in kw:
            kw['retry_interval'] = self.retry_interval
        if 'max_retry_delay' not in kw:
            kw['max_retry_delay'] = self.max_retry_delay
        if 'max_retry_time' not in kw:
            kw['max_retry_time'] = self.max_retry_time
        if 'exponential_base' not in kw:
            kw['exponential_base'] = self.exponential_base
        if 'retry_callback' not in kw:
            kw['retry_callback'] = self.retry_callback

        new = super().new(**kw)
        new.retry_timeout = self.retry_timeout
        return new

    def is_retry(self, method, status_code, has_retry_after=False):
        """is_retry doesn't require retry_after header. If there is not Retry-After we will use backoff."""
        if not self._is_method_retryable(method):
            return False

        return self.total and (status_code >= 429)

    def get_backoff_time(self):
        """Variant of exponential backoff with initial and max delay and a random jitter delay."""
        # We want to consider only the last consecutive errors sequence (Ignore redirects).
        consecutive_errors_len = len(
            list(
                takewhile(lambda x: x.redirect_location is None, reversed(self.history))
            )
        )
        # First fail doesn't increase backoff
        consecutive_errors_len -= 1
        if consecutive_errors_len < 0:
            return 0

        range_start = self.retry_interval
        range_stop = self.retry_interval * self.exponential_base

        i = 1
        while i <= consecutive_errors_len:
            i += 1
            range_start = range_stop
            range_stop = range_stop * self.exponential_base
            if range_stop > self.max_retry_delay:
                break

        if range_stop > self.max_retry_delay:
            range_stop = self.max_retry_delay

        return range_start + (range_stop - range_start) * self._random()

    def get_retry_after(self, response):
        """Get the value of Retry-After header and append random jitter delay."""
        retry_after = super().get_retry_after(response)
        if retry_after:
            retry_after += self._jitter_delay()
        return retry_after

    def increment(self, method=None, url=None, response=None, error=None, _pool=None, _stacktrace=None):
        """Return a new Retry object with incremented retry counters."""
        if self.retry_timeout < datetime.now():
            raise MaxRetryError(_pool, url, error or ResponseError("max_retry_time exceeded"))

        new_retry = super().increment(method, url, response, error, _pool, _stacktrace)

        if response is not None:
            parsed_error = InfluxDBError(response=response)
        elif error is not None:
            parsed_error = error
        else:
            parsed_error = f"Failed request to: {url}"

        message = f"The retriable error occurred during request. Reason: '{parsed_error}'."
        if isinstance(parsed_error, InfluxDBError):
            message += f" Retry in {parsed_error.retry_after}s."

        if self.retry_callback:
            self.retry_callback(parsed_error)

        logger.warning(message)

        return new_retry

    def _jitter_delay(self):
        return self.jitter_interval * random()

    def _random(self):
        return random()
