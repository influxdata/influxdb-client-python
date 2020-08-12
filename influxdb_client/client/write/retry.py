"""Implementation for Retry strategy during HTTP requests."""

from itertools import takewhile
from random import random

from urllib3 import Retry


class WritesRetry(Retry):
    """
    Writes retry configuration.

    :param int jitter_interval: random milliseconds when retrying writes
    :param int max_retry_delay: maximum delay when retrying write
    :param int exponential_base: base for the exponential retry delay, the next delay is computed as
                                 `backoff_factor * exponential_base^(attempts-1) + random(jitter_interval)`
    """

    def __init__(self, jitter_interval=0, max_retry_delay=180, exponential_base=5, **kw):
        """Initialize defaults."""
        super().__init__(**kw)
        self.jitter_interval = jitter_interval
        self.max_retry_delay = max_retry_delay
        self.exponential_base = exponential_base

    def new(self, **kw):
        """Initialize defaults."""
        if 'jitter_interval' not in kw:
            kw['jitter_interval'] = self.jitter_interval
        if 'max_retry_delay' not in kw:
            kw['max_retry_delay'] = self.max_retry_delay
        if 'exponential_base' not in kw:
            kw['exponential_base'] = self.exponential_base
        return super().new(**kw)

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

        backoff_value = self.backoff_factor * (self.exponential_base ** consecutive_errors_len) + self._jitter_delay()
        return min(self.max_retry_delay, backoff_value)

    def get_retry_after(self, response):
        """Get the value of Retry-After header and append random jitter delay."""
        retry_after = super().get_retry_after(response)
        if retry_after:
            retry_after += self._jitter_delay()
        return retry_after

    def _jitter_delay(self):
        return self.jitter_interval * random()
