"""Utils to get right Date parsing function."""
import datetime

from dateutil import parser
from pytz import UTC

date_helper = None


class DateHelper:
    """DateHelper to groups different implementations of date operations."""

    def __init__(self, timezone: datetime.tzinfo = UTC) -> None:
        """
        Initialize defaults.

        :param timezone: Default timezone used for serialization "datetime" without "tzinfo".
                         Default value is "UTC".
        """
        self.timezone = timezone

    def parse_date(self, date_string: str):
        """
        Parse string into Date or Timestamp.

        :return: Returns a :class:`datetime.datetime` object or compliant implementation
                 like :class:`class 'pandas._libs.tslibs.timestamps.Timestamp`
        """
        pass

    def to_nanoseconds(self, delta):
        """
        Get number of nanoseconds in timedelta.

        Solution comes from v1 client. Thx.
        https://github.com/influxdata/influxdb-python/pull/811
        """
        nanoseconds_in_days = delta.days * 86400 * 10 ** 9
        nanoseconds_in_seconds = delta.seconds * 10 ** 9
        nanoseconds_in_micros = delta.microseconds * 10 ** 3

        return nanoseconds_in_days + nanoseconds_in_seconds + nanoseconds_in_micros

    def to_utc(self, value: datetime):
        """
        Convert datetime to UTC timezone.

        :param value: datetime
        :return: datetime in UTC
        """
        if not value.tzinfo:
            return self.to_utc(value.replace(tzinfo=self.timezone))
        else:
            return value.astimezone(UTC)


def get_date_helper() -> DateHelper:
    """
    Return DateHelper with proper implementation.

    If there is a 'ciso8601' than use 'ciso8601.parse_datetime' else use 'dateutil.parse'.
    """
    global date_helper
    if date_helper is None:
        date_helper = DateHelper()
        try:
            import ciso8601
            date_helper.parse_date = ciso8601.parse_datetime
        except ModuleNotFoundError:
            date_helper.parse_date = parser.parse

    return date_helper
