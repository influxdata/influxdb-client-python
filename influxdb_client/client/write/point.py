import math
from builtins import int
from datetime import datetime, timedelta
from decimal import Decimal
from numbers import Integral

import dateutil.parser
from pytz import UTC
from six import iteritems

from influxdb_client.domain.write_precision import WritePrecision

EPOCH = UTC.localize(datetime.utcfromtimestamp(0))
DEFAULT_WRITE_PRECISION = WritePrecision.NS

class Point(object):
    """
    Point defines the values that will be written to the database.
    Ref: http://bit.ly/influxdata-point
    """

    @staticmethod
    def measurement(measurement):
        p = Point(measurement)
        return p

    def __init__(self, measurement_name):
        self._tags = {}
        self._fields = {}
        self._name = measurement_name
        self._time = None
        self._write_precision = DEFAULT_WRITE_PRECISION

    def time(self, time, write_precision=DEFAULT_WRITE_PRECISION):
        self._write_precision = write_precision
        self._time = time
        return self

    def tag(self, key, value):
        self._tags[key] = value
        return self

    def field(self, field, value):
        self._fields[field] = value
        return self

    def to_line_protocol(self):
        ret = _escape_tag(self._name)
        ret += _append_tags(self._tags)
        _fields = _append_fields(self._fields)
        if not _fields:
            return ""
        ret += _fields
        ret += _append_time(self._time, self._write_precision)
        return ret


def _append_tags(tags):
    _ret = ""
    for tag_key, tag_value in sorted(iteritems(tags)):

        if tag_value is None:
            continue

        tag = _escape_tag(tag_key)
        value = _escape_tag_value(tag_value)
        if tag != '' and value != '':
            _ret += ',' + tag_key + '=' + value
    return _ret + ' '


def _append_fields(fields):
    _ret = ""

    for field, value in sorted(iteritems(fields)):
        if value is None:
            continue

        if isinstance(value, float) or isinstance(value, Decimal):
            if not math.isfinite(value):
                continue
            _ret += _escape_tag(field) + '=' + str(value) + ','
        elif isinstance(value, int) and not isinstance(value, bool):
            _ret += _escape_tag(field) + '=' + str(value) + 'i' + ','
        elif isinstance(value, bool):
            _ret += _escape_tag(field) + '=' + str(value).lower() + ','
        elif isinstance(value, str):
            value = escape_value(str(value))
            _ret += _escape_tag(field) + "=" + '"' + value + '"' + ","
        else:
            raise ValueError()

    if _ret.endswith(","):
        return _ret[:-1]
    else:
        return _ret


def _append_time(time, write_precission):
    if time is None:
        return ''
    _ret = " " + str(int(_convert_timestamp(time, write_precission)))
    return _ret


def _escape_tag(tag):
    return str(tag).replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")


def _escape_tag_value(value):
    ret = _escape_tag(value)
    if ret.endswith('\\'):
        ret += ' '
    return ret


def escape_value(value):
    return value.translate(str.maketrans({'\"': r"\"", "\\": r"\\"}))


def _convert_timestamp(timestamp, precision=DEFAULT_WRITE_PRECISION):
    if isinstance(timestamp, Integral):
        return timestamp  # assume precision is correct if timestamp is int

    if isinstance(timestamp, str):
        timestamp = dateutil.parser.parse(timestamp)

    if isinstance(timestamp, timedelta) or isinstance(timestamp, datetime):

        if isinstance(timestamp, datetime):
            if not timestamp.tzinfo:
                timestamp = UTC.localize(timestamp)
            ns = (timestamp - EPOCH).total_seconds() * 1e9
        else:
            ns = timestamp.total_seconds() * 1e9

        if precision is None or precision == WritePrecision.NS:
            return ns
        elif precision == WritePrecision.US:
            return ns / 1e3
        elif precision == WritePrecision.MS:
            return ns / 1e6
        elif precision == WritePrecision.S:
            return ns / 1e9

        # elif precision == 'm':
        #     return ns / 1e9 / 60
        # elif precision == 'h':
        #     return ns / 1e9 / 3600

    raise ValueError(timestamp)
