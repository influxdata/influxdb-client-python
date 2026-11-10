"""Timestamp boundary regressions through the public Point API."""

import pandas as pd
import pytest

from influxdb_client import Point, WritePrecision
from influxdb_client.client.util import date_utils
from influxdb_client.client.util.date_utils_pandas import PandasDateTimeHelper


@pytest.mark.parametrize("precision, divisor", [
    (WritePrecision.US, 1000),
    (WritePrecision.MS, 1000000),
    (WritePrecision.S, 1000000000),
])
@pytest.mark.parametrize("sign", [1, -1])
@pytest.mark.parametrize("on_boundary", [False, True])
def test_point_timestamp_boundary(monkeypatch, precision, divisor, sign, on_boundary):
    monkeypatch.setattr(date_utils, "date_helper", PandasDateTimeHelper())
    magnitude = 1700000000000000000 + divisor - (0 if on_boundary else 1)
    stamp = pd.Timestamp(sign * magnitude, unit="ns", tz="UTC")
    point = Point("sensor").field("value", 1).time(stamp, precision)
    assert point.to_line_protocol() == f"sensor value=1i {sign * (magnitude // divisor)}"


@pytest.mark.parametrize("sign", [1, -1])
def test_point_nanosecond_timestamp_unchanged(monkeypatch, sign):
    monkeypatch.setattr(date_utils, "date_helper", PandasDateTimeHelper())
    ns = sign * 1700000000000000999
    point = Point("sensor").field("value", 1).time(pd.Timestamp(ns, unit="ns", tz="UTC"))
    assert point.to_line_protocol() == f"sensor value=1i {ns}"
