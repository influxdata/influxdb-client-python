"""Collect and async write time series data to InfluxDB Cloud or InfluxDB OSS."""
import logging
from asyncio import ensure_future, gather
from collections import defaultdict
from typing import Union, Iterable, NamedTuple

from influxdb_client import Point, WritePrecision
from influxdb_client.client._base import _BaseWriteApi, _HAS_DATACLASS
from influxdb_client.client.util.helpers import get_org_query_param
from influxdb_client.client.write.point import DEFAULT_WRITE_PRECISION
from influxdb_client.client.write_api import PointSettings

logger = logging.getLogger('influxdb_client.client.write_api_async')

if _HAS_DATACLASS:
    from dataclasses import dataclass


class WriteApiAsync(_BaseWriteApi):
    """
    Implementation for '/api/v2/write' endpoint.

    Example:
        .. code-block:: python

            from influxdb_client_async import InfluxDBClientAsync


            # Initialize async/await instance of Write API
            async with InfluxDBClientAsync(url="http://localhost:8086", token="my-token", org="my-org") as client:
                write_api = client.write_api()
    """

    def __init__(self, influxdb_client, point_settings: PointSettings = PointSettings()) -> None:
        """
        Initialize defaults.

        :param influxdb_client: with default settings (organization)
        :param point_settings: settings to store default tags.
        """
        super().__init__(influxdb_client=influxdb_client, point_settings=point_settings)

    async def write(self, bucket: str, org: str = None,
                    record: Union[str, Iterable['str'], Point, Iterable['Point'], dict, Iterable['dict'], bytes,
                                  Iterable['bytes'], NamedTuple, Iterable['NamedTuple'], 'dataclass',
                                  Iterable['dataclass']] = None,
                    write_precision: WritePrecision = DEFAULT_WRITE_PRECISION, **kwargs) -> bool:
        """
        Write time-series data into InfluxDB.

        :param str bucket: specifies the destination bucket for writes (required)
        :param str, Organization org: specifies the destination organization for writes;
                                      take the ID, Name or Organization.
                                      If not specified the default value from ``InfluxDBClientAsync.org`` is used.
        :param WritePrecision write_precision: specifies the precision for the unix timestamps within
                                               the body line-protocol. The precision specified on a Point has precedes
                                               and is use for write.
        :param record: Point, Line Protocol, Dictionary, NamedTuple, Data Classes, Pandas DataFrame
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
        :return: ``True`` for successfully accepted data, otherwise raise an exception

        Example:
            .. code-block:: python

                # Record as Line Protocol
                await write_api.write("my-bucket", "my-org", "h2o_feet,location=us-west level=125i 1")

                # Record as Dictionary
                dictionary = {
                    "measurement": "h2o_feet",
                    "tags": {"location": "us-west"},
                    "fields": {"level": 125},
                    "time": 1
                }
                await write_api.write("my-bucket", "my-org", dictionary)

                # Record as Point
                from influxdb_client import Point
                point = Point("h2o_feet").tag("location", "us-west").field("level", 125).time(1)
                await write_api.write("my-bucket", "my-org", point)

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

        payloads = defaultdict(list)
        self._serialize(record, write_precision, payloads, precision_from_point=True, **kwargs)

        futures = []
        for payload_precision, payload_line in payloads.items():
            futures.append(ensure_future
                           (self._write_service.post_write_async(org=org, bucket=bucket,
                                                                 body=b'\n'.join(payload_line),
                                                                 precision=payload_precision, async_req=False,
                                                                 _return_http_data_only=False,
                                                                 content_type="text/plain; charset=utf-8")))

        results = await gather(*futures, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                raise result

        return False not in [re[1] in (201, 204) for re in results]
