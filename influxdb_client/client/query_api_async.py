"""
Querying InfluxDB by FluxLang.

Flux is InfluxData’s functional data scripting language designed for querying, analyzing, and acting on data.
"""
from typing import List, AsyncGenerator

from influxdb_client.client._base import _BaseQueryApi
from influxdb_client.client.flux_table import FluxTable, FluxRecord
from influxdb_client.client.query_api import QueryOptions
from influxdb_client.rest import _UTF_8_encoding


class QueryApiAsync(_BaseQueryApi):
    """Asynchronous implementation for '/api/v2/query' endpoint."""

    def __init__(self, influxdb_client, query_options=QueryOptions()):
        """
        Initialize query client.

        :param influxdb_client: influxdb client
        """
        super().__init__(influxdb_client=influxdb_client, query_options=query_options)

    async def query(self, query: str, org=None, params: dict = None) -> List['FluxTable']:
        """
        Execute asynchronous Flux query and return result as a List[:class:`~influxdb_client.client.flux_table.FluxTable`].

        :param query: the Flux query
        :param str, Organization org: specifies the organization for executing the query;
                                      Take the ``ID``, ``Name`` or ``Organization``.
                                      If not specified the default value from ``InfluxDBClientAsync.org`` is used.
        :param params: bind parameters
        :return: List of :class:`~influxdb_client.client.flux_table.FluxTable`.
        """  # noqa: E501
        org = self._org_param(org)

        response = await self._query_api.post_query_async(org=org,
                                                          query=self._create_query(query, self.default_dialect, params),
                                                          async_req=False, _preload_content=False,
                                                          _return_http_data_only=True)

        return await self._to_tables_async(response, query_options=self._get_query_options())

    async def query_stream(self, query: str, org=None, params: dict = None) -> AsyncGenerator['FluxRecord', None]:
        """
        Execute asynchronous Flux query and return stream of :class:`~influxdb_client.client.flux_table.FluxRecord` as an AsyncGenerator[:class:`~influxdb_client.client.flux_table.FluxRecord`].

        :param query: the Flux query
        :param str, Organization org: specifies the organization for executing the query;
                                      Take the ``ID``, ``Name`` or ``Organization``.
                                      If not specified the default value from ``InfluxDBClientAsync.org`` is used.
        :param params: bind parameters
        :return: AsyncGenerator[:class:`~influxdb_client.client.flux_table.FluxRecord`]
        """  # noqa: E501
        org = self._org_param(org)

        response = await self._query_api.post_query_async(org=org,
                                                          query=self._create_query(query, self.default_dialect, params),
                                                          async_req=False, _preload_content=False,
                                                          _return_http_data_only=True)

        return await self._to_flux_record_stream_async(response, query_options=self._get_query_options())

    async def query_data_frame(self, query: str, org=None, data_frame_index: List[str] = None, params: dict = None):
        """
        Execute asynchronous Flux query and return :class:`~pandas.core.frame.DataFrame`.

        Note that if a query returns tables with differing schemas than the client generates
        a DataFrame for each of them.

        :param query: the Flux query
        :param str, Organization org: specifies the organization for executing the query;
                                      Take the ``ID``, ``Name`` or ``Organization``.
                                      If not specified the default value from ``InfluxDBClientAsync.org`` is used.
        :param data_frame_index: the list of columns that are used as DataFrame index
        :param params: bind parameters
        :return: :class:`~pandas.core.frame.DataFrame` or List of :class:`~pandas.core.frame.DataFrame`
        """
        _generator = await self.query_data_frame_stream(query, org=org, data_frame_index=data_frame_index,
                                                        params=params)

        dataframes = []
        async for dataframe in _generator:
            dataframes.append(dataframe)

        return self._to_data_frames(dataframes)

    async def query_data_frame_stream(self, query: str, org=None, data_frame_index: List[str] = None,
                                      params: dict = None):
        """
        Execute asynchronous Flux query and return stream of :class:`~pandas.core.frame.DataFrame` as an AsyncGenerator[:class:`~pandas.core.frame.DataFrame`].

        Note that if a query returns tables with differing schemas than the client generates
        a DataFrame for each of them.

        :param query: the Flux query
        :param str, Organization org: specifies the organization for executing the query;
                                      Take the ``ID``, ``Name`` or ``Organization``.
                                      If not specified the default value from ``InfluxDBClientAsync.org`` is used.
        :param data_frame_index: the list of columns that are used as DataFrame index
        :param params: bind parameters
        :return: AsyncGenerator[:class:`~pandas.core.frame.DataFrame`]
        """  # noqa: E501
        org = self._org_param(org)

        response = await self._query_api.post_query_async(org=org,
                                                          query=self._create_query(query, self.default_dialect, params),
                                                          async_req=False, _preload_content=False,
                                                          _return_http_data_only=True)

        return await self._to_data_frame_stream_async(data_frame_index=data_frame_index, response=response,
                                                      query_options=self._get_query_options())

    async def query_raw(self, query: str, org=None, dialect=_BaseQueryApi.default_dialect, params: dict = None):
        """
        Execute asynchronous Flux query and return result as raw unprocessed result as a str.

        :param query: a Flux query
        :param str, Organization org: specifies the organization for executing the query;
                                      Take the ``ID``, ``Name`` or ``Organization``.
                                      If not specified the default value from ``InfluxDBClientAsync.org`` is used.
        :param dialect: csv dialect format
        :param params: bind parameters
        :return: :class:`~str`
        """
        org = self._org_param(org)
        result = await self._query_api.post_query_async(org=org, query=self._create_query(query, dialect, params),
                                                        async_req=False, _preload_content=False,
                                                        _return_http_data_only=True)
        raw_bytes = await result.read()
        return raw_bytes.decode(_UTF_8_encoding)
