"""
Querying InfluxDB bu FluxLang.

Flux is InfluxData’s functional data scripting language designed for querying, analyzing, and acting on data.
"""

import codecs
import csv
from typing import List, Generator, Any

from influxdb_client import Dialect
from influxdb_client import Query, QueryService
from influxdb_client.client.flux_csv_parser import FluxCsvParser, FluxSerializationMode
from influxdb_client.client.flux_table import FluxTable, FluxRecord


class QueryApi(object):
    """Implementation for '/api/v2/query' endpoint."""

    default_dialect = Dialect(header=True, delimiter=",", comment_prefix="#",
                              annotations=["datatype", "group", "default"], date_time_format="RFC3339")

    def __init__(self, influxdb_client):
        """
        Initialize query client.

        :param influxdb_client: influxdb client
        """
        self._influxdb_client = influxdb_client
        self._query_api = QueryService(influxdb_client.api_client)

    def query_csv(self, query: str, org=None, dialect: Dialect = default_dialect):
        """
        Execute the Flux query and return results as a CSV iterator. Each iteration returns a row of the CSV file.

        :param query: a Flux query
        :param org: organization name (optional if already specified in InfluxDBClient)
        :param dialect: csv dialect format
        :return: The returned object is an iterator.  Each iteration returns a row of the CSV file
                 (which can span multiple input lines).
        """
        if org is None:
            org = self._influxdb_client.org
        response = self._query_api.post_query(org=org, query=self._create_query(query, dialect), async_req=False,
                                              _preload_content=False)

        return csv.reader(codecs.iterdecode(response, 'utf-8'))

    def query_raw(self, query: str, org=None, dialect=default_dialect):
        """
        Execute synchronous Flux query and return result as raw unprocessed result as a str.

        :param query: a Flux query
        :param org: organization name (optional if already specified in InfluxDBClient)
        :param dialect: csv dialect format
        :return: str
        """
        if org is None:
            org = self._influxdb_client.org
        result = self._query_api.post_query(org=org, query=self._create_query(query, dialect), async_req=False,
                                            _preload_content=False)

        return result

    def query(self, query: str, org=None) -> List['FluxTable']:
        """
        Execute synchronous Flux query and return result as a List['FluxTable'].

        :param query: the Flux query
        :param org: organization name (optional if already specified in InfluxDBClient)
        :return:
        """
        if org is None:
            org = self._influxdb_client.org

        response = self._query_api.post_query(org=org, query=self._create_query(query, self.default_dialect),
                                              async_req=False, _preload_content=False, _return_http_data_only=False)

        _parser = FluxCsvParser(response=response, serialization_mode=FluxSerializationMode.tables)

        list(_parser.generator())

        return _parser.tables

    def query_stream(self, query: str, org=None) -> Generator['FluxRecord', Any, None]:
        """
        Execute synchronous Flux query and return stream of FluxRecord as a Generator['FluxRecord'].

        :param query: the Flux query
        :param org: organization name (optional if already specified in InfluxDBClient)
        :return:
        """
        if org is None:
            org = self._influxdb_client.org

        response = self._query_api.post_query(org=org, query=self._create_query(query, self.default_dialect),
                                              async_req=False, _preload_content=False, _return_http_data_only=False)

        _parser = FluxCsvParser(response=response, serialization_mode=FluxSerializationMode.stream)

        return _parser.generator()

    def query_data_frame(self, query: str, org=None, data_frame_index: List[str] = None):
        """
        Execute synchronous Flux query and return Pandas DataFrame.

        Note that if a query returns more then one table than the client generates a DataFrame for each of them.

        :param query: the Flux query
        :param org: organization name (optional if already specified in InfluxDBClient)
        :param data_frame_index: the list of columns that are used as DataFrame index
        :return:
        """
        from ..extras import pd

        _generator = self.query_data_frame_stream(query, org=org, data_frame_index=data_frame_index)
        _dataFrames = list(_generator)

        if len(_dataFrames) == 0:
            return pd.DataFrame(columns=[], index=None)
        elif len(_dataFrames) == 1:
            return _dataFrames[0]
        else:
            return _dataFrames

    def query_data_frame_stream(self, query: str, org=None, data_frame_index: List[str] = None):
        """
        Execute synchronous Flux query and return stream of Pandas DataFrame as a Generator['pd.DataFrame'].

        Note that if a query returns more then one table than the client generates a DataFrame for each of them.

        :param query: the Flux query
        :param org: organization name (optional if already specified in InfluxDBClient)
        :param data_frame_index: the list of columns that are used as DataFrame index
        :return:
        """
        if org is None:
            org = self._influxdb_client.org

        response = self._query_api.post_query(org=org, query=self._create_query(query, self.default_dialect),
                                              async_req=False, _preload_content=False, _return_http_data_only=False)

        _parser = FluxCsvParser(response=response, serialization_mode=FluxSerializationMode.dataFrame,
                                data_frame_index=data_frame_index)
        return _parser.generator()

    # private helper for c
    @staticmethod
    def _create_query(query, dialect=default_dialect):
        created = Query(query=query, dialect=dialect)
        return created

    def __del__(self):
        """Close QueryAPI."""
        pass
