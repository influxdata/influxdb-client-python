import codecs
import csv
from typing import List, Generator, Any

from pandas import DataFrame

from influxdb_client import Dialect
from influxdb_client import Query, QueryService
from influxdb_client.client.flux_csv_parser import FluxCsvParser
from influxdb_client.client.flux_table import FluxTable, FluxRecord


class QueryApi(object):
    default_dialect = Dialect(header=True, delimiter=",", comment_prefix="#",
                              annotations=["datatype", "group", "default"], date_time_format="RFC3339")

    def __init__(self, influxdb_client):
        """
        Initializes query client.

        :param influxdb_client: influxdb client
        """
        self._influxdb_client = influxdb_client
        self._query_api = QueryService(influxdb_client.api_client)

    def query_csv(self, query: str, org=None, dialect: Dialect = default_dialect):
        """
        Executes the Flux query and return results as a CSV iterator. Each iteration returns a row of the CSV file.

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
        Synchronously executes the Flux query and return result as raw unprocessed result as a str

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
        Synchronously executes the Flux query and return result as a List['FluxTable']

        :param query: the Flux query
        :param org: organization name (optional if already specified in InfluxDBClient)
        :return:
        """
        if org is None:
            org = self._influxdb_client.org

        response = self._query_api.post_query(org=org, query=self._create_query(query, self.default_dialect),
                                              async_req=False, _preload_content=False, _return_http_data_only=False)

        _parser = FluxCsvParser(response=response, stream=False)

        list(_parser.generator())

        return _parser.tables

    def query_stream(self, query: str, org=None) -> Generator['FluxRecord', Any, None]:
        """
        Synchronously executes the Flux query and return stream of FluxRecord as a Generator['FluxRecord']

        :param query: the Flux query
        :param org: organization name (optional if already specified in InfluxDBClient)
        :return:
        """
        if org is None:
            org = self._influxdb_client.org

        response = self._query_api.post_query(org=org, query=self._create_query(query, self.default_dialect),
                                              async_req=False, _preload_content=False, _return_http_data_only=False)

        _parser = FluxCsvParser(response=response, stream=True)

        return _parser.generator()

    def query_data_frame(self, query: str, org=None):
        """
        Synchronously executes the Flux query and return Pandas DataFrame

        :param query: the Flux query
        :param org: organization name (optional if already specified in InfluxDBClient)
        :return:
        """
        if org is None:
            org = self._influxdb_client.org

        flux_tables = self.query(query=query, org=org)

        if len(flux_tables) == 0:
            return DataFrame

        if len(flux_tables) > 1:
            raise Exception("Flux query result must contain one table.")

        table = flux_tables[0]
        data = []
        column_names = list(map(lambda c: c.label, table.columns))
        for record in table:
            row = []
            for column_name in column_names:
                row.append(record[column_name])
            data.append(row)
        return DataFrame(data=data, columns=column_names, index=None)

    # private helper for c
    @staticmethod
    def _create_query(query, dialect=default_dialect):
        created = Query(query=query, dialect=dialect)
        return created

    def __del__(self):
        pass
