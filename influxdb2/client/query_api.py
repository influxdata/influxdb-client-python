from influxdb2 import Query, QueryService
import codecs
import csv

from influxdb2 import Dialect
from influxdb2.client.flux_csv_parser import FluxCsvParser, FluxResponseConsumerTable


class QueryApi(object):
    default_dialect = Dialect(header=True, delimiter=",", comment_prefix="#",
                              annotations=["datatype", "group", "default"], date_time_format="RFC3339")

    def __init__(self, influxdb_client):
        self._influxdb_client = influxdb_client
        self._query_api = QueryService(influxdb_client.api_client)

    def query_csv(self, query, org=None, dialect=default_dialect):
        if org is None:
            org = self._influxdb_client.org
        response = self._query_api.post_query(org=org, query=self._create_query(query, dialect), async_req=False,
                                              _preload_content=False)
        return csv.reader(codecs.iterdecode(response, 'utf-8'))

    def query_raw(self, query, org=None, dialect=default_dialect):
        if org is None:
            org = self._influxdb_client.org
        result = self._query_api.post_query(org=org, query=self._create_query(query, dialect), async_req=False,
                                            _preload_content=False)
        return result
        # return codecs.iterdecode(result, 'utf-8')

    def query(self, query, org=None, dialect=default_dialect):
        if org is None:
            org = self._influxdb_client.org
        response = self._query_api.post_query(org=org, query=self._create_query(query, dialect), async_req=False,
                                              _preload_content=False, _return_http_data_only=False)
        consumer = FluxResponseConsumerTable()
        parser = FluxCsvParser()

        parser.parse_flux_response(response=response, cancellable=None, consumer=consumer)
        return consumer.tables

    # private helper for c
    @staticmethod
    def _create_query(query, dialect=default_dialect):
        created = Query(query=query, dialect=dialect)
        return created

    def __del__(self):
        pass
