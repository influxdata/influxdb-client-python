"""
Querying InfluxDB by FluxLang.

Flux is InfluxData’s functional data scripting language designed for querying, analyzing, and acting on data.
"""

from influxdb_client import QueryService
from influxdb_client.client._base import _BaseQueryApi
from influxdb_client.client.query_api import QueryOptions


class QueryApiAsync(_BaseQueryApi):
    """Asynchronous implementation for '/api/v2/query' endpoint."""

    def __init__(self, influxdb_client, query_options=QueryOptions()):
        """
        Initialize query client.

        :param influxdb_client: influxdb client
        """
        self._influxdb_client = influxdb_client
        self._query_options = query_options
        self._query_api = QueryService(influxdb_client.api_client)
