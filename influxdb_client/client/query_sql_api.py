"""Query InfluxDB with SQL."""
from urllib.parse import urlparse

from influxdb_client.client._base import _BaseQueryApi


class QuerySQLApi(_BaseQueryApi):
    """Implementation for grpc+TLS client for SQL queries."""

    def __init__(self, influxdb_client):
        """
        Initialize SQL query client.

        To complete SQL requests, a different client is

        :param influxdb_client: influxdb client
        """
        super().__init__(influxdb_client=influxdb_client)

    def query(self, bucket: str, query: str):
        """Execute synchronous SQL query and return result as an Arrow reader.

        :param str, bucket: the Flux query
        :param str, query: the SQL query to execute
        :return: Arrow reader

        .. code-block:: python

            from influxdb_client import InfluxDBClient

            with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:
                query_sql_api = client.query_sql_api()
                result = query_sql_api.query("test", "select * from cpu limit 10")

        """  # noqa: E501
        from flightsql import FlightSQLClient

        client = FlightSQLClient(
            host=urlparse(self._influxdb_client.url).hostname,
            token=self._influxdb_client.token,
            metadata={"bucket-name": bucket},
        )
        info = client.execute(query)
        return client.do_get(info.endpoints[0].ticket)
