"""InfluxDB SQL Client."""
from urllib.parse import urlparse

from influxdb_client.client._base import _BaseSQLClient


class SQLClient(_BaseSQLClient):
    """
    Implementation for gRPC+TLS client for SQL.

    This class provides basic operations for interacting with InfluxDB via SQL.
    """

    def __init__(self, influxdb_client, bucket, **kwargs):
        """
        Initialize SQL client.

        Unlike the previous APIs, this client is is produced for a specific
        bucket to query against. Queries to different buckets require different
        clients.

        To complete SQL requests, a different client is used. The rest of this
        client library utilizes REST requests against the published API.
        However, for SQL support connections are handled over gRPC+TLS. As such
        this client takes the host and client and creates a new client
        connection for SQL operations.

        :param influxdb_client: influxdb client
        """
        super().__init__(influxdb_client=influxdb_client)

        from flightsql import FlightSQLClient

        namespace = f'{influxdb_client.org}_{bucket}'
        url = urlparse(self._influxdb_client.url)
        port = url.port if url.port else 443
        self._client = FlightSQLClient(
            host=url.hostname,
            port=port,
            metadata={
                "bucket-name": bucket,             # for cloud
                "iox-namespace-name": namespace,   # for local instance
            },
            **kwargs
        )

    def __enter__(self):
        """
        Enter the runtime context related to this object.

        It will bind this method’s return value to the target(s)
        specified in the `as` clause of the statement.

        return: self instance
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the runtime context related to this object and close the SQLClient."""
        self.close()

    def close(self):
        """Close the client connection."""
        self._client.close()

    def query(self, query: str):
        """
        Execute synchronous SQL query and return result as an Arrow reader.

        :param str, query: the SQL query to execute
        :return: PyArrow RecordbatchReader

        .. code-block:: python

            with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:
                # Each connection is specific to a bucket
                sql_client = client.sql_client("my-bucket")

                # The returned result is a stream of data. For large result-sets users can
                # iterate through those one-by-one to avoid using large chunks of memory.
                with sql_client.query("select * from cpu") as result:
                    for r in result:
                        print(r)

                # For smaller results you might want to read the results at once. You
                # can do so by using the `read_all()` method.
                with sql_client.query("select * from cpu limit 10") as result:
                    data = result.read_all()
                    print(data)

                # To get you data into a Pandas DataFrame use the following helper function
                df = data.to_pandas()

        """  # noqa: E501
        return self._get_ticket_info(self._client.execute(query))

    def schemas(self):
        """
        Return the schema of the specified bucket.

        :return: PyArrow Table

        .. code-block:: python

            with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:
                sql_client = client.sql_client("my-bucket")
                print(sql_client.schemas())

        """  # noqa: E501
        return self._get_ticket_info(self._client.get_db_schemas()).read_all()

    def tables(self):
        """
        Return tables available from the specified bucket.

        :return: PyArrow Table

        .. code-block:: python

            with InfluxDBClient(url="http://localhost:8086", token="my-token", org="my-org") as client:
                sql_client = client.sql_client("my-bucket")
                print(sql_client.tables())

        """  # noqa: E501
        return self._get_ticket_info(self._client.get_table_types()).read_all()

    def _get_ticket_info(self, flightInfo):
        """Collect results from FlightInfo."""
        if len(flightInfo.endpoints) == 0:
            raise ValueError("no endpoints received")
        return self._client.do_get(flightInfo.endpoints[0].ticket).to_reader()
