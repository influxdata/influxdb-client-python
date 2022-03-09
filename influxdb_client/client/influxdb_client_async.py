"""InfluxDBClientAsync is client for API defined in https://github.com/influxdata/openapi/blob/master/contracts/oss.yml."""  # noqa: E501
import logging

from influxdb_client import PingService
from influxdb_client.client._base import _BaseClient
from influxdb_client.client.delete_api_async import DeleteApiAsync
from influxdb_client.client.query_api import QueryOptions
from influxdb_client.client.query_api_async import QueryApiAsync
from influxdb_client.client.write_api import PointSettings
from influxdb_client.client.write_api_async import WriteApiAsync

logger = logging.getLogger('influxdb_client.client.influxdb_client')


class InfluxDBClientAsync(_BaseClient):
    """InfluxDBClientAsync is client for InfluxDB v2."""

    def __init__(self, url, token, org: str = None, **kwargs) -> None:
        """
        Initialize defaults.

        :param url: InfluxDB server API url (ex. http://localhost:8086).
        :param token: auth token
        :param org: organization name (used as a default in Query, Write and Delete API)
        """
        super().__init__(url=url, token=token, org=org, **kwargs)

        from .._async.api_client import ApiClientAsync
        self.api_client = ApiClientAsync(configuration=self.conf, header_name=self.auth_header_name,
                                         header_value=self.auth_header_value, retries=self.retries)

    async def __aenter__(self) -> 'InfluxDBClientAsync':
        """
        Enter the runtime context related to this object.

        return: self instance
        """
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """Shutdown the client."""
        await self.close()

    async def close(self):
        """Shutdown the client."""
        if self.api_client:
            await self.api_client.close()
            self.api_client = None

    async def ping(self) -> bool:
        """
        Return the status of InfluxDB instance.

        :return: The status of InfluxDB.
        """
        ping_service = PingService(self.api_client)

        try:
            await ping_service.get_ping_with_http_info()
            return True
        except Exception as ex:
            logger.debug("Unexpected error during /ping: %s", ex)
            raise ex

    async def version(self) -> str:
        """
        Return the version of the connected InfluxDB Server.

        :return: The version of InfluxDB.
        """
        ping_service = PingService(self.api_client)

        response = await ping_service.get_ping_with_http_info(_return_http_data_only=False)
        return self._version(response)

    def query_api(self, query_options: QueryOptions = QueryOptions()) -> QueryApiAsync:
        """
        Create an asynchronous Query API instance.

        :param query_options: optional query api configuration
        :return: Query api instance
        """
        return QueryApiAsync(self, query_options)

    def write_api(self, point_settings=PointSettings()) -> WriteApiAsync:
        """
        Create an asynchronous Write API instance.

        Example:
            .. code-block:: python

                from influxdb_client_async import InfluxDBClientAsync


                # Initialize async/await instance of Write API
                async with InfluxDBClientAsync(url="http://localhost:8086", token="my-token", org="my-org") as client:
                    write_api = client.write_api()

        :param point_settings: settings to store default tags
        :return: write api instance
        """
        return WriteApiAsync(influxdb_client=self, point_settings=point_settings)

    def delete_api(self) -> DeleteApiAsync:
        """
        Get the asynchronous delete metrics API instance.

        :return: delete api
        """
        return DeleteApiAsync(self)
