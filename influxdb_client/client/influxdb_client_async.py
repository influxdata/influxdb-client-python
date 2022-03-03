"""InfluxDBClientAsync is client for API defined in https://github.com/influxdata/openapi/blob/master/contracts/oss.yml."""  # noqa: E501
import logging

from influxdb_client import PingService
from influxdb_client.client._base import _BaseClient

logger = logging.getLogger('influxdb_client.client.influxdb_client')


class InfluxDBClientAsync(_BaseClient):
    """InfluxDBClient is client for InfluxDB v2."""

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