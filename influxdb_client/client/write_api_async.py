"""Collect and async write time series data to InfluxDB Cloud or InfluxDB OSS."""
from influxdb_client.client._base import _BaseWriteApi
from influxdb_client.client.write_api import PointSettings


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
