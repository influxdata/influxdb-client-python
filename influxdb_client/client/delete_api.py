"""Delete time series data from InfluxDB."""

from datetime import datetime
from typing import Union

from influxdb_client import Organization
from influxdb_client.client._base import _BaseDeleteApi
from influxdb_client.client.util.helpers import get_org_query_param


class DeleteApi(_BaseDeleteApi):
    """Implementation for '/api/v2/delete' endpoint."""

    def __init__(self, influxdb_client):
        """Initialize defaults."""
        super().__init__(influxdb_client)

    def delete(self, start: Union[str, datetime], stop: Union[str, datetime], predicate: str, bucket: str,
               org: Union[str, Organization, None] = None) -> None:
        """
        Delete Time series data from InfluxDB.

        :param str, datetime.datetime start: start time
        :param str, datetime.datetime stop: stop time
        :param str predicate: predicate
        :param str bucket: bucket id or name from which data will be deleted
        :param str, Organization org: specifies the organization to delete data from.
                                      Take the ``ID``, ``Name`` or ``Organization``.
                                      If not specified the default value from ``InfluxDBClient.org`` is used.
        :return:
        """
        predicate_request = self._prepare_predicate_request(start, stop, predicate)
        org_param = get_org_query_param(org=org, client=self._influxdb_client, required_id=False)

        return self._service.post_delete(delete_predicate_request=predicate_request, bucket=bucket, org=org_param)
