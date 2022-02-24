"""Delete time series data from InfluxDB."""

from datetime import datetime
from typing import Union

from influxdb_client import DeleteService, DeletePredicateRequest, Organization
from influxdb_client.client.util.date_utils import get_date_helper
from influxdb_client.client.util.helpers import get_org_query_param


class DeleteApi(object):
    """Implementation for '/api/v2/delete' endpoint."""

    def __init__(self, influxdb_client):
        """Initialize defaults."""
        self._influxdb_client = influxdb_client
        self._service = DeleteService(influxdb_client.api_client)

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
        date_helper = get_date_helper()
        if isinstance(start, datetime):
            start = date_helper.to_utc(start)
        if isinstance(stop, datetime):
            stop = date_helper.to_utc(stop)

        org_param = get_org_query_param(org=org, client=self._influxdb_client, required_id=False)

        predicate_request = DeletePredicateRequest(start=start, stop=stop, predicate=predicate)
        return self._service.post_delete(delete_predicate_request=predicate_request, bucket=bucket, org=org_param)
