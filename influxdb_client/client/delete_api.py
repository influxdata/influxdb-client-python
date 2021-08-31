"""Delete time series data from InfluxDB."""

from datetime import datetime

from influxdb_client import DeleteService, DeletePredicateRequest
from influxdb_client.client.util.date_utils import get_date_helper


class DeleteApi(object):
    """Implementation for '/api/v2/delete' endpoint."""

    def __init__(self, influxdb_client):
        """Initialize defaults."""
        self._influxdb_client = influxdb_client
        self._service = DeleteService(influxdb_client.api_client)

    def delete(self, start: datetime, stop: object, predicate: object, bucket: str, org: str) -> None:
        """
        Delete Time series data from InfluxDB.

        :param start: start time
        :param stop: stop time
        :param predicate: predicate
        :param bucket: bucket id or name from which data will be deleted
        :param org: organization id or name
        :return:
        """
        date_helper = get_date_helper()
        if isinstance(start, datetime):
            start = date_helper.to_utc(start)
        if isinstance(stop, datetime):
            stop = date_helper.to_utc(stop)

        predicate_request = DeletePredicateRequest(start=start, stop=stop, predicate=predicate)
        return self._service.post_delete(delete_predicate_request=predicate_request, bucket=bucket, org=org)
