import datetime

from influxdb_client import DefaultService, DeletePredicateRequest


class DeleteApi(object):

    def __init__(self, influxdb_client):
        self._influxdb_client = influxdb_client
        self._service = DefaultService(influxdb_client.api_client)

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
        predicate_request = DeletePredicateRequest(start=start, stop=stop, predicate=predicate)
        return self._service.delete_post(delete_predicate_request=predicate_request, bucket=bucket, org=org)
