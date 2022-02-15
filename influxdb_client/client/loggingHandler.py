"""
Use the influxdb_client together with python native logging
"""
import logging

from influxdb_client import InfluxDBClient


class InfluxLoggingHandler(logging.Handler):
    DEFAULT_LOG_RECORD_KEYS = logging.makeLogRecord({}).__dict__.keys()

    def __init__(self, *, url, token, org, bucket, client_args=None, write_api_args=None):
        super().__init__()

        self.bucket = bucket

        client_args = {} if client_args is None else client_args
        self.client = InfluxDBClient(url=url, token=token, org=org, **client_args)

        write_api_args = {} if write_api_args is None else write_api_args
        self.write_api = self.client.write_api(**write_api_args)

    def __del__(self):
        self.close()

    def close(self) -> None:
        self.write_api.close()
        self.client.close()
        super().close()

    def emit(self, record: logging.LogRecord) -> None:
        """ Emit a record via the influxDB WriteApi """
        try:
            extra = self._get_extra_values(record)
            return self.write_api.write(record=record.msg, **extra)
        except (KeyboardInterrupt, SystemExit):
            raise
        except (Exception,):
            self.handleError(record)

    def _get_extra_values(self, record: logging.LogRecord) -> dict:
        """extracts all items from the record that were injected by logging.debug(msg, extra={key: value, ...})"""
        extra = {key: value
                 for key, value in record.__dict__.items() if key not in self.DEFAULT_LOG_RECORD_KEYS}
        if 'bucket' not in extra.keys():
            extra['bucket'] = self.bucket
        return extra
