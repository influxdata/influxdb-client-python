"""Use the influxdb_client with python native logging."""
import logging

from influxdb_client import InfluxDBClient


class InfluxLoggingHandler(logging.Handler):
    """
    InfluxLoggingHandler instances dispatch logging events to influx.

    There is no need to set a Formatter.
    The raw input will be passed on to the influx write api.
    """

    DEFAULT_LOG_RECORD_KEYS = list(logging.makeLogRecord({}).__dict__.keys()) + ['message']

    def __init__(self, *, url, token, org, bucket, client_args=None, write_api_args=None):
        """
        Initialize defaults.

        The arguments `client_args` and `write_api_args` can be dicts of kwargs.
        They are passed on to the InfluxDBClient and write_api calls respectively.
        """
        super().__init__()

        self.bucket = bucket

        client_args = {} if client_args is None else client_args
        self.client = InfluxDBClient(url=url, token=token, org=org, **client_args)

        write_api_args = {} if write_api_args is None else write_api_args
        self.write_api = self.client.write_api(**write_api_args)

    def __del__(self):
        """Make sure all resources are closed."""
        self.close()

    def close(self) -> None:
        """Close the write_api, client and logger."""
        self.write_api.close()
        self.client.close()
        super().close()

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a record via the influxDB WriteApi."""
        try:
            message = self.format(record)
            extra = self._get_extra_values(record)
            return self.write_api.write(record=message, **extra)
        except (KeyboardInterrupt, SystemExit):
            raise
        except (Exception,):
            self.handleError(record)

    def _get_extra_values(self, record: logging.LogRecord) -> dict:
        """
        Extract all items from the record that were injected via extra.

        Example: `logging.debug(msg, extra={key: value, ...})`.
        """
        extra = {'bucket': self.bucket}
        extra.update({key: value for key, value in record.__dict__.items()
                      if key not in self.DEFAULT_LOG_RECORD_KEYS})
        return extra
